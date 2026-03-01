"""LangGraph-based Autonomous Data Quality Agent.

REWRITE v7 - ReAct (Reason + Act) Architecture with Robust Sanitization.
Bug fixes:
  - Aggressive sanitization to strip hallucinated multi-turn content
  - Better detection of bundled queries vs structural tags
  - Strict single-action enforcement per response
  - Context window management to prevent token explosion
  - Retry logic for incomplete JSON blocks
"""
import json
import logging
import re
from typing import Dict, Any, List, Optional
from datetime import datetime
from langgraph.graph import StateGraph, END

from app.agents.state import (
    AgentState, ValidationMode, AgentStatus,
    DataSourceInfo, ValidationResult, DataProfile
)
from app.agents.llm_service import get_llm_service
from app.agents.rag_service import get_rag_service
from app.agents.llm_sanitizer import sanitize_llm_response
from app.connectors.factory import ConnectorFactory
from app.core.config import get_settings

logger = logging.getLogger(__name__)

# ══════════════════════════════════════════════════════════════
# AGENT CONFIGURATION — change these values to tune behaviour
# ══════════════════════════════════════════════════════════════

# Max LLM output tokens per call
LLM_MAX_TOKENS = 2048

# Hard limits to prevent infinite agent loops
MAX_EXPLORATION_STEPS = 10
MAX_VALIDATION_STEPS = 10

# LangGraph recursion limit
GRAPH_RECURSION_LIMIT = 50

# Max conversation history to prevent context explosion
MAX_HISTORY_MESSAGES = 6  # Only keep last N messages (3 turns)

# ==========================================
# SYSTEM PROMPTS FOR AGENTIC LOOPS
# ==========================================

EXPLORATION_PROMPT = """You are an autonomous Data Exploration Agent. Your goal is to deeply understand the provided dataset by executing exploratory queries.

CRITICAL RULES:
1. You MUST output EXACTLY ONE action per response - either a query OR a metadata report, NEVER both.
2. To execute a query, reply with ONLY this JSON block (no other text):
```json
{
  "action": "execute_query",
  "query": "SELECT name, type, \"notnull\", dflt_value, pk FROM pragma_table_info('customers');",
  "query_type": "sql"
}
```
3. When you have gathered enough information, output ONLY the metadata report (no queries):
<METADATA>
This table represents e-commerce transactions.
- transaction_id: unique identifier, INTEGER, no nulls, 10000 rows.
...
</METADATA>

=== SQLite SYNTAX ===
- Inspect columns: SELECT name, type, "notnull", dflt_value, pk FROM pragma_table_info('table_name');
- Count rows: SELECT COUNT(*) AS total_rows FROM table_name;
- Check NULLs: SELECT COUNT(*) AS null_count FROM table_name WHERE col IS NULL;
- Sample data: SELECT * FROM table_name LIMIT 10;

DO NOT:
- Output multiple queries in one response
- Include explanatory text with your JSON
- Use "ASSISTANT:" or role prefixes in your output
- Output both a query and <METADATA> in the same response"""


VALIDATION_PROMPT = """You are an autonomous Data Quality Validation Agent.

CRITICAL RULES:
1. You MUST output EXACTLY ONE action per response - either a validation query OR a final report, NEVER both.
2. To execute a validation query, reply with ONLY this JSON block:
```json
{
  "action": "execute_query",
  "query": "SELECT * FROM customers WHERE email IS NULL",
  "query_type": "sql",
  "rule_name": "CheckNullEmails",
  "severity": "warning"
}
```
3. When finished with all checks, output ONLY the report:
<REPORT>
Summary of what passed, what failed, and recommended fixes.
</REPORT>

=== VALIDATION RULES ===
1. Write queries to RETURN FAILED ROWS (e.g., WHERE col IS NULL).
2. If rows are returned, the check FAILED.
3. Run at least 3 different validation checks before finalizing.
4. NEVER output a query and <REPORT> in the same message.

DO NOT:
- Output multiple queries in one response
- Include "ASSISTANT:" prefixes or fake conversation turns
- Bundle queries with your final report"""


class DataQualityAgent:
    """LangGraph agent for autonomous data quality validation."""
    
    exploration_prompt = EXPLORATION_PROMPT
    validation_prompt = VALIDATION_PROMPT

    @staticmethod
    def _sanitize_llm_response(raw: str) -> str:
        """Sanitize LLM output using the universal model-agnostic pipeline."""
        return sanitize_llm_response(raw)

    @staticmethod
    def _truncate_history(messages: List[Dict[str, str]], max_messages: int = MAX_HISTORY_MESSAGES) -> List[Dict[str, str]]:
        """
        Keep only recent messages to prevent context explosion.
        Always keep system messages, truncate from the middle.
        """
        if len(messages) <= max_messages:
            return messages
        
        # Always keep first system message and last N messages
        system_messages = [m for m in messages[:2] if m.get('role') == 'system']
        recent_messages = messages[-max_messages:]
        
        # Combine, avoiding duplicates
        seen = set()
        result = []
        for m in system_messages + recent_messages:
            key = (m.get('role'), m.get('content', '')[:100])
            if key not in seen:
                seen.add(key)
                result.append(m)
        
        return result[-max_messages:]  # Ensure we don't exceed limit

    def __init__(self):
        self.llm_service = get_llm_service()
        self.graph = self._build_graph()

    def _build_graph(self) -> StateGraph:
        """Build the cyclic ReAct LangGraph workflow."""
        workflow = StateGraph(AgentState)

        # Add nodes
        workflow.add_node("setup_connection", self._setup_connection)
        workflow.add_node("explore_data", self._explore_data)
        workflow.add_node("execute_exploration_query", self._execute_query_tool)
        workflow.add_node("save_metadata", self._save_metadata)
        workflow.add_node("validate_data", self._validate_data)
        workflow.add_node("generate_dashboard_report", self._generate_report)

        # Graph Edges
        workflow.set_entry_point("setup_connection")
        workflow.add_edge("setup_connection", "explore_data")

        workflow.add_conditional_edges(
            "explore_data",
            self._route_exploration,
            {
                "execute_tool": "execute_exploration_query",
                "finished": "save_metadata",
                "retry": "explore_data",  # New: for truncated responses
            }
        )
        workflow.add_edge("execute_exploration_query", "explore_data")
        workflow.add_edge("save_metadata", "validate_data")
        workflow.add_edge("validate_data", "generate_dashboard_report")
        workflow.add_edge("generate_dashboard_report", END)

        return workflow.compile()

    # ==========================================
    # ROUTING LOGIC
    # ==========================================

    def _route_exploration(self, state: AgentState) -> str:
        """Route exploration phase with strict completion checks."""
        exploration_steps = state.get("exploration_steps", 0)

        if exploration_steps >= MAX_EXPLORATION_STEPS:
            logger.warning(f"Exploration max iterations ({MAX_EXPLORATION_STEPS}) reached. Forcing finish.")
            return "finished"

        if not state.get("messages"):
            return "execute_tool"

        last_message = state["messages"][-1]["content"]
        sanitized = self._sanitize_llm_response(last_message)

        # If sanitization returned empty (truncated), retry
        if not sanitized:
            logger.warning("Last response was truncated - requesting retry")
            return "retry"

        # Check for complete METADATA block (must have both tags)
        has_metadata = bool(re.search(r'<METADATA>[\s\S]+?</METADATA>', sanitized, re.IGNORECASE))
        
        # Check for pending query - must be valid JSON with action
        has_pending_query = bool(re.search(r'"action"\s*:\s*"execute_query"', sanitized))

        # PRIORITY: If we have both, execute query first (don't skip to finished)
        if has_metadata and has_pending_query:
            logger.info("Both query and <METADATA> detected - executing query first")
            # Update the message to remove the METADATA tag for now, keep only query
            cleaned = re.sub(r'<METADATA>[\s\S]*?</METADATA>', '', sanitized, flags=re.IGNORECASE).strip()
            state["messages"][-1]["content"] = cleaned
            return "execute_tool"

        if has_metadata and not has_pending_query:
            logger.info("Complete <METADATA> block found - finishing exploration")
            return "finished"

        return "execute_tool"

    # The _route_validation function is removed as validation is now a self-contained node.

    # ==========================================
    # NODE IMPLEMENTATIONS
    # ==========================================

    async def _setup_connection(self, state: AgentState) -> Dict[str, Any]:
        """Connect to data source and fetch initial schema context."""
        logger.info("=" * 60)
        logger.info("🔌 STEP 1: Connecting to data source...")

        try:
            connector = ConnectorFactory.create_connector(
                state['data_source_info'].source_type,
                state['data_source_info'].connection_config
            )
            await connector.connect()
            schema = await connector.get_schema(state['data_source_info'].target_path)

            full_scan = getattr(state['data_source_info'], 'full_scan_requested', False)
            slice_filters = getattr(state['data_source_info'], 'slice_filters', None)
            
            sample_data = await connector.sample_data(
                state['data_source_info'].target_path,
                sample_size=state.get('execution_config', {}).get('sample_size', 1000),
                full_scan=full_scan,
                slice_filters=slice_filters
            )

            state['data_source_info'].schema = schema
            state['data_source_info'].sample_data = sample_data

            target = state['data_source_info'].target_path
            
            filter_context = ""
            if slice_filters:
                filter_context = f" NOTE: Filtered to {slice_filters}."

            # Concise init message to save tokens
            init_msg = (
                f"Connected to '{target}'. "
                f"Schema: {json.dumps(schema, default=str)}."
                f"{filter_context} Begin exploration."
            )

            return {
                "data_source_info": state['data_source_info'],
                "status": AgentStatus.EXPLORING,
                "current_step": "explore_data",
                "exploration_steps": 0,
                "validation_steps": 0,
                "messages": [{"role": "system", "content": init_msg}],
            }
        except Exception as e:
            logger.error(f"❌ CONNECTION FAILED: {str(e)}")
            return {"status": AgentStatus.ERROR, "error_message": str(e)}

    async def _explore_data(self, state: AgentState) -> Dict[str, Any]:
        """Agent node for exploratory data analysis with context management."""
        logger.info("=" * 60)
        logger.info("🔍 STEP 2: Agent Exploring Data...")

        target_table = state['data_source_info'].target_path
        exploration_steps = state.get("exploration_steps", 0) + 1

        # Truncate history to prevent context explosion
        truncated_messages = self._truncate_history(state["messages"])
        
        # Build minimal prompt context
        history = "\n".join([
            f"{m['role'][:3].upper()}: {m['content'][:500]}..." if len(m['content']) > 500 
            else f"{m['role'][:3].upper()}: {m['content']}"
            for m in truncated_messages[-4:]  # Last 2 turns max
        ])

        # Stronger nudge as we approach limit
        if exploration_steps >= MAX_EXPLORATION_STEPS - 1:
            nudge = "\n\nFINAL STEP: Output ONLY your <METADATA>...</METADATA> report now. NO queries."
        elif exploration_steps >= MAX_EXPLORATION_STEPS - 3:
            nudge = "\n\nNOTE: Steps remaining limited. Finish exploration and output <METADATA> report soon."
        else:
            nudge = ""

        prompt = f"""Table: {target_table}

Recent history:
{history}

Step {exploration_steps}/{MAX_EXPLORATION_STEPS}. Output ONE query JSON or your final <METADATA> report.{nudge}"""

        response = await self.llm_service.generate(
            prompt=prompt,
            system_prompt=self.exploration_prompt,
            max_tokens=LLM_MAX_TOKENS,
            temperature=0.1  # Lower temp for more deterministic output
        )
        
        response = self._sanitize_llm_response(response)
        
        # Handle empty/truncated response
        if not response:
            response = '{"action": "execute_query", "query": "SELECT COUNT(*) FROM ' + target_table + '", "query_type": "sql"}'

        logger.info(f"Agent Output: {response[:200]}...")
        return {
            "messages": [{"role": "assistant", "content": response}],
            "exploration_steps": exploration_steps,
        }

    async def _validate_data(self, state: AgentState) -> Dict[str, Any]:
        """Agent node for data validation running ColumnAnalysisAgent sequentially over columns."""
        logger.info("=" * 60)
        logger.info("⚙️ STEP 4: Orchestrator Delegating Deep-Dive Column Analysis...")

        target_table = state['data_source_info'].target_path
        schema = state['data_source_info'].schema
        sample_data = state['data_source_info'].sample_data
        mode_str = state["validation_mode"].value

        from app.agents.column_analysis_agent import ColumnAnalysisAgent
        
        all_results = []
        
        # We need a wrapper executor for the sub-agent to run DB queries
        async def query_executor(query: str, q_type: str = "sql") -> Dict[str, Any]:
            return await self._execute_with_pushdown(query, q_type, state)

        if not schema:
            logger.warning("No schema available. Cannot perform column-level validation.")
            return {"validation_results": []}

        for column in schema:
            col_name = column["name"]
            col_type = column["type"]
            
            # Extract samples for this specific column, safely handling missing keys
            col_samples = list({
                row.get(col_name) 
                for row in sample_data.get("rows", []) 
                if row.get(col_name) is not None
            })[:50]
            
            logger.info(f"Delegating analysis for column: {col_name} (Mode: {mode_str})")
            
            # Send progress update to frontend
            if "progress_callback" in state and callable(state["progress_callback"]):
                state["progress_callback"](f"Analyzing column {col_name}...")
            
            # Instantiate the specialized agent
            col_agent = ColumnAnalysisAgent(
                column_name=col_name,
                dtype=col_type,
                samples=col_samples,
                mode=mode_str,
                engine_executor=query_executor
            )
            
            # Run the agent for this specific column
            col_report = await col_agent.analyze(table_name=target_table)
            all_results.extend(col_report.validation_results)
            logger.info(f"Finished {col_name}: Score {col_report.score}")
            
        # Convert dictionaries to ValidationResult dataclasses compatible with the state
        dataclass_results = [
            ValidationResult(
                rule_id=f"agent_rule_{idx}",
                rule_name=res["rule_name"],
                status=res["status"],
                passed_count=max(0, state["data_source_info"].row_count - res["failed_count"]),
                failed_count=res["failed_count"],
                failure_examples=res["failure_examples"],
                severity=res["severity"],
                rule_type=res["rule_type"],
                executed_query=res["executed_query"]
            )
            for idx, res in enumerate(all_results)
        ]

        logger.info(f"Orchestrator finished column delegation. Collected {len(dataclass_results)} total rules.")
        return {
            "validation_results": dataclass_results
        }

    async def _execute_query_tool(self, state: AgentState) -> Dict[str, Any]:
        """Execute agent query with robust parsing."""
        logger.info("=" * 60)
        logger.info("🛠️ TOOL EXECUTION: Running Agent's Query...")

        last_message = state["messages"][-1]["content"]
        last_message = self._sanitize_llm_response(last_message)

        # Check if this is actually a structural tag completion (no query to run)
        has_metadata = bool(re.search(r'<METADATA>[\s\S]+?</METADATA>', last_message, re.IGNORECASE))
        has_report = bool(re.search(r'<REPORT>[\s\S]+?</REPORT>', last_message, re.IGNORECASE))
        
        # If message has structural tag but no JSON query, skip execution
        has_json_action = bool(re.search(r'"action"\s*:\s*"execute_query"', last_message))
        
        if (has_metadata or has_report) and not has_json_action:
            tag = "METADATA" if has_metadata else "REPORT"
            logger.info(f"No query to execute - <{tag}> block present. Skipping tool execution.")
            return {"messages": [{"role": "assistant", "content": last_message}]}

        # Extract JSON block
        match = re.search(r'```json\s*([\s\S]*?)\s*```', last_message, re.IGNORECASE)
        if not match:
            # Try raw JSON without fences
            match = re.search(r'(\{[\s\S]*?"action"\s*:\s*"execute_query"[\s\S]*?\})', last_message)

        if not match:
            logger.error("No valid JSON query block found in message")
            return {
                "messages": [{
                    "role": "user",
                    "content": "Error: No query found. Please output exactly one JSON block or your final report."
                }]
            }

        try:
            action_data = json.loads(match.group(1))
            query = action_data.get("query")
            q_type = action_data.get("query_type", "sql")

            if not query:
                raise ValueError("Query string is empty")

            logger.info(f"Executing: {query[:100]}...")

            from app.validation.engine import ValidationEngine
            engine = ValidationEngine()

            # Execute query (with pushdown logic)
            tool_result = await self._execute_with_pushdown(query, q_type, state)

            # Record validation result if in validation phase
            validation_updates = {}
            query_errored = tool_result.get("status") == "error"
            
            if state["status"] == AgentStatus.VALIDATING:
                # DON'T record errored queries as results — they didn't 
                # actually validate anything (e.g. referenced non-existent columns)
                if query_errored:
                    logger.warning(f"Query errored — NOT recording as passed/failed. Query: {query[:100]}")
                else:
                    rule_name = action_data.get("rule_name") or self._derive_rule_name(query)
                    
                    # Normalize severity: LLMs often output "high"/"medium"/"low" 
                    # instead of "critical"/"warning"/"info"
                    raw_severity = action_data.get("severity", "warning").lower()
                    severity_map = {
                        "high": "critical", "critical": "critical",
                        "medium": "warning", "warning": "warning",
                        "low": "info", "info": "info",
                    }
                    severity = severity_map.get(raw_severity, "warning")
                    
                    current_results = list(state.get("validation_results", []))
                    
                    # DEDUP: Skip if a rule with the same name already exists
                    existing_names = {r.rule_name for r in current_results}
                    if rule_name in existing_names:
                        logger.warning(f"Skipping duplicate rule '{rule_name}' — already recorded.")
                    else:
                        failed_count = tool_result.get("row_count", 0)
                        row_count = state["data_source_info"].row_count or 0

                        res = ValidationResult(
                            rule_id=f"agent_rule_{len(current_results)}",
                            rule_name=rule_name,
                            status="failed" if failed_count > 0 else "passed",
                            passed_count=max(0, row_count - failed_count),
                            failed_count=failed_count,
                            failure_examples=tool_result.get("sample_rows", [])[:5],
                            severity=severity,
                            rule_type="agent_query",
                            executed_query=query
                        )
                        current_results.append(res)
                        validation_updates["validation_results"] = current_results

            # Concise result to save context tokens
            result_summary = {
                "status": tool_result.get("status", "unknown"),
                "row_count": tool_result.get("row_count", 0),
                "sample": tool_result.get("sample_rows", [])[:3]  # Limit sample size
            }
            
            return {
                "messages": [{
                    "role": "user",
                    "content": f"Result: {json.dumps(result_summary, default=str)}"
                }],
                **validation_updates
            }

        except json.JSONDecodeError as e:
            logger.error(f"JSON parse error: {e}")
            return {
                "messages": [{
                    "role": "user",
                    "content": "Error: Invalid JSON format. Please use exact format: ```json { ... } ```"
                }]
            }
        except Exception as e:
            logger.error(f"Tool execution failed: {e}")
            return {
                "messages": [{
                    "role": "user",
                    "content": f"Query failed: {str(e)[:200]}. Please fix and retry."
                }]
            }

    async def _execute_with_pushdown(self, query: str, q_type: str, state: AgentState) -> Dict[str, Any]:
        """Execute query with native pushdown where possible."""
        from app.validation.engine import ValidationEngine
        
        source_type = str(getattr(state['data_source_info'], 'source_type', '')).lower()
        
        if q_type.lower() == 'sql' and source_type in ['postgresql', 'postgres', 'mysql', 'sqlite']:
            try:
                connector = ConnectorFactory.create_connector(
                    source_type,
                    state['data_source_info'].connection_config
                )
                await connector.connect()
                
                slice_filters = getattr(state['data_source_info'], 'slice_filters', None)
                result = await connector.execute_raw_query(query, slice_filters=slice_filters)
                await connector.disconnect()
                
                if result.get("status") == "success":
                    return result
            except Exception as e:
                logger.warning(f"Pushdown failed: {e}, falling back to sandbox")
        
        # Fallback to validation engine
        engine = ValidationEngine()
        return await engine.execute_agent_query(
            query=query,
            query_type=q_type,
            data_source_info=state['data_source_info'],
            sample_data=state['data_source_info'].sample_data
        )

    @staticmethod
    def _derive_rule_name(query: str) -> str:
        """Derive a readable rule name from SQL query."""
        where_match = re.search(r'WHERE\s+(.+?)(?:;|\s*$)', query, re.IGNORECASE)
        if where_match:
            condition = where_match.group(1)[:40].strip()
            return f"Check_{condition.replace(' ', '_').replace('=', '_eq_')}"
        return "agent_validation_check"

    async def _save_metadata(self, state: AgentState) -> Dict[str, Any]:
        """Extract and save metadata to RAG."""
        logger.info("=" * 60)
        logger.info("💾 STEP 3: Saving Metadata...")

        metadata_text = None
        for msg in reversed(state["messages"]):
            m = re.search(r'<METADATA>([\s\S]*?)</METADATA>', msg.get("content", ""), re.IGNORECASE)
            if m:
                metadata_text = m.group(1).strip()
                break

        if not metadata_text:
            # Fallback to schema
            schema = state['data_source_info'].schema or {}
            metadata_text = f"Schema: {json.dumps(schema, default=str)}"
            logger.warning("No <METADATA> found, using schema fallback")

        try:
            rag_service = await get_rag_service()
            await rag_service.add_document(
                document_type="agent_metadata",
                source_id=state['data_source_info'].target_path,
                title=f"Metadata: {state['data_source_info'].target_path}",
                content=metadata_text[:2000]  # Limit size
            )
        except Exception as e:
            logger.error(f"RAG save failed: {e}")

        return {
            "retrieved_context": [{"content": metadata_text[:1500]}],
            "current_step": "validate_data",
            "status": AgentStatus.VALIDATING,
            "messages": [{"role": "system", "content": "Metadata saved. Starting validation."}],
        }

    async def _generate_report(self, state: AgentState) -> Dict[str, Any]:
        """Generate final validation report."""
        logger.info("=" * 60)
        logger.info("📄 STEP 5: Finalizing Report...")

        agent_narrative = None
        for msg in reversed(state["messages"]):
            m = re.search(r'<REPORT>([\s\S]*?)</REPORT>', msg.get("content", ""), re.IGNORECASE)
            if m:
                agent_narrative = m.group(1).strip()
                break

        if not agent_narrative:
            agent_narrative = "Validation completed. See rule results for details."

        results = state.get("validation_results", [])
        total = len(results)
        passed = sum(1 for r in results if r.status == "passed")
        failed = sum(1 for r in results if r.status == "failed")
        critical = sum(1 for r in results if r.status == "failed" and r.severity == "critical")

        # Calculate score using weighted approach:
        #  - Each rule contributes equally to a 100-point scale
        #  - Passed rules contribute full weight
        #  - Failed warnings contribute partial weight (50%)
        #  - Failed criticals contribute 0
        score = 0.0
        if total > 0:
            per_rule = 100.0 / total
            for r in results:
                if r.status == "passed":
                    score += per_rule  # Full score
                elif r.severity == "warning":
                    score += per_rule * 0.3  # Partial credit for warnings
                # Critical failures get 0
            score = round(max(0.0, min(100.0, score)), 2)

        summary = {
            "quality_score": score,
            "total_rules": total,
            "passed_rules": passed,
            "failed_rules": failed,
            "critical_failures": critical,
            "agent_analysis": agent_narrative[:1000],  # Limit length
            "data_source": state['data_source_info'].target_path,
        }

        return {
            "quality_score": score,
            "summary_report": summary,
            "status": AgentStatus.COMPLETED,
            "completed_at": datetime.utcnow().isoformat(),
        }

    async def run(
        self,
        validation_id: str,
        validation_mode: ValidationMode,
        data_source_info: DataSourceInfo,
        custom_rules: Optional[List[Any]] = None,
        execution_config: Optional[Dict[str, Any]] = None,
    ) -> AgentState:
        """Run the autonomous data quality agent."""
        
        logger.info("\n" + "🚀" * 30)
        logger.info(f"STARTING AGENT | ID: {validation_id} | Target: {data_source_info.target_path}")
        logger.info("🚀" * 30 + "\n")

        initial_state: AgentState = {
            "validation_id": validation_id,
            "validation_mode": validation_mode,
            "data_source_info": data_source_info,
            "custom_rules": custom_rules or [],
            "execution_config": execution_config or {},
            "status": AgentStatus.CONNECTING,
            "current_step": "setup_connection",
            "messages": [],
            "data_profile": None,
            "ai_recommended_rules": [],
            "all_rules": [],
            "validation_results": [],
            "retrieved_context": [],
            "quality_score": None,
            "summary_report": None,
            "error_message": None,
            "started_at": datetime.utcnow().isoformat(),
            "completed_at": None,
            "execution_metrics": {},
            "exploration_steps": 0,
            "validation_steps": 0,
        }

        result = await self.graph.ainvoke(
            initial_state,
            config={"recursion_limit": GRAPH_RECURSION_LIMIT}
        )

        logger.info("\n" + "🏁" * 30)
        logger.info(f"COMPLETE: {result.get('status')} | Score: {result.get('quality_score')}")
        logger.info("🏁" * 30 + "\n")

        return result


# Singleton instance
_agent: Optional[DataQualityAgent] = None


def get_data_quality_agent() -> DataQualityAgent:
    """Get agent singleton."""
    global _agent
    if _agent is None:
        _agent = DataQualityAgent()
    return _agent