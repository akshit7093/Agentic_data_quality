"""LangGraph-based Autonomous Data Quality Agent.

REWRITE v6 - ReAct (Reason + Act) Architecture.
Bug fixes:
  - Routing now checks for COMPLETE <METADATA>...</METADATA> and <REPORT>...</REPORT>
    blocks, not just the opening tag (prevents false-positive "finished" routing when
    the LLM merely *mentions* the tag name in prose).
  - Validation routing prioritises executing an embedded query before declaring "finished"
    even when the LLM emits both a query block and a <REPORT> in the same message.
  - System prompts now include correct SQLite PRAGMA / introspection syntax examples so
    the model stops trying to use `column_name` as a real column.
  - Iteration guards prevent infinite loops and force the agent to conclude after
    MAX_EXPLORATION_STEPS / MAX_VALIDATION_STEPS attempts.
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
from app.connectors.factory import ConnectorFactory
from app.core.config import get_settings

logger = logging.getLogger(__name__)

# Hard limits to prevent infinite agent loops
MAX_EXPLORATION_STEPS = 10
MAX_VALIDATION_STEPS  = 10


# ==========================================
# SYSTEM PROMPTS FOR AGENTIC LOOPS
# ==========================================

EXPLORATION_PROMPT = """You are an autonomous Data Exploration Agent. Your goal is to deeply understand the provided dataset by executing exploratory queries.

You are interacting with a system that can execute your queries.
To execute a query, you MUST reply with a JSON block in this exact format:
```json
{
  "action": "execute_query",
  "query": "SELECT COUNT(*) AS total_rows FROM target_table",
  "query_type": "sql"
}
```

=== CRITICAL SQLite SYNTAX RULES ===
1. To inspect a table's columns, use the TABLE-VALUED FUNCTION form of PRAGMA:
      SELECT name, type, "notnull", dflt_value, pk
      FROM pragma_table_info('your_table_name');
   The columns returned are: cid, name, type, notnull, dflt_value, pk
   Do NOT write:  SELECT column_name ... FROM pragma_table_info(...)
   Do NOT write:  SELECT data_type  ... FROM pragma_table_info(...)

2. To count rows:           SELECT COUNT(*) AS total_rows FROM your_table;
3. To check for NULLs:      SELECT COUNT(*) AS null_count FROM your_table WHERE col IS NULL;
4. To get distinct values:  SELECT col, COUNT(*) AS cnt FROM your_table GROUP BY col ORDER BY cnt DESC LIMIT 20;
5. To see sample rows:      SELECT * FROM your_table LIMIT 10;

=== EXPLORATION RULES ===
1. Start with schema inspection (pragma_table_info), then row count, then sample rows.
2. Check for null distributions, unique values, and outliers per column.
3. Look at the actual data values to understand the business context.
4. You can run as many queries as you need (one at a time).

Once you have gathered enough information and deeply understand the schema and data,
you MUST conclude your exploration by outputting a comprehensive metadata report
wrapped in COMPLETE opening AND closing tags, like this:

<METADATA>
This table represents e-commerce transactions.
- transaction_id: unique identifier, INTEGER, no nulls, 10 000 rows.
- transaction_amount: REAL, has negative values which indicate refunds.
- ...
</METADATA>

IMPORTANT: Do NOT mention <METADATA> in your regular text. Only use those tags when
you are ready to submit your final metadata report."""


VALIDATION_PROMPT = """You are an autonomous Data Quality Validation Agent.
Your job is to run specific queries to check the data for anomalies, inconsistencies,
and quality issues based on the metadata context provided.

To execute a validation query, reply with a JSON block in this exact format:
```json
{
  "action": "execute_query",
  "query": "SELECT * FROM target_table WHERE transaction_amount < 0",
  "query_type": "sql",
  "rule_name": "NegativeTransactionAmount",
  "severity": "critical"
}
```

=== VALIDATION RULES ===
1. Write queries designed to RETURN FAILED ROWS (e.g., WHERE col IS NULL).
2. If a query returns rows, it means the data FAILED the quality check.
3. Write follow-up queries if you need to find the root cause of a failure.
4. Run AT LEAST 3 different validation checks before finalising.
5. Do NOT include a <REPORT> block in the same message as a query block.

When you have finished running all necessary quality checks, output your final
validation report wrapped in COMPLETE opening AND closing tags, like this:

<REPORT>
Summary of what passed, what failed, and recommended fixes.
</REPORT>

IMPORTANT: Do NOT mention <REPORT> in your regular text. Only use those tags when
you are ready to submit your final validation report."""


class DataQualityAgent:
    """LangGraph agent for autonomous data quality validation."""

    def __init__(self):
        self.llm_service = get_llm_service()
        self.graph = self._build_graph()

    def _build_graph(self) -> StateGraph:
        """Build the cyclic ReAct LangGraph workflow."""
        workflow = StateGraph(AgentState)

        # Add nodes
        workflow.add_node("setup_connection",           self._setup_connection)

        # Exploration Sub-Graph (Cyclic)
        workflow.add_node("explore_data",               self._explore_data)
        workflow.add_node("execute_exploration_query",  self._execute_query_tool)
        workflow.add_node("save_metadata",              self._save_metadata)

        # Validation Sub-Graph (Cyclic)
        workflow.add_node("validate_data",              self._validate_data)
        workflow.add_node("execute_validation_query",   self._execute_query_tool)
        workflow.add_node("generate_dashboard_report",  self._generate_report)

        # Graph Edges
        workflow.set_entry_point("setup_connection")
        workflow.add_edge("setup_connection", "explore_data")

        # Conditional routing for Exploration
        workflow.add_conditional_edges(
            "explore_data",
            self._route_exploration,
            {
                "execute_tool": "execute_exploration_query",
                "finished":     "save_metadata",
            }
        )
        workflow.add_edge("execute_exploration_query", "explore_data")

        workflow.add_edge("save_metadata", "validate_data")

        # Conditional routing for Validation
        workflow.add_conditional_edges(
            "validate_data",
            self._route_validation,
            {
                "execute_tool": "execute_validation_query",
                "finished":     "generate_dashboard_report",
            }
        )
        workflow.add_edge("execute_validation_query", "validate_data")

        workflow.add_edge("generate_dashboard_report", END)

        return workflow.compile()

    # ==========================================
    # ROUTING LOGIC  (FIXED)
    # ==========================================

    def _route_exploration(self, state: AgentState) -> str:
        """
        Route to 'execute_tool' unless the agent has emitted a COMPLETE
        <METADATA>...</METADATA> block (both opening AND closing tags present).

        FIX: Previously checked `if "<METADATA>" in message` which matched when
        the LLM merely *mentioned* the tag name in prose (e.g. "I will compile a
        <METADATA> report"), causing premature termination.
        FIX: Iteration guard forces conclusion after MAX_EXPLORATION_STEPS.
        """
        exploration_steps = state.get("exploration_steps", 0)

        # Hard limit: force the agent to conclude
        if exploration_steps >= MAX_EXPLORATION_STEPS:
            logger.warning(
                f"Exploration reached max iterations ({MAX_EXPLORATION_STEPS}). "
                "Forcing metadata save with whatever we have."
            )
            return "finished"

        if not state.get("messages"):
            return "execute_tool"

        last_message = state["messages"][-1]["content"]

        # FIXED: require BOTH opening and closing tags
        if re.search(r'<METADATA>[\s\S]+?</METADATA>', last_message, re.IGNORECASE):
            return "finished"

        return "execute_tool"

    def _route_validation(self, state: AgentState) -> str:
        """
        Route to 'execute_tool' unless the agent has emitted a COMPLETE
        <REPORT>...</REPORT> block AND there is no pending query to execute.

        FIX 1: Previously checked `if "<REPORT>" in message` which matched when
        the LLM emitted BOTH a query block AND a <REPORT> in the same message,
        skipping the query execution so zero ValidationResults were ever saved.
        FIX 2: Iteration guard forces conclusion after MAX_VALIDATION_STEPS.
        """
        validation_steps = state.get("validation_steps", 0)

        # Hard limit
        if validation_steps >= MAX_VALIDATION_STEPS:
            logger.warning(
                f"Validation reached max iterations ({MAX_VALIDATION_STEPS}). "
                "Forcing report generation."
            )
            return "finished"

        if not state.get("messages"):
            return "execute_tool"

        last_message = state["messages"][-1]["content"]

        has_complete_report = bool(
            re.search(r'<REPORT>[\s\S]+?</REPORT>', last_message, re.IGNORECASE)
        )
        has_pending_query = bool(
            re.search(r'"action"\s*:\s*"execute_query"', last_message)
        )

        # FIXED: if there is still a query to run, execute it first even when
        # a <REPORT> block is also present in the same message.
        if has_complete_report and not has_pending_query:
            return "finished"

        return "execute_tool"

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

            full_scan   = state['data_source_info'].full_scan_requested
            sample_data = await connector.sample_data(
                state['data_source_info'].target_path,
                sample_size=state.get('execution_config', {}).get('sample_size', 1000),
                full_scan=full_scan
            )

            state['data_source_info'].schema      = schema
            state['data_source_info'].sample_data = sample_data

            target   = state['data_source_info'].target_path
            init_msg = (
                f"System: Connected to target '{target}'. "
                f"Schema: {json.dumps(schema, default=str)}. "
                "You may now begin exploration."
            )

            return {
                "data_source_info":  state['data_source_info'],
                "status":            AgentStatus.EXPLORING,
                "current_step":      "explore_data",
                "exploration_steps": 0,
                "validation_steps":  0,
                "messages":          [{"role": "system", "content": init_msg}],
            }
        except Exception as e:
            logger.error(f"❌ CONNECTION FAILED: {str(e)}")
            return {"status": AgentStatus.ERROR, "error_message": str(e)}

    async def _explore_data(self, state: AgentState) -> Dict[str, Any]:
        """Agent node for exploratory data analysis."""
        logger.info("=" * 60)
        logger.info("🔍 STEP 2: Agent Exploring Data...")

        target_table     = state['data_source_info'].target_path
        exploration_steps = state.get("exploration_steps", 0) + 1

        # Build prompt context from conversation history (last 10 messages)
        history = "\n\n".join(
            [f"{m['role'].upper()}: {m['content']}" for m in state["messages"][-10:]]
        )

        # Nudge the agent to conclude when approaching the limit
        conclusion_nudge = ""
        if exploration_steps >= MAX_EXPLORATION_STEPS - 2:
            conclusion_nudge = (
                "\n\nNOTE: You are approaching the maximum number of exploration steps. "
                "Please output your <METADATA>...</METADATA> report now based on what you have gathered."
            )

        prompt = f"""TARGET TABLE: {target_table}

CONVERSATION HISTORY:
{history}

What query do you want to run next to understand this data?
If you have enough information, output your complete <METADATA>...</METADATA> report.{conclusion_nudge}"""

        response = await self.llm_service.generate(
            prompt=prompt,
            system_prompt=EXPLORATION_PROMPT
        )

        logger.info(f"Agent Output: {response[:150]}...")
        return {
            "messages":          [{"role": "assistant", "content": response}],
            "exploration_steps": exploration_steps,
        }

    async def _validate_data(self, state: AgentState) -> Dict[str, Any]:
        """Agent node for writing and running validation queries."""
        logger.info("=" * 60)
        logger.info("⚙️ STEP 4: Agent Validating Data...")

        target_table     = state['data_source_info'].target_path
        rag_context      = state.get("retrieved_context", [{}])[0].get(
                               "content", "No metadata context found."
                           )
        validation_steps = state.get("validation_steps", 0) + 1

        history = "\n\n".join(
            [f"{m['role'].upper()}: {m['content']}" for m in state["messages"][-10:]]
        )

        rules_run = len(state.get("validation_results", []))

        conclusion_nudge = ""
        if validation_steps >= MAX_VALIDATION_STEPS - 2 or rules_run >= 5:
            conclusion_nudge = (
                "\n\nNOTE: You have run enough checks. "
                "Please output your final <REPORT>...</REPORT> now (without any query block)."
            )

        prompt = f"""TARGET TABLE: {target_table}

METADATA CONTEXT:
{rag_context}

CONVERSATION HISTORY:
{history}

Rules executed so far: {rules_run}

What validation query do you want to run next? Remember to select rows that FAIL the check.
If you have run all checks and investigated failures, output your final <REPORT>...</REPORT>.{conclusion_nudge}"""

        response = await self.llm_service.generate(
            prompt=prompt,
            system_prompt=VALIDATION_PROMPT
        )

        logger.info(f"Agent Output: {response[:150]}...")
        return {
            "messages":         [{"role": "assistant", "content": response}],
            "validation_steps": validation_steps,
        }

    async def _execute_query_tool(self, state: AgentState) -> Dict[str, Any]:
        """Tool node: Parses LLM JSON action and executes the query via the Engine."""
        logger.info("=" * 60)
        logger.info("🛠️ TOOL EXECUTION: Running Agent's Query...")

        last_message = state["messages"][-1]["content"]

        # Extract JSON block using regex (markdown-fenced first, raw JSON fallback)
        match = re.search(r'```json\s*([\s\S]*?)\s*```', last_message, re.IGNORECASE)
        if not match:
            match = re.search(
                r'(\{[\s\S]*?"action"\s*:\s*"execute_query"[\s\S]*?\})',
                last_message
            )

        if not match:
            msg = (
                "System Error: Could not parse query JSON block from your last message. "
                "Please format exactly as requested using ```json ... ```."
            )
            logger.warning("Failed to parse tool call from LLM.")
            return {"messages": [{"role": "user", "content": msg}]}

        try:
            action_data = json.loads(match.group(1))
            query       = action_data.get("query")
            q_type      = action_data.get("query_type", "sql")

            logger.info(f"Executing {q_type.upper()}: {query}")

            from app.validation.engine import ValidationEngine
            engine = ValidationEngine()

            # Attempt Native Pushdown First
            tool_result = None
            if q_type.lower() == 'sql':
                # Only pushdown for actual databases, not files
                source_type = str(getattr(state['data_source_info'], 'source_type', '')).lower()
                if source_type in ['postgresql', 'postgres', 'mysql', 'sqlite']:
                    try:
                        logger.info(f"Pushing down native SQL query to {source_type} connector.")
                        connector = ConnectorFactory.create_connector(
                            source_type, 
                            state['data_source_info'].connection_config
                        )
                        await connector.connect()
                        
                        # Native execution
                        pushdown_results = await connector.execute_raw_query(query)
                        await connector.disconnect()
                        
                        tool_result = {
                            "status": "success",
                            "row_count": len(pushdown_results),
                            "sample_rows": pushdown_results[:5]  # Limit to 5 for context limit
                        }
                    except Exception as pg_err:
                        logger.warning(f"Native SQL execution failed: {pg_err}. Falling back to sandbox.")
                        tool_result = None # Fall through to engine evaluation
            
            if tool_result is None:
                # Engine sandbox fallback
                tool_result = await engine.execute_agent_query(
                    query           = query,
                    query_type      = q_type,
                    data_source_info= state['data_source_info'],
                    sample_data     = state['data_source_info'].sample_data
                )

            # Record ValidationResult when in validation phase
            validation_updates = {}
            if (
                state["status"] == AgentStatus.VALIDATING
                and "rule_name" in action_data
            ):
                current_results = state.get("validation_results", [])
                failed_count    = tool_result.get("row_count", 0)
                row_count       = state["data_source_info"].row_count or 0

                res = ValidationResult(
                    rule_id          = f"agent_rule_{len(current_results)}",
                    rule_name        = action_data.get("rule_name", "agent_validation"),
                    status           = "failed" if failed_count > 0 else "passed",
                    passed_count     = max(0, row_count - failed_count),
                    failed_count     = failed_count,
                    total_count      = row_count,
                    failure_examples = tool_result.get("sample_rows", []),
                    severity         = action_data.get("severity", "warning"),
                    rule_type        = "agent_query",
                    executed_query   = query
                )
                current_results.append(res)
                validation_updates["validation_results"] = current_results

            result_str = json.dumps(tool_result, default=str, indent=2)
            return {
                "messages": [{"role": "user", "content": f"QUERY RESULTS:\n{result_str}"}],
                **validation_updates
            }

        except Exception as e:
            logger.error(f"Tool Execution Failed: {str(e)}")
            return {
                "messages": [{
                    "role":    "user",
                    "content": (
                        f"Query execution failed: {str(e)}. "
                        "Please fix your query and try again."
                    )
                }]
            }

    async def _save_metadata(self, state: AgentState) -> Dict[str, Any]:
        """Extracts the <METADATA> tag from exploration and saves to RAG."""
        logger.info("=" * 60)
        logger.info("💾 STEP 3: Saving Metadata to Vector DB...")

        # Search all messages for a complete METADATA block (not just the last one)
        metadata_text = None
        for msg in reversed(state["messages"]):
            m = re.search(
                r'<METADATA>([\s\S]*?)</METADATA>',
                msg.get("content", ""),
                re.IGNORECASE
            )
            if m:
                metadata_text = m.group(1).strip()
                break

        if not metadata_text:
            # Fallback: use the schema info we already have
            schema = state['data_source_info'].schema or {}
            metadata_text = (
                f"Schema for {state['data_source_info'].target_path}: "
                + json.dumps(schema, default=str)
            )
            logger.warning(
                "No complete <METADATA> block found in messages. "
                "Using raw schema as fallback."
            )
        else:
            logger.info("Metadata extracted successfully.")

        try:
            rag_service = await get_rag_service()
            await rag_service.add_document(
                document_type = "agent_metadata",
                source_id     = state['data_source_info'].target_path,
                title         = f"Exploration Metadata: {state['data_source_info'].target_path}",
                content       = metadata_text
            )
            return {
                "retrieved_context": [{"content": metadata_text}],
                "current_step":      "validate_data",
                "status":            AgentStatus.VALIDATING,
                "messages":          [{
                    "role":    "system",
                    "content": "Metadata saved successfully. Moving to Validation phase."
                }],
            }
        except Exception as e:
            logger.error(f"RAG Save Failed: {e}")
            return {
                "retrieved_context": [{"content": metadata_text}],
                "current_step":      "validate_data",
                "status":            AgentStatus.VALIDATING,
                "messages":          [{
                    "role":    "system",
                    "content": f"RAG Error: {e}. Moving to validation anyway."
                }],
            }

    async def _generate_report(self, state: AgentState) -> Dict[str, Any]:
        """Extracts the <REPORT> tag and finalizes the run facts correctly."""
        logger.info("=" * 60)
        logger.info("📄 STEP 5: Finalizing Report...")

        # Search all messages for a complete REPORT block
        agent_narrative = None
        for msg in reversed(state["messages"]):
            m = re.search(
                r'<REPORT>([\s\S]*?)</REPORT>',
                msg.get("content", ""),
                re.IGNORECASE
            )
            if m:
                agent_narrative = m.group(1).strip()
                break

        if not agent_narrative:
            agent_narrative = "Agent completed validation. See individual rule results for details."

        results     = state.get("validation_results", [])
        total_rules = len(results)
        passed_rules = sum(1 for r in results if r.status == "passed")
        failed_rules = sum(1 for r in results if r.status == "failed")
        warning_rules = sum(
            1 for r in results
            if r.status == "failed" and r.severity == "warning"
        )
        
        # Build factual analysis prefix instead of trusting LLM numbers
        factual_prefix = f"Validation Execution complete. {passed_rules}/{total_rules} checks passed.\n\nAgent Narrative:\n"
        report_text = factual_prefix + agent_narrative

        quality_score = 0.0
        if total_rules > 0:
            critical_failed = sum(
                1 for r in results
                if r.status == "failed" and r.severity == "critical"
            )
            warning_failed  = sum(
                1 for r in results
                if r.status == "failed" and r.severity == "warning"
            )
            base_score    = (passed_rules / total_rules) * 100
            quality_score = max(0, base_score - (critical_failed * 20) - (warning_failed * 5))
            quality_score = round(quality_score, 2)

        summary_report = {
            "quality_score":      quality_score,
            "total_rules":        total_rules,
            "passed_rules":       passed_rules,
            "failed_rules":       failed_rules,
            "warning_rules":      warning_rules,
            "agent_analysis":     report_text,
            "data_source":        state['data_source_info'].target_path,
            "records_processed":  state['data_source_info'].row_count,
        }

        return {
            "quality_score":  quality_score,
            "summary_report": summary_report,
            "status":         AgentStatus.COMPLETED,
            "completed_at":   datetime.utcnow().isoformat(),
        }

    async def run(
        self,
        validation_id:    str,
        validation_mode:  ValidationMode,
        data_source_info: DataSourceInfo,
        custom_rules:     Optional[List[Any]] = None,
        execution_config: Optional[Dict[str, Any]] = None,
    ) -> AgentState:
        """Run the autonomous data quality agent."""

        logger.info("\n" + "🚀" * 30)
        logger.info("STARTING AUTONOMOUS DATA QUALITY AGENT")
        logger.info(f"   Validation ID: {validation_id}")
        logger.info(f"   Target: {data_source_info.target_path}")
        logger.info("🚀" * 30 + "\n")

        initial_state: AgentState = {
            "validation_id":      validation_id,
            "validation_mode":    validation_mode,
            "data_source_info":   data_source_info,
            "custom_rules":       custom_rules or [],
            "execution_config":   execution_config or {},
            "status":             AgentStatus.CONNECTING,
            "current_step":       "setup_connection",
            "messages":           [],
            "data_profile":       None,
            "ai_recommended_rules": [],
            "all_rules":          [],
            "validation_results": [],
            "retrieved_context":  [],
            "quality_score":      None,
            "summary_report":     None,
            "error_message":      None,
            "started_at":         datetime.utcnow().isoformat(),
            "completed_at":       None,
            "execution_metrics":  {},
            # Iteration counters (new)
            "exploration_steps":  0,
            "validation_steps":   0,
        }

        result = await self.graph.ainvoke(initial_state)

        logger.info("\n" + "🏁" * 30)
        logger.info(f"AGENT EXECUTION COMPLETE: {result.get('status')}")
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