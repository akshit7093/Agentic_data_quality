"""LangGraph-based Autonomous Data Quality Agent.
REWRITE v8 - TOOL-BASED ARCHITECTURE (No Raw SQL from LLM).
LLM selects pre-built tools → Tools execute validated SQL → Eliminates all syntax errors.
"""
import json
import logging
import re
from typing import Dict, Any, List, Optional
from datetime import datetime
from langgraph.graph import StateGraph, END

from app.agents.state import (
    AgentState, AgentStatus, ValidationMode,
    DataSourceInfo, DataProfile, ValidationResult
)
from app.agents.llm_service import get_llm_service
from app.agents.rag_service import get_rag_service
from app.agents.llm_sanitizer import sanitize_llm_response, validate_protocol
from app.connectors.factory import ConnectorFactory
from app.core.config import get_settings
from app.agents.tool_based_agent import (
    ValidationToolExecutor,
    TABLE_TOOLS,
    COLUMN_TOOLS,
    DataType,
    ToolResult
)
from app.agents.column_analysis_agent import ColumnAnalysisAgent, BatchColumnAnalysisAgent
from app.agents.healing_service import HealingService

logger = logging.getLogger(__name__)

# ═══════════════════════════════════════════════════════════════
# AGENT CONFIGURATION
# ═══════════════════════════════════════════════════════════════
LLM_MAX_TOKENS = 8192   # Expanded to support long <think> blocks on local reasoning models
MAX_EXPLORATION_STEPS = 5  # Cap exploration to prevent stuck LLM loops
MAX_VALIDATION_STEPS = 60  # Increased for LLM-driven column validation
GRAPH_RECURSION_LIMIT = 250
MAX_HISTORY_MESSAGES = 6
BATCH_SIZE = 5  # Max tools per batch for exploration
MAX_QUERIES_PER_COLUMN = 5  # Safety limit for LLM queries per column

# ═══════════════════════════════════════════════════════════════
# SYSTEM PROMPTS - TOOL-BASED (No Raw SQL from LLM)
# ═══════════════════════════════════════════════════════════════

EXPLORATION_PROMPT = """You are a Data Exploration Agent. Understand the dataset structure by selecting pre-built tools.

AVAILABLE TABLE-LEVEL TOOLS (select by tool_id):
{table_tools_list}

OUTPUT FORMAT — pick EXACTLY ONE per response (raw, no markdown, no explanation):

Option A: Select tools
{"action": "execute_tools", "tool_selections": [{"tool_id": "table_row_count"}, {"tool_id": "table_sample_rows"}]}

Option B: Done exploring — submit metadata
<METADATA>
This table represents ...
- column_name: TYPE, null_count=N, unique_count=N, notes
</METADATA>

RULES:
1. Output raw JSON or <METADATA> only — no prose, no fences, no extra text.
2. Select 2-4 tools at a time. After 2-3 batches, submit <METADATA>.
3. For column tools that need a column name, include it: {"tool_id": "table_null_scan", "column": "email"}
4. Never combine JSON and <METADATA> in one response.
5. Use ONLY single braces. Never double braces.
"""

# Appended to EXPLORATION_PROMPT for flat-file sources only
FILE_SOURCE_EXPLORATION_ADDENDUM = """
NOTE — FILE SOURCE:
This dataset was loaded from a flat file (CSV/JSON/Excel/Parquet).
A pandas profile has already been run automatically and appears in the system context.
The table is available under its sanitised name for all SQL tools.
Columns and row counts are pre-populated — you can submit <METADATA> after just 1 tool batch.
"""


# VALIDATION_PROMPT is no longer used for rule generation —
# rules are generated deterministically in Python.
# This prompt is only used for optional LLM business-logic supplements.
VALIDATION_PROMPT = """You are a SQL expert adding extra data quality rules.

Output a raw JSON array only. No markdown, no explanation, nothing before or after the array.
Each rule: {"rule_name": "colname_checktype", "severity": "critical|warning|info", "query": "SELECT COUNT(*) AS alias FROM table WHERE failing_condition"}

Rules:
- Every query MUST return ONE integer (count of failing rows). 0 = passed, >0 = failed.
- COUNT(*) only. Never SELECT *.
- rule_name must start with the column name.
- Single braces only. Never double braces.
"""


# Mode-specific exploration focus areas — injected as a suffix to EXPLORATION_PROMPT
MODE_EXPLORATION_FOCUS = {
    "schema_only": """
FOCUS FOR THIS MODE (schema_only):
- Prioritize: column types, null counts, unique counts, empty strings, type mismatches.
- Skip: value distribution, business logic, cross-column relationships.
- In <METADATA>, note structural issues only (e.g. column appears to be DATE stored as TEXT).
""",
    "ai_recommended": """
FOCUS FOR THIS MODE (ai_recommended):
- Prioritize: value distributions, outlier patterns, null hotspots, enum cardinality.
- Look for: unexpected value ranges, suspicious uniformity, ID columns with gaps.
- In <METADATA>, document anomaly signals the AI validation phase can target.
""",
    "hybrid": """
FOCUS FOR THIS MODE (hybrid):
- Balance structural analysis (nulls, types) with business signal detection.
- Note both schema-level issues AND value-level anomalies.
- In <METADATA>, tag each observation as either structural or semantic.
""",
    "business_analysis": """
FOCUS FOR THIS MODE (business_analysis):
- Prioritize: cross-column relationships, date ordering, price/cost/quantity coherence.
- Look for: logical inconsistencies (e.g. end_date < start_date, negative amounts).
- In <METADATA>, highlight columns that interact with each other for downstream checks.
""",
    "custom_rules": """
FOCUS FOR THIS MODE (custom_rules):
- Exploration is minimal — custom rules will drive validation.
- Collect only: row count, column list, and obvious structural issues.
- Submit <METADATA> after a single tool batch.
""",
}


def get_exploration_prompt(mode: str, source_type: str = "") -> str:
    """Return the exploration prompt enriched with mode-specific + source-type-specific instructions."""
    focus = MODE_EXPLORATION_FOCUS.get(mode, "")
    file_note = FILE_SOURCE_EXPLORATION_ADDENDUM if DataQualityAgent._is_file_source(source_type) else ""
    return EXPLORATION_PROMPT + focus + file_note


class DataQualityAgent:
    """LangGraph agent for autonomous data quality validation using pre-built tools."""
    validation_prompt = VALIDATION_PROMPT

    @property
    def exploration_prompt(self):
        """Kept for backward compatibility — use get_exploration_prompt(mode) directly."""
        return EXPLORATION_PROMPT

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
        self.healing_service = HealingService()
        self.graph = self._build_graph()

    def _build_graph(self) -> StateGraph:
        """Build the cyclic ReAct LangGraph workflow."""
        workflow = StateGraph(AgentState)

        # Add nodes
        workflow.add_node("setup_connection", self._setup_connection)
        workflow.add_node("explore_data", self._explore_data)
        workflow.add_node("execute_exploration_tools", self._execute_tool)
        workflow.add_node("save_metadata", self._save_metadata)
        workflow.add_node("prepare_column_validation", self._prepare_column_validation)
        workflow.add_node("validate_column", self._validate_column)
        workflow.add_node("advance_column", self._advance_column)
        workflow.add_node("generate_dashboard_report", self._generate_report)

        # Graph Edges
        workflow.set_entry_point("setup_connection")
        workflow.add_edge("setup_connection", "explore_data")

        workflow.add_conditional_edges(
            "explore_data",
            self._route_exploration,
            {
                "execute_tools": "execute_exploration_tools",
                "finished": "save_metadata",
                "retry": "explore_data",
            }
        )
        workflow.add_edge("execute_exploration_tools", "explore_data")
        workflow.add_edge("save_metadata", "prepare_column_validation")
        workflow.add_edge("prepare_column_validation", "validate_column")
        
        workflow.add_conditional_edges(
            "validate_column",
            self._route_validation,
            {
                "next_column": "advance_column",
                "finished": "generate_dashboard_report",
            }
        )
        workflow.add_edge("advance_column", "validate_column")
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
            return "execute_tools"

        last_message = state["messages"][-1]["content"]
        sanitized = self._sanitize_llm_response(last_message)

        # If sanitization returned empty (truncated), retry
        if not sanitized:
            logger.warning("Last response was truncated - requesting retry")
            return "retry"

        # Check for complete METADATA block (must have both tags)
        has_metadata = bool(re.search(r'<METADATA>[\s\S]+?</METADATA>', sanitized, re.IGNORECASE))
        
        # Check for pending tools - any JSON with "action" key
        has_pending_tools = bool(re.search(r'"action"\s*:', sanitized))

        # PRIORITY: If we have both, execute tools first (don't skip to finished)
        if has_metadata and has_pending_tools:
            logger.info("Both tool selection and <METADATA> detected - executing tools first")
            # Update the message to remove the METADATA tag for now, keep only tools
            cleaned = re.sub(r'<METADATA>[\s\S]*?</METADATA>', '', sanitized, flags=re.IGNORECASE).strip()
            state["messages"][-1]["content"] = cleaned
            return "execute_tools"

        if has_metadata and not has_pending_tools:
            logger.info("Complete <METADATA> block found - finishing exploration")
            return "finished"

        return "execute_tools"

    def _route_validation(self, state: AgentState) -> str:
        """Route validation — each _validate_column call processes ONE full column.
        BUG 9 FIX: removed the dead 'execute_tools' branch. Both the COLUMN_COMPLETE
        check and the default both returned 'next_column', making the branch pointless.
        The execute_validation_tools node it would have routed to was also removed from
        the graph since _route_validation never returned 'execute_tools'."""
        current_column_idx = state.get("current_column_index", 0)
        columns_to_validate = state.get("columns_to_validate", [])
        validation_steps = state.get("validation_steps", 0)

        if validation_steps >= MAX_VALIDATION_STEPS:
            logger.warning("Validation step limit reached. Forcing finish.")
            return "finished"

        if current_column_idx >= len(columns_to_validate):
            logger.info("All columns validated — moving to report.")
            return "finished"

        return "next_column"

    # ==========================================
    # NODE IMPLEMENTATIONS
    # ==========================================

    @staticmethod
    def _is_file_source(source_type: str) -> bool:
        """Return True for flat-file source types that need DuckDBFileConnector."""
        return str(source_type).lower() in (
            "local_file", "csv", "json", "jsonl", "excel", "parquet", "feather", "tsv",
        )

    @staticmethod
    def _get_connector(
        source_type: str, 
        connection_config: dict,
        selected_columns: Optional[List[str]] = None,
        column_mapping: Optional[Dict[str, str]] = None,
        slice_filters: Optional[Dict[str, Any]] = None
    ):
        """Return the appropriate connector — DuckDB for flat files, factory default otherwise."""
        if DataQualityAgent._is_file_source(source_type):
            from app.connectors.dataframe_connector import DuckDBFileConnector
            return DuckDBFileConnector(
                connection_config,
                selected_columns=selected_columns,
                column_mapping=column_mapping,
                slice_filters=slice_filters
            )
        return ConnectorFactory.create_connector(source_type, connection_config)

    async def _setup_connection(self, state: AgentState) -> Dict[str, Any]:
        """Connect to data source and fetch initial schema context.

        For flat-file sources (CSV / JSON / Excel / Parquet) the DuckDBFileConnector
        is used transparently so the rest of the agent pipeline is unchanged.
        """
        logger.info("=" * 60)
        logger.info("🔌 STEP 1: Connecting to data source...")

        try:
            source_type = state['data_source_info'].source_type
            connection_config = state['data_source_info'].connection_config
            target = state['data_source_info'].target_path

            ds_info = state['data_source_info']
            connector = self._get_connector(
                source_type, 
                connection_config,
                selected_columns=ds_info.selected_columns,
                column_mapping=ds_info.column_mapping,
                slice_filters=ds_info.slice_filters
            )
            await connector.connect(resource_path=target)
            schema = await connector.get_schema(target)

            # ── OPTIMIZATION: Filter schema if selected_columns is provided ──
            selected_columns = getattr(state['data_source_info'], 'selected_columns', None)
            column_mapping = getattr(state['data_source_info'], 'column_mapping', {}) or {}
            
            if selected_columns:
                logger.info(f"  🎯 Filtering schema to {len(selected_columns)} selected columns (handling aliases)")
                original_cols = schema.get('columns', {})
                
                # Build reverse mapping: alias -> original_name
                # (so we can find original_name for each selected alias)
                rev_map = {alias: orig for orig, alias in column_mapping.items()}
                
                filtered_cols = {}
                for col in selected_columns:
                    # Case 1: selection is an alias
                    orig_name = rev_map.get(col)
                    if orig_name and orig_name in original_cols:
                        filtered_cols[orig_name] = original_cols[orig_name]
                    # Case 2: selection is an original name
                    elif col in original_cols:
                        filtered_cols[col] = original_cols[col]
                
                schema['columns'] = filtered_cols
                schema['column_count'] = len(filtered_cols)
                print(f"[DEBUG] _setup_connection: Schema filtered to {len(filtered_cols)} columns: {list(filtered_cols.keys())}")

            full_scan    = getattr(state['data_source_info'], 'full_scan_requested', False)
            slice_filters = getattr(state['data_source_info'], 'slice_filters', None)

            sample_data = await connector.sample_data(
                target,
                sample_size=state.get('execution_config', {}).get('sample_size', 1000),
                full_scan=full_scan,
                slice_filters=slice_filters,   # DuckDBFileConnector handles this natively
            )

            state['data_source_info'].schema      = schema
            state['data_source_info'].sample_data = sample_data

            filter_context = f" NOTE: Filtered to {slice_filters}." if slice_filters else ""

            # ── For flat-file sources: run pandas profile tools immediately ──
            df_profile_summary = ""
            if self._is_file_source(source_type):
                try:
                    from app.connectors.dataframe_connector import DuckDBFileConnector
                    from app.agents.dataframe_tools import DataFrameToolExecutor
                    if isinstance(connector, DuckDBFileConnector):
                        df_executor = DataFrameToolExecutor(connector, target)
                        profile_results = await df_executor.run_profile_tools()
                        issues = [r for r in profile_results if r.failed_count > 0]
                        summary_lines = []
                        for r in profile_results:
                            summary_lines.append(f"  [{r.tool_id}] {r.message}")
                        df_profile_summary = (
                            "\n\nPANDAS PROFILE SUMMARY:\n"
                            + "\n".join(summary_lines)
                            + f"\n  → {len(issues)} profile issue(s) detected."
                        )
                        logger.info(f"  📊 Pandas profile complete: {len(profile_results)} tools, {len(issues)} issues")
                except Exception as prof_err:
                    logger.warning(f"  Pandas profiling failed (non-critical): {prof_err}")

            # Concise init message (schema as JSON + any profile summary)
            schema_excerpt = json.dumps(
                {k: v.get("type") for k, v in schema.get("columns", {}).items()},
                default=str
            )
            init_msg = (
                f"Connected to '{target}'. "
                f"Columns: {schema_excerpt}. "
                f"Rows: {schema.get('row_count', '?')}. "
                f"{filter_context}"
                f"{df_profile_summary}"
                f" Begin exploration."
            )

            return {
                "data_source_info": state['data_source_info'],
                "status":           AgentStatus.EXPLORING,
                "current_step":     "explore_data",
                "exploration_steps": 0,
                "validation_steps":  0,
                "messages":         [{"role": "system", "content": init_msg}],
                "tool_execution_history": [],
            }
        except Exception as e:
            logger.error(f"❌ CONNECTION FAILED: {str(e)}")
            return {"status": AgentStatus.ERROR, "error_message": str(e)}

    async def _explore_data(self, state: AgentState) -> Dict[str, Any]:
        """Agent node for exploratory data analysis with tool selection."""
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
            nudge = "\n\nFINAL STEP: Output ONLY your <METADATA>...</METADATA> report now. NO tool selections."
        elif exploration_steps >= MAX_EXPLORATION_STEPS - 3:
            nudge = "\n\nNOTE: Steps remaining limited. Finish exploration and output <METADATA> report soon."
        else:
            nudge = ""

        # Build available tools list for prompt
        # Use provided selected_columns if available
        selected_columns = state['data_source_info'].selected_columns
        column_mapping = state['data_source_info'].column_mapping or {}
        rev_map = {alias: orig for orig, alias in column_mapping.items()}
        source_cols = []
        if selected_columns:
            for c in selected_columns:
                source_cols.append(rev_map.get(c, c))
        
        print(f"\n[DEBUG] _explore_data node")
        print(f"[DEBUG] selected_columns (raw): {selected_columns}")
        print(f"[DEBUG] source_cols (original): {source_cols}")
        
        tool_executor = ValidationToolExecutor(None, target_table, source_cols)
        table_tools = tool_executor.get_table_tools()
        table_tools_list = "\n".join([
            f"  - {t['tool_id']}: {t['name']} ({t['description']}) [Parameters: {', '.join(t['parameters']) or 'none'}]"
            for t in table_tools
        ])

        scope_note = ""
        if selected_columns:
            scope_note = f"\nCOLUMN SCOPE: You MUST only analyze these columns: {', '.join(selected_columns)}\n"

        prompt = f"""Table: {target_table}
Recent history:
{history}{scope_note}
Step {exploration_steps}/{MAX_EXPLORATION_STEPS}. Output ONE tool selection or your final <METADATA> report.{nudge}

═══════════════════════════════════════════════════════════════
AVAILABLE TOOLS:
{table_tools_list}
═══════════════════════════════════════════════════════════════
"""

        response = await self.llm_service.generate(
            prompt=prompt,
            system_prompt=get_exploration_prompt(
                str(state.get("validation_mode", "ai_recommended")),
                source_type=str(getattr(state["data_source_info"], "source_type", "")),
            ).replace('{table_tools_list}', table_tools_list),
            max_tokens=LLM_MAX_TOKENS,
            temperature=0.1
        )
        
        response = self._sanitize_llm_response(response)
        
        # Protocol validation — if the LLM broke format, inject a corrective message
        check = validate_protocol(response, expected="json_or_tag")
        if not check.is_valid:
            logger.warning(f"Protocol violation ({check.violation_type}): injecting correction hint")
            return {
                "messages": [{"role": "user", "content": f"⚠️ {check.correction_hint}"}],
                "exploration_steps": exploration_steps,
            }

        logger.info(f"Agent Output: {response[:200]}...")
        return {
            "messages": [{"role": "assistant", "content": response}],
            "exploration_steps": exploration_steps,
        }

    async def _execute_tool(self, state: AgentState) -> Dict[str, Any]:
        """Execute agent-selected tools (v8 tool-based) or raw queries (v7 fallback)."""
        logger.info("=" * 60)
        logger.info("🛠️ TOOL EXECUTION: Processing Agent's Selection...")

        last_message = state["messages"][-1]["content"]
        last_message = self._sanitize_llm_response(last_message)
        
        # Initialize connector for scope safety
        connector = None

        # ── Extract LLM reasoning if present ──
        reasoning_text = ""
        reasoning_match = re.search(r'<REASONING>([\s\S]*?)</REASONING>', last_message, re.IGNORECASE)
        if reasoning_match:
            reasoning_text = reasoning_match.group(1).strip()
            logger.info(f"LLM Reasoning: {reasoning_text[:200]}")
            # Strip reasoning from the message for JSON extraction
            last_message = last_message[:reasoning_match.start()] + last_message[reasoning_match.end():]
            last_message = last_message.strip()

        # Check if this is actually a structural tag completion (no tool to run)
        has_metadata = bool(re.search(r'<METADATA>[\s\S]+?</METADATA>', last_message, re.IGNORECASE))
        has_report = bool(re.search(r'<REPORT>[\s\S]+?</REPORT>', last_message, re.IGNORECASE))
        has_json_action = bool(re.search(r'"action"\s*:', last_message))
        
        if (has_metadata or has_report) and not has_json_action:
            tag = "METADATA" if has_metadata else "REPORT"
            logger.info(f"No tool to execute - <{tag}> block present. Skipping.")
            messages = []
            if reasoning_text:
                messages.append({"role": "assistant", "content": f"🧠 Reasoning: {reasoning_text}"})
            messages.append({"role": "assistant", "content": last_message})
            return {"messages": messages}

        # ── EXTRACTION: Find JSON action block ──
        json_block = None
        
        # Pattern 1: Fenced JSON block
        match = re.search(r'```(?:json)?\s*([\s\S]*?)\s*```', last_message, re.IGNORECASE)
        if match:
            json_block = match.group(1).strip()
        
        # Pattern 2: Raw JSON object
        if not json_block:
            start = last_message.find('{')
            if start != -1:
                end = last_message.rfind('}')
                if end > start:
                    json_block = last_message[start:end+1].strip()

        if not json_block:
            logger.error(f"No valid JSON found: {last_message[:200]}...")
            return {
                "messages": [{"role": "user", "content": 
                    'Error: No JSON found. Output exactly: {"action": "execute_tools", "tool_selections": [{"tool_id": "table_row_count"}]}'}],
                "exploration_steps": state.get("exploration_steps", 0),
            }

        # ── PARSING ──
        # Fix double-brace issue: LLM copies {{ }} from prompt escaping
        json_block = json_block.replace('{{', '{').replace('}}', '}')
        try:
            action_data = json.loads(json_block)
            if not isinstance(action_data, dict) or "action" not in action_data:
                raise ValueError("JSON missing required 'action' field")
        except (json.JSONDecodeError, ValueError) as e:
            logger.error(f"JSON parse error: {e}. Block: {json_block[:300]}")
            correction = (
                f'Error: Invalid JSON ({str(e)[:100]}). '
                'Output raw JSON only, single braces, no markdown. '
                'Example: {"action": "execute_tools", "tool_selections": [{"tool_id": "table_row_count"}]}'
            )
            return {
                "messages": [{"role": "user", "content": correction}],
                "exploration_steps": state.get("exploration_steps", 0),
            }

        action = action_data.get("action", "")
        target_table = state['data_source_info'].target_path

        # ═══════════════════════════════════════════════════════════
        # NORMALIZE: Convert any LLM output format to tool_selections list
        # ═══════════════════════════════════════════════════════════
        tool_selections = None

        if action == "execute_tools":
            # Standard v8 format
            raw = action_data.get("tool_selections", [])
            if isinstance(raw, dict):
                # LLM returned a single dict instead of a list
                tool_selections = [raw]
            elif isinstance(raw, list):
                tool_selections = raw
            else:
                tool_selections = []

        elif action == "execute_query":
            # V7 legacy raw SQL — pass through to legacy handler
            pass

        else:
            # LLM used the tool_id as the "action" value directly
            # e.g. {"action": "table_null_scan", "column": "email"}
            # OR   {"action": "table_null_scan", "parameters": {"column": "email"}}
            # OR   {"action": "table_null_scan", "tool_selections": [{"tool_id": "table_null_scan", "column": "email"}]}
            # Extract column from wherever the LLM put it
            col = (
                action_data.get("column")
                or (action_data.get("parameters") or {}).get("column")
            )
            # Also check if there are nested tool_selections with a column
            nested_sels = action_data.get("tool_selections", [])
            if not col and nested_sels and isinstance(nested_sels, list):
                col = (nested_sels[0] or {}).get("column") if nested_sels else None
            tool_selections = [{"tool_id": action, "column": col}]
            logger.info(f"Normalized non-standard action '{action}' → tool with column='{col}'")

        # ═══════════════════════════════════════════════════════════
        # V8 PATH: Tool-based execution
        # ═══════════════════════════════════════════════════════════
        if tool_selections is not None:
            # Normalize each selection to have 'tool_id' and 'column' at the top level
            normalized = []
            for sel in tool_selections:
                if not isinstance(sel, dict):
                    continue
                tid = sel.get("tool_id") or sel.get("tool") or sel.get("id") or "unknown"
                # column may be nested inside a "parameters" dict
                col = sel.get("column")
                if col is None and isinstance(sel.get("parameters"), dict):
                    col = sel["parameters"].get("column")
                entry = {k: v for k, v in sel.items() if k not in ("parameters",)}
                entry["tool_id"] = tid
                if col is not None:
                    entry["column"] = col
                normalized.append(entry)
            tool_selections = normalized

            if not tool_selections:
                return {
                    "messages": [{"role": "user", "content":
                        'Error: No valid tool selections. Use format: {"action": "execute_tools", "tool_selections": [{"tool_id": "table_row_count"}]}'}],
                    "exploration_steps": state.get("exploration_steps", 0),
                }

            # Create connector with session scope for tool executor
            ds_info = state['data_source_info']
            connector = self._get_connector(
                ds_info.source_type,
                ds_info.connection_config,
                selected_columns=ds_info.selected_columns,
                column_mapping=ds_info.column_mapping,
                slice_filters=ds_info.slice_filters
            )
            await connector.connect(resource_path=target_table)

            # Resolve original names for executor
            selected_columns = state['data_source_info'].selected_columns
            column_mapping = state['data_source_info'].column_mapping or {}
            rev_map = {alias: orig for orig, alias in column_mapping.items()}
            source_cols = [rev_map.get(c, c) for c in (selected_columns or [])]
            
            tool_executor = ValidationToolExecutor(connector, target_table, source_cols or None)
            results_summary = []
            validation_updates = {}
            current_results = list(state.get("validation_results", []))

            for sel in tool_selections[:BATCH_SIZE]:
                tool_id = sel.get("tool_id", "unknown")
                column = sel.get("column")
                kwargs = {k: v for k, v in sel.items() if k not in ("tool_id", "column", "tool", "id")}

                logger.info(f"Executing tool: {tool_id}" + (f" on column '{column}'" if column else ""))
                
                # ── Tool Execution with Healing Loop ──
                max_healing_attempts = 1 # Lower for main agent to keep it fast
                tool_result = None
                
                for attempt in range(max_healing_attempts + 1):
                    tool_result = await tool_executor.execute_tool(tool_id, column=column, **kwargs)
                    
                    if tool_result.status == "success":
                        break
                        
                    if attempt < max_healing_attempts:
                        logger.warning(self.healing_service.format_healing_message(attempt + 1, tool_result.message))
                        # For V8 tools, healing is limited because queries are pre-built.
                        # We just retry once in case it was a transient/quoting glitch 
                        # (The ColumnAnalysisAgent has more aggressive healing as it generates SQL).
                        continue

                # Build concise summary for LLM
                results_summary.append({
                    "tool_id": tool_id,
                    "column": column,
                    "status": tool_result.status,
                    "row_count": tool_result.row_count,
                    "failed_count": tool_result.failed_count,
                    "sample": tool_result.sample_rows[:3],
                    "message": tool_result.message,
                })

                # Record validation result if in validation phase
                if state["status"] == AgentStatus.VALIDATING and tool_result.status == "success":
                    rule_name = f"{column}_{tool_id}" if column else tool_id
                    existing_names = {
                        (r.get("rule_name") if isinstance(r, dict) else r.rule_name)
                        for r in current_results
                    }
                    if rule_name not in existing_names:
                        row_count = state["data_source_info"].row_count or 0
                        failed = tool_result.failed_count or 0
                        res = ValidationResult(
                            rule_id=f"agent_rule_{len(current_results)}",
                            rule_name=rule_name,
                            status="failed" if failed > 0 else "passed",
                            passed_count=max(0, row_count - failed),
                            failed_count=failed,
                            failure_examples=tool_result.sample_rows[:5],
                            severity=tool_result.severity or "info",
                            rule_type="tool_based",
                            executed_query=tool_result.command_executed
                        )
                        current_results.append(res)

            if state["status"] == AgentStatus.VALIDATING:
                validation_updates["validation_results"] = current_results

            # BUG 1 FIX: 'query' is never assigned in the V8 tool path (only exists in the V7
            # execute_query branch). Deduplication must track tool_ids, not raw SQL strings.
            executed_queries = list(state.get("executed_queries", []))
            for sel in tool_selections:
                tid = sel.get("tool_id", "unknown")
                if tid not in executed_queries:
                    executed_queries.append(tid)

            queries_per_column = dict(state.get("queries_per_column", {}))
            columns_to_validate = state.get("columns_to_validate", [])
            current_column_idx = state.get("current_column_index", 0)
            if columns_to_validate and current_column_idx < len(columns_to_validate):
                col_name = columns_to_validate[current_column_idx].get("name", "unknown")
                queries_per_column[col_name] = queries_per_column.get(col_name, 0) + len(tool_selections)

            # BUG 3 FIX: disconnect before returning — was unreachable after the premature return
            try:
                await connector.disconnect()
            except Exception:
                pass

            result_messages = []
            if reasoning_text:
                result_messages.append({"role": "assistant", "content": f"🧠 Reasoning: {reasoning_text}"})
            # BUG 2 FIX: was 'result_summary' (NameError) — correct variable is 'results_summary'
            result_messages.append({"role": "user", "content": f"Tool Results: {json.dumps(results_summary, default=str)}"})

            return {
                "messages": result_messages,
                "exploration_steps": state.get("exploration_steps", 0),
                # BUG 3 FIX: tool_execution_history update was in dead code after premature return
                "tool_execution_history": state.get("tool_execution_history", []) + results_summary,
                "executed_queries": executed_queries,
                "queries_per_column": queries_per_column,
                **validation_updates
            }

        # ═══════════════════════════════════════════════════════════
        # V7 FALLBACK: Raw SQL execution (legacy compatibility)
        # ═══════════════════════════════════════════════════════════
        elif action == "execute_query":
            query = action_data.get("query")
            q_type = action_data.get("query_type", "sql")
            
            # V8 FIX: Initialize connector for legacy V7 path
            connector = self._get_connector(
                state['data_source_info'].source_type,
                state['data_source_info'].connection_config
            )
            await connector.connect(resource_path=target_table)

            if not query:
                return {
                    "messages": [{"role": "user", "content": "Error: Query string is empty."}],
                    "exploration_steps": state.get("exploration_steps", 0),
                }

            logger.info(f"Legacy SQL execution: {query[:100]}...")
            
            # ── V7 Execution with Healing Loop ──
            max_healing_attempts = 2
            current_query = query
            tool_result = None

            for attempt in range(max_healing_attempts + 1):
                try:
                    # execute_raw_query/ValidationEngine execution
                    if DataQualityAgent._is_file_source(state['data_source_info'].source_type):
                        from app.validation.engine import ValidationEngine
                        engine = ValidationEngine()
                        res = await engine.execute_agent_query(
                            current_query, q_type, state['data_source_info'], state['data_source_info'].sample_data
                        )
                    else:
                        res = await connector.execute_raw_query(current_query, q_type)
                    
                    if res.get("status") == "success":
                        tool_result = res
                        query = current_query # Track the final successful query
                        break
                    
                    # Error handling and healing
                    error_msg = res.get("error", "Unknown error")
                    if attempt < max_healing_attempts:
                        logger.warning(self.healing_service.format_healing_message(attempt + 1, error_msg))
                        # Derive metadata for healing
                        target_col = action_data.get("column_name") or self._derive_column_name(current_query)
                        col_info = next((c for c in state.get("data_profile", {}).get("columns", []) if c.get("name") == target_col), {})
                        
                        corrected = await self.healing_service.get_correction(
                            error_msg=error_msg,
                            original_query=current_query,
                            query_type=q_type,
                            column_name=target_col,
                            dtype=col_info.get("dtype", "unknown"),
                            samples=col_info.get("samples", []),
                            table_name=state['data_source_info'].target_path.split('.')[-1]
                        )
                        if corrected:
                            current_query = corrected
                            continue
                    
                    tool_result = res
                except Exception as e:
                    logger.error(f"Raw query execution failed: {e}")
                    if attempt < max_healing_attempts:
                        continue
                    tool_result = {"status": "error", "error": str(e)}
                    break
            
            try:
                await connector.disconnect()
            except Exception:
                pass

            validation_updates = {}
            if state["status"] == AgentStatus.VALIDATING and tool_result.get("status") != "error":
                rule_name = action_data.get("rule_name") or self._derive_rule_name(query)
                raw_severity = action_data.get("severity", "warning").lower()
                severity_map = {
                    "high": "critical", "critical": "critical",
                    "medium": "warning", "warning": "warning",
                    "low": "info", "info": "info",
                }
                severity = severity_map.get(raw_severity, "warning")
                current_results = list(state.get("validation_results", []))
                existing_names = {
                    (r.get("rule_name") if isinstance(r, dict) else r.rule_name)
                    for r in current_results
                }
                if rule_name not in existing_names:
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

            result_summary = {
                "status": tool_result.get("status", "unknown"),
                "row_count": tool_result.get("row_count", 0),
                "sample": tool_result.get("sample_rows", [])[:3],
                "error": tool_result.get("error", "")
            }
            return {
                "messages": [{"role": "user", "content": f"Result: {json.dumps(result_summary, default=str)}"}],
                **validation_updates
            }

        else:
            # Should not reach here after normalization, but safety net
            logger.error(f"Unhandled action: {action}")
            return {
                "messages": [{"role": "user", "content":
                    f'Error: Unhandled action "{action}". Use "execute_tools" with tool_selections.'}],
                "exploration_steps": state.get("exploration_steps", 0),
            }

    async def _prepare_column_validation(self, state: AgentState) -> Dict[str, Any]:
        """Prepare column-level validation by building available tools for each column."""
        logger.info("=" * 60)
        logger.info("⚙️ STEP 4: Preparing Column Validation...")

        target_table = state['data_source_info'].target_path
        schema = state['data_source_info'].schema
        sample_data = state['data_source_info'].sample_data
        mode_str = state["validation_mode"].value
        source_type = str(getattr(state['data_source_info'], 'source_type', 'sqlite')).lower()

        if not schema:
            logger.warning("No schema available. Cannot perform column-level validation.")
            return {
                "columns_to_validate": [],
                "available_column_tools": {},
                "current_column_index": 0,
                "current_step": "generate_dashboard_report",
            }

        # ── Normalize schema to List[Dict] format ──
        if isinstance(schema, str):
            try:
                schema = json.loads(schema)
                logger.info("Schema was a string — parsed as JSON.")
            except (json.JSONDecodeError, TypeError):
                logger.error(f"Schema is an unparseable string: {schema[:200]}")
                return {"columns_to_validate": [], "available_column_tools": {}}

        if isinstance(schema, dict):
            # Connector dict format: { "columns ": { "col_name ": {...}}, ...}
            columns_dict = schema.get("columns", {})
            if isinstance(columns_dict, dict) and columns_dict:
                schema = [
                    {"name": col_name, "type": col_info.get("type", "TEXT"), **col_info}
                    for col_name, col_info in columns_dict.items()
                ]
                logger.info(f"Normalized dict schema → {len(schema)} columns.")
            else:
                logger.error(f"Schema dict has no usable 'columns' key. Keys: {list(schema.keys())}")
                return {"columns_to_validate": [], "available_column_tools": {}}

        if not isinstance(schema, list):
            logger.error(f"Schema is not a list after normalization (got {type(schema).__name__}).")
            return {"columns_to_validate": [], "available_column_tools": {}}

        # ── Resolve selected columns (set once, used throughout this node) ──
        # selected_columns contains the *output/renamed* names chosen during
        # template matching.  column_mapping is original_name → renamed_name.
        selected_columns: Optional[List[str]] = state['data_source_info'].selected_columns
        column_mapping: Dict[str, str] = state['data_source_info'].column_mapping or {}

        # Build reverse map: renamed_name → original_name  (for tool executor)
        rev_map: Dict[str, str] = {alias: orig for orig, alias in column_mapping.items()}

        # Resolve the *original* column names that the SQL tool executor needs
        source_cols: List[str] = []
        if selected_columns:
            for c in selected_columns:
                source_cols.append(rev_map.get(c, c))  # fall back to name-as-is

        logger.info(
            f"  Column scope — selected={selected_columns}, "
            f"source_cols (original names)={source_cols}"
        )

        tool_executor = ValidationToolExecutor(None, target_table, source_cols)
        columns_to_validate = []
        available_column_tools = {}

        # ── Filter schema to only the selected columns ──
        # _setup_connection already filtered schema['columns'] dict, but that only
        # fires when the agent first connects.  We re-filter here defensively so
        # _prepare_column_validation is always self-consistent regardless of call order.
        if selected_columns:
            # Build a lookup set of *original* names that are in scope
            original_names_in_scope: set = set(source_cols)

            before = len(schema)
            schema = [
                c for c in schema
                if c.get("name") in original_names_in_scope
            ]
            logger.info(
                f"  Schema filtered: {before} → {len(schema)} columns "
                f"(selected_columns={selected_columns})"
            )
        
        for column in schema:
            if isinstance(column, dict) and "name" in column:
                col_name = column["name"]
                col_type = column.get("type", "TEXT")
                columns_to_validate.append(column)
                available_column_tools[col_name] = tool_executor.get_available_tools(col_name, col_type)

        logger.info(f"Prepared {len(columns_to_validate)} columns for validation")
        
        # ── Optional: Cloud Model Batching ──
        from app.core.config import get_settings
        settings = get_settings()
        provider = settings.LLM_PROVIDER.strip().lower()
        is_cloud = provider in ("gemini", "groq", "openrouter", "openai", "anthropic")
        
        batch_results = []
        if is_cloud and columns_to_validate and mode_str != "schema_only":
            logger.info("☁️ Cloud provider detected. Running Batch LLM Analysis for all columns simultaneously...")
            
            sample_data_list = state['data_source_info'].sample_data or []
            column_mapping = state['data_source_info'].column_mapping or {}
            for col in columns_to_validate:
                col_name = col["name"]
                display_name = column_mapping.get(col_name, col_name)
                seen = set()
                samples = []
                for row in sample_data_list:
                    v = row.get(col_name)
                    sv = str(v) if v is not None else None
                    if sv and sv not in seen:
                        seen.add(sv)
                        samples.append(v)
                    if len(samples) >= 10: break
                schema_data[col_name] = {
                    "dtype": col.get("type", "TEXT"), 
                    "samples": samples,
                    "display_name": display_name
                }
                
            ds_info = state["data_source_info"]
            llm_connector = self._get_connector(
                ds_info.source_type,
                ds_info.connection_config,
                selected_columns=ds_info.selected_columns,
                column_mapping=ds_info.column_mapping,
                slice_filters=ds_info.slice_filters
            )
            await llm_connector.connect()

            async def query_executor_batch(query: str, q_type: str = "sql"):
                return await llm_connector.execute_raw_query(query, q_type)
            
            batch_agent = BatchColumnAnalysisAgent(
                schema_data=schema_data,
                mode=mode_str,
                engine_executor=query_executor_batch,
                source_type=source_type
            )
            
            reports = await batch_agent.analyze(target_table)
            total_rows = state["data_source_info"].row_count or 1000
            
            for col_nm, report in reports.items():
                for llm_rule in report.validation_results:
                    failed = llm_rule.get("failed_count", 0)
                    failure_pct = round((failed / total_rows) * 100, 2) if total_rows else 0
                    batch_results.append(ValidationResult(
                        rule_id=f"batch_llm_{col_nm}_{len(batch_results)}",
                        rule_name=llm_rule.get("rule_name", f"{col_nm}_batch_check"),
                        status=llm_rule.get("status", "passed"),
                        passed_count=max(0, total_rows - failed),
                        failed_count=failed,
                        failure_examples=llm_rule.get("failure_examples", [])[:5],
                        failure_percentage=failure_pct,
                        severity=llm_rule.get("severity", "warning"),
                        rule_type="llm_generated_sql",
                        executed_query=llm_rule.get("executed_query", ""),
                        column_name=col_nm,
                        check_origin="llm_generated",
                        agent_reasoning=f"Batch AI analysis across all columns in '{mode_str}' mode.",
                        agent_comprehension=f"Query returned {failed} failing row(s) out of {total_rows}.",
                    ))
                    
            try:
                await llm_connector.disconnect()
            except Exception:
                pass
                
            logger.info(f"✅ Batch LLM Analysis complete. Generated {len(batch_results)} custom rules.")
            
        # Append batch results into current state validation_results
        current_results = list(state.get("validation_results", []))
        if batch_results:
            current_results.extend(batch_results)

        return {
            "validation_results": current_results,
            "columns_to_validate": columns_to_validate,
            "available_column_tools": available_column_tools,
            "current_column_index": 0,
            "current_step": "validate_column",
            "status": AgentStatus.VALIDATING,
            "messages": [{"role": "system", "content": f"Starting validation for {len(columns_to_validate)} columns."}],
            "validation_steps": state.get("validation_steps", 0),
        }

    async def _validate_column(self, state: AgentState) -> Dict[str, Any]:
        """
        DETERMINISTIC BATCH VALIDATOR.
        Processes ONE full column per call — no LLM ReAct loop.
        Generates 5-15 rules from Python logic (mode-aware), executes them all,
        stores results tagged with column_name for grouped dashboard display.
        """
        logger.info("=" * 60)
        logger.info("🔍 STEP 5: Validating Column (deterministic batch mode)...")

        current_column_idx = state.get("current_column_index", 0)
        columns_to_validate  = state.get("columns_to_validate", [])
        validation_steps     = state.get("validation_steps", 0) + 1

        if current_column_idx >= len(columns_to_validate):
            return {
                "validation_steps": validation_steps,
                "messages": [{"role": "system", "content": "COLUMN_COMPLETE:all_done"}],
            }

        col_meta   = columns_to_validate[current_column_idx]
        col_name   = col_meta["name"]
        column_mapping = state['data_source_info'].column_mapping or {}
        display_name = column_mapping.get(col_name, col_name)
        
        col_type   = col_meta.get("type", "TEXT")
        table_name = state["data_source_info"].target_path
        mode       = state["validation_mode"].value if hasattr(state["validation_mode"], "value") else str(state["validation_mode"])
        total_rows = state["data_source_info"].row_count or 1000

        logger.info(f"  Column {current_column_idx+1}/{len(columns_to_validate)}: '{col_name}' (Display: '{display_name}') ({col_type}) | mode={mode}")

        # ── Sample values for this column ─────────────────────────────
        sample_data = state["data_source_info"].sample_data or []
        sample_values: List[Any] = []
        seen: set = set()
        for row in sample_data:
            v = row.get(col_name)
            sv = str(v) if v is not None else None
            if sv and sv not in seen:
                seen.add(sv)
                sample_values.append(v)
            if len(sample_values) >= 30:
                break

        # ── Generate deterministic rules (always) ─────────────────────
        rules = self._build_rules(col_name, col_type, table_name, col_meta, mode, total_rows, sample_values)
        logger.info(f"  → {len(rules)} rules generated")

        # ── Execute all rules ──────────────────────────────────────────
        ds_info = state["data_source_info"]
        connector = self._get_connector(
            ds_info.source_type,
            ds_info.connection_config,
            selected_columns=ds_info.selected_columns,
            column_mapping=ds_info.column_mapping,
            slice_filters=ds_info.slice_filters
        )
        await connector.connect(resource_path=table_name)

        current_results = list(state.get("validation_results", []))
        existing_names = {
            (r.get("rule_name") if isinstance(r, dict) else r.rule_name)
            for r in current_results
        }
        sev_map = {"high": "critical", "critical": "critical",
                   "medium": "warning", "warning": "warning",
                   "low": "info",      "info":     "info"}

        for rule in rules:
            query     = rule.get("query", "").strip()
            rule_name = rule.get("rule_name", f"{col_name}_check")
            severity  = sev_map.get(rule.get("severity", "warning").lower(), "warning")

            if not query or rule_name in existing_names:
                continue

            try:
                result = await connector.execute_raw_query(query, "sql")
            except Exception as exc:
                logger.error(f"  Rule '{rule_name}' failed: {exc}")
                continue

            if result.get("status") == "error":
                logger.warning(f"  Rule '{rule_name}' error: {result.get('error')}")
                continue

            # BUG-1 FIX: COUNT(*) returns 1 row — read the scalar, not row_count
            sample_rows = result.get("sample_rows", [])
            failed_count = 0
            if sample_rows and isinstance(sample_rows[0], dict):
                first_val = next(iter(sample_rows[0].values()), None)
                try:
                    failed_count = int(first_val) if first_val is not None else 0
                except (ValueError, TypeError):
                    failed_count = 0
            elif not sample_rows:
                failed_count = result.get("row_count", 0)

            failure_pct = round((failed_count / total_rows) * 100, 2) if total_rows else 0

            res = ValidationResult(
                rule_id=f"rule_{current_column_idx}_{len(current_results)}",
                rule_name=rule_name,
                status="failed" if failed_count > 0 else "passed",
                passed_count=max(0, total_rows - failed_count),
                failed_count=failed_count,
                failure_examples=sample_rows[:5],
                failure_percentage=failure_pct,
                severity=severity,
                rule_type="deterministic_sql",
                executed_query=query,
                column_name=display_name,  # Use display name for grouped dashboard display
                internal_column=col_name,  # Keep original name internally
                check_origin="pre_built",
                agent_reasoning=DataQualityAgent._describe_check(rule_name, display_name, col_type, query),
                agent_comprehension=f"Query returned {failed_count} failing row(s) out of {total_rows}. {'PASS — no issues detected.' if failed_count == 0 else f'FAIL — {failure_pct}% of rows violated this rule.'}",
            )
            current_results.append(res)
            existing_names.add(rule_name)
            status_str = f"FAIL ({failed_count})" if failed_count else "PASS"
            logger.info(f"  ✓ {rule_name}: {status_str}")

        try:
            await connector.disconnect()
        except Exception:
            pass

        # ── LLM-generated custom checks (mode-aware) ──────────────────
        from app.core.config import get_settings
        settings = get_settings()
        provider = settings.LLM_PROVIDER.strip().lower()
        is_cloud = provider in ("gemini", "groq", "openrouter", "openai", "anthropic")
        
        if is_cloud and mode != "schema_only":
            logger.info(f"  🤖 Skipping single-column LLM analysis for '{col_name}' (handled by Cloud Batch).")
        else:
            try:
                source_type = str(getattr(state['data_source_info'], 'source_type', 'sqlite')).lower()
                logger.info(f"  🤖 Starting SINGLE LLM analysis for column '{col_name}' in mode='{mode}'...")

                # BUG 6 FIX: retrieve_context() was never called anywhere. The agent had no memory
                # of what exploration found — metadata was saved to RAG but immediately forgotten.
                rag_context = ""
                try:
                    rag_svc = await get_rag_service()
                    ctx_items = await rag_svc.retrieve_context(
                        query=f"column {col_name} {col_type} data quality rules {table_name}",
                        source_ids=[table_name],
                        top_k=3,
                    )
                    if ctx_items:
                        rag_context = "\n\n".join(c["content"] for c in ctx_items)
                        logger.info(f"  🧠 RAG: {len(ctx_items)} context chunk(s) retrieved for '{col_name}'")
                except Exception as rag_err:
                    logger.warning(f"  RAG retrieval failed for '{col_name}': {rag_err}")

                llm_connector = self._get_connector(
                    state["data_source_info"].source_type,
                    state["data_source_info"].connection_config,
                )
                await llm_connector.connect(resource_path=table_name)

                async def query_executor(query: str, q_type: str = "sql"):
                    return await llm_connector.execute_raw_query(query, q_type)

                col_agent = ColumnAnalysisAgent(
                    column_name=col_name,
                    dtype=col_type,
                    samples=sample_values,
                    mode=mode,
                    engine_executor=query_executor,
                    source_type=source_type,
                    rag_context=rag_context,  # BUG 6 FIX: pass retrieved context into LLM prompt
                    existing_rule_names=list(existing_names),  # Prevent LLM from duplicating pre-built rules
                    display_name=display_name, # Pass display name
                )

                report = await col_agent.analyze(table_name)
                logger.info(f"  🤖 LLM generated {len(report.validation_results)} extra rules for '{col_name}'")

                for llm_rule in report.validation_results:
                    rname = llm_rule.get("rule_name", f"{col_name}_llm_check")
                    if rname in existing_names:
                        continue

                    failed = llm_rule.get("failed_count", 0)
                    failure_pct = round((failed / total_rows) * 100, 2) if total_rows else 0
                    sev = sev_map.get(llm_rule.get("severity", "warning").lower(), "warning")

                    current_results.append(ValidationResult(
                        rule_id=f"llm_{current_column_idx}_{len(current_results)}",
                        rule_name=rname,
                        status=llm_rule.get("status", "passed"),
                        passed_count=max(0, total_rows - failed),
                        failed_count=failed,
                        failure_examples=llm_rule.get("failure_examples", [])[:5],
                        failure_percentage=failure_pct,
                        severity=sev,
                        rule_type="llm_generated_sql",
                        executed_query=llm_rule.get("executed_query", ""),
                        column_name=display_name,
                        internal_column=col_name,
                        check_origin="llm_generated",
                        agent_reasoning=f"LLM analyzed column '{display_name}' ({col_type}) in '{mode}' mode and generated this custom check: {rname}.",
                        agent_comprehension=f"Query returned {failed} failing row(s) out of {total_rows}. {'PASS — no issues detected.' if failed == 0 else f'FAIL — {failure_pct}% of rows violated this rule.'}",
                    ))
                    existing_names.add(rname)
                    status_str = f"FAIL ({failed})" if failed else "PASS"
                    logger.info(f"  🤖 {rname}: {status_str}")

                try:
                    await llm_connector.disconnect()
                except Exception:
                    pass

            except Exception as e:
                logger.warning(f"  LLM column analysis failed for '{col_name}': {e}")

        det_count = len(rules)
        llm_count = len(current_results) - (len(state.get('validation_results', [])) + det_count)
        return {
            "validation_results": current_results,
            "validation_steps": validation_steps,
            "messages": [{"role": "system", "content": f"COLUMN_COMPLETE:{col_name} ({det_count} pre-built + {max(0, llm_count)} LLM rules)"}],
        }

    # ─────────────────────────────────────────────────────────────────────
    # CHECK REASONING — specific per check type
    # ─────────────────────────────────────────────────────────────────────
    @staticmethod
    def _describe_check(rule_name: str, col_name: str, col_type: str, query: str) -> str:
        """Generate specific reasoning for pre-built checks based on the check type."""
        rn = rule_name.lower()
        col_desc = f"column '{col_name}' (type: {col_type})"

        if "null_check" in rn:
            return f"NULL check for {col_desc}. NULLs can break aggregations, JOINs, and downstream analytics. This check counts rows where the value is missing (IS NULL). Selected because every column should be checked for completeness."
        if "negative_check" in rn:
            return f"Negative value check for {col_desc}. Detects rows with values below zero, which may indicate data entry errors, refunds, or invalid calculations. Selected because numeric columns should be validated for sign correctness."
        if "zero_value_check" in rn:
            return f"Zero value check for {col_desc}. Identifies rows where the value is exactly 0. Depending on the domain, zeros may indicate missing data, default values, or intentional entries. Selected for numeric columns."
        if "pk_uniqueness" in rn:
            return f"Primary key uniqueness check for {col_desc}. Ensures every row has a distinct value — a fundamental requirement for primary keys. Duplicate IDs cause JOIN ambiguity and data integrity violations."
        if "empty_string" in rn:
            return f"Empty string check for {col_desc}. Detects rows that appear populated but contain empty text (''). Unlike NULLs, empty strings pass NOT NULL constraints but carry no information."
        if "whitespace_padding" in rn:
            return f"Whitespace padding check for {col_desc}. Detects leading/trailing spaces that cause silent JOIN failures, duplicate entries, and display issues. Common in user-entered data."
        if "format_no_at" in rn:
            return f"Email '@' symbol check for {col_desc}. Every valid email address must contain exactly one '@' character. This catches truncated, placeholder, or incorrectly formatted email values."
        if "format_no_domain" in rn:
            return f"Email domain check for {col_desc}. Valid emails must have a domain after '@' containing a dot (e.g. @example.com). Catches entries like 'user@localhost' or 'user@domain' without a TLD."
        if "duplicate_check" in rn:
            return f"Duplicate value check for {col_desc}. Identifies non-unique values that may indicate data quality issues — duplicate records, merge failures, or insufficient deduplication."
        if "too_short" in rn:
            return f"Minimum length check for {col_desc}. Detects suspiciously short text values (1 character) that likely represent data entry errors, abbreviations, or placeholder values."
        if "too_long" in rn:
            return f"Maximum length check for {col_desc}. Detects unusually long values that may indicate data concatenation errors or lack of input validation."
        if "invalid_value" in rn:
            return f"Valid values check for {col_desc}. Compares entries against known valid categories derived from the data distribution. Catches misspellings, unexpected categories, and data entry errors."
        if "future_date" in rn:
            return f"Future date check for {col_desc}. Detects dates set in the future, which may indicate data entry errors, timezone issues, or test data. Selected because dates should generally be in the past."
        if "ancient_date" in rn:
            return f"Ancient date check for {col_desc}. Detects dates before 1900, which likely indicate parsing errors, default values (1970-01-01), or invalid data."
        if "impossible_age" in rn or "extreme_age" in rn:
            return f"Age plausibility check for {col_desc}. Ensures birth dates produce realistic ages (0-120 years). Catches impossible or extreme values from data entry errors."
        if "non_numeric" in rn:
            return f"Non-numeric character check for {col_desc}. Detects values containing letters or special characters in fields that should be purely numeric (e.g. phone numbers)."
        if "country" in rn and "cardinality" in rn:
            return f"Country cardinality check for {col_desc}. Validates that the number of distinct country values falls within expected bounds. Too few or too many may indicate data issues."

        # Fallback
        return f"Pre-built validation rule '{rule_name}' for {col_desc}. Auto-selected based on data type and column name semantics to catch common data quality issues. Query: {query[:100]}..."

    # ─────────────────────────────────────────────────────────────────────
    # DETERMINISTIC RULE BUILDER — the core engine
    # ─────────────────────────────────────────────────────────────────────
    @staticmethod
    def _build_rules(
        col_name: str, col_type: str, table_name: str,
        col_meta: Dict, mode: str, total_rows: int,
        sample_values: List[Any],
    ) -> List[Dict[str, Any]]:
        """
        Generate a comprehensive set of SQL validation rules based on:
        - Column data type (INTEGER, REAL, TEXT, TIMESTAMP, etc.)
        - Column name semantics (email, phone, price, id, status, etc.)
        - Validation mode (schema_only → structural only; business_analysis → full checks)
        - Sample values (for categorical valid-values check)

        All queries return COUNT of failing rows. 0 = passed.
        """
        rules: List[Dict[str, Any]] = []
        T  = f'"{table_name}"'
        C  = f'"{col_name}"'
        t  = col_type.upper()
        cn = col_name.lower()

        null_count   = col_meta.get("null_count", 0)
        unique_count = col_meta.get("unique_count", 0)
        # BUG 8 FIX: connectors set 'nullable' (bool), not 'null_percent' (float).
        # null_percent was always missing → null_pct = 0 → every column got 'info' severity.
        is_nullable  = col_meta.get("nullable", True)

        def rule(name: str, severity: str, query: str) -> Dict:
            return {"rule_name": f"{col_name}_{name}", "severity": severity, "query": query}

        # ── Type flags ────────────────────────────────────────────────
        is_int   = any(s in t for s in ("INT", "INTEGER", "BIGINT", "SMALLINT"))
        is_num   = is_int or any(s in t for s in ("REAL", "FLOAT", "NUMERIC", "DOUBLE", "DECIMAL", "NUMBER"))
        is_text  = any(s in t for s in ("TEXT", "VARCHAR", "CHAR", "STRING", "NVARCHAR", "CLOB"))
        is_date  = any(s in t for s in ("DATE", "TIME", "TIMESTAMP", "DATETIME"))
        is_bool  = "BOOL" in t

        # ── Semantic flags from column name ────────────────────────────
        is_pk      = (unique_count == total_rows and null_count == 0 and unique_count > 0)
        is_id      = is_pk or any(k in cn for k in ("_id", "_key", "_code")) or cn == "id"
        is_email   = "email" in cn
        is_phone   = any(k in cn for k in ("phone", "mobile", "cell", "tel"))
        is_price   = any(k in cn for k in ("price", "amount", "total", "cost", "revenue", "fee", "salary", "value", "lifetime", "balance"))
        is_qty     = any(k in cn for k in ("qty", "quantity", "units", "count", "num_"))
        is_name    = any(k in cn for k in ("first_name", "last_name", "full_name", "username", "firstname", "lastname"))
        is_dob     = any(k in cn for k in ("birth", "dob", "born", "birthday"))
        is_created = any(k in cn for k in ("created", "updated", "modified", "registered", "created_at", "updated_at"))
        is_status  = any(k in cn for k in ("status", "state", "stage", "tier", "type", "category", "channel", "source"))
        is_zip     = any(k in cn for k in ("zip", "postal", "postcode"))
        is_addr    = any(k in cn for k in ("address", "street", "city", "country"))
        is_cat     = (0 < unique_count <= 25) and (total_rows > unique_count) and not is_id

        do_biz = mode in ("business_analysis", "hybrid", "ai_recommended")
        do_schema = True  # always

        # ════════════════════════════════════════════════════════════
        # 1. NULL CHECK — every column
        # ════════════════════════════════════════════════════════════
        # BUG 8 FIX: use nullable flag — not null_pct (which was always 0)
        null_sev = "critical" if is_pk else ("warning" if not is_nullable else "info")
        rules.append(rule("null_check", null_sev,
            f"SELECT COUNT(*) AS null_cnt FROM {T} WHERE {C} IS NULL"))

        # ════════════════════════════════════════════════════════════
        # 2. NUMERIC COLUMNS
        # ════════════════════════════════════════════════════════════
        if is_num:
            # Negative values
            neg_sev = "critical" if (is_price or is_id or is_qty) else "warning"
            rules.append(rule("negative_check", neg_sev,
                f"SELECT COUNT(*) AS neg_cnt FROM {T} WHERE {C} < 0"))

            # Zero values for IDs and amounts
            if is_price or is_id:
                rules.append(rule("zero_value_check", "warning",
                    f"SELECT COUNT(*) AS zero_cnt FROM {T} WHERE {C} = 0"))

            # PK uniqueness
            if is_pk and is_int:
                rules.append(rule("pk_uniqueness_check", "critical",
                    f"SELECT COUNT(*) - COUNT(DISTINCT {C}) AS dup_cnt FROM {T}"))
            # Duplicate non-PK id columns
            if is_id and not is_pk:
                rules.append(rule("duplicate_id_check", "critical",
                    f"SELECT COUNT(*) AS dup_cnt FROM (SELECT {C} FROM {T} WHERE {C} IS NOT NULL GROUP BY {C} HAVING COUNT(*) > 1)"))

            if do_biz:
                # BUG 10 FIX: old rule used WHERE col > 0 in both the scan and the subquery,
                # so negative outliers (e.g. price = -50000) were never caught, and the average
                # excluded negatives making the threshold meaningless for mixed-sign data.
                # Replaced with 3-sigma: any value more than 3 standard deviations from the mean.
                if not is_id:
                    rules.append(rule("outlier_extreme_check", "info",
                        f"SELECT COUNT(*) AS outlier_cnt FROM {T} "
                        f"CROSS JOIN (SELECT AVG({C}) AS mu, "
                        f"(AVG({C}*{C}) - AVG({C})*AVG({C})) AS var2 "
                        f"FROM {T} WHERE {C} IS NOT NULL) s "
                        f"WHERE {C} IS NOT NULL AND ({C} - s.mu) * ({C} - s.mu) > 9.0 * MAX(s.var2, 0.001)"))

                if is_qty:
                    rules.append(rule("extreme_quantity_check", "warning",
                        f"SELECT COUNT(*) AS extreme_cnt FROM {T} WHERE {C} > 99999"))
                if is_price:
                    rules.append(rule("extreme_amount_check", "warning",
                        f"SELECT COUNT(*) AS extreme_cnt FROM {T} WHERE {C} > 10000000"))

        # ════════════════════════════════════════════════════════════
        # 3. TEXT COLUMNS
        # ════════════════════════════════════════════════════════════
        if is_text or is_email or is_phone or is_name or is_status or is_addr or is_zip:
            # Empty string
            rules.append(rule("empty_string_check", "warning",
                f"SELECT COUNT(*) AS empty_cnt FROM {T} WHERE {C} IS NOT NULL AND TRIM({C}) = ''"))

            # Whitespace padding
            rules.append(rule("whitespace_padding_check", "info",
                f"SELECT COUNT(*) AS padded_cnt FROM {T} WHERE {C} IS NOT NULL AND TRIM({C}) != {C}"))

            if is_email:
                rules.append(rule("format_no_at_check", "critical",
                    f"SELECT COUNT(*) AS no_at FROM {T} WHERE {C} IS NOT NULL AND {C} NOT LIKE '%@%'"))
                rules.append(rule("format_no_domain_check", "critical",
                    f"SELECT COUNT(*) AS no_domain FROM {T} WHERE {C} IS NOT NULL AND {C} NOT LIKE '%@%.%'"))
                rules.append(rule("duplicate_check", "warning",
                    f"SELECT COUNT(*) - COUNT(DISTINCT {C}) AS dup_email FROM {T}"))
                if do_biz:
                    rules.append(rule("test_domain_check", "info",
                        f"SELECT COUNT(*) AS test_cnt FROM {T} WHERE {C} IS NOT NULL "
                        f"AND ({C} LIKE '%@test.%' OR {C} LIKE '%@example.%' OR {C} LIKE '%@placeholder.%')"))
                    rules.append(rule("typo_domain_check", "warning",
                        f"SELECT COUNT(*) AS typo_cnt FROM {T} WHERE {C} IS NOT NULL "
                        f"AND ({C} LIKE '%@gmil.%' OR {C} LIKE '%@yaho.%' OR {C} LIKE '%@hotmial.%')"))

            if is_phone:
                rules.append(rule("too_short_check", "warning",
                    f"SELECT COUNT(*) AS short_cnt FROM {T} WHERE {C} IS NOT NULL AND LENGTH(TRIM({C})) < 7"))
                rules.append(rule("too_long_check", "info",
                    f"SELECT COUNT(*) AS long_cnt FROM {T} WHERE {C} IS NOT NULL AND LENGTH(TRIM({C})) > 20"))
                rules.append(rule("non_numeric_check", "warning",
                    f"SELECT COUNT(*) AS non_num FROM {T} WHERE {C} IS NOT NULL "
                    f"AND REPLACE(REPLACE(REPLACE({C}, '+', ''), '-', ''), ' ', '') GLOB '*[!0-9]*'"))

            if is_name:
                rules.append(rule("too_short_check", "info",
                    f"SELECT COUNT(*) AS short_cnt FROM {T} WHERE {C} IS NOT NULL AND LENGTH(TRIM({C})) < 2"))
                if do_biz:
                    rules.append(rule("digit_only_check", "warning",
                        f"SELECT COUNT(*) AS digit_cnt FROM {T} WHERE {C} IS NOT NULL AND {C} GLOB '[0-9]*'"))

            if is_zip:
                rules.append(rule("too_short_check", "warning",
                    f"SELECT COUNT(*) AS short_zip FROM {T} WHERE {C} IS NOT NULL AND LENGTH(TRIM({C})) < 4"))
                rules.append(rule("non_numeric_check", "info",
                    f"SELECT COUNT(*) AS alpha_zip FROM {T} WHERE {C} IS NOT NULL AND {C} GLOB '*[!0-9]*'"))

            if is_cat and sample_values:
                distinct = list(dict.fromkeys(str(v) for v in sample_values if v is not None))[:15]
                if 2 <= len(distinct) <= 15:
                    in_list = ", ".join(f"'{v.replace(chr(39), chr(39)*2)}'" for v in distinct)
                    rules.append(rule("invalid_value_check", "critical",
                        f"SELECT COUNT(*) AS invalid_cnt FROM {T} "
                        f"WHERE {C} IS NOT NULL AND {C} NOT IN ({in_list})"))
                if is_status and do_biz:
                    rules.append(rule("case_inconsistency_check", "info",
                        f"SELECT COUNT(*) AS mixed_case FROM {T} WHERE {C} IS NOT NULL "
                        f"AND {C} != UPPER({C}) AND {C} != LOWER({C}) AND {C} != TRIM({C})"))

            if do_biz:
                rules.append(rule("extreme_length_check", "info",
                    f"SELECT COUNT(*) AS too_long FROM {T} WHERE {C} IS NOT NULL AND LENGTH({C}) > 1000"))

        # ════════════════════════════════════════════════════════════
        # 4. DATE / TIMESTAMP COLUMNS
        # ════════════════════════════════════════════════════════════
        if is_date and not is_num:
            rules.append(rule("future_date_check", "critical" if is_dob else "warning",
                f"SELECT COUNT(*) AS future_cnt FROM {T} WHERE {C} > DATE('now')"))
            rules.append(rule("ancient_date_check", "warning",
                f"SELECT COUNT(*) AS ancient_cnt FROM {T} WHERE {C} < '1900-01-01'"))
            # BUG 7 FIX: null_check_date removed — it duplicated the null_check already added for
            # every column above (IS NULL), inflating passed rule counts and quality scores.

            if is_dob:
                rules.append(rule("impossible_age_check", "critical",
                    f"SELECT COUNT(*) AS impossible FROM {T} WHERE {C} > DATE('now', '-1 year')"))
                rules.append(rule("extreme_age_check", "warning",
                    f"SELECT COUNT(*) AS ancient FROM {T} WHERE {C} < DATE('now', '-130 years')"))

            if is_created and do_biz:
                rules.append(rule("future_created_check", "critical",
                    f"SELECT COUNT(*) AS fut_created FROM {T} WHERE {C} > DATETIME('now', '+1 hour')"))

        # ════════════════════════════════════════════════════════════
        # 5. BOOLEAN COLUMNS
        # ════════════════════════════════════════════════════════════
        if is_bool:
            rules.append(rule("invalid_bool_check", "critical",
                f"SELECT COUNT(*) AS bad_bool FROM {T} WHERE {C} IS NOT NULL AND {C} NOT IN (0, 1, 'true', 'false', 'True', 'False')"))

        return rules

    async def _advance_column(self, state: AgentState) -> Dict[str, Any]:
        """Advance to the next column for validation."""
        new_idx = state.get("current_column_index", 0) + 1
        cols    = state.get("columns_to_validate", [])

        if new_idx < len(cols):
            msg = f"Moved to column {new_idx + 1}/{len(cols)}: {cols[new_idx]['name']}"
        else:
            msg = "All columns completed."

        logger.info(f"⏭️  {msg}")
        return {
            "current_column_index": new_idx,
            "validation_steps": 0,
            "messages": [{"role": "system", "content": msg}],
        }


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
            table_name_rag = state['data_source_info'].target_path
            rag_chunks_saved = state.get("rag_chunks_saved", 0)
            if hasattr(rag_service, 'add_document'):
                await rag_service.add_document(
                    document_type="agent_metadata",
                    source_id=table_name_rag,
                    title=f"Metadata: {table_name_rag}",
                    content=metadata_text[:2000]
                )
                rag_chunks_saved += 1
            # BUG 6 FIX: add_schema_context was never called — schema was saved to state but
            # never written to RAG, so retrieve_context() during validation could not find it.
            ds_info = state['data_source_info']
            schema = ds_info.schema
            if schema and hasattr(rag_service, 'add_schema_context'):
                await rag_service.add_schema_context(
                    source_id=table_name_rag,
                    schema=schema if isinstance(schema, dict) else {"columns": {}},
                    sample_data=(ds_info.sample_data or [])[:10],
                    selected_columns=ds_info.selected_columns,
                    column_mapping=ds_info.column_mapping
                )
                rag_chunks_saved += 1
            logger.info(f"  💾 RAG: {rag_chunks_saved} chunk(s) saved for '{table_name_rag}'")
        except Exception as e:
            logger.warning(f"RAG save failed: {e}")
            rag_chunks_saved = state.get("rag_chunks_saved", 0)

        return {
            "retrieved_context": [{"content": metadata_text[:1500]}],
            "current_step": "validate_data",
            "status": AgentStatus.VALIDATING,
            "rag_chunks_saved": rag_chunks_saved,
            "messages": [{"role": "system", "content": "Metadata saved. Starting validation."}],
        }

    async def _generate_report(self, state: AgentState) -> Dict[str, Any]:
        """Generate final validation report with structured execution narrative."""
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
        
        def _g(o, k, d=None):
            return o.get(k, d) if isinstance(o, dict) else getattr(o, k, d)

        passed = sum(1 for r in results if _g(r, "status") == "passed")
        failed = sum(1 for r in results if _g(r, "status") == "failed")
        critical = sum(1 for r in results if _g(r, "status") == "failed" and _g(r, "severity") == "critical")

        # Calculate score using weighted approach
        score = 0.0
        if total > 0:
            per_rule = 100.0 / total
            for r in results:
                r_status = _g(r, "status")
                r_severity = _g(r, "severity")
                if r_status == "passed":
                    score += per_rule  # Full score
                elif r_severity == "warning":
                    score += per_rule * 0.3  # Partial credit for warnings
                # Critical failures get 0
            score = round(max(0.0, min(100.0, score)), 2)

        # ── Build structured execution narrative ──────────────────────
        tool_history = state.get("tool_execution_history", [])
        columns_validated = state.get("columns_to_validate", [])
        mode = str(state.get("validation_mode", "unknown"))

        execution_log_lines = [
            f"▶ Validation ID : {state.get('validation_id', 'N/A')}",
            f"▶ Mode           : {mode}",
            f"▶ Data source    : {state['data_source_info'].target_path}",
            f"▶ Started        : {state.get('started_at', 'N/A')}",
            f"▶ RAG chunks saved: {state.get('rag_chunks_saved', 0)}",
            "",
            "── PHASE 1: Exploration ──────────────────────────────────",
            f"  Tools executed  : {len(tool_history)}",
            f"  Exploration steps: {state.get('exploration_steps', 0)}",
        ]

        # Summarise which exploration tools ran
        tool_counts: Dict[str, int] = {}
        for t in tool_history:
            tid = t.get("tool_id", "unknown") if isinstance(t, dict) else str(t)
            tool_counts[tid] = tool_counts.get(tid, 0) + 1
        for tid, cnt in sorted(tool_counts.items()):
            execution_log_lines.append(f"    • {tid}: ×{cnt}")

        execution_log_lines += [
            "",
            "── PHASE 2: Column Validation ────────────────────────────",
            f"  Columns analysed: {len(columns_validated)}",
        ]

        # Per-column rule summary
        by_col: Dict[str, Dict[str, int]] = {}
        for r in results:
            col = _g(r, "column_name") or "table_level"
            if col not in by_col:
                by_col[col] = {"passed": 0, "failed": 0, "critical": 0}
            r_status = _g(r, "status")
            r_severity = _g(r, "severity")
            by_col[col]["passed" if r_status == "passed" else "failed"] += 1
            if r_status == "failed" and r_severity == "critical":
                by_col[col]["critical"] += 1
        for col, counts in by_col.items():
            flag = "⚠" if counts["critical"] else ("✗" if counts["failed"] else "✓")
            execution_log_lines.append(
                f"    {flag} {col}: {counts['passed']} passed, {counts['failed']} failed"
                + (f" ({counts['critical']} critical)" if counts["critical"] else "")
            )

        execution_log_lines += [
            "",
            "── PHASE 3: Results ──────────────────────────────────────",
            f"  Total rules     : {total}",
            f"  Passed          : {passed}",
            f"  Failed          : {failed}",
            f"  Critical fails  : {critical}",
            f"  Quality score   : {score:.2f}/100",
        ]

        # Top failed rules
        failed_rules = [r for r in results if _g(r, "status") == "failed"]
        failed_rules.sort(key=lambda r: (_g(r, "severity") != "critical", -_g(r, "failed_count", 0)))
        if failed_rules:
            execution_log_lines.append("")
            execution_log_lines.append("── TOP ISSUES ────────────────────────────────────────────")
            for r in failed_rules[:10]:
                r_severity = _g(r, "severity")
                r_rule_name = _g(r, "rule_name")
                r_failed_count = _g(r, "failed_count", 0)
                r_failure_percentage = _g(r, "failure_percentage", 0)
                sev_icon = "🔴" if r_severity == "critical" else "🟡"
                execution_log_lines.append(
                    f"  {sev_icon} {r_rule_name} [{r_severity}]: {r_failed_count} failing rows "
                    f"({r_failure_percentage:.1f}%)"
                )

        execution_log = "\n".join(execution_log_lines)
        logger.info("\n" + execution_log)

        # ── Mermaid diagram (best-effort) ─────────────────────────────
        mermaid_diagram = None
        try:
            from app.agents.mermaid_agent import MermaidDiagramAgent
            mermaid_agent = MermaidDiagramAgent(use_llm=False)  # deterministic, no extra LLM call
            diagram_result = await mermaid_agent.generate(state)
            mermaid_diagram = diagram_result.mermaid_code
            logger.info(f"  📊 Mermaid diagram: {diagram_result.node_count} nodes, {diagram_result.edge_count} edges")
        except Exception as mermaid_err:
            logger.warning(f"  Mermaid diagram generation failed (non-critical): {mermaid_err}")

        summary = {
            "quality_score": score,
            "total_rules": total,
            "passed_rules": passed,
            "failed_rules": failed,
            "critical_failures": critical,
            "agent_analysis": agent_narrative[:1000],  # Limit length
            "data_source": state['data_source_info'].target_path,
            "tools_executed": len(tool_history),
            "columns_validated": len(columns_validated),
            "execution_log": execution_log,
            "mermaid_diagram": mermaid_diagram,
        }

        return {
            "quality_score": score,
            "summary_report": summary,
            "status": AgentStatus.COMPLETED,
            "completed_at": datetime.utcnow().isoformat(),
        }


    def _derive_column_name(self, query: str) -> str:
        """Attempt to extract the primary column name from a SQL query."""
        # Simple regex for "column" or [column] or column
        match = re.search(r'["\[]?(\w+)["\]]?\s*(?:=|LIKE|GLOB|IN|IS)', query, re.IGNORECASE)
        if match:
            return match.group(1)
        return "unknown"

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
            "current_column_index": 0,
            "columns_to_validate": [],
            "available_column_tools": {},
            "tool_execution_history": [],
            "rag_chunks_saved": 0,  # Tracks how many RAG chunks were persisted during this run
        }

        result = await self.graph.ainvoke(
            initial_state,
            config={"recursion_limit": GRAPH_RECURSION_LIMIT}
        )

        logger.info("\n" + "🏁" * 30)
        logger.info(f"COMPLETE: {result.get('status')} | Score: {result.get('quality_score')}")
        logger.info("🏁" * 30 + "\n")

        return result

# ═══════════════════════════════════════════════════════════════
# SINGLETON
# ═══════════════════════════════════════════════════════════════
_agent: Optional[DataQualityAgent] = None

def get_data_quality_agent() -> DataQualityAgent:
    """Get agent singleton."""
    global _agent
    if _agent is None:
        _agent = DataQualityAgent()
    return _agent