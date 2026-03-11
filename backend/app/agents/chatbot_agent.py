"""Hybrid Chatbot Agent for Advanced Data Reasoning and Manipulation.
Combines RAG context with tool-based execution and dynamic command generation.
"""
import json
import logging
import re
from typing import Dict, Any, List, Optional, Union
from datetime import datetime
from langgraph.graph import StateGraph, END

from app.agents.state import (
    AgentState, AgentStatus, DataSourceInfo
)
from app.agents.llm_service import get_llm_service
from app.agents.rag_service import get_rag_service
from app.agents.llm_sanitizer import sanitize_llm_response
from app.connectors.factory import ConnectorFactory
from app.connectors.dataframe_connector import DuckDBFileConnector

_FILE_SOURCE_TYPES = {"local_file", "csv", "json", "jsonl", "excel", "parquet", "feather", "tsv"}

def _make_connector(
    source_type: str, 
    connection_config: dict,
    selected_columns: Optional[List[str]] = None,
    column_mapping: Optional[Dict[str, str]] = None,
    slice_filters: Optional[Dict[str, Any]] = None
):
    """Return DuckDBFileConnector for flat files, factory default otherwise."""
    if str(source_type).lower() in _FILE_SOURCE_TYPES:
        return DuckDBFileConnector(
            connection_config,
            selected_columns=selected_columns,
            column_mapping=column_mapping,
            slice_filters=slice_filters
        )
    return ConnectorFactory.create_connector(
        source_type, 
        connection_config,
        selected_columns=selected_columns,
        column_mapping=column_mapping,
        slice_filters=slice_filters
    )
from app.agents.tool_based_agent import ValidationToolExecutor
from app.agents.filter_discovery import DiscoveryManager, UserFilterSelection, UserPivotSelection
from app.agents.chart_engine import ChartEngine

logger = logging.getLogger(__name__)

# ═══════════════════════════════════════════════════════════════
# SYSTEM PROMPT
# ═══════════════════════════════════════════════════════════════

CHATBOT_SYSTEM_PROMPT = """You are an Advanced Data Quality Assistant. Your goal is to help users understand, analyze, and modify their data.
You have access to a RAG system containing schema information, validation history, and business rules.

CAPABILITIES:
1. REASONING: You can take complex requests and break them down into steps.
2. DATA EXPLORATION: Use tools to check row counts, sample data, and scanning columns.
3. DATA MANIPULATION: Apply filters and pivots to the data.
4. VISUALIZATION: Generate charts to represent data patterns.
5. SQL EXECUTION: Execute raw SQL queries if needed for custom analysis.

OUTPUT FORMAT:
- Use Markdown for your responses.
- For data previews, use Markdown Tables.
- If you need to call a tool, output a JSON block with "action" and "parameters".

TOOL CALLING FORMAT:
{"action": "execute_tools", "tool_selections": [{"tool_id": "table_row_count"}]}
{"action": "apply_filters", "filters": [{"column": "age", "filter_type": "greater_than", "min_value": 30}]}
{"action": "apply_pivot", "dimensions": ["region"], "measures": [{"column": "sales", "aggregation": "sum"}]}
{"action": "execute_query", "query": "SELECT COUNT(*) FROM table WHERE status = 'active'"}

RULES:
1. Always check RAG context first for schema details.
2. Favor tool-based exploration over raw SQL for standard checks.
3. If a user asks to modify data, explain what you are doing.
4. If you generate a chart or pivot, provide a clear interpretation.
"""

class ChatbotAgent:
    """Hybrid Chatbot Agent using LangGraph."""

    def __init__(self):
        self.llm_service = get_llm_service()
        self.graph = self._build_graph()

    def _build_graph(self) -> StateGraph:
        workflow = StateGraph(AgentState)

        workflow.add_node("retrieve_context", self._retrieve_context)
        workflow.add_node("reason", self._reason)
        workflow.add_node("execute_tools", self._execute_tools)
        workflow.add_node("format_response", self._format_response)

        workflow.set_entry_point("retrieve_context")
        workflow.add_edge("retrieve_context", "reason")
        
        workflow.add_conditional_edges(
            "reason",
            self._route_logic,
            {
                "execute": "execute_tools",
                "respond": "format_response",
                "end": END
            }
        )
        
        workflow.add_edge("execute_tools", "reason")
        workflow.add_edge("format_response", END)

        return workflow.compile()

    def _route_logic(self, state: AgentState) -> str:
        last_msg = state["messages"][-1]["content"]
        if '"action"' in last_msg:
            return "execute"
        return "respond"

    async def _retrieve_context(self, state: AgentState) -> Dict[str, Any]:
        """Fetch schema and RAG context."""
        query = state["messages"][-1]["content"]
        target = state["data_source_info"].target_path
        
        # Get rag service (async singleton)
        rag_service = await get_rag_service()
        context_items = await rag_service.retrieve_context(query, top_k=5)
        context = "\n".join([item["content"] for item in context_items]) if context_items else "No specific RAG context found."
        schema = state["data_source_info"].schema
        
        context_msg = f"CONTEXT FROM RAG:\n{context}\n\nSCHEMA:\n{json.dumps(schema, default=str)}"
        
        return {
            "messages": [{"role": "system", "content": context_msg}],
            "status": AgentStatus.ANALYZING
        }

    async def _reason(self, state: AgentState) -> Dict[str, Any]:
        """LLM reasoning step."""
        messages = state["messages"]
        if not messages:
            return {"messages": [{"role": "assistant", "content": "I'm sorry, I lost our conversation context."}]}
            
        last_msg = messages[-1]["content"]
        history = messages[:-1]
        
        response = await self.llm_service.generate(
            prompt=last_msg,
            chat_history=history,
            system_prompt=CHATBOT_SYSTEM_PROMPT,
            temperature=0.1
        )
        
        return {"messages": [{"role": "assistant", "content": response}]}

    async def _execute_tools(self, state: AgentState) -> Dict[str, Any]:
        """Execute the selected tools."""
        last_message = state["messages"][-1]["content"]
        
        # Parse JSON from response
        try:
            # Basic JSON extraction
            match = re.search(r'\{[\s\S]*\}', last_message)
            if not match:
                return {"messages": [{"role": "user", "content": "Error: Action block not found."}]}
            
            action_data = json.loads(match.group(0))
            action = action_data.get("action")
            
            ds_info = state['data_source_info']
            connector = _make_connector(
                ds_info.source_type,
                ds_info.connection_config,
                selected_columns=ds_info.selected_columns,
                column_mapping=ds_info.column_mapping,
                slice_filters=ds_info.slice_filters
            )
            await connector.connect()
            
            results = []
            
            if action == "execute_tools":
                # Resolve original names for executor
                ds_info = state['data_source_info']
                selected_columns = ds_info.selected_columns
                column_mapping = ds_info.column_mapping or {}
                rev_map = {alias: orig for orig, alias in column_mapping.items()}
                source_cols = [rev_map.get(c, c) for c in (selected_columns or [])]
                
                tool_executor = ValidationToolExecutor(connector, ds_info.target_path, source_cols or None)
                for sel in action_data.get("tool_selections", []):
                    res = await tool_executor.execute_tool(sel["tool_id"], column=sel.get("column"))
                    results.append(res.__dict__)
            
            elif action == "apply_filters":
                rows = await connector.sample_data(state['data_source_info'].target_path, sample_size=5000)
                import pandas as pd
                df = pd.DataFrame(rows)
                
                from app.agents.filter_discovery import DynamicFilterExecutor, UserFilterSelection
                executor = DynamicFilterExecutor()
                selections = [UserFilterSelection(**f) for f in action_data.get("filters", [])]
                filtered_df, log = executor.apply_filters(df, selections)
                
                results.append({
                    "action": "filters_applied",
                    "rows_before": len(df),
                    "rows_after": len(filtered_df),
                    "preview": filtered_df.head(10).to_dict(orient="records"),
                    "log": log
                })
                
            elif action == "apply_pivot":
                rows = await connector.sample_data(state['data_source_info'].target_path, sample_size=5000)
                import pandas as pd
                df = pd.DataFrame(rows)
                
                from app.agents.filter_discovery import DynamicPivotExecutor, UserPivotSelection
                executor = DynamicPivotExecutor()
                selection = UserPivotSelection(
                    dimensions=action_data.get("dimensions", []),
                    measures=action_data.get("measures", [])
                )
                pivoted_df = executor.apply_pivot(df, selection)
                
                # Charting support
                chart_engine = ChartEngine()
                chart_base64 = chart_engine.generate_pivot_chart(
                    pivoted_df, selection.dimensions, selection.measures
                )
                
                results.append({
                    "action": "pivot_applied",
                    "columns": list(pivoted_df.columns),
                    "rows": pivoted_df.head(20).to_dict(orient="records"),
                    "chart": chart_base64
                })

            elif action == "execute_query":
                query = action_data.get("query")
                res = await connector.execute_raw_query(query)
                results.append(res)

            await connector.disconnect()
            
            return {"messages": [{"role": "user", "content": f"TOOL RESULTS:\n{json.dumps(results, default=str)}"}]}

        except Exception as e:
            logger.exception("Chatbot tool execution failed")
            return {"messages": [{"role": "user", "content": f"Error during tool execution: {str(e)}"}]}

    async def _format_response(self, state: AgentState) -> Dict[str, Any]:
        """Final formatting (handled by reasoning or a dedicated node).
        For now, we just pass through as the LLM should have formatted it.
        """
        return {"status": AgentStatus.COMPLETED}

    async def run(self, source_info: DataSourceInfo, query: str, history: List[Dict] = None) -> Dict[str, Any]:
        """Entry point for the Chatbot Agent."""
        initial_state = AgentState(
            data_source_info=source_info,
            messages=history or [],
            status=AgentStatus.IDLE
        )
        
        # Add the new query
        initial_state["messages"].append({"role": "user", "content": query})
        
        final_state = await self.graph.ainvoke(initial_state)
        return {
            "response": final_state["messages"][-1]["content"],
            "history": final_state["messages"],
            "status": final_state["status"]
        }