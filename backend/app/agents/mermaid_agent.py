"""
Mermaid Diagram Agent
======================
Post-validation agent that synthesises the entire agent run
into a production-grade interactive Mermaid flowchart.

Understands every phase of the agentic workflow:
  1. Connection & Schema fetch
  2. Exploration (tool selection → execution → metadata)
  3. RAG save / retrieve
  4. Column-level validation (pre-built + LLM rules)
  5. Report generation

The diagram node represents decisions, branches, loops, and outcomes.
"""

import json
import logging
import re
from typing import Any, Dict, List, Optional
from dataclasses import dataclass

from app.agents.llm_service import get_llm_service

logger = logging.getLogger(__name__)


# ─── Result container ────────────────────────────────────────────────────────

@dataclass
class MermaidDiagramResult:
    """Output of MermaidDiagramAgent."""
    mermaid_code: str
    diagram_title: str
    node_count: int
    edge_count: int
    phases_covered: List[str]
    summary_stats: Dict[str, Any]


# ─── Agent ───────────────────────────────────────────────────────────────────

class MermaidDiagramAgent:
    """
    Generates a detailed Mermaid flowchart from a completed validation run.

    Strategy:
      1. Build deterministic skeleton from structured state data
         (guaranteed accurate, no hallucination risk).
      2. Ask LLM to enrich nodes with reasoning narratives and
         annotate decision branches.
      3. Merge into final Mermaid code.
    """

    _SYSTEM_PROMPT = """\
You are an expert at creating clear, informative Mermaid flowcharts for AI agent workflows.

Your task: annotate the provided Mermaid skeleton with SHORT (≤8 words) reasoning labels
on each node and decision edge. Do NOT change node IDs, do NOT add/remove nodes.
ONLY add/modify the display text inside brackets [] and edge labels with |-text-|.

Output ONLY the complete Mermaid code block, starting with `flowchart TD` and
ending after the last edge/style line.  No prose, no markdown fences, no explanation.

Rules:
- Keep all node IDs exactly as given.
- Replace placeholder LABEL text with concise, clear descriptions.
- For decision nodes {}, add meaningful Yes/No or conditional labels on edges.
- For style lines, keep them unchanged.
- Total output must remain valid Mermaid syntax.
"""

    def __init__(self, use_llm: bool = True):
        self.llm_service = get_llm_service()
        self.use_llm = use_llm

    async def generate(
        self,
        state: Dict[str, Any]
    ) -> MermaidDiagramResult:
        """Generate the full Mermaid diagram from AgentState."""
        
        # Unpack state
        validation_id = state.get("validation_id", "unknown")
        target_table = state["data_source_info"].target_path
        validation_mode = str(state.get("validation_mode", "unknown"))
        quality_score = state.get("quality_score", 0.0)
        if quality_score is None:
            quality_score = 0.0
            
        validation_results = state.get("validation_results", [])
        tool_execution_history = state.get("tool_execution_history", [])
        columns_to_validate = state.get("columns_to_validate", [])
        exploration_steps = state.get("exploration_steps", 0)
        rag_chunks_saved = state.get("rag_chunks_saved", 0)

        # Build deterministic skeleton
        skeleton, stats = self._build_skeleton(
            target_table=target_table,
            validation_mode=validation_mode,
            quality_score=quality_score,
            validation_results=validation_results,
            tool_execution_history=tool_execution_history,
            columns_to_validate=columns_to_validate,
            exploration_steps=exploration_steps,
            rag_chunks_saved=rag_chunks_saved,
        )

        # Enrich with LLM if requested
        enriched = skeleton
        if self.use_llm:
            try:
                user_prompt = (
                    f"Table: {target_table} | Mode: {validation_mode} | "
                    f"Score: {quality_score:.1f}/100\n\n"
                    f"Skeleton to annotate:\n{skeleton}"
                )
                raw = await self.llm_service.generate(
                    prompt=user_prompt,
                    system_prompt=self._SYSTEM_PROMPT,
                    max_tokens=3000,
                    temperature=0.05,
                )
                candidate = self._extract_mermaid(raw)
                if candidate and "flowchart" in candidate.lower():
                    enriched = candidate
                    logger.info("MermaidAgent: LLM enrichment applied.")
                else:
                    logger.warning("MermaidAgent: LLM output not valid Mermaid — using skeleton.")
            except Exception as exc:
                logger.warning(f"MermaidAgent: LLM call failed ({exc}) — using skeleton.")

        # ── 3. Count nodes / edges ─────────────────────────────────
        node_count = len(re.findall(r'^\s+\w+[\[\({]', enriched, re.MULTILINE))
        edge_count = len(re.findall(r'-->', enriched))

        return MermaidDiagramResult(
            mermaid_code=enriched,
            diagram_title=f"Agent Workflow: {target_table} ({validation_mode})",
            node_count=node_count,
            edge_count=edge_count,
            phases_covered=stats["phases"],
            summary_stats=stats,
        )

    # ─────────────────────────────────────────────────────────────────────
    # SKELETON BUILDER — deterministic, always accurate
    # ─────────────────────────────────────────────────────────────────────

    def _build_skeleton(
        self,
        target_table: str,
        validation_mode: str,
        quality_score: float,
        validation_results: List[Any],
        tool_execution_history: List[Dict],
        columns_to_validate: List[Dict],
        exploration_steps: int,
        rag_chunks_saved: int,
    ) -> tuple:
        """Build the base Mermaid diagram from structured state data."""

        # ── Aggregate stats ────────────────────────────────────────
        total_rules = len(validation_results)
        passed = sum(1 for r in validation_results if getattr(r, "status", "") == "passed")
        failed = sum(1 for r in validation_results if getattr(r, "status", "") == "failed")
        critical_fails = sum(
            1 for r in validation_results
            if getattr(r, "status", "") == "failed" and getattr(r, "severity", "") == "critical"
        )
        llm_rules = sum(1 for r in validation_results if getattr(r, "check_origin", "") == "llm_generated")
        prebuilt_rules = total_rules - llm_rules

        col_names = [c.get("name", f"col_{i}") for i, c in enumerate(columns_to_validate)]
        num_cols = len(col_names)
        tools_used = list({t.get("tool_id", "unknown") for t in tool_execution_history if isinstance(t, dict)})
        # Handle None score safely (already handled at start of generate, but keeping safe here too)
        q_score = quality_score if quality_score is not None else 0.0
        score_class = "pass" if q_score >= 85 else ("warn" if q_score >= 65 else "fail")

        phases = ["connection", "exploration", "rag", "column_validation", "report"]
        if exploration_steps == 0:
            phases.remove("exploration")

        # ── Column result summaries ────────────────────────────────
        col_result_map: Dict[str, Dict] = {}
        for r in validation_results:
            col = getattr(r, "column_name", None) or "unknown"
            if col not in col_result_map:
                col_result_map[col] = {"pass": 0, "fail": 0, "crit": 0}
            if getattr(r, "status", "") == "passed":
                col_result_map[col]["pass"] += 1
            else:
                col_result_map[col]["fail"] += 1
                if getattr(r, "severity", "") == "critical":
                    col_result_map[col]["crit"] += 1

        # ── Build Mermaid lines ─────────────────────────────────────
        lines: List[str] = [
            "flowchart TD",
            f'    %% Auto-generated diagram for validation: {target_table}',
            "",
            "    %% ═══ PHASE 1: SETUP ═══",
            f'    START([🚀 Start Validation]) --> CONNECT',
            f'    CONNECT["🔌 Connect to Database<br/>{_safe(target_table)}"] --> SCHEMA',
            f'    SCHEMA["📋 Fetch Schema<br/>{num_cols} columns detected"] --> CHECK_MODE',
            "",
            "    %% ═══ PHASE 2: MODE ROUTING ═══",
            f'    CHECK_MODE{{"🎯 Validation Mode<br/>{_safe(validation_mode)}"}}',
        ]

        # Mode routing
        mode_lower = validation_mode.lower()
        if "schema" in mode_lower:
            lines.append('    CHECK_MODE -->|Schema Only| SKIP_EXPLORE["⏭️ Skip Exploration<br/>Structural checks only"]')
            lines.append('    SKIP_EXPLORE --> SAVE_META')
        elif "custom" in mode_lower:
            lines.append('    CHECK_MODE -->|Custom Rules| SKIP_EXPLORE')
            lines.append('    SKIP_EXPLORE --> SAVE_META')
        else:
            lines.append('    CHECK_MODE -->|AI/Hybrid/Business| EXPLORE_START')
            lines += [
                "",
                "    %% ═══ PHASE 3: EXPLORATION ═══",
                f'    EXPLORE_START["🔍 Exploration Phase<br/>{exploration_steps} steps"] --> TOOL_SELECT',
                f'    TOOL_SELECT{{"🛠️ Agent Selects Tools<br/>{len(tools_used)} unique tools"}}',
            ]
            for i, tool in enumerate(tools_used[:6]):
                tid = f"TOOL_{i}"
                lines.append(f'    TOOL_SELECT --> {tid}["{_tool_icon(tool)} {tool}"]')
                lines.append(f'    {tid} --> TOOL_RESULT')
            lines += [
                f'    TOOL_RESULT["📊 Tool Results Aggregated"] --> CHECK_DONE',
                f'    CHECK_DONE{{"Complete?"}}',
                f'    CHECK_DONE -->|More tools needed| TOOL_SELECT',
                f'    CHECK_DONE -->|Done| META_GEN',
                f'    META_GEN["📝 Generate METADATA Report<br/>LLM synthesises findings"] --> SAVE_META',
            ]

        lines += [
            "",
            "    %% ═══ PHASE 4: RAG ═══",
            f'    SAVE_META["💾 Save to RAG Vector Store<br/>{rag_chunks_saved} chunk(s) stored"]',
            f'    SAVE_META --> COL_PREP',
        ]

        # Column validation loop
        lines += [
            "",
            "    %% ═══ PHASE 5: COLUMN VALIDATION ═══",
            f'    COL_PREP["⚙️ Prepare {num_cols} Columns<br/>Build tool catalog per column"] --> COL_LOOP',
            f'    COL_LOOP{{"📍 More Columns?"}}',
        ]

        # Render first N columns in detail, rest as summary
        detail_limit = min(num_cols, 4)
        for i, col_name in enumerate(col_names[:detail_limit]):
            cres = col_result_map.get(col_name, {})
            cfail = cres.get("fail", 0)
            cpass = cres.get("pass", 0)
            ccrit = cres.get("crit", 0)
            cid = f"COL_{i}"
            rag_id = f"RAG_{i}"
            pre_id = f"PRE_{i}"
            llm_id = f"LLM_{i}"
            res_id = f"RES_{i}"
            status_icon = "✅" if cfail == 0 else ("🔴" if ccrit > 0 else "⚠️")

            lines += [
                f'    COL_LOOP -->|Column {i+1}: {_safe(col_name)}| {cid}',
                f'    {cid}["🔬 Validate: {_safe(col_name)}<br/>type: {_col_type(col_name, columns_to_validate)}"]',
                f'    {cid} --> {rag_id}["🧠 RAG Context Retrieval<br/>Query for column patterns"]',
                f'    {rag_id} --> {pre_id}["⚙️ Pre-built Rules<br/>{cpass + cfail} rules executed"]',
                f'    {pre_id} --> {llm_id}["🤖 LLM Rules<br/>Custom semantic checks"]',
                f'    {llm_id} --> {res_id}["{status_icon} Result: {cpass}✓ {cfail}✗<br/>{"🔴 " + str(ccrit) + " critical" if ccrit else ""}"]',
                f'    {res_id} --> COL_LOOP',
            ]

        if num_cols > detail_limit:
            remaining = num_cols - detail_limit
            lines += [
                f'    COL_LOOP -->|Remaining {remaining} columns| COL_BATCH["📦 +{remaining} more columns<br/>validated same way"]',
                f'    COL_BATCH --> COL_LOOP',
            ]

        lines.append('    COL_LOOP -->|All done| REPORT')

        # Report phase
        q_score = quality_score if quality_score is not None else 0.0
        score_emoji = "🏆" if q_score >= 90 else ("📊" if q_score >= 70 else "⚠️")
        lines += [
            "",
            "    %% ═══ PHASE 6: REPORT ═══",
            f'    REPORT["📄 Generate Final Report"]',
            f'    REPORT --> SCORE["{score_emoji} Quality Score: {quality_score:.1f}/100<br/>{passed} passed · {failed} failed · {critical_fails} critical"]',
            f'    SCORE --> RULES_BREAKDOWN["📋 Rules Summary<br/>{prebuilt_rules} pre-built · {llm_rules} LLM-generated · {total_rules} total"]',
            f'    RULES_BREAKDOWN --> END([✅ Validation Complete])',
        ]

        # ── Styles ────────────────────────────────────────────────
        lines += [
            "",
            "    %% ═══ STYLES ═══",
            "    classDef startEnd fill:#6366f1,stroke:#4f46e5,color:#fff,rx:20",
            "    classDef phase fill:#1e293b,stroke:#334155,color:#e2e8f0",
            "    classDef decision fill:#0f172a,stroke:#6366f1,color:#a5b4fc",
            "    classDef tool fill:#064e3b,stroke:#059669,color:#d1fae5",
            "    classDef rag fill:#1e3a5f,stroke:#3b82f6,color:#bfdbfe",
            "    classDef result_pass fill:#052e16,stroke:#16a34a,color:#bbf7d0",
            "    classDef result_fail fill:#450a0a,stroke:#dc2626,color:#fca5a5",
            "    classDef score_pass fill:#134e4a,stroke:#14b8a6,color:#ccfbf1",
            "    classDef score_warn fill:#422006,stroke:#f59e0b,color:#fde68a",
            "    classDef score_fail fill:#450a0a,stroke:#dc2626,color:#fca5a5",
            "",
            "    class START,END startEnd",
            "    class CONNECT,SCHEMA,META_GEN,SAVE_META,COL_PREP,REPORT phase",
            "    class CHECK_MODE,COL_LOOP,CHECK_DONE decision",
        ]

        # Apply tool styles
        for i in range(len(tools_used[:6])):
            lines.append(f"    class TOOL_{i} tool")

        # Apply RAG styles
        for i in range(detail_limit):
            lines.append(f"    class RAG_{i} rag")

        # Apply result styles
        for i, col_name in enumerate(col_names[:detail_limit]):
            cres = col_result_map.get(col_name, {})
            style = "result_pass" if cres.get("fail", 0) == 0 else "result_fail"
            lines.append(f"    class RES_{i} {style}")

        score_style = f"score_{score_class}"
        lines.append(f"    class SCORE {score_style}")
        lines.append(f"    class RULES_BREAKDOWN phase")

        mermaid_code = "\n".join(lines)

        stats = {
            "phases": phases,
            "total_rules": total_rules,
            "passed": passed,
            "failed": failed,
            "critical_fails": critical_fails,
            "prebuilt_rules": prebuilt_rules,
            "llm_rules": llm_rules,
            "tools_used": tools_used,
            "columns": col_names,
            "quality_score": quality_score,
            "rag_chunks": rag_chunks_saved,
            "exploration_steps": exploration_steps,
        }

        return mermaid_code, stats

    @staticmethod
    def _extract_mermaid(raw: str) -> Optional[str]:
        """Extract Mermaid code from LLM response."""
        # Try fenced block
        m = re.search(r'```(?:mermaid)?\s*(flowchart[\s\S]+?)```', raw, re.IGNORECASE)
        if m:
            return m.group(1).strip()
        # Try raw flowchart
        m = re.search(r'(flowchart\s+TD[\s\S]+)', raw, re.IGNORECASE)
        if m:
            return m.group(1).strip()
        return None


# ─── Helpers ─────────────────────────────────────────────────────────────────

def _safe(text: str) -> str:
    """Make text safe for Mermaid node labels."""
    return text.replace('"', "'").replace('<', '&lt;').replace('>', '&gt;')[:40]


def _tool_icon(tool_id: str) -> str:
    icons = {
        "table_row_count": "🔢", "table_sample_rows": "👀",
        "table_duplicate_scan": "🔄", "table_null_scan": "❌",
        "table_empty_check": "📭", "table_schema_check": "📐",
        "universal_null_check": "❌", "universal_distinct_count": "🔢",
        "universal_sample_values": "📋", "int_negative_check": "➖",
        "currency_negative_check": "💸", "date_future_check": "📅",
    }
    return icons.get(tool_id, "🔧")


def _col_type(col_name: str, columns: List[Dict]) -> str:
    for c in columns:
        if c.get("name") == col_name:
            return c.get("type", "?")[:8]
    return "?"


# ─── Singleton ───────────────────────────────────────────────────────────────

_mermaid_agent: Optional[MermaidDiagramAgent] = None


def get_mermaid_agent() -> MermaidDiagramAgent:
    global _mermaid_agent
    if _mermaid_agent is None:
        _mermaid_agent = MermaidDiagramAgent()
    return _mermaid_agent
