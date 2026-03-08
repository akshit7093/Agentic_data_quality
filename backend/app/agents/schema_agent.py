"""
Schema Validation Agent — Deep Exhaustive Analysis Mode.

ARCHITECTURE UPGRADE v2:
  Phase 1 – Exploration:
    1. Run ALL pre-built TABLE_TOOLS first.
    2. For every column, run ALL applicable pre-built COLUMN_TOOLS (no mode-gating).
    3. After exhaustive tool coverage, ask LLM to generate ADDITIONAL custom SQL
       queries per column that go beyond the pre-built set.
    4. Execute those custom queries.
    5. Synthesise ALL results into a rich, comprehensive METADATA document via LLM.

  Phase 2 – Validation:
    1. For every column, run ALL pre-built deterministic rules (mode restriction lifted).
    2. ALWAYS run LLM (ColumnAnalysisAgent) for each column — even for schema_only.
    3. Rich metadata context is forwarded to the validation LLM.

  METADATA document structure:
    - Dataset overview + purpose
    - Sample rows
    - Per-column: type, nulls, unique count, range, sample values, patterns, anomalies
    - Cross-column relationships / potential FK
    - Critical data quality issues
    - Overall quality score estimate
"""

import json
import logging
import math
import re
from typing import Any, Dict, List, Optional

from app.agents.data_quality_agent import (
    DataQualityAgent,
    LLM_MAX_TOKENS,
    BATCH_SIZE,
)
from app.agents.state import AgentState, AgentStatus, ValidationMode
from app.agents.llm_sanitizer import sanitize_llm_response
from app.agents.tool_based_agent import (
    ValidationToolExecutor,
    TABLE_TOOLS,
    COLUMN_TOOLS,
    UNIVERSAL_TOOLS,
    DataType,
)
from app.agents.column_analysis_agent import ColumnAnalysisAgent
from app.connectors.factory import ConnectorFactory

logger = logging.getLogger(__name__)

# ═══════════════════════════════════════════════════════════════
# CONFIGURATION
# ═══════════════════════════════════════════════════════════════
MAX_SCHEMA_EXPLORATION_STEPS = 30   # Enough for large wide tables (up to ~80 cols)
SCHEMA_COLS_PER_BATCH        = 3    # Columns explored per step
SCHEMA_LLM_CUSTOM_STEPS      = 3    # Steps dedicated to LLM-custom SQL generation


# ═══════════════════════════════════════════════════════════════
# PROMPTS
# ═══════════════════════════════════════════════════════════════

# Drives the LLM-custom-query generation step (after all pre-built tools ran)
SCHEMA_CUSTOM_QUERY_PROMPT = """You are an expert Data Analyst generating ADDITIONAL SQL checks
for a column that go BEYOND the standard pre-built validation tools.

You have been given:
- The column name, data type, and sample values
- Results from standard pre-built tool checks already executed

Your task: write 2-4 ORIGINAL SQL queries that uncover insights NOT covered by:
  null checks, empty checks, length checks, format checks, range checks.

Focus on:
  - Semantic patterns (e.g. values that look like wrong category for this column)
  - Cross-row patterns (e.g. suspiciously uniform distribution)
  - Business-domain anomalies inferred from column name and samples
  - Encoding issues, mixed scripts, placeholder values

OUTPUT FORMAT — raw JSON array ONLY, no markdown:
[
  {
    "rule_name": "col_description_of_check",
    "severity": "critical|warning|info",
    "query": "SELECT COUNT(*) AS cnt FROM {table} WHERE {condition}",
    "rationale": "one-line explanation"
  }
]

RULES:
- Every query MUST return ONE integer (count of failing rows).  0 = pass.
- Use COUNT(*) only. Never SELECT *.
- Single-line strings. No literal newlines inside JSON strings.
- Replace {table} with the actual table name, and reference the column by name.
- Output ONLY the JSON array. Nothing else.
"""

# Drives final metadata synthesis (after ALL exploration data collected)
SCHEMA_METADATA_SYNTHESIS_PROMPT = """You are a Senior Data Engineer writing the definitive
documentation file for a dataset.  Based on the exploration results provided, produce a
COMPREHENSIVE, STRUCTURED metadata report that serves as the ultimate reference guide.

The report MUST follow this exact structure inside <METADATA> tags:

<METADATA>
# Dataset: {table_name}
## Overview
- **Purpose / Domain**: <infer from column names and data>
- **Total Rows**: <from exploration>
- **Total Columns**: <count>
- **Data Source Type**: <infer>
- **Overall Health**: <Excellent / Good / Fair / Poor — brief reason>

---
## Sample Data
<markdown table showing 5-10 representative rows>

---
## Column-by-Column Analysis

### `column_name` (TYPE)
- **Null Count**: N (X%)
- **Unique Values**: N
- **Min / Max / Avg**: (if numeric/date)
- **Top Sample Values**: val1, val2, val3 …
- **Detected Pattern / Format**: e.g. "ISO-8601 dates", "US phone numbers", "Free-text"
- **Business Meaning**: <infer from name + samples>
- **Data Quality Issues**:
  - ⚠️ issue 1
  - 🔴 issue 2 (if critical)
- **LLM-Detected Anomalies**: <from custom SQL results>

(repeat for every column)

---
## Cross-Column Observations
- Potential foreign keys / relationships
- Correlated columns
- Multi-column anomalies

---
## Critical Issues Summary
| Column | Issue | Severity |
|--------|-------|----------|
| ... | ... | ... |

---
## Data Quality Score Estimate
- **Estimated Score**: X / 100
- **Reasoning**: <brief>
</METADATA>

Write ONLY the content between <METADATA> tags. Be thorough — this is the primary reference document."""


# ═══════════════════════════════════════════════════════════════
# HELPER: Map a column's type/name → ALL applicable pre-built tool IDs
# ═══════════════════════════════════════════════════════════════

def get_all_column_tool_selections(col_name: str, col_type: str) -> List[Dict[str, Any]]:
    """
    Return ALL pre-built tool selections for a column:
      1. UNIVERSAL tools first (null, empty, distinct count, sample values, whitespace)
      2. ALL type-specific COLUMN_TOOLS

    Mode-agnostic — every applicable tool runs regardless of business vs. schema mode.
    This ensures null and empty checks ALWAYS fire for EVERY column.
    """
    sels: List[Dict[str, Any]] = []

    def add(*tool_ids: str) -> None:
        for tid in tool_ids:
            sels.append({"tool_id": tid, "column": col_name})

    # ── 1. UNIVERSAL CHECKS — run for every single column ────────────
    add(
        "universal_null_check",
        "universal_empty_check",
        "universal_distinct_count",
        "universal_sample_values",
        "universal_whitespace_padding",
    )

    # ── 2. TYPE-SPECIFIC CHECKS ───────────────────────────────────────
    t  = col_type.upper()
    cn = col_name.lower()

    is_email    = "email" in cn
    is_phone    = any(k in cn for k in ("phone", "mobile", "cell", "tel"))
    is_uuid     = "uuid" in cn or "guid" in cn
    is_url      = "url" in cn or "link" in cn or "website" in cn
    is_ip       = "ip_address" in cn or "ipaddress" in cn or "ip_addr" in cn
    is_postal   = any(k in cn for k in ("zip", "postal", "postcode"))
    is_country  = "country" in cn
    is_currency = any(k in cn for k in ("price", "amount", "cost", "revenue", "fee", "salary", "balance"))
    is_pct      = any(k in cn for k in ("percent", "pct", "rate", "ratio"))
    is_age      = "age" in cn
    is_json     = any(k in cn for k in ("json", "metadata", "payload"))
    is_text_col = any(k in cn for k in ("description", "notes", "comment", "body", "message", "bio"))

    is_int      = any(s in t for s in ("INT", "INTEGER", "BIGINT", "SMALLINT", "TINYINT"))
    is_float    = any(s in t for s in ("REAL", "FLOAT", "NUMERIC", "DOUBLE", "DECIMAL", "NUMBER"))
    is_datetime = any(s in t for s in ("DATETIME", "TIMESTAMP"))
    is_date     = "DATE" in t and not is_datetime
    is_bool     = any(s in t for s in ("BOOL", "BOOLEAN"))
    is_json_t   = "JSON" in t

    if is_email:
        add(
            "email_format_check", "email_no_at_check", "email_multiple_at_check",
            "email_duplicate_check", "email_domain_check", "email_typo_check",
            "email_test_domain_check", "email_length_check", "email_uppercase_check",
        )
    elif is_phone:
        add(
            "phone_format_check", "phone_length_check", "phone_placeholder_check",
            "phone_country_code_check", "phone_all_same_check", "phone_local_only_check",
        )
    elif is_uuid:
        add(
            "uuid_format_check", "uuid_uniqueness_check",
            "uuid_uppercase_check", "uuid_nil_check",
        )
    elif is_url:
        add(
            "url_format_check", "url_http_check", "url_localhost_check",
            "url_length_check", "url_sample_check",
        )
    elif is_ip:
        add(
            "ip_format_check", "ip_private_check",
            "ip_loopback_check", "ip_distribution",
        )
    elif is_postal:
        add(
            "postal_us_format_check", "postal_length_check",
            "postal_distribution", "postal_all_zeros_check",
        )
    elif is_country:
        add(
            "country_iso2_length_check", "country_distribution",
            "country_lowercase_check", "country_cardinality_check",
        )
    elif is_currency and (is_float or is_int):
        add(
            "currency_negative_check", "currency_zero_check", "currency_extreme_check",
            "currency_range_check", "currency_precision_check", "currency_symbol_check",
        )
    elif is_pct:
        add(
            "pct_range_check", "pct_decimal_range_check", "pct_stats_check",
        )
    elif is_age and is_int:
        add(
            "age_negative_check", "age_impossible_check", "age_range_check",
            "age_zero_check", "age_distribution_check",
        )
    elif is_json or is_json_t:
        add(
            "json_empty_check", "json_start_check",
            "json_length_check", "json_sample_check",
        )
    elif is_text_col:
        add(
            "text_very_short_check", "text_very_long_check",
            "text_length_stats", "text_html_check", "text_placeholder_check",
        )
    elif is_datetime:
        add(
            "datetime_future_check", "datetime_format_check",
            "datetime_timezone_check", "datetime_epoch_check",
            "datetime_range_check", "datetime_midnight_check",
        )
    elif is_date:
        add(
            "date_future_check", "date_past_check", "date_format_check",
            "date_range_check", "date_unix_epoch_check",
            "date_day_distribution", "date_month_distribution",
        )
    elif is_float:
        add(
            "float_negative_check", "float_zero_check", "float_range_check",
            "float_nan_check", "float_extreme_outlier_check",
            "float_negative_amount_check", "float_round_number_check",
        )
    elif is_int:
        add(
            "int_negative_check", "int_zero_check", "int_uniqueness_check",
            "int_range_check", "int_stddev_check", "int_extreme_outlier_check",
            "int_suspicious_seq_check", "int_pk_gap_check", "int_distribution_check",
        )
    elif is_bool:
        add(
            "bool_invalid_check", "bool_distribution_check", "bool_all_same_check",
        )
    else:
        # General string / text fallback
        add(
            "str_whitespace_padding", "str_min_length_check",
            "str_special_char_check", "str_control_char_check",
            "str_sample_values", "str_distinct_count",
            "str_placeholder_check", "str_mixed_case_check",
        )

    return sels


# ═══════════════════════════════════════════════════════════════
# MAIN AGENT
# ═══════════════════════════════════════════════════════════════

class SchemaValidationAgent(DataQualityAgent):
    """
    Extended DataQualityAgent for SCHEMA_ONLY validation mode.

    Key overrides:
      _route_exploration  — higher step limit for exhaustive column coverage
      _explore_data       — systematic multi-phase exploration (pre-built → custom)
      _save_metadata      — rich LLM-synthesised metadata document
      _build_rules        — all rules, mode-gating lifted
    """

    # ── Class-level prompt overrides ────────────────────────────────────
    # (exploration_prompt is used in the BASE _explore_data — we override
    #  _explore_data entirely, so this is just informational)
    exploration_prompt = (
        "You are a Schema Deep-Dive Agent. Run all pre-built tools, then generate "
        "custom SQL queries, then synthesise a comprehensive metadata report."
    )

    # ═══════════════════════════════════════════════════════════════
    # ROUTING OVERRIDE — higher step cap
    # ═══════════════════════════════════════════════════════════════

    def _route_exploration(self, state: AgentState) -> str:
        """Override with a higher step limit to accommodate wide table exploration."""
        exploration_steps = state.get("exploration_steps", 0)

        if exploration_steps >= MAX_SCHEMA_EXPLORATION_STEPS:
            logger.warning(
                f"Schema exploration: step cap ({MAX_SCHEMA_EXPLORATION_STEPS}) reached. "
                "Forcing metadata generation."
            )
            return "finished"

        if not state.get("messages"):
            return "execute_tools"

        last_content = state["messages"][-1]["content"]
        sanitized = self._sanitize_llm_response(last_content)

        if not sanitized:
            return "retry"

        has_metadata     = bool(re.search(r"<METADATA>[\s\S]+?</METADATA>", sanitized, re.IGNORECASE))
        has_pending_json = bool(re.search(r'"action"\s*:', sanitized))

        if has_metadata and has_pending_json:
            cleaned = re.sub(r"<METADATA>[\s\S]*?</METADATA>", "", sanitized, flags=re.IGNORECASE).strip()
            state["messages"][-1]["content"] = cleaned
            return "execute_tools"

        if has_metadata and not has_pending_json:
            return "finished"

        return "execute_tools"

    # ═══════════════════════════════════════════════════════════════
    # EXPLORATION OVERRIDE — systematic, exhaustive, multi-phase
    # ═══════════════════════════════════════════════════════════════

    async def _explore_data(self, state: AgentState) -> Dict[str, Any]:
        """
        Deterministic multi-phase exploration:

          Phase 1  (step 1)                    → Table-level tools
          Phase 2  (steps 2 … B+1)             → Column pre-built tools, B batches
          Phase 3  (steps B+2 … B+2+C-1)       → LLM custom queries per column
          Phase 4  (step B+2+C)                → LLM metadata synthesis → <METADATA>
        """
        exploration_steps = state.get("exploration_steps", 0) + 1
        target = state["data_source_info"].target_path

        # ── Resolve column list from schema ──────────────────────────────
        schema = state["data_source_info"].schema or {}
        cols_dict: Dict[str, Any] = {}
        if isinstance(schema, dict):
            cols_dict = schema.get("columns", {})
        elif isinstance(schema, list):
            cols_dict = {c["name"]: c for c in schema if isinstance(c, dict) and "name" in c}

        columns: List[tuple] = list(cols_dict.items())   # [(name, info), ...]
        n_cols = len(columns)
        batches = math.ceil(n_cols / SCHEMA_COLS_PER_BATCH) if n_cols else 0

        # ── Boundaries ───────────────────────────────────────────────────
        PHASE1_END   = 1                            # Table tools
        PHASE2_END   = PHASE1_END + batches         # Column tool batches
        PHASE3_END   = PHASE2_END + SCHEMA_LLM_CUSTOM_STEPS  # LLM custom queries

        logger.info(
            f"Schema explore step {exploration_steps} | "
            f"cols={n_cols} batches={batches} | "
            f"P1≤{PHASE1_END} P2≤{PHASE2_END} P3≤{PHASE3_END}"
        )

        # ══════════════════════════════════════════════════════════════
        # PHASE 1 — Table-level overview tools
        # ══════════════════════════════════════════════════════════════
        if exploration_steps <= PHASE1_END:
            tool_selections = [
                {"tool_id": "table_row_count"},
                {"tool_id": "table_sample_rows"},
                {"tool_id": "table_duplicate_scan"},
                {"tool_id": "table_empty_check"},
            ]
            # Also scan nulls for first few columns immediately
            for col_name, _ in columns[:4]:
                tool_selections.append({"tool_id": "table_null_scan", "column": col_name})

            logger.info("Phase 1: Running table-level overview tools")
            return self._emit_tool_selections(tool_selections, exploration_steps)

        # ══════════════════════════════════════════════════════════════
        # PHASE 2 — Per-column pre-built tools (batched)
        # ══════════════════════════════════════════════════════════════
        if exploration_steps <= PHASE2_END:
            batch_idx = exploration_steps - PHASE1_END - 1   # 0-indexed
            batch_start = batch_idx * SCHEMA_COLS_PER_BATCH
            batch = columns[batch_start : batch_start + SCHEMA_COLS_PER_BATCH]

            tool_selections: List[Dict[str, Any]] = []
            col_names_in_batch = []
            for col_name, col_info in batch:
                col_type = col_info.get("type", "TEXT") if isinstance(col_info, dict) else "TEXT"
                col_tools = get_all_column_tool_selections(col_name, col_type)
                tool_selections.extend(col_tools)
                col_names_in_batch.append(col_name)

            logger.info(
                f"Phase 2 batch {batch_idx+1}/{batches}: "
                f"columns={col_names_in_batch}, tools={len(tool_selections)}"
            )
            # Respect BATCH_SIZE to avoid huge payloads
            return self._emit_tool_selections(tool_selections[:BATCH_SIZE * 2], exploration_steps)

        # ══════════════════════════════════════════════════════════════
        # PHASE 3 — LLM-generated custom SQL queries per column batch
        # ══════════════════════════════════════════════════════════════
        if exploration_steps <= PHASE3_END:
            custom_step = exploration_steps - PHASE2_END - 1   # 0-indexed, 0…SCHEMA_LLM_CUSTOM_STEPS-1
            # Distribute columns across LLM custom steps
            cols_per_llm_step = math.ceil(n_cols / SCHEMA_LLM_CUSTOM_STEPS) if n_cols else 1
            col_start = custom_step * cols_per_llm_step
            col_batch = columns[col_start : col_start + cols_per_llm_step]

            if not col_batch:
                # Nothing to query — skip to metadata
                return await self._synthesise_metadata(state, exploration_steps, target, columns)

            # Collect sample values & prior tool results for context
            sample_data = state["data_source_info"].sample_data or []
            prior_results = self._summarise_prior_results(state)

            # Ask LLM to generate custom queries for this column batch
            custom_queries = await self._generate_llm_custom_queries(
                col_batch=col_batch,
                table_name=target,
                sample_data=sample_data,
                prior_results=prior_results,
            )

            if not custom_queries:
                logger.info(f"Phase 3 step {custom_step+1}: LLM produced no custom queries — skipping")
                return {
                    "messages": [{"role": "system", "content": f"Phase3 step {custom_step+1}: no custom queries produced."}],
                    "exploration_steps": exploration_steps,
                }

            # Emit first custom query as execute_query action
            # (the graph loop will come back and we'll emit the next ones)
            q = custom_queries[0]
            action = {
                "action": "execute_query",
                "query": q["query"],
                "query_type": "sql",
                "rule_name": q.get("rule_name", f"schema_custom_{col_start}"),
                "severity": q.get("severity", "info"),
            }
            logger.info(
                f"Phase 3 step {custom_step+1}: executing custom query: "
                f"{q.get('rule_name', 'unnamed')} — {q.get('rationale', '')}"
            )
            return {
                "messages": [{"role": "assistant", "content": json.dumps(action)}],
                "exploration_steps": exploration_steps,
            }

        # ══════════════════════════════════════════════════════════════
        # PHASE 4 — LLM metadata synthesis
        # ══════════════════════════════════════════════════════════════
        return await self._synthesise_metadata(state, exploration_steps, target, columns)

    # ─────────────────────────────────────────────────────────────────────
    # HELPER — emit tool selections as a properly formatted assistant message
    # ─────────────────────────────────────────────────────────────────────

    @staticmethod
    def _emit_tool_selections(
        selections: List[Dict[str, Any]], exploration_steps: int
    ) -> Dict[str, Any]:
        action = {
            "action": "execute_tools",
            "tool_selections": selections,
        }
        return {
            "messages": [{"role": "assistant", "content": json.dumps(action)}],
            "exploration_steps": exploration_steps,
        }

    # ─────────────────────────────────────────────────────────────────────
    # HELPER — summarise prior tool execution results for LLM context
    # ─────────────────────────────────────────────────────────────────────

    @staticmethod
    def _summarise_prior_results(state: AgentState, max_chars: int = 3000) -> str:
        """Collect tool result messages for LLM context."""
        parts: List[str] = []
        for msg in state.get("messages", []):
            content = msg.get("content", "")
            if "Tool Results:" in content or '"tool_id"' in content or '"status"' in content:
                parts.append(content[:400])
        combined = "\n".join(parts[-20:])  # Last 20 tool result snippets
        return combined[:max_chars]

    # ─────────────────────────────────────────────────────────────────────
    # PHASE 3 — Generate custom LLM SQL queries beyond pre-built tools
    # ─────────────────────────────────────────────────────────────────────

    async def _generate_llm_custom_queries(
        self,
        col_batch: List[tuple],
        table_name: str,
        sample_data: List[Dict],
        prior_results: str,
    ) -> List[Dict[str, Any]]:
        """Ask LLM to generate additional bespoke SQL checks for a batch of columns."""
        # Build per-column context
        col_summaries = []
        for col_name, col_info in col_batch:
            col_type = col_info.get("type", "TEXT") if isinstance(col_info, dict) else "TEXT"
            samples = [
                str(row.get(col_name, ""))
                for row in sample_data[:20]
                if row.get(col_name) is not None
            ][:10]
            col_summaries.append(
                f"Column: {col_name!r}  Type: {col_type}  Samples: {samples}"
            )

        user_prompt = (
            f"Table: {table_name}\n\n"
            f"Columns to analyse:\n" + "\n".join(col_summaries) +
            f"\n\nPrior tool results (context):\n{prior_results[:1500]}\n\n"
            "Generate 2-3 CUSTOM SQL checks per column that go beyond the standard checks already run. "
            "Output ONLY a raw JSON array."
        )

        try:
            response = await self.llm_service.generate(
                prompt=user_prompt,
                system_prompt=SCHEMA_CUSTOM_QUERY_PROMPT.replace("{table}", table_name),
                max_tokens=2048,
                temperature=0.2,
            )
            response = sanitize_llm_response(response)

            # Extract JSON array
            arr_match = re.search(r"\[[\s\S]*\]", response, re.DOTALL)
            if not arr_match:
                logger.warning("LLM custom query generation: no JSON array found")
                return []

            raw = arr_match.group(0)
            # Fix unescaped newlines inside strings
            raw = re.sub(
                r'("(?:[^"\\]|\\.)*")',
                lambda m: m.group(1).replace("\n", "\\n"),
                raw,
            )
            queries = json.loads(raw)
            if not isinstance(queries, list):
                return []

            # Validate each entry has required fields
            valid = [
                q for q in queries
                if isinstance(q, dict) and "query" in q and "rule_name" in q
            ]
            logger.info(f"LLM generated {len(valid)} custom queries for {[c for c,_ in col_batch]}")
            return valid

        except Exception as e:
            logger.warning(f"LLM custom query generation failed: {e}")
            return []

    # ─────────────────────────────────────────────────────────────────────
    # PHASE 4 — Synthesise all exploration data into comprehensive metadata
    # ─────────────────────────────────────────────────────────────────────

    async def _synthesise_metadata(
        self,
        state: AgentState,
        exploration_steps: int,
        target: str,
        columns: List[tuple],
    ) -> Dict[str, Any]:
        """Call LLM to synthesise ALL exploration results into a rich <METADATA> document."""
        logger.info("Phase 4: Synthesising comprehensive metadata via LLM...")

        schema = state["data_source_info"].schema or {}
        sample_data = state["data_source_info"].sample_data or []

        # ── Build column type map for context ────────────────────────────
        col_type_map = {}
        cols_dict: Dict[str, Any] = {}
        if isinstance(schema, dict):
            cols_dict = schema.get("columns", {})
        for col_name, col_info in columns:
            col_type_map[col_name] = (
                col_info.get("type", "TEXT") if isinstance(col_info, dict) else "TEXT"
            )

        # ── Sample rows (up to 10) formatted as text ─────────────────────
        sample_rows_text = ""
        if sample_data:
            header = " | ".join(col_type_map.keys())
            sample_rows_text = f"| {header} |\n"
            sample_rows_text += "| " + " | ".join(["---"] * len(col_type_map)) + " |\n"
            for row in sample_data[:10]:
                vals = " | ".join(str(row.get(c, "")) for c in col_type_map.keys())
                sample_rows_text += f"| {vals} |\n"

        # ── Collect all exploration results from message history ──────────
        all_results = self._summarise_prior_results(state, max_chars=6000)

        # ── Build tool execution summary per column ───────────────────────
        tool_history = state.get("tool_execution_history", [])
        tool_summary_by_col: Dict[str, List[str]] = {}
        for th in tool_history:
            col = th.get("column") or "table"
            summary = (
                f"{th.get('tool_id','?')}: "
                f"status={th.get('status','?')}, "
                f"failed={th.get('failed_count', th.get('row_count', 0))}, "
                f"msg={str(th.get('message',''))[:80]}"
            )
            tool_summary_by_col.setdefault(col, []).append(summary)

        tool_summary_text = ""
        for col, summaries in tool_summary_by_col.items():
            tool_summary_text += f"\n### {col}\n" + "\n".join(f"  - {s}" for s in summaries)

        synthesis_prompt = (
            f"Table: {target}\n"
            f"Columns ({len(columns)}): {', '.join(col_type_map.keys())}\n"
            f"Schema: {json.dumps(col_type_map, default=str)}\n\n"
            f"## Sample Rows\n{sample_rows_text or 'Not available'}\n\n"
            f"## Tool Execution Results\n{tool_summary_text or 'See raw results below'}\n\n"
            f"## All Raw Exploration Results\n{all_results}\n\n"
            "Based on ALL the above exploration data, generate the comprehensive metadata report "
            "following the exact template in your instructions. "
            "Be detailed, factual, and thorough — this is the primary reference document for this dataset."
        )

        try:
            response = await self.llm_service.generate(
                prompt=synthesis_prompt,
                system_prompt=SCHEMA_METADATA_SYNTHESIS_PROMPT.replace("{table_name}", target),
                max_tokens=LLM_MAX_TOKENS,
                temperature=0.15,
            )
            response = sanitize_llm_response(response)

            # Ensure METADATA tags exist
            if "<METADATA>" not in response.upper():
                response = f"<METADATA>\n{response}\n</METADATA>"

            logger.info(f"Phase 4: Metadata generated ({len(response)} chars)")
            return {
                "messages": [{"role": "assistant", "content": response}],
                "exploration_steps": exploration_steps,
            }

        except Exception as e:
            logger.error(f"Metadata synthesis LLM call failed: {e}")
            # Produce a fallback metadata from schema alone
            fallback = self._build_fallback_metadata(target, columns, col_type_map, sample_data, tool_history)
            return {
                "messages": [{"role": "assistant", "content": fallback}],
                "exploration_steps": exploration_steps,
            }

    @staticmethod
    def _build_fallback_metadata(
        target: str,
        columns: List[tuple],
        col_type_map: Dict[str, str],
        sample_data: List[Dict],
        tool_history: List[Dict],
    ) -> str:
        """Build a basic metadata document without LLM when synthesis fails."""
        lines = [f"<METADATA>", f"# Dataset: {target}", "", "## Overview"]
        lines.append(f"- **Total Columns**: {len(columns)}")
        lines.append(f"- **Note**: Metadata generated from schema + tool results (LLM synthesis unavailable)")
        lines.append("")

        # Sample data table
        if sample_data and col_type_map:
            lines.append("## Sample Data")
            header = " | ".join(col_type_map.keys())
            lines.append(f"| {header} |")
            lines.append("| " + " | ".join(["---"] * len(col_type_map)) + " |")
            for row in sample_data[:5]:
                vals = " | ".join(str(row.get(c, ""))[:30] for c in col_type_map.keys())
                lines.append(f"| {vals} |")
            lines.append("")

        lines.append("## Column Analysis")
        th_by_col: Dict[str, List[Dict]] = {}
        for th in tool_history:
            col = th.get("column") or "table"
            th_by_col.setdefault(col, []).append(th)

        for col_name, col_info in columns:
            col_type = col_type_map.get(col_name, "UNKNOWN")
            lines.append(f"\n### `{col_name}` ({col_type})")

            # Extract stats from tool results
            for th in th_by_col.get(col_name, []):
                tid = th.get("tool_id", "")
                fc = th.get("failed_count", th.get("row_count", 0))
                if fc and fc > 0:
                    lines.append(f"- ⚠️ `{tid}`: {fc} failing rows")
                else:
                    lines.append(f"- ✅ `{tid}`: passed")

            # Sample values
            samples = list({
                str(row.get(col_name, ""))
                for row in sample_data[:20]
                if row.get(col_name) is not None
            })[:8]
            if samples:
                lines.append(f"- **Sample Values**: {', '.join(samples)}")

        lines.append("\n</METADATA>")
        return "\n".join(lines)

    # ═══════════════════════════════════════════════════════════════
    # METADATA SAVE OVERRIDE — preserve full rich content
    # ═══════════════════════════════════════════════════════════════

    async def _save_metadata(self, state: AgentState) -> Dict[str, Any]:
        """
        Override base _save_metadata to:
        1. Extract the FULL metadata (no 2000 char cap)
        2. Store it completely in RAG
        3. Pass the full text forward as retrieved_context
        """
        logger.info("=" * 60)
        logger.info("💾 Saving Comprehensive Schema Metadata...")

        metadata_text = None
        for msg in reversed(state["messages"]):
            m = re.search(
                r"<METADATA>([\s\S]*?)</METADATA>",
                msg.get("content", ""),
                re.IGNORECASE,
            )
            if m:
                metadata_text = m.group(1).strip()
                break

        if not metadata_text:
            # Fallback — synthesise now
            schema = state["data_source_info"].schema or {}
            cols_dict = schema.get("columns", {}) if isinstance(schema, dict) else {}
            columns = list(cols_dict.items())
            col_type_map = {
                n: (i.get("type", "TEXT") if isinstance(i, dict) else "TEXT")
                for n, i in columns
            }
            fallback = self._build_fallback_metadata(
                state["data_source_info"].target_path,
                columns,
                col_type_map,
                state["data_source_info"].sample_data or [],
                state.get("tool_execution_history", []),
            )
            m = re.search(r"<METADATA>([\s\S]*?)</METADATA>", fallback, re.IGNORECASE)
            metadata_text = m.group(1).strip() if m else fallback
            logger.warning("No <METADATA> found — using fallback metadata")

        logger.info(f"Metadata length: {len(metadata_text)} chars")

        # ── Save to RAG (no character cap) ─────────────────────────────
        try:
            from app.agents.rag_service import get_rag_service
            rag = await get_rag_service()
            if hasattr(rag, "add_document"):
                # Save in chunks if very large (RAG may have limits)
                chunk_size = 4000
                for i, chunk_start in enumerate(range(0, len(metadata_text), chunk_size)):
                    chunk = metadata_text[chunk_start : chunk_start + chunk_size]
                    await rag.add_document(
                        document_type="schema_metadata",
                        source_id=state["data_source_info"].target_path,
                        title=f"Schema Metadata: {state['data_source_info'].target_path} (part {i+1})",
                        content=chunk,
                        metadata={"part": i + 1, "total_length": len(metadata_text)},
                    )
        except Exception as e:
            logger.warning(f"RAG save failed (non-critical): {e}")

        return {
            "retrieved_context": [{"content": metadata_text}],
            "current_step": "validate_data",
            "status": AgentStatus.VALIDATING,
            "messages": [
                {
                    "role": "system",
                    "content": (
                        f"✅ Comprehensive metadata saved ({len(metadata_text)} chars). "
                        "Starting deep schema validation."
                    ),
                }
            ],
        }

    # ═══════════════════════════════════════════════════════════════
    # RULE BUILDER OVERRIDE — ALL rules, no mode gating
    # ═══════════════════════════════════════════════════════════════

    @staticmethod
    def _build_rules(
        col_name: str,
        col_type: str,
        table_name: str,
        col_meta: Dict,
        mode: str,
        total_rows: int,
        sample_values: List[Any],
    ) -> List[Dict[str, Any]]:
        """
        Override base _build_rules to remove ALL schema_only mode restrictions.
        Runs the full rule suite regardless of mode — including business-logic checks
        that are normally gated to business_analysis / hybrid modes.
        """
        # Force mode to 'hybrid' internally so all rule branches fire
        return DataQualityAgent._build_rules(
            col_name=col_name,
            col_type=col_type,
            table_name=table_name,
            col_meta=col_meta,
            mode="hybrid",          # ← lifts all mode gates
            total_rows=total_rows,
            sample_values=sample_values,
        )

    # ═══════════════════════════════════════════════════════════════
    # VALIDATION OVERRIDE — always run LLM even for schema_only
    # ═══════════════════════════════════════════════════════════════

    async def _validate_column(self, state: AgentState) -> Dict[str, Any]:
        """
        Override _validate_column to patch the mode before delegation.

        Ensures:
        1. ALL pre-built rules fire (via _build_rules override above).
        2. LLM ColumnAnalysisAgent always runs — even for schema_only on cloud providers.
        """
        # Temporarily patch validation_mode in state so base code doesn't skip LLM
        patched_state = dict(state)
        original_mode = state.get("validation_mode")

        # Use a mode string that won't trigger the "schema_only skip" branch
        # in data_quality_agent._validate_column line: `if is_cloud and mode != "schema_only":`
        # By injecting a surrogate mode value the skip branch fires for cloud (good — base
        # handles it), but for local providers we want LLM always.  The _build_rules
        # override above already uses "hybrid" internally, so validation SQL is unaffected.

        # The critical guard in base _validate_column:
        #   if is_cloud and mode != "schema_only":  → skip single-col LLM (cloud batch handles it)
        #   else:                                   → run single-col LLM
        # For schema_only, `mode != "schema_only"` = False → always runs LLM. ✓
        # So the base class already does the right thing — just delegate.

        result = await super()._validate_column(patched_state)

        # Restore original mode in returned state
        if original_mode is not None:
            result["validation_mode"] = original_mode  # type: ignore

        return result