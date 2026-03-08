"""
Column-level Deep-Dive Analysis Agent — v2

Key improvements over v1:
  - FIX: Unescape double-escaped column names (\"col\" → "col") before SQL execution
  - FIX: SQLite-compatible queries (no SQRT/POWER; squared-distance outlier check)
  - NEW: Chain-of-thought (think → verify → output) prompting
  - NEW: Explicit deduplication — pass existing rule names so LLM never generates duplicates
  - NEW: Mode-specific prompts with richer context per validation mode
  - NEW: Sanity-check validation expanded to catch more LLM hallucination patterns
  - IMPROVED: Type-specific prompt examples aligned to SQLite syntax
"""
import json
import logging
import re
from typing import Dict, Any, List, Optional
from dataclasses import dataclass

from app.agents.llm_service import get_llm_service
from app.agents.llm_sanitizer import sanitize_llm_response

logger = logging.getLogger(__name__)

MAX_RETRIES = 3

FILE_SOURCE_TYPES = frozenset([
    "local_file", "csv", "excel", "parquet", "json", "jsonl",
    "file-upload", "uploaded_file",
])


# ─────────────────────────────────────────────────────────────────────────────
# PER-MODE PROMPT REGISTRY
# ─────────────────────────────────────────────────────────────────────────────

MODE_INSTRUCTIONS: Dict[str, str] = {
    "schema_only": """\
MODE — SCHEMA ONLY:
Focus exclusively on structural and type-integrity checks:
  • Correct data type storage (e.g. integers stored as text)
  • Value length bounds (too short / too long for domain)
  • Encoding issues (mixed scripts, control characters, BOM)
  • Format compliance (dates parseable, numbers castable)
DO NOT generate business-logic rules. DO NOT check value ranges or business thresholds.""",

    "ai_recommended": """\
MODE — AI RECOMMENDED:
Generate the most impactful data quality rules based on column name semantics and sample values.
Include a balanced mix of:
  • Structural completeness (null/empty patterns)
  • Format validity (patterns, lengths, encodings)
  • Statistical outliers (use squared-distance, NOT SQRT)
  • Domain-specific plausibility (price > 0, quantity integer, date not in future)
Prioritise rules that would expose real business risk if they fail.""",

    "hybrid": """\
MODE — HYBRID:
Combine structural AND business-logic checks:
  • Start with format/type validation
  • Then add 2-3 semantic checks based on column name
  • Cross-reference sample values to detect realistic anomalies
Avoid generating checks that are purely statistical noise.""",

    "business_analysis": """\
MODE — BUSINESS ANALYSIS:
Focus ONLY on business-logic plausibility and domain rules:
  • Price/cost margins (cost should be ≤ price)
  • Temporal logic (created_at ≤ updated_at, future dates)
  • Referential plausibility (supplier_id reasonable range)
  • KPI outliers (revenue spikes, zero-quantity active items)
  • Cross-column anomalies when column semantics suggest correlation
SKIP basic null/format checks — focus on BUSINESS MEANING.""",

    "custom_rules": """\
MODE — CUSTOM RULES SUPPLEMENT:
The user has provided explicit validation rules. Your task is to add ONLY rules
that complement the user's rules without duplicating them.
  • Check coverage gaps in nulls, ranges, and formats
  • Add any semantic checks the user may have missed""",
}

SQLITE_COMPAT_NOTE = """\
SQLite Compatibility REQUIRED:
  ✗ NO SQRT() — use squared comparison: (x-mean)*(x-mean) > 9.0 * variance
  ✗ NO POWER() / POW()
  ✗ NO ~ regex operator — use GLOB or LIKE
  ✓ GLOB: col GLOB '*[!0-9]*'
  ✓ CAST(), LENGTH(), TRIM(), UPPER(), LOWER(), DATE(), DATETIME()"""

THINK_VERIFY_TEMPLATE = """\
REASONING PROTOCOL — follow these steps before outputting:
<think>
1. SEMANTICS: What does this column represent in the business domain?
2. SAMPLES: What patterns/outliers do the sample values reveal?
3. GAPS: Which checks listed as "already executed" are NOT yet covered?
4. DESIGN: For each gap — write the WHERE clause catching FAILING rows.
5. VERIFY: Is each query SQLite-compatible? Does it use COUNT(*) AS cnt?
6. DEDUPLICATE: Remove any rule matching an already-executed name.
</think>
Output ONLY the JSON array after </think>. No prose, no fences."""


@dataclass
class ColumnQualityReport:
    column_name: str
    dtype: str
    mode: str
    validation_results: List[Dict[str, Any]]
    score: float
    summary: str


class ColumnAnalysisAgent:
    """Dedicated sub-agent that deeply analyses a single column."""

    def __init__(
        self,
        column_name: str,
        dtype: str,
        samples: List[Any],
        mode: str,
        engine_executor,
        source_type: str = "sqlite",
        rag_context: str = "",
        existing_rule_names: Optional[List[str]] = None,
    ):
        self.column_name = column_name
        self.dtype = dtype
        self.samples = samples
        self.mode = mode.lower()
        self.source_type = source_type.lower()
        self.rag_context = rag_context
        self.existing_rule_names = set(existing_rule_names or [])
        self.llm_service = get_llm_service()
        self.engine_executor = engine_executor
        self.use_pandas = self.source_type in FILE_SOURCE_TYPES
        self.rules_to_run: List[Dict[str, Any]] = []
        self.execution_results: List[Dict[str, Any]] = []

    async def analyze(self, table_name: str) -> ColumnQualityReport:
        lang = "Pandas" if self.use_pandas else "SQL"
        logger.info(f"Deep-dive: column='{self.column_name}' mode={self.mode} lang={lang}")
        await self._generate_rules(table_name)
        await self._execute_rules()
        return await self._evaluate_results()

    # ── Stage 1: Rule Generation ─────────────────────────────────

    async def _generate_rules(self, table_name: str) -> None:
        system_prompt = self._build_system_prompt(table_name)
        user_prompt = self._build_user_prompt(table_name)

        last_error = None
        for attempt in range(1, MAX_RETRIES + 1):
            try:
                prompt = user_prompt
                if last_error:
                    prompt += (
                        f"\n\n⚠️ RETRY {attempt}/{MAX_RETRIES}: "
                        f"Previous response invalid — {last_error}. "
                        "Output ONLY a raw JSON array."
                    )

                response = await self.llm_service.generate(
                    prompt=prompt, system_prompt=system_prompt
                )
                response = sanitize_llm_response(response)
                logger.warning(
                    f"RAW SANITIZED LLM RESPONSE for {self.column_name} (Attempt {attempt}):\n"
                    f"{response}\n---END---"
                )

                rules = self._parse_rules_json(response)
                if rules is not None:
                    rules = [r for r in rules if r.get("rule_name") not in self.existing_rule_names]
                    self.rules_to_run = rules
                    logger.info(f"Generated {len(rules)} rules for {self.column_name} (attempt {attempt})")
                    return
                else:
                    last_error = "No valid JSON array found in response"

            except json.JSONDecodeError as e:
                last_error = f"JSON parse error: {e}"
                logger.warning(f"Attempt {attempt}/{MAX_RETRIES} for {self.column_name}: {last_error}")
            except Exception as e:
                last_error = str(e)
                logger.error(f"Attempt {attempt}/{MAX_RETRIES} for {self.column_name}: {last_error}")

        logger.error(f"All {MAX_RETRIES} attempts failed for {self.column_name}. Last: {last_error}")
        self.rules_to_run = []

    def _build_user_prompt(self, table_name: str) -> str:
        existing_list = "\n".join(f"  - {r}" for r in sorted(self.existing_rule_names)[:30]) or "  (none)"
        rag_section = ""
        if self.rag_context:
            rag_section = f"\n\nRELEVANT CONTEXT FROM PRIOR ANALYSIS:\n{self.rag_context[:800]}\n"

        return (
            f"Target Column: {self.column_name}\n"
            f"Data Type: {self.dtype}\n"
            f"Sample Values: {self.samples[:20]}\n"
            f"{rag_section}"
            f"\nALREADY EXECUTED RULES (DO NOT DUPLICATE THESE):\n{existing_list}\n\n"
            "Output EXACTLY a JSON array of NEW rules not already listed above. "
            "No markdown, no extra text."
        )

    @staticmethod
    def _parse_rules_json(response: str) -> Optional[List[Dict[str, Any]]]:
        if not response:
            return None

        # Strip <think>...</think> blocks
        response = re.sub(r'<think>[\s\S]*?</think>', '', response, flags=re.IGNORECASE).strip()

        fenced = re.search(r'```(?:json)?\s*([\s\S]*?)\s*```', response, re.IGNORECASE)
        raw = fenced.group(1).strip() if fenced else None

        if not raw:
            arr = re.search(r'\[[\s\S]*\]', response, re.DOTALL)
            raw = arr.group(0) if arr else None

        if not raw:
            return None

        raw = re.sub(
            r'("(?:[^"\\]|\\.)*")',
            lambda m: m.group(1).replace('\n', '\\n'),
            raw,
        )

        try:
            rules = json.loads(raw)
        except json.JSONDecodeError as e:
            logger.warning(f"json.loads failed after newline fix: {e}")
            return None

        if not isinstance(rules, list):
            return None

        return [r for r in rules if isinstance(r, dict) and "query" in r and "rule_name" in r]

    # ── Stage 2: Rule Execution ─────────────────────────────────

    async def _execute_rules(self) -> None:
        q_type = "pandas" if self.use_pandas else "sql"

        for rule in self.rules_to_run:
            raw_query = rule["query"]
            # FIX: Unescape double-escaped column names
            # LLM sometimes emits \\\"col\\\" in JSON which Python stores as \"col\"
            # SQLite rejects backslash-quote — strip the backslashes.
            query = raw_query.replace('\\"', '"')

            if not self.use_pandas:
                if not self._validate_query_sanity(query, self.column_name, self.dtype):
                    logger.warning(f"Query failed sanity check for {self.column_name}: {query[:100]}")
                    self.execution_results.append({
                        "rule_name": rule["rule_name"],
                        "severity": rule.get("severity", "warning"),
                        "query": query,
                        "result": {"status": "error", "error": "Pre-execution sanity check failed"},
                    })
                    continue

            try:
                result = await self.engine_executor(query=query, q_type=q_type)
                self.execution_results.append({
                    "rule_name": rule["rule_name"],
                    "severity": rule.get("severity", "warning"),
                    "query": query,
                    "result": result,
                })
            except Exception as e:
                logger.error(f"Execution failed for {self.column_name} rule '{rule['rule_name']}': {e}")

    def _validate_query_sanity(self, query: str, column_name: str, dtype: str) -> bool:
        query_upper = query.upper()
        col_lower = column_name.lower()

        # Block email patterns on non-email columns
        if "email" not in col_lower:
            for pat in ("'%@%.%'", '"%@%.%"', "'%@%'"):
                if pat in query:
                    logger.warning(f"BLOCKED email pattern on non-email col: {column_name}")
                    return False

        # Block SELECT * without COUNT
        if re.search(r'\bSELECT\s+\*\b', query_upper) and "COUNT(*)" not in query_upper:
            logger.warning(f"BLOCKED SELECT *: {column_name}")
            return False

        # Block SQRT — not in standard SQLite
        if "SQRT(" in query_upper:
            logger.warning(f"BLOCKED SQRT() (SQLite-incompatible): {column_name}")
            return False

        # Block POWER/POW
        if "POWER(" in query_upper or "POW(" in query_upper:
            logger.warning(f"BLOCKED POWER()/POW(): {column_name}")
            return False

        # Block date format specifiers on non-date columns
        non_date = all(s not in dtype.lower() for s in ("date", "time", "timestamp"))
        if non_date:
            for fmt in ("'%Y-%m-%d'", '"%Y-%m-%d"', "'%Y-%m-%d %H:%M:%S'"):
                if fmt in query:
                    logger.warning(f"BLOCKED date format on non-date col: {column_name}")
                    return False

        # Block integer-only patterns on non-integer columns
        if "int" not in dtype.lower():
            if " % 1 = 0" in query or " % 1=0" in query or "MOD(" in query_upper:
                logger.warning(f"BLOCKED integer mod on non-int col: {column_name}")
                return False

        # Must use COUNT(*)
        if "COUNT(*)" not in query_upper and "COUNT( *)" not in query_upper:
            logger.warning(f"BLOCKED non-count query for {column_name}: must use COUNT(*)")
            return False

        return True

    # ── Stage 3: Result Evaluation ──────────────────────────────

    async def _evaluate_results(self) -> ColumnQualityReport:
        parsed = []
        total = passed = warns = crits = 0

        for ex in self.execution_results:
            res = ex["result"]
            if res.get("status") == "error":
                continue

            total += 1
            sample_rows = res.get("sample_rows", [])
            if sample_rows and isinstance(sample_rows[0], dict):
                first_val = next(iter(sample_rows[0].values()), None)
                try:
                    failed_count = int(first_val) if first_val is not None else 0
                except (ValueError, TypeError):
                    failed_count = 0
            else:
                failed_count = res.get("row_count", 0)

            sev_map = {
                "high": "critical", "critical": "critical",
                "medium": "warning", "warning": "warning",
                "low": "info", "info": "info",
            }
            severity = sev_map.get(ex.get("severity", "warning").lower(), "warning")
            status = "failed" if failed_count > 0 else "passed"

            if status == "passed":
                passed += 1
            elif severity == "critical":
                crits += 1
            else:
                warns += 1

            rule_nm = ex["rule_name"]
            if not rule_nm.startswith(f"{self.column_name}_"):
                rule_nm = f"{self.column_name}_{rule_nm}"

            parsed.append({
                "rule_name": rule_nm,
                "status": status,
                "failed_count": failed_count,
                "failure_examples": res.get("sample_rows", [])[:5],
                "severity": severity,
                "rule_type": "llm_generated_sql",
                "executed_query": ex["query"],
                "check_origin": "llm_generated",
                "column_name": self.column_name,
            })

        score = 100.0 if total == 0 else round(((passed + warns * 0.5) / total) * 100, 2)

        return ColumnQualityReport(
            column_name=self.column_name,
            dtype=self.dtype,
            mode=self.mode,
            validation_results=parsed,
            score=score,
            summary=(
                f"Analyzed {total} LLM rules for {self.column_name}: "
                f"{passed} passed, {warns} warnings, {crits} critical."
            ),
        )

    # ── Prompt Builders ─────────────────────────────────────────

    def _build_system_prompt(self, table_name: str) -> str:
        if self.use_pandas:
            return self._get_pandas_prompt()
        return self._get_sql_prompt(table_name)

    def _get_sql_prompt(self, table_name: str) -> str:
        dtype_upper = self.dtype.upper()

        if any(s in dtype_upper for s in ("INT", "INTEGER", "BIGINT")):
            example_q = f'SELECT COUNT(*) AS cnt FROM "{table_name}" WHERE "{self.column_name}" < 0'
            example_rule = (
                f'{{"rule_name": "{self.column_name}_negative_check", '
                f'"severity": "critical", "query": "{example_q}"}}'
            )
            type_hint = "INTEGER. Checks: negative values, unrealistic ranges, impossibly large values."
        elif any(s in dtype_upper for s in ("REAL", "FLOAT", "DECIMAL", "NUMERIC", "DOUBLE")):
            example_q = f'SELECT COUNT(*) AS cnt FROM "{table_name}" WHERE "{self.column_name}" < 0'
            example_rule = (
                f'{{"rule_name": "{self.column_name}_negative_check", '
                f'"severity": "critical", "query": "{example_q}"}}'
            )
            type_hint = "REAL/FLOAT. Checks: negative, zero where not allowed, outliers (squared distance, NOT SQRT)."
        elif any(s in dtype_upper for s in ("DATE", "TIME", "TIMESTAMP", "DATETIME")):
            example_q = f"SELECT COUNT(*) AS cnt FROM \"{table_name}\" WHERE \"{self.column_name}\" > DATE('now')"
            example_rule = (
                f'{{"rule_name": "{self.column_name}_future_date_check", '
                f'"severity": "warning", "query": "{example_q}"}}'
            )
            type_hint = "DATETIME. Checks: future dates, dates before 1900, invalid formats."
        elif "email" in self.column_name.lower():
            example_q = f"SELECT COUNT(*) AS cnt FROM \"{table_name}\" WHERE \"{self.column_name}\" NOT LIKE '%@%.%'"
            example_rule = (
                f'{{"rule_name": "{self.column_name}_format_check", '
                f'"severity": "critical", "query": "{example_q}"}}'
            )
            type_hint = "EMAIL. Checks: missing @, missing domain, test domains, duplicates."
        elif any(s in dtype_upper for s in ("BOOL", "BOOLEAN")):
            example_q = f'SELECT COUNT(*) AS cnt FROM "{table_name}" WHERE "{self.column_name}" NOT IN (0, 1)'
            example_rule = (
                f'{{"rule_name": "{self.column_name}_invalid_bool_check", '
                f'"severity": "critical", "query": "{example_q}"}}'
            )
            type_hint = "BOOLEAN. Checks: values outside 0/1/true/false."
        else:
            example_q = (
                f'SELECT COUNT(*) AS cnt FROM "{table_name}" '
                f'WHERE "{self.column_name}" IS NULL OR TRIM("{self.column_name}") = \'\''
            )
            example_rule = (
                f'{{"rule_name": "{self.column_name}_null_empty_check", '
                f'"severity": "warning", "query": "{example_q}"}}'
            )
            type_hint = "TEXT. Checks: null/empty, whitespace padding, suspicious patterns."

        mode_instruction = MODE_INSTRUCTIONS.get(self.mode, MODE_INSTRUCTIONS["ai_recommended"])

        return f"""You are an Advanced Data Quality AI specialising in SQL-based column validation.

COLUMN: "{self.column_name}" | TYPE: {self.dtype}
TABLE: "{table_name}"
COLUMN HINT: {type_hint}

{SQLITE_COMPAT_NOTE}

{mode_instruction}

OUTPUT FORMAT — raw JSON array ONLY:
[
  {{
    "rule_name": "{self.column_name}_descriptive_name",
    "severity": "critical|warning|info",
    "query": "SELECT COUNT(*) AS cnt FROM \\"{table_name}\\" WHERE <failing_condition>"
  }}
]

EXAMPLE (correct format):
[{example_rule}]

CRITICAL RULES:
1. Every query returns ONE integer: count of FAILING rows (0 = pass, >0 = fail).
2. Use COUNT(*) with alias 'cnt'. Never SELECT *.
3. Column identifiers use double quotes: "{self.column_name}" — NO backslashes before quotes.
4. Single-line queries only — no literal newlines inside JSON strings.
5. Generate 2-3 HIGH-VALUE rules. Quality > quantity.
6. DO NOT duplicate any rule already listed in the user message.
7. For outlier detection: ("{self.column_name}" - mean)*("{self.column_name}" - mean) > 9.0 * variance

{THINK_VERIFY_TEMPLATE}"""

    def _get_pandas_prompt(self) -> str:
        mode_instruction = MODE_INSTRUCTIONS.get(self.mode, MODE_INSTRUCTIONS["ai_recommended"])
        return f"""You are an Advanced Data Quality AI generating Pandas validation rules.

COLUMN: "{self.column_name}" | TYPE: {self.dtype}

{mode_instruction}

OUTPUT FORMAT — raw JSON array ONLY:
[
  {{
    "rule_name": "{self.column_name}_descriptive_name",
    "severity": "critical|warning|info",
    "query": "len(df[df['{self.column_name}'] < 0])"
  }}
]

CRITICAL RULES:
1. Every query evaluates to an integer (count of failing rows).
2. Use df['{self.column_name}'] syntax.
3. Generate 2-3 HIGH-VALUE rules.
4. DO NOT duplicate any rule already listed in the user message.

{THINK_VERIFY_TEMPLATE}"""


# ─────────────────────────────────────────────────────────────────────────────
# BATCH COLUMN ANALYSIS AGENT (cloud providers)
# ─────────────────────────────────────────────────────────────────────────────

class BatchColumnAnalysisAgent:
    """
    Batch variant: sends all columns to LLM in one call.
    Used by cloud providers (Gemini, OpenAI, Anthropic, Groq) to reduce API calls.
    """

    def __init__(
        self,
        schema_data: Dict[str, Dict[str, Any]],
        mode: str,
        engine_executor,
        source_type: str = "sqlite",
        existing_rule_names: Optional[List[str]] = None,
    ):
        self.schema_data = schema_data
        self.mode = mode.lower()
        self.engine_executor = engine_executor
        self.source_type = source_type.lower()
        self.use_pandas = source_type in FILE_SOURCE_TYPES
        self.existing_rule_names = set(existing_rule_names or [])
        self.llm_service = get_llm_service()
        self.rules_to_run: List[Dict[str, Any]] = []
        self.execution_results: List[Dict[str, Any]] = []

    async def analyze(self, table_name: str) -> Dict[str, ColumnQualityReport]:
        await self._generate_rules(table_name)
        await self._execute_rules()
        return await self._evaluate_results()

    async def _generate_rules(self, table_name: str) -> None:
        mode_instruction = MODE_INSTRUCTIONS.get(self.mode, MODE_INSTRUCTIONS["ai_recommended"])
        existing_list = "\n".join(f"  - {r}" for r in sorted(self.existing_rule_names)[:40]) or "  (none)"

        sys_prompt = f"""You are an Advanced Data Quality AI generating SQL validation rules for ALL columns.

TABLE: "{table_name}"
{SQLITE_COMPAT_NOTE}

{mode_instruction}

OUTPUT FORMAT — raw JSON array for ALL columns combined:
[{{
  "rule_name": "[column_name]_RuleName",
  "severity": "critical|warning|info",
  "query": "SELECT COUNT(*) AS cnt FROM \\"{table_name}\\" WHERE ..."
}}]

RULES:
- Always COUNT(*) AS cnt — never SELECT *.
- Double-quoted identifiers: "column_name" — NO backslashes.
- Single-line query strings.
- 2-3 semantic/domain rules per column — skip trivial null/empty checks.
- DO NOT duplicate rules listed below.
- For outliers: (col - mean)*(col - mean) > 9.0 * variance  (no SQRT!)

ALREADY EXECUTED (DO NOT DUPLICATE):
{existing_list}

{THINK_VERIFY_TEMPLATE}"""

        schema_dump = json.dumps({
            col: {"type": info["dtype"], "samples": info["samples"][:10]}
            for col, info in self.schema_data.items()
        }, indent=2)

        user_prompt = (
            f"Target Table: {table_name}\nSchema & Samples:\n{schema_dump}\n\n"
            "Output EXACTLY a JSON array of rules for all columns. No markdown, no extra text."
        )

        last_error = None
        for attempt in range(1, 4):
            try:
                prompt = user_prompt
                if last_error:
                    prompt += f"\n\n⚠️ RETRY {attempt}/3: {last_error}. Output ONLY raw JSON array."

                response = await self.llm_service.generate(prompt=prompt, system_prompt=sys_prompt)
                response = sanitize_llm_response(response)
                logger.warning(f"RAW BATCH LLM RESPONSE (Attempt {attempt}):\n{response[:1000]}...\n---END---")

                rules = ColumnAnalysisAgent._parse_rules_json(response)
                if rules is not None:
                    rules = [r for r in rules if r.get("rule_name") not in self.existing_rule_names]
                    self.rules_to_run = rules
                    logger.info(f"Generated {len(rules)} batch rules (attempt {attempt})")
                    return
                else:
                    last_error = "No valid JSON array found"

            except Exception as e:
                last_error = str(e)
                logger.error(f"Batch Attempt {attempt}/3 failed: {last_error}")

        logger.error(f"All batch attempts failed. Last error: {last_error}")
        self.rules_to_run = []

    async def _execute_rules(self) -> None:
        q_type = "pandas" if self.use_pandas else "sql"
        for rule in self.rules_to_run:
            # FIX: Unescape double-escaped column names
            query = rule["query"].replace('\\"', '"')
            try:
                result = await self.engine_executor(query=query, q_type=q_type)
                self.execution_results.append({
                    "rule_name": rule["rule_name"],
                    "severity": rule.get("severity", "warning"),
                    "query": query,
                    "result": result,
                })
            except Exception as e:
                logger.error(f"Batch execution failed for rule '{rule['rule_name']}': {e}")

    async def _evaluate_results(self) -> Dict[str, ColumnQualityReport]:
        reports = {}
        col_executions: Dict[str, List] = {col: [] for col in self.schema_data}

        for ex in self.execution_results:
            name = ex["rule_name"]
            target_col = next((col for col in self.schema_data if name.startswith(col)), None)
            if not target_col and self.schema_data:
                target_col = list(self.schema_data.keys())[0]
            if target_col:
                if not name.startswith(f"{target_col}_"):
                    ex["rule_name"] = f"{target_col}_{name}"
                col_executions[target_col].append(ex)

        for col_name, executions in col_executions.items():
            parsed = []
            total = passed = warns = crits = 0
            for ex in executions:
                res = ex["result"]
                if res.get("status") == "error":
                    continue
                total += 1
                sample_rows = res.get("sample_rows", [])
                if sample_rows and isinstance(sample_rows[0], dict):
                    first_val = next(iter(sample_rows[0].values()), None)
                    try:
                        failed_count = int(first_val) if first_val is not None else 0
                    except (ValueError, TypeError):
                        failed_count = 0
                else:
                    failed_count = res.get("row_count", 0)

                sev = ex.get("severity", "warning").lower()
                severity = sev if sev in ("critical", "warning", "info") else "warning"
                status = "failed" if failed_count > 0 else "passed"

                if status == "passed":
                    passed += 1
                elif severity == "critical":
                    crits += 1
                else:
                    warns += 1

                parsed.append({
                    "rule_name": ex["rule_name"],
                    "status": status,
                    "failed_count": failed_count,
                    "failure_examples": res.get("sample_rows", [])[:5],
                    "severity": severity,
                    "rule_type": "llm_generated_sql",
                    "executed_query": ex["query"],
                    "check_origin": "llm_generated",
                    "column_name": col_name,
                })

            score = 100.0 if total == 0 else round(((passed + warns * 0.5) / total) * 100, 2)
            reports[col_name] = ColumnQualityReport(
                column_name=col_name,
                dtype=self.schema_data[col_name]["dtype"],
                mode=self.mode,
                validation_results=parsed,
                score=score,
                summary=f"Batch: {total} rules for {col_name}: {passed} passed, {warns} warnings, {crits} critical.",
            )

        return reports