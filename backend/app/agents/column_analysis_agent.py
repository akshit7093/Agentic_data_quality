"""
Column-level Deep-Dive Analysis Agent.

Generates, executes, and evaluates an exhaustive suite of data quality rules
for a single column. Supports both SQL (for database sources) and Pandas
(for file-based sources like CSV, JSON, Excel, Parquet).

Includes built-in LLM retry logic — if the model breaks protocol or returns
invalid JSON, we re-prompt with a corrective hint up to MAX_RETRIES times.
"""
import json
import logging
import re
from typing import Dict, Any, List, Optional
from dataclasses import dataclass

from app.agents.llm_service import get_llm_service
from app.agents.llm_sanitizer import sanitize_llm_response

logger = logging.getLogger(__name__)

# ── Configuration ──────────────────────────────────────────
MAX_RETRIES = 3  # Max LLM retry attempts on protocol violation

# Source types that should use Pandas code generation instead of SQL
FILE_SOURCE_TYPES = frozenset([
    "local_file", "csv", "excel", "parquet", "json", "jsonl",
    "file-upload", "uploaded_file",
])


@dataclass
class ColumnQualityReport:
    column_name: str
    dtype: str
    mode: str
    validation_results: List[Dict[str, Any]]
    score: float
    summary: str


class ColumnAnalysisAgent:
    """
    A dedicated sub-agent that deeply analyzes a single column.
    Automatically detects whether to generate SQL or Pandas rules
    based on the source type.
    """
    def __init__(
        self,
        column_name: str,
        dtype: str,
        samples: List[Any],
        mode: str,
        engine_executor,
        source_type: str = "sqlite",
    ):
        self.column_name = column_name
        self.dtype = dtype
        self.samples = samples
        self.mode = mode.lower()
        self.source_type = source_type.lower()
        self.llm_service = get_llm_service()
        self.engine_executor = engine_executor

        # Determine execution mode based on source type
        self.use_pandas = self.source_type in FILE_SOURCE_TYPES

        self.rules_to_run: List[Dict[str, Any]] = []
        self.execution_results: List[Dict[str, Any]] = []

    async def analyze(self, table_name: str) -> ColumnQualityReport:
        """Run the complete column analysis pipeline."""
        lang = "Pandas" if self.use_pandas else "SQL"
        logger.info(f"Deep-dive: column='{self.column_name}' mode={self.mode} lang={lang}")

        await self._generate_rules(table_name)
        await self._execute_rules()
        return await self._evaluate_results()

    # ── Stage 1: Rule Generation with Retry ─────────────────

    async def _generate_rules(self, table_name: str) -> None:
        """Generate rules with built-in retry on protocol violation."""
        system_prompt = self._build_system_prompt(table_name)
        user_prompt = (
            f"Target Column: {self.column_name}\n"
            f"Data Type: {self.dtype}\n"
            f"Sample Values: {self.samples[:20]}\n\n"
            f"Output EXACTLY a JSON array of rules. No markdown, no extra text."
        )

        last_error = None
        for attempt in range(1, MAX_RETRIES + 1):
            try:
                # On retry, append a correction hint
                prompt = user_prompt
                if last_error:
                    prompt += (
                        f"\n\n⚠️ RETRY {attempt}/{MAX_RETRIES}: "
                        f"Your previous response was invalid — {last_error}. "
                        f"Output ONLY a raw JSON array. No markdown, no explanation."
                    )

                response = await self.llm_service.generate(
                    prompt=prompt, system_prompt=system_prompt
                )
                response = sanitize_llm_response(response)

                rules = self._parse_rules_json(response)
                if rules is not None:
                    self.rules_to_run = rules
                    logger.info(
                        f"Generated {len(rules)} rules for {self.column_name} "
                        f"(attempt {attempt})"
                    )
                    return
                else:
                    last_error = "No valid JSON array found in response"

            except json.JSONDecodeError as e:
                last_error = f"JSON parse error: {e}"
                logger.warning(
                    f"Attempt {attempt}/{MAX_RETRIES} for {self.column_name}: {last_error}"
                )
            except Exception as e:
                last_error = str(e)
                logger.error(
                    f"Attempt {attempt}/{MAX_RETRIES} for {self.column_name}: {last_error}"
                )

        # All retries exhausted
        logger.error(
            f"All {MAX_RETRIES} attempts failed for {self.column_name}. "
            f"Last error: {last_error}"
        )
        self.rules_to_run = []

    @staticmethod
    def _parse_rules_json(response: str) -> Optional[List[Dict[str, Any]]]:
        """Extract and parse JSON rules array from LLM response."""
        if not response:
            return None

        # Try markdown fenced block first
        fenced = re.search(r'```(?:json)?\s*([\s\S]*?)\s*```', response, re.IGNORECASE)
        if fenced:
            raw = fenced.group(1).strip()
        else:
            # Try raw JSON array
            arr = re.search(r'\[[\s\S]*\]', response, re.DOTALL)
            raw = arr.group(0) if arr else None

        if not raw:
            return None

        rules = json.loads(raw)
        if not isinstance(rules, list):
            return None

        # Filter valid rules — must have "query" and "rule_name"
        return [r for r in rules if isinstance(r, dict) and "query" in r and "rule_name" in r]

    # ── Stage 2: Rule Execution ─────────────────────────────

    async def _execute_rules(self) -> None:
        """Execute rules via the provided engine executor."""
        q_type = "pandas" if self.use_pandas else "sql"

        for rule in self.rules_to_run:
            query = rule["query"]
            try:
                result = await self.engine_executor(query=query, q_type=q_type)
                self.execution_results.append({
                    "rule_name": rule["rule_name"],
                    "severity": rule.get("severity", "warning"),
                    "query": query,
                    "result": result,
                })
            except Exception as e:
                logger.error(
                    f"Execution failed for {self.column_name} "
                    f"rule '{rule['rule_name']}': {e}"
                )

    # ── Stage 3: Result Evaluation ──────────────────────────

    async def _evaluate_results(self) -> ColumnQualityReport:
        """Score and format the execution results."""
        parsed = []
        total = passed = warns = crits = 0

        for ex in self.execution_results:
            res = ex["result"]
            if res.get("status") == "error":
                continue

            total += 1
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

            parsed.append({
                "rule_name": f"{self.column_name}_{ex['rule_name']}",
                "status": status,
                "failed_count": failed_count,
                "failure_examples": res.get("sample_rows", [])[:5],
                "severity": severity,
                "rule_type": "column_agent_query",
                "executed_query": ex["query"],
            })

        score = 100.0 if total == 0 else round(
            ((passed + warns * 0.5) / total) * 100, 2
        )

        return ColumnQualityReport(
            column_name=self.column_name,
            dtype=self.dtype,
            mode=self.mode,
            validation_results=parsed,
            score=score,
            summary=(
                f"Analyzed {total} rules for {self.column_name}: "
                f"{passed} passed, {warns} warnings, {crits} critical."
            ),
        )

    # ── Prompt Builders ─────────────────────────────────────

    def _build_system_prompt(self, table_name: str) -> str:
        """Build the full system prompt based on source type and mode."""
        if self.use_pandas:
            return self._get_pandas_prompt()
        return self._get_sql_prompt(table_name)

    def _get_sql_prompt(self, table_name: str) -> str:
        """SQL-based prompt for database sources."""
        base = f"""You are a Deep-Dive Data Quality Expert. Output a JSON array of validation rules as SQL queries.
Each query MUST SELECT the INVALID rows from `{table_name}`.

Required JSON format (raw array, no markdown):
[
    {{
        "rule_name": "{self.column_name}_NullCheck",
        "severity": "critical",
        "query": "SELECT * FROM {table_name} WHERE {self.column_name} IS NULL"
    }}
]

RULES:
- Use table name `{table_name}` in every query
- Each query must return failing/invalid rows only
- Use standard SQLite-compatible SQL syntax
- Output ONLY the JSON array — no explanation, no markdown fences"""

        return f"{base}\n\n{self._get_mode_instructions()}"

    def _get_pandas_prompt(self) -> str:
        """Pandas-based prompt for file-based sources (CSV, JSON, Excel, etc.)."""
        base = f"""You are a Deep-Dive Data Quality Expert. Output a JSON array of validation rules as Pandas expressions.
The DataFrame is named `df`. Each expression MUST return the INVALID rows.

Required JSON format (raw array, no markdown):
[
    {{
        "rule_name": "{self.column_name}_NullCheck",
        "severity": "critical",
        "query": "df[df['{self.column_name}'].isna()]"
    }},
    {{
        "rule_name": "{self.column_name}_EmptyStringCheck",
        "severity": "critical",
        "query": "df[df['{self.column_name}'].astype(str).str.strip() == '']"
    }}
]

RULES:
- The DataFrame variable is always `df`
- Column access: df['{self.column_name}']
- Each expression must return a DataFrame of failing rows
- Use `.isna()`, `.str.contains()`, `.str.match()`, `.between()`, `.duplicated()`, `.astype()` etc.
- You may use `pd` (pandas) and `np` (numpy) — they are available
- Output ONLY the JSON array — no explanation, no markdown fences"""

        return f"{base}\n\n{self._get_mode_instructions()}"

    def _get_mode_instructions(self) -> str:
        """Mode-specific instructions appended to any prompt."""
        if self.mode in ("schema_only",):
            return """MODE: SCHEMA ONLY — Generate 3-5 rules:
- NULLs and empty strings
- Type validation (numeric columns should not contain text)
- Basic format/structure (email regex, phone format, date parsing)
DO NOT infer business logic."""

        if self.mode in ("business_analysis", "business_intelligence", "bi"):
            return """MODE: BUSINESS INTELLIGENCE — Generate 4-7 rules:
- Schema checks (NULLs, blanks, type)
- Domain bounds (positive numbers, valid date ranges, known categoricals)
- Uniqueness where applicable
- Use the Sample Values to infer valid domains."""

        return """MODE: FULL AUTONOMOUS — Generate 5-10 exhaustive rules:
- All schema + BI checks
- Statistical outlier detection
- Leading/trailing whitespace or hidden characters
- Cardinality anomalies
- Suspicious zero-variance patterns"""
