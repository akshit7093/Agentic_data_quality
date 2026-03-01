import json
import logging
import re
from typing import Dict, Any, List, Optional
from dataclasses import dataclass, asdict

from app.agents.llm_service import get_llm_service
from app.agents.llm_sanitizer import sanitize_llm_response

logger = logging.getLogger(__name__)

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
    It generates, executes, and evaluates a comprehensive suite of rules
    based on the column's data type, sample values, and the rigorousness Mode.
    """
    def __init__(self, column_name: str, dtype: str, samples: List[Any], mode: str, engine_executor):
        self.column_name = column_name
        self.dtype = dtype
        self.samples = samples
        self.mode = mode.lower()  # schema_only, business_intelligence, full_autonomous
        self.llm_service = get_llm_service()
        self.engine_executor = engine_executor # The function from core agent to run DB queries
        
        # We store the raw LLM-generated rules here
        self.rules_to_run = []
        # Store execution results
        self.execution_results = []
    
    async def analyze(self, table_name: str) -> ColumnQualityReport:
        """Run the complete column analysis pipeline."""
        logger.info(f"Starting deep-dive analysis for column '{self.column_name}' (Mode: {self.mode})")
        
        # Stage 1: Generate Rules
        await self._generate_rules(table_name)
        
        # Stage 2: Execute Rules
        await self._execute_rules()
        
        # Stage 3: Evaluate Results
        report = await self._evaluate_results()
        
        return report

    async def _generate_rules(self, table_name: str) -> None:
        """Generate specific SQL rules for the column based on type and mode."""
        system_prompt = self._get_rule_generation_prompt(table_name)
        user_prompt = f"""
        Target Column: {self.column_name}
        Data Type: {self.dtype}
        Sample Values: {self.samples[:20]}
        
        Output EXACTLY a JSON array of rules. No markdown, no extra text.
        """
        
        try:
            response = await self.llm_service.generate(prompt=user_prompt, system_prompt=system_prompt)
            # Apply the universal model-agnostic sanitizer
            response = sanitize_llm_response(response)
            
            # Try extracting JSON from markdown fenced block first
            fenced_match = re.search(r'```(?:json)?\s*([\s\S]*?)\s*```', response, re.IGNORECASE)
            if fenced_match:
                raw_json = fenced_match.group(1).strip()
            else:
                # Fallback: find raw JSON array
                array_match = re.search(r'\[.*\]', response, re.DOTALL)
                raw_json = array_match.group(0) if array_match else None
            
            if raw_json:
                rules = json.loads(raw_json)
                self.rules_to_run = [r for r in rules if "query" in r and "rule_name" in r]
                logger.info(f"Generated {len(self.rules_to_run)} rules for {self.column_name}")
            else:
                logger.warning(f"Could not parse rule JSON for {self.column_name}: {response[:200]}")
        except json.JSONDecodeError as e:
            logger.error(f"JSON parse error for {self.column_name}: {e}. Response: {response[:300]}")
            self.rules_to_run = []
        except Exception as e:
            logger.error(f"Rule generation failed for {self.column_name}: {e}")
            self.rules_to_run = []

    async def _execute_rules(self) -> None:
        """Execute the generated rules via the provided DB executor engine."""
        for rule in self.rules_to_run:
            query = rule["query"]
            # To isolate, we just pass the query. The orchestrator's executor handles the rest.
            try:
                # Expected to return: {"status": "success/error", "row_count": N, "sample_rows": []}
                result = await self.engine_executor(query=query, q_type="sql")
                self.execution_results.append({
                    "rule_name": rule["rule_name"],
                    "severity": rule.get("severity", "warning"),
                    "query": query,
                    "result": result
                })
            except Exception as e:
                logger.error(f"Execution failed for {self.column_name} rule '{rule['rule_name']}': {e}")
                
    async def _evaluate_results(self) -> ColumnQualityReport:
        """Parse the results into standard ValidationResult dictionaries and score them."""
        parsed_results = []
        total_rules = 0
        passed_rules = 0
        warnings = 0
        criticals = 0
        
        for exec_data in self.execution_results:
            res = exec_data["result"]
            
            # Skip if the query flat out errored (e.g. invalid SQL, wrong column)
            if res.get("status") == "error":
                continue
                
            total_rules += 1
            failed_count = res.get("row_count", 0)
            
            # Severity mapping
            raw_severity = exec_data.get("severity", "warning").lower()
            severity_map = {"high": "critical", "critical": "critical", "medium": "warning", "warning": "warning", "low": "info", "info": "info"}
            severity = severity_map.get(raw_severity, "warning")
            
            status = "failed" if failed_count > 0 else "passed"
            
            if status == "passed":
                passed_rules += 1
            elif severity == "critical":
                criticals += 1
            else:
                warnings += 1

            parsed_results.append({
                "rule_name": f"{self.column_name}_{exec_data['rule_name']}",
                "status": status,
                "failed_count": failed_count,
                "failure_examples": res.get("sample_rows", [])[:5],
                "severity": severity,
                "rule_type": "column_agent_query",
                "executed_query": exec_data["query"]
            })
            
        # Score calculation weighting
        if total_rules == 0:
            score = 100.0
        else:
            # Full credit for passed, half credit for warnings, no credit for critical failures
            weighted_passes = passed_rules + (warnings * 0.5)
            score = (weighted_passes / total_rules) * 100
            
        return ColumnQualityReport(
            column_name=self.column_name,
            dtype=self.dtype,
            mode=self.mode,
            validation_results=parsed_results,
            score=round(score, 2),
            summary=f"Analyzed {total_rules} rules for {self.column_name}: {passed_rules} passed, {warnings} warnings, {criticals} critical."
        )

    def _get_rule_generation_prompt(self, table_name: str) -> str:
        """Get the specific rule generation prompt based on the validation mode."""
        base_instructions = f"""
        You are a Deep-Dive Data Quality Expert Agent computing validation queries for a single database column.
        Your ONLY job is to output a JSON array of comprehensive data quality rules structured as SQL queries.
        The SQL queries MUST return the INVALID rows (e.g. `SELECT * FROM {table_name} WHERE {self.column_name} IS NULL`).
        Make sure the queries use the generic table name `{table_name}`.
        
        Required JSON Array Format:
        [
            {{
                "rule_name": "{self.column_name}_NullCheck",
                "severity": "critical",
                "query": "SELECT * FROM {table_name} WHERE {self.column_name} IS NULL OR CAST({self.column_name} AS VARCHAR) TRIM() = ''"
            }}
        ]
        
        Ensure you only output the raw JSON array. Do not include markdown formatting or backticks around the JSON.
        """
        
        if self.mode in ("schema_only",):
            mode_instructions = """
            Strictly focus on SCHEMA validation. Generate 3-5 rules checking:
            - Explicit NULLs and empty strings.
            - Correct Type parsing (e.g. if numeric, check for characters).
            - Expected length and basic structural formatting (e.g., standard email/phone regex).
            DO NOT infer complex business logic.
            """
        elif self.mode in ("business_analysis", "business_intelligence", "bi"):
            mode_instructions = """
            Focus on Business Intelligence validity. Generate 4-7 rules checking:
            - Standard Schema checks (NULLs, blanks).
            - Domain logic bounds (e.g., numbers > 0, reasonable dates, valid categoricals).
            - Uniqueness if applicable.
            Look strictly at the Sample Values provided to infer the domain (e.g., if samples are active/inactive, check for values OUTSIDE that set).
            """
        else: # full_autonomous
            mode_instructions = """
            Running in FULL AUTONOMOUS mode. Generate 5-10 exhaustive rules checking:
            - Everything in schema and BI logic.
            - Statistical outlier detection.
            - Leading/trailing whitespace or hidden characters.
            - Suspicious zero-variance or extreme cardinality variations based on the samples.
            """

        return f"{base_instructions.strip()}\n\n{mode_instructions.strip()}"
