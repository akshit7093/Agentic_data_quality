"""LangGraph-based Data Quality Agent.

COMPLETE FIX v4 - Proper LangGraph state management and debug logging.

CRITICAL: In LangGraph, you MUST return ALL state changes in the return dictionary.
In-place modifications like state['key'] = value are NOT persisted between nodes!
"""
import json
import logging
from typing import Dict, Any, List, Optional
from datetime import datetime
from langgraph.graph import StateGraph, END
from langgraph.checkpoint.memory import MemorySaver

from app.agents.state import (
    AgentState, ValidationMode, AgentStatus,
    DataSourceInfo, ValidationRule, ValidationResult, DataProfile
)
from app.agents.llm_service import get_llm_service
from app.agents.rag_service import get_rag_service
from app.connectors.factory import ConnectorFactory

# ✅ Enable detailed logging
logger = logging.getLogger(__name__)


# System prompts for different stages
SCHEMA_ANALYSIS_PROMPT = """You are an expert data quality analyst. Analyze the provided data schema and sample data to understand:
1. The purpose and meaning of each column
2. Data types and formats
3. Potential quality issues
4. Relationships between columns
5. Business context

Be thorough but concise. Focus on actionable insights."""

RULE_GENERATION_PROMPT = """You are an expert data quality engineer. Based on the data profile and schema analysis, generate validation rules.

For each rule, provide:
- rule_name: A clear, descriptive name
- rule_type: One of: column, row, table, statistical, pattern
- severity: critical, warning, or info
- target_columns: List of columns this rule applies to
- config: Rule-specific configuration (e.g., min, max, pattern, etc.)
- rationale: Why this rule is important

Generate 5-10 rules that cover:
1. Completeness (null checks)
2. Validity (range, format, type checks)
3. Consistency (cross-field validations)
4. Uniqueness (duplicate detection)
5. Statistical anomalies (outliers)

Base all rules ONLY on the provided data. Do not assume external context.

IMPORTANT: Return ONLY valid JSON. No markdown, no code blocks, no explanation."""

ANALYSIS_PROMPT = """You are an expert data quality analyst. Analyze the validation results and provide insights.

For each failed validation:
1. Explain why it failed
2. Suggest root causes
3. Recommend fixes
4. Assess business impact

Also provide:
- Overall quality assessment
- Priority order for fixing issues
- Long-term recommendations"""


class DataQualityAgent:
    """LangGraph agent for data quality validation."""
    
    def __init__(self):
        self.llm_service = get_llm_service()
        self.graph = self._build_graph()
    
    def _build_graph(self) -> StateGraph:
        """Build the LangGraph workflow."""
        workflow = StateGraph(AgentState)
        
        # Add nodes
        workflow.add_node("connect_data_source", self._connect_data_source)
        workflow.add_node("profile_data", self._profile_data)
        workflow.add_node("retrieve_context", self._retrieve_context)
        workflow.add_node("analyze_schema", self._analyze_schema)
        workflow.add_node("generate_rules", self._generate_rules)
        workflow.add_node("execute_validations", self._execute_validations)
        workflow.add_node("analyze_results", self._analyze_results)
        workflow.add_node("generate_report", self._generate_report)
        
        # Add edges
        workflow.set_entry_point("connect_data_source")
        
        workflow.add_edge("connect_data_source", "profile_data")
        workflow.add_edge("profile_data", "retrieve_context")
        workflow.add_edge("retrieve_context", "analyze_schema")
        
        workflow.add_conditional_edges(
            "analyze_schema",
            self._should_generate_rules,
            {
                "generate_rules": "generate_rules",
                "skip_to_validation": "execute_validations"
            }
        )
        
        workflow.add_edge("generate_rules", "execute_validations")
        workflow.add_edge("execute_validations", "analyze_results")
        workflow.add_edge("analyze_results", "generate_report")
        workflow.add_edge("generate_report", END)
        
        return workflow.compile()
    
    def _should_generate_rules(self, state: AgentState) -> str:
        """Determine if AI rule generation is needed."""
        mode = state.get("validation_mode", ValidationMode.CUSTOM_RULES)
        if mode in [ValidationMode.AI_RECOMMENDED, ValidationMode.HYBRID]:
            return "generate_rules"
        return "skip_to_validation"
    
    async def _connect_data_source(self, state: AgentState) -> Dict[str, Any]:
        """Connect to data source and load sample data."""
        logger.info("=" * 60)
        logger.info("🔌 STEP 1: Connecting to data source...")
        logger.info(f"   Source type: {state['data_source_info'].source_type}")
        logger.info(f"   Target: {state['data_source_info'].target_path}")
        logger.info("=" * 60)
        
        try:
            connector = ConnectorFactory.create_connector(
                state['data_source_info'].source_type,
                state['data_source_info'].connection_config
            )
            
            await connector.connect()
            schema = await connector.get_schema(state['data_source_info'].target_path)
            
            sample_size = state.get('execution_config', {}).get('sample_size', 1000)
            sample_data = await connector.sample_data(
                state['data_source_info'].target_path,
                sample_size
            )
            
            row_count = await connector.get_row_count(state['data_source_info'].target_path)
            await connector.disconnect()
            
            # Update the dataclass
            state['data_source_info'].schema = schema
            state['data_source_info'].sample_data = sample_data
            state['data_source_info'].row_count = row_count
            state['data_source_info'].column_count = len(schema.get('columns', {}))
            
            logger.info(f"✅ CONNECTED: {row_count} rows, {len(schema.get('columns', {}))} columns")
            logger.info(f"   Sample data rows: {len(sample_data)}")
            
            # ✅ CRITICAL: Return ALL state changes
            return {
                "data_source_info": state['data_source_info'],
                "status": AgentStatus.PROFILING,
                "current_step": "profile_data",
                "messages": [{"role": "system", "content": f"Connected. Found {row_count} rows, {len(schema.get('columns', {}))} columns."}]
            }
            
        except Exception as e:
            logger.error(f"❌ CONNECTION FAILED: {str(e)}")
            return {
                "status": AgentStatus.ERROR,
                "error_message": f"Data source connection failed: {str(e)}",
                "messages": [{"role": "error", "content": str(e)}]
            }
    
    async def _profile_data(self, state: AgentState) -> Dict[str, Any]:
        """Profile the data to understand its characteristics."""
        logger.info("=" * 60)
        logger.info("📊 STEP 2: Profiling data...")
        logger.info("=" * 60)
        
        try:
            sample_data = state['data_source_info'].sample_data
            schema = state['data_source_info'].schema
            
            if not sample_data:
                raise ValueError("No sample data available for profiling")
            
            import pandas as pd
            df = pd.DataFrame(sample_data)
            
            column_profiles = {}
            patterns_detected = {}
            
            for col_name, col_info in schema.get('columns', {}).items():
                if col_name not in df.columns:
                    continue
                
                col_data = df[col_name]
                profile = {
                    'type': col_info.get('type', 'unknown'),
                    'null_count': int(col_data.isnull().sum()),
                    'null_percentage': float(col_data.isnull().sum() / len(col_data) * 100),
                    'unique_count': int(col_data.nunique()),
                    'unique_percentage': float(col_data.nunique() / len(col_data) * 100),
                }
                
                if pd.api.types.is_numeric_dtype(col_data):
                    profile.update({
                        'min': float(col_data.min()) if not pd.isna(col_data.min()) else None,
                        'max': float(col_data.max()) if not pd.isna(col_data.max()) else None,
                        'mean': float(col_data.mean()) if not pd.isna(col_data.mean()) else None,
                        'median': float(col_data.median()) if not pd.isna(col_data.median()) else None,
                        'std': float(col_data.std()) if not pd.isna(col_data.std()) else None,
                    })
                
                elif pd.api.types.is_string_dtype(col_data):
                    non_null = col_data.dropna()
                    if len(non_null) > 0:
                        profile.update({
                            'min_length': int(non_null.str.len().min()),
                            'max_length': int(non_null.str.len().max()),
                            'avg_length': float(non_null.str.len().mean()),
                        })
                        
                        patterns = []
                        if non_null.str.match(r'^[^@]+@[^@]+\.[^@]+$').any():
                            patterns.append('email')
                        if non_null.str.match(r'^\d{3}-\d{3}-\d{4}$').any():
                            patterns.append('phone_us')
                        if non_null.str.match(r'^\d{4}-\d{2}-\d{2}$').any():
                            patterns.append('date_iso')
                        if patterns:
                            patterns_detected[col_name] = patterns
                
                column_profiles[col_name] = profile
            
            data_profile = DataProfile(
                column_profiles=column_profiles,
                row_count=state['data_source_info'].row_count or len(sample_data),
                column_count=state['data_source_info'].column_count or len(column_profiles),
                patterns_detected=patterns_detected,
            )
            
            logger.info(f"✅ PROFILING COMPLETE: {len(column_profiles)} columns analyzed")
            logger.info(f"   Patterns detected: {patterns_detected}")
            
            # ✅ CRITICAL: Return ALL state changes
            return {
                "data_profile": data_profile,
                "status": AgentStatus.ANALYZING,
                "current_step": "retrieve_context",
                "messages": [{"role": "system", "content": f"Profiled {len(column_profiles)} columns."}]
            }
            
        except Exception as e:
            logger.error(f"❌ PROFILING FAILED: {str(e)}")
            return {
                "status": AgentStatus.ERROR,
                "error_message": f"Data profiling failed: {str(e)}",
                "messages": [{"role": "error", "content": str(e)}]
            }
    
    async def _retrieve_context(self, state: AgentState) -> Dict[str, Any]:
        """Retrieve relevant context using RAG."""
        logger.info("=" * 60)
        logger.info("🔍 STEP 3: Retrieving context (RAG)...")
        logger.info("=" * 60)
        
        try:
            rag_service = await get_rag_service()
            
            if state['data_source_info'].schema:
                await rag_service.add_schema_context(
                    source_id=state['data_source_info'].target_path,
                    schema=state['data_source_info'].schema,
                    sample_data=state['data_source_info'].sample_data,
                )
            
            context = await rag_service.get_relevant_context_for_validation(
                source_id=state['data_source_info'].target_path,
                schema=state['data_source_info'].schema or {},
            )
            
            logger.info("✅ CONTEXT RETRIEVED")
            
            # ✅ CRITICAL: Return ALL state changes
            return {
                "retrieved_context": [{"content": context}],
                "current_step": "analyze_schema",
                "messages": [{"role": "system", "content": "Context retrieved."}]
            }
            
        except Exception as e:
            logger.warning(f"⚠️ Context retrieval failed (continuing without): {str(e)}")
            return {
                "retrieved_context": [],
                "current_step": "analyze_schema",
                "messages": [{"role": "warning", "content": f"Context retrieval failed: {str(e)}"}]
            }
    
    async def _analyze_schema(self, state: AgentState) -> Dict[str, Any]:
        """Analyze schema using LLM."""
        logger.info("=" * 60)
        logger.info("🧠 STEP 4: Analyzing schema with LLM...")
        logger.info("=" * 60)
        
        try:
            schema_json = json.dumps(state['data_source_info'].schema, indent=2, default=str)
            
            data_profile = state.get('data_profile')
            if data_profile and data_profile.column_profiles:
                profile_json = json.dumps({
                    col: {k: v for k, v in info.items() if v is not None}
                    for col, info in data_profile.column_profiles.items()
                }, indent=2, default=str)
                patterns = json.dumps(data_profile.patterns_detected)
            else:
                profile_json = '{"note": "Data profiling was not available"}'
                patterns = '[]'
            
            prompt = f"""Analyze this data schema and profile:

Schema:
{schema_json}

Data Profile:
{profile_json}

Patterns Detected: {patterns}

Provide a concise analysis of:
1. What this data represents
2. Key quality concerns
3. Column relationships
4. Potential business rules"""
            
            if state.get('retrieved_context'):
                prompt = state['retrieved_context'][0]['content'] + "\n\n" + prompt
            
            analysis = await self.llm_service.generate(
                prompt=prompt,
                system_prompt=SCHEMA_ANALYSIS_PROMPT,
            )
            
            logger.info("✅ SCHEMA ANALYSIS COMPLETE")
            logger.debug(f"Analysis preview: {analysis[:200]}...")
            
            # ✅ CRITICAL: Return ALL state changes
            return {
                "current_step": "generate_rules",
                "messages": [{"role": "assistant", "content": analysis}]
            }
            
        except Exception as e:
            logger.error(f"❌ SCHEMA ANALYSIS FAILED: {str(e)}")
            return {
                "current_step": "generate_rules",
                "messages": [{"role": "error", "content": f"Schema analysis failed: {str(e)}"}]
            }
    
    async def _generate_rules(self, state: AgentState) -> Dict[str, Any]:
        """Generate validation rules using LLM."""
        logger.info("=" * 60)
        logger.info("⚙️ STEP 5: Generating validation rules with LLM...")
        logger.info("=" * 60)
        
        try:
            data_profile = state.get('data_profile')
            if data_profile and data_profile.column_profiles:
                schema_summary = {
                    "columns": {
                        name: {
                            "type": info.get('type'),
                            "null_percentage": info.get('null_percentage'),
                            "unique_percentage": info.get('unique_percentage'),
                        }
                        for name, info in data_profile.column_profiles.items()
                    }
                }
                patterns = json.dumps(data_profile.patterns_detected)
            else:
                schema_summary = state['data_source_info'].schema or {"columns": {}}
                patterns = '[]'
            
            prompt = f"""Generate validation rules for this data:

Schema Summary:
{json.dumps(schema_summary, indent=2)}

Patterns Detected: {patterns}

Generate rules in this JSON format:
{{
  "rules": [
    {{
      "rule_name": "string",
      "rule_type": "column|row|table|statistical|pattern",
      "severity": "critical|warning|info",
      "target_columns": ["column_name"],
      "config": {{}},
      "rationale": "string"
    }}
  ]
}}

IMPORTANT: Return ONLY the JSON object above. No markdown, no code blocks, no explanations."""

            if state.get('retrieved_context'):
                prompt = state['retrieved_context'][0]['content'] + "\n\n" + prompt
            
            output_schema = {
                "type": "object",
                "properties": {
                    "rules": {
                        "type": "array",
                        "items": {
                            "type": "object",
                            "properties": {
                                "rule_name": {"type": "string"},
                                "rule_type": {"type": "string"},
                                "severity": {"type": "string"},
                                "target_columns": {"type": "array", "items": {"type": "string"}},
                                "config": {"type": "object"},
                                "rationale": {"type": "string"},
                            },
                            "required": ["rule_name", "rule_type", "severity", "target_columns"]
                        }
                    }
                },
                "required": ["rules"]
            }
            
            logger.info("📤 Sending rule generation prompt to LLM...")
            
            result = await self.llm_service.generate_structured(
                prompt=prompt,
                output_schema=output_schema,
                system_prompt=RULE_GENERATION_PROMPT,
            )
            
            # ✅ DEBUG: Log what we got
            rules_count = len(result.get('rules', []))
            logger.info(f"📥 LLM RETURNED {rules_count} RULES")
            
            if rules_count == 0:
                logger.warning("⚠️ NO RULES GENERATED!")
                logger.debug(f"Full LLM result: {result}")
            else:
                for i, rule in enumerate(result.get('rules', []), 1):
                    logger.info(f"   Rule {i}: {rule.get('rule_name')} ({rule.get('severity')})")
            
            # Convert to ValidationRule objects
            ai_rules = []
            for rule_data in result.get('rules', []):
                try:
                    ai_rules.append(ValidationRule(
                        id=None,
                        name=rule_data['rule_name'],
                        rule_type=rule_data['rule_type'],
                        severity=rule_data['severity'],
                        target_columns=rule_data['target_columns'],
                        config=rule_data.get('config', {}),
                        is_ai_generated=True,
                        ai_rationale=rule_data.get('rationale'),
                    ))
                except KeyError as e:
                    logger.warning(f"⚠️ Skipping malformed rule: missing {e}")
                    continue
            
            # Combine with custom rules if hybrid mode
            custom_rules = state.get('custom_rules', [])
            if state['validation_mode'] == ValidationMode.HYBRID:
                all_rules = custom_rules + ai_rules
            else:
                all_rules = ai_rules
            
            logger.info(f"✅ RULE GENERATION COMPLETE")
            logger.info(f"   AI rules: {len(ai_rules)}")
            logger.info(f"   Total rules to execute: {len(all_rules)}")
            
            # ✅✅✅ CRITICAL FIX: Return ALL state changes!
            return {
                "ai_recommended_rules": ai_rules,
                "all_rules": all_rules,
                "status": AgentStatus.VALIDATING,
                "current_step": "execute_validations",
                "messages": [{"role": "assistant", "content": f"Generated {len(ai_rules)} AI rules."}]
            }
            
        except Exception as e:
            logger.error(f"❌ RULE GENERATION FAILED: {str(e)}")
            logger.exception("Full exception details:")
            
            # ✅ CRITICAL: Return ALL state changes even on error
            return {
                "all_rules": state.get('custom_rules', []),
                "error_message": f"Rule generation failed: {str(e)}",
                "status": AgentStatus.VALIDATING,
                "current_step": "execute_validations",
                "messages": [{"role": "error", "content": f"Rule generation failed: {str(e)}"}]
            }
    
    async def _execute_validations(self, state: AgentState) -> Dict[str, Any]:
        """Execute validation rules."""
        all_rules = state.get('all_rules', [])
        
        logger.info("=" * 60)
        logger.info("🚀 STEP 6: Executing validation rules...")
        logger.info(f"   Number of rules to execute: {len(all_rules)}")
        logger.info("=" * 60)
        
        # ✅ DEBUG: Log what we actually have
        if not all_rules:
            logger.warning("⚠️ NO RULES TO EXECUTE!")
            logger.warning("   This means rules were not passed from generate_rules step")
            logger.debug(f"   State keys: {state.keys()}")
            return {
                "validation_results": [],
                "current_step": "analyze_results",
                "messages": [{"role": "warning", "content": "No rules to execute"}]
            }
        
        # Log each rule
        for i, rule in enumerate(all_rules, 1):
            logger.info(f"   Rule {i}: {rule.name} ({rule.severity}) - {rule.target_columns}")
        
        from app.validation.engine import ValidationEngine
        
        try:
            engine = ValidationEngine()
            
            results = await engine.execute_rules(
                rules=all_rules,
                sample_data=state['data_source_info'].sample_data,
                schema=state['data_source_info'].schema,
            )
            
            passed = sum(1 for r in results if r.status == 'passed')
            failed = sum(1 for r in results if r.status == 'failed')
            
            logger.info(f"✅ VALIDATION COMPLETE")
            logger.info(f"   Passed: {passed}")
            logger.info(f"   Failed: {failed}")
            
            # ✅ CRITICAL: Return ALL state changes
            return {
                "validation_results": results,
                "current_step": "analyze_results",
                "messages": [{"role": "system", "content": f"Validation complete. Passed: {passed}, Failed: {failed}"}]
            }
            
        except Exception as e:
            logger.error(f"❌ VALIDATION EXECUTION FAILED: {str(e)}")
            logger.exception("Full exception details:")
            return {
                "status": AgentStatus.ERROR,
                "error_message": f"Validation execution failed: {str(e)}",
                "messages": [{"role": "error", "content": str(e)}]
            }
    
    async def _analyze_results(self, state: AgentState) -> Dict[str, Any]:
        """Analyze validation results using LLM."""
        logger.info("=" * 60)
        logger.info("📈 STEP 7: Analyzing validation results...")
        logger.info("=" * 60)
        
        validation_results = state.get('validation_results', [])
        
        if not validation_results:
            logger.warning("⚠️ No validation results to analyze")
            return {
                "current_step": "generate_report",
                "messages": [{"role": "warning", "content": "No results to analyze"}]
            }
        
        try:
            failed_results = [r for r in validation_results if r.status == 'failed']
            
            if not failed_results:
                logger.info("✅ All validations passed!")
                return {
                    "current_step": "generate_report",
                    "messages": [{"role": "assistant", "content": "All validations passed!"}]
                }
            
            results_summary = [
                {
                    "rule_name": r.rule_name,
                    "failure_percentage": r.failure_percentage,
                    "failure_examples": r.failure_examples[:3] if r.failure_examples else [],
                }
                for r in failed_results
            ]
            
            prompt = f"""Analyze these validation failures:

{json.dumps(results_summary, indent=2, default=str)}

Provide:
1. Summary of key issues
2. Root cause analysis
3. Recommended fixes
4. Priority order"""
            
            analysis = await self.llm_service.generate(
                prompt=prompt,
                system_prompt=ANALYSIS_PROMPT,
            )
            
            # Add AI insights to results
            for result in validation_results:
                if result.status == 'failed':
                    result.ai_insights = analysis[:500]
            
            logger.info("✅ RESULTS ANALYSIS COMPLETE")
            
            # ✅ CRITICAL: Return ALL state changes
            return {
                "validation_results": validation_results,
                "current_step": "generate_report",
                "messages": [{"role": "assistant", "content": analysis}]
            }
            
        except Exception as e:
            logger.error(f"❌ RESULT ANALYSIS FAILED: {str(e)}")
            return {
                "current_step": "generate_report",
                "messages": [{"role": "error", "content": f"Analysis failed: {str(e)}"}]
            }
    
    async def _generate_report(self, state: AgentState) -> Dict[str, Any]:
        """Generate final quality report."""
        logger.info("=" * 60)
        logger.info("📄 STEP 8: Generating quality report...")
        logger.info("=" * 60)
        
        try:
            validation_results = state.get('validation_results', [])
            
            total_rules = len(validation_results)
            passed_rules = sum(1 for r in validation_results if r.status == 'passed')
            failed_rules = sum(1 for r in validation_results if r.status == 'failed')
            warning_rules = sum(1 for r in validation_results if r.status == 'warning')
            
            # ✅ FIX: Use getattr to safely access severity
            critical_failed = sum(
                1 for r in validation_results 
                if r.status == 'failed' and getattr(r, 'severity', 'info') == 'critical'
            )

            if total_rules > 0:
                base_score = (passed_rules / total_rules) * 100
                quality_score = max(0, base_score - (critical_failed * 20) - (warning_rules * 5))
            else:
                quality_score = 0
                logger.warning("⚠️ No rules were validated - quality score is 0")
            
            quality_score = round(quality_score, 2)

            summary_report = {
                "quality_score": quality_score,
                "total_rules": total_rules,
                "passed_rules": passed_rules,
                "failed_rules": failed_rules,
                "warning_rules": warning_rules,
                "critical_issues": critical_failed,
                "validation_mode": state['validation_mode'],
                "data_source": state['data_source_info'].target_path,
                "records_processed": state['data_source_info'].row_count,
                "execution_time_ms": state.get('execution_metrics', {}).get('total_time_ms', 0),
            }
            
            logger.info("=" * 60)
            logger.info("📊 FINAL REPORT")
            logger.info("=" * 60)
            logger.info(f"   Quality Score: {quality_score}%")
            logger.info(f"   Total Rules: {total_rules}")
            logger.info(f"   Passed: {passed_rules}")
            logger.info(f"   Failed: {failed_rules}")
            logger.info(f"   Warnings: {warning_rules}")
            logger.info(f"   Critical Issues: {critical_failed}")
            logger.info("=" * 60)
            
            # ✅ CRITICAL: Return ALL state changes
            return {
                "quality_score": quality_score,
                "summary_report": summary_report,
                "status": AgentStatus.COMPLETED,
                "completed_at": datetime.utcnow().isoformat(),
                "messages": [{"role": "system", "content": f"Report generated. Quality Score: {quality_score}"}]
            }
            
        except Exception as e:
            logger.error(f"❌ REPORT GENERATION FAILED: {str(e)}")
            logger.exception("Full exception details:")
            return {
                "status": AgentStatus.ERROR,
                "error_message": f"Report generation failed: {str(e)}",
                "messages": [{"role": "error", "content": str(e)}]
            }
    
    async def run(
        self,
        validation_id: str,
        validation_mode: ValidationMode,
        data_source_info: DataSourceInfo,
        custom_rules: Optional[List[ValidationRule]] = None,
        execution_config: Optional[Dict[str, Any]] = None,
    ) -> AgentState:
        """Run the data quality agent."""
        
        logger.info("\n" + "🚀" * 30)
        logger.info("STARTING DATA QUALITY VALIDATION")
        logger.info(f"   Validation ID: {validation_id}")
        logger.info(f"   Mode: {validation_mode}")
        logger.info(f"   Target: {data_source_info.target_path}")
        logger.info("🚀" * 30 + "\n")
        
        initial_state: AgentState = {
            "validation_id": validation_id,
            "validation_mode": validation_mode,
            "data_source_info": data_source_info,
            "custom_rules": custom_rules or [],
            "execution_config": execution_config or {},
            "status": AgentStatus.IDLE,
            "current_step": "connect_data_source",
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
        }
        
        result = await self.graph.ainvoke(initial_state)
        
        logger.info("\n" + "🏁" * 30)
        logger.info(f"VALIDATION COMPLETE: {result.get('status')}")
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