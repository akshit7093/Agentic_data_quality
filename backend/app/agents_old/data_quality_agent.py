"""LangGraph-based Data Quality Agent."""
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

Base all rules ONLY on the provided data. Do not assume external context."""

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
        # Create graph
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
        
        # Conditional edge based on validation mode
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
        
        # Compile graph with memory checkpointing
        memory = MemorySaver()
        return workflow.compile(checkpointer=memory)
    
    def _should_generate_rules(self, state: AgentState) -> str:
        """Determine if AI rule generation is needed."""
        mode = state.get("validation_mode", ValidationMode.CUSTOM_RULES)
        
        if mode in [ValidationMode.AI_RECOMMENDED, ValidationMode.HYBRID]:
            return "generate_rules"
        return "skip_to_validation"
    
    async def _connect_data_source(self, state: AgentState) -> Dict[str, Any]:
        """Connect to data source and load sample data."""
        logger.info(f"Connecting to data source: {state['data_source_info'].source_type}")
        
        try:
            # Create connector
            connector = ConnectorFactory.create_connector(
                state['data_source_info'].source_type,
                state['data_source_info'].connection_config
            )
            
            # Connect and get schema
            await connector.connect()
            schema = await connector.get_schema(state['data_source_info'].target_path)
            
            # Sample data
            sample_size = state.get('execution_config', {}).get('sample_size', 1000)
            sample_data = await connector.sample_data(
                state['data_source_info'].target_path,
                sample_size
            )
            
            # Get row count
            row_count = await connector.get_row_count(state['data_source_info'].target_path)
            
            await connector.disconnect()
            
            # Update state
            state['data_source_info'].schema = schema
            state['data_source_info'].sample_data = sample_data
            state['data_source_info'].row_count = row_count
            state['data_source_info'].column_count = len(schema.get('columns', {}))
            state['status'] = AgentStatus.PROFILING
            state['current_step'] = 'profile_data'
            
            return {"messages": [{"role": "system", "content": f"Connected to data source. Found {row_count} rows, {len(schema.get('columns', {}))} columns."}]}
            
        except Exception as e:
            logger.error(f"Failed to connect to data source: {str(e)}")
            state['status'] = AgentStatus.ERROR
            state['error_message'] = f"Data source connection failed: {str(e)}"
            return {"messages": [{"role": "error", "content": str(e)}]}
    
    async def _profile_data(self, state: AgentState) -> Dict[str, Any]:
        """Profile the data to understand its characteristics."""
        logger.info("Profiling data...")
        
        try:
            sample_data = state['data_source_info'].sample_data
            schema = state['data_source_info'].schema
            
            if not sample_data:
                raise ValueError("No sample data available for profiling")
            
            # Import pandas for profiling
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
                
                # Numeric column statistics
                if pd.api.types.is_numeric_dtype(col_data):
                    profile.update({
                        'min': float(col_data.min()) if not pd.isna(col_data.min()) else None,
                        'max': float(col_data.max()) if not pd.isna(col_data.max()) else None,
                        'mean': float(col_data.mean()) if not pd.isna(col_data.mean()) else None,
                        'median': float(col_data.median()) if not pd.isna(col_data.median()) else None,
                        'std': float(col_data.std()) if not pd.isna(col_data.std()) else None,
                    })
                
                # String column statistics
                elif pd.api.types.is_string_dtype(col_data):
                    non_null = col_data.dropna()
                    if len(non_null) > 0:
                        profile.update({
                            'min_length': int(non_null.str.len().min()),
                            'max_length': int(non_null.str.len().max()),
                            'avg_length': float(non_null.str.len().mean()),
                        })
                        
                        # Detect patterns
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
            
            # Create data profile
            state['data_profile'] = DataProfile(
                column_profiles=column_profiles,
                row_count=state['data_source_info'].row_count or len(sample_data),
                column_count=state['data_source_info'].column_count or len(column_profiles),
                patterns_detected=patterns_detected,
            )
            
            state['status'] = AgentStatus.ANALYZING
            state['current_step'] = 'retrieve_context'
            
            return {"messages": [{"role": "system", "content": f"Data profiling complete. Analyzed {len(column_profiles)} columns."}]}
            
        except Exception as e:
            logger.error(f"Data profiling failed: {str(e)}")
            state['status'] = AgentStatus.ERROR
            state['error_message'] = f"Data profiling failed: {str(e)}"
            return {"messages": [{"role": "error", "content": str(e)}]}
    
    async def _retrieve_context(self, state: AgentState) -> Dict[str, Any]:
        """Retrieve relevant context using RAG."""
        logger.info("Retrieving context...")
        
        try:
            rag_service = await get_rag_service()
            
            # Add current schema to context store
            if state['data_source_info'].schema:
                await rag_service.add_schema_context(
                    source_id=state['data_source_info'].target_path,
                    schema=state['data_source_info'].schema,
                    sample_data=state['data_source_info'].sample_data,
                )
            
            # Retrieve relevant context
            context = await rag_service.get_relevant_context_for_validation(
                source_id=state['data_source_info'].target_path,
                schema=state['data_source_info'].schema or {},
            )
            
            state['retrieved_context'] = [{"content": context}]
            state['current_step'] = 'analyze_schema'
            
            return {"messages": [{"role": "system", "content": "Context retrieved successfully."}]}
            
        except Exception as e:
            logger.warning(f"Context retrieval failed (continuing without): {str(e)}")
            state['retrieved_context'] = []
            state['current_step'] = 'analyze_schema'
            return {"messages": [{"role": "warning", "content": f"Context retrieval failed: {str(e)}"}]}
    
    async def _analyze_schema(self, state: AgentState) -> Dict[str, Any]:
        """Analyze schema using LLM."""
        logger.info("Analyzing schema with LLM...")
        
        try:
            # Prepare prompt
            schema_json = json.dumps(state['data_source_info'].schema, indent=2, default=str)
            
            # Guard against None data_profile
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
            
            # Add retrieved context if available
            if state['retrieved_context']:
                prompt = state['retrieved_context'][0]['content'] + "\n\n" + prompt
            
            analysis = await self.llm_service.generate(
                prompt=prompt,
                system_prompt=SCHEMA_ANALYSIS_PROMPT,
            )
            
            state['messages'].append({"role": "assistant", "content": analysis})
            state['current_step'] = 'generate_rules'
            
            return {"messages": [{"role": "assistant", "content": analysis}]}
            
        except Exception as e:
            logger.error(f"Schema analysis failed: {str(e)}")
            state['current_step'] = 'generate_rules'
            return {"messages": [{"role": "error", "content": f"Schema analysis failed: {str(e)}"}]}
    
    # logger.info(f"DEBUG: Raw LLM response (first 300 chars): {result[:300]}")
    # logger.info(f"DEBUG: Parsed rule count: {len(ai_rules)}")
    # if not ai_rules:
    #     logger.warning("⚠️ NO RULES GENERATED - Check LLM output format!")
    
    async def _generate_rules(self, state: AgentState) -> Dict[str, Any]:
        """Generate validation rules using LLM."""
        logger.info("Generating validation rules with LLM...")
        
        try:
            # Guard against None data_profile
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
                # Fall back to raw schema if profiling failed
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
}}"""
            
            # Add retrieved context
            if state['retrieved_context']:
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
            
            result = await self.llm_service.generate_structured(
                prompt=prompt,
                output_schema=output_schema,
                system_prompt=RULE_GENERATION_PROMPT,
            )
            
            # Convert to ValidationRule objects
            ai_rules = []
            for rule_data in result.get('rules', []):
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
            
            state['ai_recommended_rules'] = ai_rules
            
            # Combine with custom rules if hybrid mode
            if state['validation_mode'] == ValidationMode.HYBRID:
                state['all_rules'] = state['custom_rules'] + ai_rules
            else:
                state['all_rules'] = ai_rules
            
            state['status'] = AgentStatus.VALIDATING
            state['current_step'] = 'execute_validations'
            
            return {"messages": [{"role": "assistant", "content": f"Generated {len(ai_rules)} AI rules."}]}
            
        except Exception as e:
            logger.error(f"Rule generation failed: {str(e)}")
            # Fall back to custom rules only
            state['all_rules'] = state['custom_rules']
            state['status'] = AgentStatus.VALIDATING
            state['current_step'] = 'execute_validations'
            return {"messages": [{"role": "error", "content": f"Rule generation failed: {str(e)}"}]}
    
    async def _execute_validations(self, state: AgentState) -> Dict[str, Any]:
        """Execute validation rules."""
        logger.info(f"Executing {len(state['all_rules'])} validation rules...")
        
        from app.validation.engine import ValidationEngine
        
        try:
            engine = ValidationEngine()
            
            results = await engine.execute_rules(
                rules=state['all_rules'],
                sample_data=state['data_source_info'].sample_data,
                schema=state['data_source_info'].schema,
            )
            
            state['validation_results'] = results
            state['current_step'] = 'analyze_results'
            
            passed = sum(1 for r in results if r.status == 'passed')
            failed = sum(1 for r in results if r.status == 'failed')
            
            return {"messages": [{"role": "system", "content": f"Validation complete. Passed: {passed}, Failed: {failed}"}]}
            
        except Exception as e:
            logger.error(f"Validation execution failed: {str(e)}")
            state['status'] = AgentStatus.ERROR
            state['error_message'] = f"Validation execution failed: {str(e)}"
            return {"messages": [{"role": "error", "content": str(e)}]}
    
    async def _analyze_results(self, state: AgentState) -> Dict[str, Any]:
        """Analyze validation results using LLM."""
        logger.info("Analyzing validation results...")
        
        try:
            # Prepare results summary
            failed_results = [r for r in state['validation_results'] if r.status == 'failed']
            
            if not failed_results:
                state['messages'].append({"role": "assistant", "content": "All validations passed! No issues detected."})
                state['current_step'] = 'generate_report'
                return {"messages": [{"role": "assistant", "content": "All validations passed!"}]}
            
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
            for result in state['validation_results']:
                if result.status == 'failed':
                    result.ai_insights = analysis[:500]  # Truncate for storage
            
            state['messages'].append({"role": "assistant", "content": analysis})
            state['current_step'] = 'generate_report'
            
            return {"messages": [{"role": "assistant", "content": analysis}]}
            
        except Exception as e:
            logger.error(f"Result analysis failed: {str(e)}")
            state['current_step'] = 'generate_report'
            return {"messages": [{"role": "error", "content": f"Analysis failed: {str(e)}"}]}
    
    async def _generate_report(self, state: AgentState) -> Dict[str, Any]:
        """Generate final quality report."""
        logger.info("Generating quality report...")
        
        try:
            # Calculate quality score
            total_rules = len(state['validation_results'])
            passed_rules = sum(1 for r in state['validation_results'] if r.status == 'passed')
            critical_failed = sum(1 for r in state['validation_results'] if r.status == 'failed' and r.severity == 'critical')
            warning_rules = sum(1 for r in state['validation_results'] if r.status == 'warning')

            if total_rules > 0:
                base_score = (passed_rules / total_rules) * 100
                # Penalize critical failures more heavily and warnings moderately
                quality_score = max(0, base_score - (critical_failed * 20) - (warning_rules * 5))
            else:
                quality_score = 0
            
            state['quality_score'] = round(quality_score, 2)
            state['status'] = AgentStatus.COMPLETED
            state['completed_at'] = datetime.utcnow().isoformat()
            
            # Count different types of issues
            failed_rules = sum(1 for r in state['validation_results'] if r.status == 'failed')
            warning_rules = sum(1 for r in state['validation_results'] if r.status == 'warning')

            # Build summary report
            state['summary_report'] = {
                "quality_score": state['quality_score'],
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
            
            return {"messages": [{"role": "system", "content": f"Report generated. Quality Score: {state['quality_score']}"}]}
            
        except Exception as e:
            logger.error(f"Report generation failed: {str(e)}")
            state['status'] = AgentStatus.ERROR
            state['error_message'] = f"Report generation failed: {str(e)}"
            return {"messages": [{"role": "error", "content": str(e)}]}
    
    async def run(
        self,
        validation_id: str,
        validation_mode: ValidationMode,
        data_source_info: DataSourceInfo,
        custom_rules: Optional[List[ValidationRule]] = None,
        execution_config: Optional[Dict[str, Any]] = None,
    ) -> AgentState:
        """Run the data quality agent."""
        
        # Initialize state
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
        
        # Run graph
        config = {"configurable": {"thread_id": validation_id}}
        result = await self.graph.ainvoke(initial_state, config=config)
        
        return result


# Singleton instance
_agent: Optional[DataQualityAgent] = None


def get_data_quality_agent() -> DataQualityAgent:
    """Get agent singleton."""
    global _agent
    if _agent is None:
        _agent = DataQualityAgent()
    return _agent
