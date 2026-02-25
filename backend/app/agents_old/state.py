"""LangGraph agent state definitions."""
from typing import List, Dict, Any, Optional, TypedDict, Annotated
from dataclasses import dataclass, field
from enum import Enum
import operator


class ValidationMode(str, Enum):
    """Validation execution modes."""
    CUSTOM_RULES = "custom_rules"
    AI_RECOMMENDED = "ai_recommended"
    HYBRID = "hybrid"


class AgentStatus(str, Enum):
    """Agent execution status."""
    IDLE = "idle"
    CONNECTING = "connecting"
    PROFILING = "profiling"
    ANALYZING = "analyzing"
    VALIDATING = "validating"
    REPORTING = "reporting"
    COMPLETED = "completed"
    ERROR = "error"


@dataclass
class DataSourceInfo:
    """Data source information."""
    source_type: str
    connection_config: Dict[str, Any]
    target_path: str
    schema: Optional[Dict[str, Any]] = None
    sample_data: Optional[List[Dict]] = None
    row_count: Optional[int] = None
    column_count: Optional[int] = None


@dataclass
class ValidationRule:
    """Validation rule definition."""
    id: Optional[str]
    name: str
    rule_type: str  # column, row, table, statistical, custom_sql, ai_generated
    severity: str  # info, warning, critical
    target_columns: List[str]
    config: Dict[str, Any]
    expression: Optional[str] = None
    is_ai_generated: bool = False
    ai_confidence: Optional[float] = None
    ai_rationale: Optional[str] = None


@dataclass
class ValidationResult:
    """Individual validation result."""
    rule_id: str
    rule_name: str
    status: str  # passed, failed, warning, error
    passed_count: int
    failed_count: int
    failure_examples: List[Dict] = field(default_factory=list)
    failure_percentage: float = 0.0
    execution_time_ms: int = 0
    ai_insights: Optional[str] = None
    ai_suggestions: List[str] = field(default_factory=list)


@dataclass
class DataProfile:
    """Data profiling results."""
    column_profiles: Dict[str, Dict[str, Any]]
    row_count: int
    column_count: int
    patterns_detected: Dict[str, List[str]] = field(default_factory=dict)
    anomalies_detected: List[Dict] = field(default_factory=list)


class AgentState(TypedDict):
    """LangGraph agent state."""
    # Input
    validation_id: str
    validation_mode: ValidationMode
    data_source_info: DataSourceInfo
    custom_rules: List[ValidationRule]
    execution_config: Dict[str, Any]
    
    # Processing state
    status: AgentStatus
    current_step: str
    messages: Annotated[List[Dict[str, Any]], operator.add]
    
    # Data
    data_profile: Optional[DataProfile]
    ai_recommended_rules: List[ValidationRule]
    all_rules: List[ValidationRule]
    validation_results: List[ValidationResult]
    
    # Context for RAG
    retrieved_context: List[Dict[str, Any]]
    
    # Output
    quality_score: Optional[float]
    summary_report: Optional[Dict[str, Any]]
    error_message: Optional[str]
    
    # Metadata
    started_at: Optional[str]
    completed_at: Optional[str]
    execution_metrics: Dict[str, Any]
