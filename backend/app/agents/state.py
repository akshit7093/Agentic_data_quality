"""LangGraph agent state definitions — v8
Fixed: exploration_steps, validation_steps (required by agent routing guards)
Added: fix_recommendations, export_config, export_result (for export & quick-fix features)
Added: total_count, data_source to ValidationResult (for accurate reporting)
"""
from typing import List, Dict, Any, Optional, TypedDict, Annotated
from dataclasses import dataclass, field
from enum import Enum
import operator


class ValidationMode(str, Enum):
    """Validation mode selection."""
    CUSTOM_RULES   = "custom_rules"
    AI_RECOMMENDED = "ai_recommended"
    HYBRID         = "hybrid"


class AgentStatus(str, Enum):
    """Agent execution status."""
    IDLE       = "idle"
    CONNECTING = "connecting"
    EXPLORING  = "exploring"
    PROFILING  = "profiling"
    ANALYZING  = "analyzing"
    VALIDATING = "validating"
    REPORTING  = "reporting"
    COMPLETED  = "completed"
    ERROR      = "error"


class ExportFormat(str, Enum):
    """Supported export formats."""
    CSV      = "csv"
    EXCEL    = "excel"
    JSON     = "json"
    DATABASE = "database"
    PARQUET  = "parquet"


class FixAction(str, Enum):
    """User action for fix recommendations."""
    MANUAL     = "manual"
    AUTO_AGENT = "auto_agent"
    SKIP       = "skip"


@dataclass
class DataSourceInfo:
    """Data source connection and metadata information."""
    source_type: str
    connection_config: Dict[str, Any]
    target_path: str
    schema: Optional[Dict[str, Any]] = None
    sample_data: Optional[List[Dict]] = None   # exploration sample only (≤200 rows)
    row_count: Optional[int] = None            # real count from full table
    column_count: Optional[int] = None
    full_scan_requested: bool = False
    full_scan_used: bool = False


@dataclass
class ValidationRule:
    """Data quality validation rule definition."""
    id: Optional[str]
    name: str
    rule_type: str
    severity: str
    target_columns: List[str]
    config: Dict[str, Any]
    query: Optional[str] = None
    query_type: Optional[str] = None
    expression: Optional[str] = None
    is_ai_generated: bool = False
    ai_confidence: Optional[float] = None
    ai_rationale: Optional[str] = None


@dataclass
class ValidationResult:
    rule_id: str
    rule_name: str
    status: str                        # passed | failed | warning | error
    passed_count: int
    failed_count: int
    total_count: int = 0               # ← NEW: Total rows checked (from full dataset)
    failure_examples: List[Dict] = field(default_factory=list)
    failure_percentage: float = 0.0
    execution_time_ms: int = 0
    ai_insights: Optional[str] = None
    ai_suggestions: List[str] = field(default_factory=list)
    severity: str = "info"
    rule_type: Optional[str] = None
    executed_query: Optional[str] = None
    data_source: str = "full_dataset"  # ← NEW: sample | full_dataset


@dataclass
class FixRecommendation:
    """Recommended fix for a failed validation rule."""
    rule_id: str
    issue_description: str
    recommended_fix: str
    fix_query: Optional[str] = None
    estimated_rows_affected: int = 0
    risk_level: str = "low"              # low | medium | high
    user_action: FixAction = FixAction.MANUAL


@dataclass
class ExportConfig:
    """Configuration for data export."""
    format: ExportFormat
    include_failed_rows: bool = True
    include_passed_rows: bool = False
    include_metadata: bool = True
    output_path: Optional[str] = None


@dataclass
class DataProfile:
    """Statistical profile of the dataset."""
    column_profiles: Dict[str, Dict[str, Any]]
    row_count: int
    column_count: int
    patterns_detected: Dict[str, List[str]] = field(default_factory=dict)
    anomalies_detected: List[Dict] = field(default_factory=list)


class AgentState(TypedDict):
    """
    LangGraph agent state definition.
    
    This TypedDict defines all fields that can be read/written by agent nodes.
    All fields must be declared here or LangGraph will reject state updates.
    """
    # ==========================================
    # INPUT PARAMETERS
    # ==========================================
    validation_id: str
    validation_mode: ValidationMode
    data_source_info: DataSourceInfo
    custom_rules: List[Any]
    execution_config: Dict[str, Any]
    
    # ==========================================
    # PROCESSING STATE
    # ==========================================
    status: AgentStatus
    current_step: str
    messages: Annotated[List[Dict[str, Any]], operator.add]   # append-only
    
    # ==========================================
    # DATA & CONTEXT
    # ==========================================
    data_profile: Optional[DataProfile]
    ai_recommended_rules: List[Any]
    all_rules: List[Any]
    validation_results: List[Any]
    retrieved_context: List[Dict[str, Any]]
    
    # ==========================================
    # FIX & EXPORT (NEW - v8)
    # ==========================================
    fix_recommendations: List[FixRecommendation]
    export_config: Optional[ExportConfig]
    export_result: Optional[Dict[str, Any]]
    
    # ==========================================
    # OUTPUT RESULTS
    # ==========================================
    quality_score: Optional[float]
    summary_report: Optional[Dict[str, Any]]
    error_message: Optional[str]
    
    # ==========================================
    # TIMING & METRICS
    # ==========================================
    started_at: Optional[str]
    completed_at: Optional[str]
    execution_metrics: Dict[str, Any]
    
    # ==========================================
    # ITERATION COUNTERS (FIXED - v8)
    # These were missing in v6, causing LangGraph key errors
    # ==========================================
    exploration_steps: int
    validation_steps: int