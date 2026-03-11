"""LangGraph agent state definitions.

REWRITE v5 - Updated for ReAct (Reason + Act) Architecture.
Supports dynamic agent queries, conversational memory, and raw execution tracking.
"""
from typing import List, Dict, Any, Optional, TypedDict, Annotated
from dataclasses import dataclass, field
from enum import Enum
import operator


class ValidationMode(str, Enum):
    """Validation execution modes."""
    CUSTOM_RULES = "custom_rules"
    AI_RECOMMENDED = "ai_recommended"
    HYBRID = "hybrid"
    SCHEMA_ONLY = "schema_only"
    BUSINESS_ANALYSIS = "business_analysis"


class AgentStatus(str, Enum):
    """Agent execution status."""
    IDLE = "idle"
    CONNECTING = "connecting"
    EXPLORING = "exploring"    # NEW: For the exploratory query phase
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
    # Full-scan support
    full_scan_requested: bool = False
    full_scan_used: bool = False
    slice_filters: Optional[Dict[str, Any]] = None
    column_mapping: Optional[Dict[str, str]] = None  # original -> target/alias
    selected_columns: Optional[List[str]] = None


@dataclass
class ValidationRule:
    """Validation rule definition.
    Updated to support dynamic Agent-written SQL/Pandas queries.
    """
    id: Optional[str]
    name: str
    rule_type: str  # column, row, table, statistical, custom_sql, agent_query
    severity: str  # info, warning, critical
    target_columns: List[str]
    config: Dict[str, Any]
    
    # NEW: Explicit fields for agentic queries
    query: Optional[str] = None
    query_type: Optional[str] = None  # 'sql', 'pandas', 'duckdb'
    
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
    severity: str = "info"  # info, warning, critical
    rule_type: Optional[str] = None
    
    # NEW: Track exactly what the agent executed
    executed_query: Optional[str] = None
    
    # Column this result belongs to — required for grouped dashboard display
    column_name: Optional[str] = None
    # Original/Internal column name (if renamed via template)
    internal_column: Optional[str] = None

    # Check origin tracking: "pre_built" (deterministic) or "llm_generated" (agent-created)
    check_origin: str = "pre_built"
    # Why the agent chose this check
    agent_reasoning: Optional[str] = None
    # What the agent understood from the results
    agent_comprehension: Optional[str] = None


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
    custom_rules: List[Any]
    execution_config: Dict[str, Any]
    
    # Processing state
    status: AgentStatus
    current_step: str
    
    # CRITICAL: Annotated with operator.add so messages append instead of overwrite!
    messages: Annotated[List[Dict[str, Any]], operator.add]
    
    # Data
    data_profile: Optional[DataProfile]
    ai_recommended_rules: List[Any]
    all_rules: List[Any]
    validation_results: List[Any]
    
    # Context for RAG
    retrieved_context: List[Dict[str, Any]]
    
    # Loop Breaker / Step Trackers
    exploration_steps: int
    validation_steps: int
    current_column_index: int
    columns_to_validate: List[Dict[str, Any]]
    available_column_tools: Dict[str, List[Dict[str, Any]]]
    tool_execution_history: List[Dict[str, Any]]
    rag_chunks_saved: Optional[int]
    
    # Output
    quality_score: Optional[float]
    summary_report: Optional[Dict[str, Any]]
    error_message: Optional[str]
    
    # Metadata
    started_at: Optional[str]
    completed_at: Optional[str]
    execution_metrics: Dict[str, Any]
    
    # NEW: Query tracking for deduplication
    executed_queries: List[str]
    queries_per_column: Dict[str, int]