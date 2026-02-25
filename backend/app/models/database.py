"""SQLAlchemy database models."""
from datetime import datetime
from enum import Enum as PyEnum
from typing import List, Optional, Dict, Any
from sqlalchemy import (
    Column, String, Integer, Float, Boolean, DateTime, 
    Text, JSON, ForeignKey, Enum, Index, create_engine
)
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship, sessionmaker, Session
from sqlalchemy.dialects.postgresql import UUID, JSONB
import uuid

Base = declarative_base()


class ValidationStatus(str, PyEnum):
    """Validation execution status."""
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"


class RuleSeverity(str, PyEnum):
    """Validation rule severity levels."""
    INFO = "info"
    WARNING = "warning"
    CRITICAL = "critical"


class DataSourceType(str, PyEnum):
    """Supported data source types."""
    LOCAL_FILE = "local_file"
    ADLS_GEN2 = "adls_gen2"
    DATABRICKS = "databricks"
    AWS_S3 = "aws_s3"
    AWS_REDSHIFT = "aws_redshift"
    AWS_RDS = "aws_rds"
    GCP_STORAGE = "gcp_storage"
    GCP_BIGQUERY = "gcp_bigquery"
    POSTGRESQL = "postgresql"
    MYSQL = "mysql"
    SQLSERVER = "sqlserver"
    MONGODB = "mongodb"
    API = "api"


class DataSource(Base):
    """Data source configuration."""
    __tablename__ = "data_sources"
    
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    name = Column(String(255), nullable=False)
    description = Column(Text)
    source_type = Column(Enum(DataSourceType), nullable=False)
    connection_config = Column(JSONB, default={})  # Encrypted connection details
    is_active = Column(Boolean, default=True)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    created_by = Column(String(255))
    
    # Relationships
    validations = relationship("ValidationRun", back_populates="data_source")
    
    __table_args__ = (
        Index('idx_datasource_type', 'source_type'),
        Index('idx_datasource_active', 'is_active'),
    )


class ValidationRule(Base):
    """Validation rule definition."""
    __tablename__ = "validation_rules"
    
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    name = Column(String(255), nullable=False)
    description = Column(Text)
    rule_type = Column(String(50), nullable=False)  # column, row, table, statistical, custom_sql
    severity = Column(Enum(RuleSeverity), default=RuleSeverity.WARNING)
    
    # Rule configuration
    target_columns = Column(JSONB, default=[])  # Columns this rule applies to
    rule_config = Column(JSONB, default={})  # Rule-specific configuration
    expression = Column(Text)  # For custom expression rules
    
    # Metadata
    is_active = Column(Boolean, default=True)
    is_ai_generated = Column(Boolean, default=False)
    ai_confidence = Column(Float)  # AI confidence score if generated
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    created_by = Column(String(255))
    
    # Relationships
    results = relationship("ValidationResult", back_populates="rule")
    
    __table_args__ = (
        Index('idx_rule_type', 'rule_type'),
        Index('idx_rule_active', 'is_active'),
    )


class ValidationRun(Base):
    """Validation execution record."""
    __tablename__ = "validation_runs"
    
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    data_source_id = Column(UUID(as_uuid=True), ForeignKey("data_sources.id"))
    
    # Execution details
    status = Column(Enum(ValidationStatus), default=ValidationStatus.PENDING)
    validation_mode = Column(String(50))  # custom, ai_recommended, hybrid
    
    # Target information
    target_path = Column(String(500))  # File path, table name, etc.
    target_schema = Column(JSONB)  # Schema of the target data
    
    # Execution metrics
    started_at = Column(DateTime)
    completed_at = Column(DateTime)
    records_processed = Column(Integer, default=0)
    records_failed = Column(Integer, default=0)
    
    # Results summary
    total_rules = Column(Integer, default=0)
    passed_rules = Column(Integer, default=0)
    failed_rules = Column(Integer, default=0)
    warning_rules = Column(Integer, default=0)
    
    # Quality score
    quality_score = Column(Float)  # 0-100
    
    # Configuration
    sample_size = Column(Integer)
    execution_config = Column(JSONB, default={})
    
    # Error information
    error_message = Column(Text)
    
    created_at = Column(DateTime, default=datetime.utcnow)
    created_by = Column(String(255))
    
    # Relationships
    data_source = relationship("DataSource", back_populates="validations")
    results = relationship("ValidationResult", back_populates="validation_run")
    
    __table_args__ = (
        Index('idx_validation_status', 'status'),
        Index('idx_validation_datasource', 'data_source_id'),
        Index('idx_validation_created', 'created_at'),
    )


class ValidationResult(Base):
    """Individual validation result."""
    __tablename__ = "validation_results"
    
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    validation_run_id = Column(UUID(as_uuid=True), ForeignKey("validation_runs.id"))
    rule_id = Column(UUID(as_uuid=True), ForeignKey("validation_rules.id"))
    
    # Result details
    status = Column(String(20))  # passed, failed, warning, error
    passed_count = Column(Integer, default=0)
    failed_count = Column(Integer, default=0)
    
    # Failure details
    failure_examples = Column(JSONB, default=[])  # Sample failed records
    failure_percentage = Column(Float, default=0.0)
    
    # AI insights
    ai_insights = Column(Text)
    ai_suggestions = Column(JSONB, default=[])
    
    # Execution metrics
    execution_time_ms = Column(Integer)
    
    created_at = Column(DateTime, default=datetime.utcnow)
    
    # Relationships
    validation_run = relationship("ValidationRun", back_populates="results")
    rule = relationship("ValidationRule", back_populates="results")
    
    __table_args__ = (
        Index('idx_result_validation', 'validation_run_id'),
        Index('idx_result_rule', 'rule_id'),
        Index('idx_result_status', 'status'),
    )


class DataProfile(Base):
    """Data profiling results."""
    __tablename__ = "data_profiles"
    
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    data_source_id = Column(UUID(as_uuid=True), ForeignKey("data_sources.id"))
    target_path = Column(String(500))
    
    # Profile data
    column_profiles = Column(JSONB)  # Per-column statistics
    row_count = Column(Integer)
    column_count = Column(Integer)
    
    # Metadata
    profiled_at = Column(DateTime, default=datetime.utcnow)
    profile_config = Column(JSONB, default={})
    
    __table_args__ = (
        Index('idx_profile_datasource', 'data_source_id'),
        Index('idx_profile_target', 'target_path'),
    )


class ContextDocument(Base):
    """RAG context documents for hallucination prevention."""
    __tablename__ = "context_documents"
    
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    document_type = Column(String(50))  # schema, business_rule, validation_history, etc.
    source_id = Column(String(255))  # Reference to source (e.g., data source ID)
    
    # Content
    title = Column(String(255))
    content = Column(Text)
    content_hash = Column(String(64))  # For cache invalidation
    
    # Embedding metadata
    embedding_id = Column(String(255))  # ID in vector DB
    
    # Metadata (attribute renamed to avoid SQLAlchemy reserved name conflict)
    doc_metadata = Column('metadata', JSONB, default={})
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    
    __table_args__ = (
        Index('idx_context_type', 'document_type'),
        Index('idx_context_source', 'source_id'),
    )


class AuditLog(Base):
    """Audit log for compliance."""
    __tablename__ = "audit_logs"
    
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    action = Column(String(50), nullable=False)
    entity_type = Column(String(50), nullable=False)  # data_source, rule, validation, etc.
    entity_id = Column(String(255))
    user_id = Column(String(255))
    
    # Change details
    previous_state = Column(JSONB)
    new_state = Column(JSONB)
    
    # Metadata
    ip_address = Column(String(45))
    user_agent = Column(String(500))
    created_at = Column(DateTime, default=datetime.utcnow)
    
    __table_args__ = (
        Index('idx_audit_entity', 'entity_type', 'entity_id'),
        Index('idx_audit_user', 'user_id'),
        Index('idx_audit_created', 'created_at'),
    )


# Database engine and session
def get_engine(database_url: str):
    """Create database engine."""
    return create_engine(database_url)


def get_session_maker(engine):
    """Get session maker."""
    return sessionmaker(autocommit=False, autoflush=False, bind=engine)


def init_db(engine):
    """Initialize database tables."""
    Base.metadata.create_all(bind=engine)
