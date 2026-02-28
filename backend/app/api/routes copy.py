"""API routes for the Data Quality Agent."""
import uuid
import os
import logging
from typing import List, Optional, Dict, Any
from datetime import datetime
from pathlib import Path

from fastapi import APIRouter, Depends, HTTPException, BackgroundTasks, Query, File, UploadFile
from fastapi.encoders import jsonable_encoder
from pydantic import BaseModel

from app.core.config import get_settings
from app.agents.state import DataSourceInfo, ValidationMode, ValidationRule as AgentValidationRule, AgentState
from app.agents.data_quality_agent import get_data_quality_agent
from app.agents.llm_service import get_llm_service
from app.agents.rag_service import get_rag_service
from app.connectors.factory import ConnectorFactory

logger = logging.getLogger(__name__)
router = APIRouter()
settings = get_settings()

# ---------------------------------------------------------------------------
# In-memory stores (no PostgreSQL required)
# ---------------------------------------------------------------------------
_validation_store: Dict[str, Dict[str, Any]] = {}
_uploaded_files: Dict[str, Dict[str, Any]] = {}

# Paths
TEST_DATA_DIR = Path(__file__).parent.parent.parent.parent / "test_data"
DB_PATH = TEST_DATA_DIR / "test_database.db"
UPLOAD_DIR = Path(__file__).parent.parent.parent.parent / "uploads"

# ---------------------------------------------------------------------------
# Built-in data sources — always available
# ---------------------------------------------------------------------------
BUILTIN_SOURCES = {
    "local-test": {
        "id": "local-test",
        "name": "Test Database",
        "description": "Local SQLite database with sample data",
        "source_type": "sqlite",
        "is_active": True,
        "created_at": "2025-01-01T00:00:00",
        "connection_config": {"connection_string": f"sqlite:///{DB_PATH}"},
    },
    "file-upload": {
        "id": "file-upload",
        "name": "Upload File",
        "description": "CSV, Excel, Parquet, or JSON files",
        "source_type": "local_file",
        "is_active": True,
        "created_at": "2025-01-01T00:00:00",
        "connection_config": {"base_path": str(UPLOAD_DIR)},
    },
    "adls-mock": {
        "id": "adls-mock",
        "name": "ADLS Gen2 Mock",
        "description": "Azure Data Lake Storage Gen2 test structure",
        "source_type": "local_file",
        "is_active": True,
        "created_at": "2025-01-01T00:00:00",
        "connection_config": {"base_path": str(TEST_DATA_DIR / "adls_mock")},
    },
    "local-files": {
        "id": "local-files",
        "name": "Local Test Files",
        "description": "Structured, semi-structured, and unstructured test files",
        "source_type": "local_file",
        "is_active": True,
        "created_at": "2025-01-01T00:00:00",
        "connection_config": {"base_path": str(TEST_DATA_DIR)},
    },
}

# User-created sources (in-memory)
_user_sources: Dict[str, Dict[str, Any]] = {}


def _get_source(source_id: str) -> Dict[str, Any]:
    """Lookup a data source by ID."""
    if source_id in BUILTIN_SOURCES:
        return BUILTIN_SOURCES[source_id]
    if source_id in _user_sources:
        return _user_sources[source_id]
    raise HTTPException(status_code=404, detail=f"Data source '{source_id}' not found")


# ---------------------------------------------------------------------------
# Pydantic models for API
# ---------------------------------------------------------------------------
class DataSourceCreate(BaseModel):
    name: str
    description: Optional[str] = None
    source_type: str
    connection_config: Dict[str, Any]

class DataSourceResponse(BaseModel):
    id: str
    name: str
    description: Optional[str] = None
    source_type: str
    is_active: bool
    created_at: str

class ValidationRuleCreate(BaseModel):
    name: str
    description: Optional[str] = None
    rule_type: str
    severity: str = "warning"
    target_columns: List[str]
    rule_config: Dict[str, Any]
    expression: Optional[str] = None

# class ValidationRequest(BaseModel):
#     data_source_id: str
#     target_path: str
#     validation_mode: str = "hybrid"
#     custom_rules: Optional[List[ValidationRuleCreate]] = []
#     sample_size: Optional[int] = 1000
#     execution_config: Optional[Dict[str, Any]] = {}

class ValidationRequest(BaseModel):
    data_source_id: str
    target_path: str
    validation_mode: str = "hybrid"
    custom_rules: Optional[List[ValidationRuleCreate]] = []
    sample_size: Optional[int] = 1000
    full_scan: bool = False                     # ← NEW
    execution_config: Optional[Dict[str, Any]] = {}

class ValidationResponse(BaseModel):
    validation_id: str
    status: str
    message: str

class ValidationStatusResponse(BaseModel):
    validation_id: str
    status: str
    current_step: Optional[str] = None
    quality_score: Optional[float] = None
    total_rules: int = 0
    passed_rules: int = 0
    failed_rules: int = 0
    started_at: Optional[str] = None
    completed_at: Optional[str] = None
    error_message: Optional[str] = None
    target_path: Optional[str] = None
    data_source_id: Optional[str] = None
    validation_mode: Optional[str] = None
    data_profile: Optional[Dict[str, Any]] = None
    result: Optional[Dict[str, Any]] = None

class LLMHealthResponse(BaseModel):
    status: str
    provider: Optional[str] = None
    model: Optional[str] = None
    response: Optional[str] = None
    error: Optional[str] = None


# ===================================================================
# Data Source Routes
# ===================================================================

@router.get("/datasources", response_model=List[DataSourceResponse])
async def list_data_sources():
    """List all configured data sources."""
    all_sources = {**BUILTIN_SOURCES, **_user_sources}
    return [
        DataSourceResponse(
            id=s["id"],
            name=s["name"],
            description=s.get("description"),
            source_type=s["source_type"],
            is_active=s.get("is_active", True),
            created_at=str(s.get("created_at", datetime.utcnow().isoformat())),
        )
        for s in all_sources.values()
    ]


@router.post("/datasources", response_model=DataSourceResponse)
async def create_data_source(data_source: DataSourceCreate):
    """Create a new data source."""
    if not ConnectorFactory.is_supported(data_source.source_type):
        raise HTTPException(
            status_code=400,
            detail=f"Unsupported source type: {data_source.source_type}",
        )

    ds_id = str(uuid.uuid4())
    now = datetime.utcnow().isoformat()
    _user_sources[ds_id] = {
        "id": ds_id,
        "name": data_source.name,
        "description": data_source.description,
        "source_type": data_source.source_type,
        "connection_config": data_source.connection_config,
        "is_active": True,
        "created_at": now,
    }

    return DataSourceResponse(
        id=ds_id,
        name=data_source.name,
        description=data_source.description,
        source_type=data_source.source_type,
        is_active=True,
        created_at=now,
    )


@router.post("/datasources/{source_id}/test")
async def test_data_source_connection(source_id: str):
    """Test data source connection."""
    source = _get_source(source_id)
    try:
        connector = ConnectorFactory.create_connector(
            source["source_type"], source["connection_config"]
        )
        await connector.connect()
        await connector.disconnect()
        return {"success": True, "message": "Connection successful"}
    except Exception as e:
        return {"success": False, "message": str(e)}


@router.get("/datasources/{source_id}/resources")
async def list_data_source_resources(
    source_id: str,
    path: Optional[str] = None,
):
    """List resources (tables, files, folders) in a data source."""
    source = _get_source(source_id)
    try:
        connector = ConnectorFactory.create_connector(
            source["source_type"], source["connection_config"]
        )
        await connector.connect()
        resources = await connector.list_resources(path)

        # Enrich resources with row/column counts for tables
        enriched = []
        for r in resources:
            item = {**r}
            if r.get("type") == "table":
                try:
                    row_count = await connector.get_row_count(r["path"])
                    schema = await connector.get_schema(r["path"])
                    item["rowCount"] = row_count
                    item["columnCount"] = len(schema.get("columns", {}))
                    item["columns"] = [
                        {"name": k, "type": v.get("type", "unknown")}
                        for k, v in schema.get("columns", {}).items()
                    ]
                except Exception:
                    pass
            elif r.get("type") == "file":
                size = r.get("size_bytes", 0)
                if size > 1_048_576:
                    item["size"] = f"{size / 1_048_576:.1f} MB"
                elif size > 1024:
                    item["size"] = f"{size / 1024:.1f} KB"
                else:
                    item["size"] = f"{size} B"
            enriched.append(item)

        await connector.disconnect()
        return enriched
    except Exception as e:
        logger.error(f"Error listing resources for {source_id}: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/datasources/{source_id}/schema")
async def get_data_source_schema(
    source_id: str,
    resource_path: str = Query(...),
):
    """Get schema for a resource in a data source."""
    source = _get_source(source_id)
    try:
        connector = ConnectorFactory.create_connector(
            source["source_type"], source["connection_config"]
        )
        await connector.connect()
        schema = await connector.get_schema(resource_path)
        await connector.disconnect()
        return schema
    except Exception as e:
        logger.error(f"Error getting schema: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/datasources/{source_id}/preview")
async def preview_data_source(
    source_id: str,
    resource_path: str = Query(...),
    limit: int = Query(default=20, le=100),
):
    """Get preview data rows for a resource."""
    source = _get_source(source_id)
    try:
        connector = ConnectorFactory.create_connector(
            source["source_type"], source["connection_config"]
        )
        await connector.connect()

        # Get schema
        schema = await connector.get_schema(resource_path)
        columns_info = [
            {"name": k, "type": v.get("type", "unknown")}
            for k, v in schema.get("columns", {}).items()
        ]

        # Get sample rows
        rows = await connector.sample_data(resource_path, sample_size=limit)

        # Get row count
        try:
            row_count = await connector.get_row_count(resource_path)
        except Exception:
            row_count = len(rows)

        await connector.disconnect()

        return {
            "columns": columns_info,
            "rows": rows[:limit],
            "total_rows": row_count,
            "preview_count": len(rows[:limit]),
        }
    except Exception as e:
        logger.error(f"Error previewing data: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# ===================================================================
# Validation Routes
# ===================================================================

@router.get("/validations")
async def list_validations():
    """List all validations."""
    validations = []
    for vid, v in _validation_store.items():
        validations.append({
            "id": vid,
            "validation_id": vid,
            "status": v["status"],
            "current_step": v.get("current_step"),
            "quality_score": v.get("quality_score"),
            "total_rules": v.get("total_rules", 0),
            "passed_rules": v.get("passed_rules", 0),
            "failed_rules": v.get("failed_rules", 0),
            "started_at": v.get("started_at"),
            "completed_at": v.get("completed_at"),
            "target_path": v.get("target_path", ""),
            "data_source_id": v.get("data_source_id", ""),
            "validation_mode": v.get("validation_mode", "hybrid"),
            "error_message": v.get("error_message"),
        })
    # Sort by started_at descending (newest first)
    validations.sort(key=lambda x: x.get("started_at", ""), reverse=True)
    return validations

@router.post("/validate", response_model=ValidationResponse)
async def submit_validation(
    request: ValidationRequest,
    background_tasks: BackgroundTasks,
):
    """Submit a validation request."""
    validation_id = str(uuid.uuid4())

    # Store initial state with metadata
    _validation_store[validation_id] = {
        "status": "pending",
        "current_step": "starting",
        "quality_score": None,
        "total_rules": 0,
        "passed_rules": 0,
        "failed_rules": 0,
        "started_at": datetime.utcnow().isoformat(),
        "completed_at": None,
        "error_message": None,
        "result": None,
        "target_path": request.target_path,
        "data_source_id": request.data_source_id,
        "validation_mode": request.validation_mode,
    }

    background_tasks.add_task(run_validation, validation_id, request)

    return ValidationResponse(
        validation_id=validation_id,
        status="pending",
        message="Validation started",
    )


async def run_validation(validation_id: str, request: ValidationRequest):
    """Run validation in background."""
    try:
        # _validation_store[validation_id]["status"] = "running"

        # # Resolve data source
        # source = _get_source(request.data_source_id)

        # # Build DataSourceInfo from the source config
        # data_source_info = DataSourceInfo(
        #     source_type=source["source_type"],
        #     connection_config=source["connection_config"],
        #     target_path=request.target_path,
        # )

        # ← NEW FULL-SCAN LOGIC 
        _validation_store[validation_id]["status"] = "running"
        settings = get_settings()
        
        source = _get_source(request.data_source_id)
        
        data_source_info = DataSourceInfo(
            source_type=source["source_type"],
            connection_config=source["connection_config"],
            target_path=request.target_path,
            full_scan_requested=request.full_scan,
        )

        # Convert custom rules
        custom_rules = []
        for rule in request.custom_rules or []:
            custom_rules.append(AgentValidationRule(
                id=str(uuid.uuid4()),
                name=rule.name,
                rule_type=rule.rule_type,
                severity=rule.severity,
                target_columns=rule.target_columns,
                config=rule.rule_config,
                expression=rule.expression,
            ))

        agent = get_data_quality_agent()
        result = await agent.run(
            validation_id=validation_id,
            validation_mode=ValidationMode(request.validation_mode.lower()),
            data_source_info=data_source_info,
            custom_rules=custom_rules,
            execution_config={
                "full_scan": request.full_scan,
                "sample_size": request.sample_size,
                **(request.execution_config or {}),
            },
        )
        
        # Persist result (new)
        _validation_store[validation_id].update({
            "status": "completed",
            "completed_at": datetime.utcnow().isoformat(),
            "quality_score": result.get("quality_score"),
            "result": result,
            "full_scan_used": request.full_scan,
        })
        
        logger.info(f"Validation {validation_id} completed (full_scan={request.full_scan})")

        # Store result
        summary = result.get("summary_report", {}) or {}
        _validation_store[validation_id].update({
            "status": "completed",
            "current_step": "done",
            "quality_score": result.get("quality_score"),
            "total_rules": summary.get("total_rules", 0),
            "passed_rules": summary.get("passed_rules", 0),
            "failed_rules": summary.get("failed_rules", 0),
            "completed_at": datetime.utcnow().isoformat(),
            "result": result,
        })

        logger.info(f"Validation {validation_id} completed with score: {result.get('quality_score')}")

    except Exception as e:
        logger.error(f"Validation {validation_id} failed: {str(e)}")
        _validation_store[validation_id].update({
            "status": "failed",
            "error_message": str(e),
            "completed_at": datetime.utcnow().isoformat(),
        })


@router.get("/validate/{validation_id}", response_model=ValidationStatusResponse)
async def get_validation_status(validation_id: str):
    """Get validation status."""
    if validation_id not in _validation_store:
        raise HTTPException(status_code=404, detail="Validation not found")

    v = _validation_store[validation_id]
    result = v.get("result", {})
    data_profile = None
    if result and "data_profile" in result:
        data_profile = jsonable_encoder(result["data_profile"])
    return ValidationStatusResponse(
        validation_id=validation_id,
        status=v["status"],
        current_step=v.get("current_step"),
        quality_score=v.get("quality_score"),
        total_rules=v.get("total_rules", 0),
        passed_rules=v.get("passed_rules", 0),
        failed_rules=v.get("failed_rules", 0),
        started_at=v.get("started_at"),
        completed_at=v.get("completed_at"),
        error_message=v.get("error_message"),
        target_path=v.get("target_path"),
        data_source_id=v.get("data_source_id"),
        validation_mode=v.get("validation_mode"),
        data_profile=data_profile,
        result=result,
    )


@router.get("/validate/{validation_id}/results")
async def get_validation_results(validation_id: str):
    """Get detailed validation results."""
    if validation_id not in _validation_store:
        raise HTTPException(status_code=404, detail="Validation not found")

    result = _validation_store[validation_id].get("result", {})
    validation_results = result.get("validation_results", []) if result else []

    serialized = []
    for r in validation_results:
        serialized.append({
            "rule_name": getattr(r, "rule_name", "unknown"),
            "status": getattr(r, "status", "unknown"),
            "severity": getattr(r, "severity", "info"),
            "rule_type": getattr(r, "rule_type", ""),
            "failed_count": getattr(r, "failed_count", 0),
            "failure_percentage": getattr(r, "failure_percentage", 0),
            "failure_examples": getattr(r, "failure_examples", [])[:5],
        })

    return {
        "validation_id": validation_id,
        "results": serialized,
        "raw_state": result,
    }


@router.get("/validate/{validation_id}/report")
async def get_validation_report(
    validation_id: str,
    format: str = "json",
):
    """Get validation report in specified format."""
    if validation_id not in _validation_store:
        raise HTTPException(status_code=404, detail="Validation not found")

    v = _validation_store[validation_id]

    if format == "json":
        result = v.get("result", {}) or {}
        summary = result.get("summary_report", {}) or {}
        return {
            "validation_id": validation_id,
            "quality_score": v.get("quality_score"),
            "summary": summary,
            "status": v["status"],
        }
    else:
        raise HTTPException(status_code=400, detail=f"Unsupported format: {format}")


# ===================================================================
# Rule Management Routes
# ===================================================================

@router.get("/rules")
async def list_validation_rules(
    data_source_id: Optional[str] = None,
    is_active: Optional[bool] = True,
):
    """List validation rules."""
    return []


@router.post("/rules")
async def create_validation_rule(rule: ValidationRuleCreate):
    """Create a new validation rule."""
    rule_id = str(uuid.uuid4())
    return {"id": rule_id, "name": rule.name, "status": "created"}


@router.put("/rules/{rule_id}")
async def update_validation_rule(rule_id: str, rule: ValidationRuleCreate):
    """Update a validation rule."""
    return {"id": rule_id, "name": rule.name, "status": "updated"}


@router.delete("/rules/{rule_id}")
async def delete_validation_rule(rule_id: str):
    """Delete a validation rule."""
    return {"id": rule_id, "status": "deleted"}


# ===================================================================
# AI Routes
# ===================================================================

@router.post("/ai/recommend-rules")
async def recommend_rules(
    data_source_id: str,
    target_path: str,
    sample_size: int = 1000,
):
    """Get AI-recommended validation rules for a dataset."""
    return {
        "rules": [],
        "explanation": "AI-generated rules based on data profiling",
    }


@router.post("/ai/analyze")
async def analyze_data_quality(
    data_source_id: str,
    target_path: str,
):
    """Get AI analysis of data quality issues."""
    return {"analysis": "", "recommendations": []}


# ===================================================================
# LLM Health Check
# ===================================================================

@router.get("/llm/health", response_model=LLMHealthResponse)
async def check_llm_health():
    """Check LLM service health."""
    llm_service = get_llm_service()
    health = await llm_service.check_health()
    return LLMHealthResponse(**health)


# ===================================================================
# File Upload Routes
# ===================================================================

@router.post("/upload")
async def upload_file(
    file: UploadFile = File(...),
):
    """Upload a file for validation."""
    os.makedirs(str(UPLOAD_DIR), exist_ok=True)

    file_path = UPLOAD_DIR / file.filename
    with open(str(file_path), "wb") as f:
        content = await file.read()
        f.write(content)

    file_id = str(uuid.uuid4())
    _uploaded_files[file_id] = {
        "id": file_id,
        "filename": file.filename,
        "path": str(file_path),
        "size": len(content),
        "uploaded_at": datetime.utcnow().isoformat(),
    }

    return {
        "id": file_id,
        "filename": file.filename,
        "path": str(file_path),
        "size": len(content),
    }


# ===================================================================
# System Routes
# ===================================================================

@router.get("/health")
async def health_check():
    """System health check."""
    return {
        "status": "healthy",
        "version": settings.APP_VERSION,
        "timestamp": datetime.utcnow().isoformat(),
    }


@router.get("/supported-sources")
async def get_supported_source_types():
    """Get list of supported data source types."""
    return {"source_types": ConnectorFactory.get_supported_types()}
