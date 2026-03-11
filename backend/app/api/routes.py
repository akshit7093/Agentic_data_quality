"""API routes for the Data Quality Agent — v8
Fixed: Export endpoints, Fix recommendation endpoints, Full-scan support, String spacing
"""
import uuid
import os
import json
import logging
from dataclasses import asdict
from typing import List, Optional, Dict, Any
from datetime import datetime
from pathlib import Path
from fastapi import APIRouter, Depends, HTTPException, BackgroundTasks, Query, File, UploadFile
from fastapi.encoders import jsonable_encoder
from fastapi.responses import FileResponse
from pydantic import BaseModel
from app.core.config import get_settings
from app.agents.state import (
    DataSourceInfo, ValidationMode, ValidationRule as AgentValidationRule,
    AgentState
)
from app.agents.data_quality_agent import get_data_quality_agent
from app.agents.llm_service import get_llm_service
from app.agents.rag_service import get_rag_service
from app.connectors.factory import ConnectorFactory
from app.agents.ticketing_agent import TicketingAgent
from app.agents.template_routes import get_applied_session
from app.models.rule_groups import get_rule_group_store

logger = logging.getLogger(__name__)
router = APIRouter()
settings = get_settings()

# ============================================================================
# In-memory stores (no PostgreSQL required)
# ============================================================================
_validation_store: Dict[str, Dict[str, Any]] = {}
_uploaded_files: Dict[str, Dict[str, Any]] = {}

# Paths
TEST_DATA_DIR = Path(__file__).parent.parent.parent.parent / "test_data"
DB_PATH = TEST_DATA_DIR / "test_database.db"
UPLOAD_DIR = Path(__file__).parent.parent.parent.parent / "uploads"
EXPORT_DIR = Path(__file__).parent.parent.parent.parent / "exports"

# Ensure directories exist
os.makedirs(str(EXPORT_DIR), exist_ok=True)
os.makedirs(str(UPLOAD_DIR), exist_ok=True)

VALIDATION_STORE_PATH = TEST_DATA_DIR / "validation_store.json"

def _load_store():
    global _validation_store
    if VALIDATION_STORE_PATH.exists():
        try:
            with open(VALIDATION_STORE_PATH, "r") as f:
                _validation_store = json.load(f)
            logger.info(f"Loaded {len(_validation_store)} validations from persistence")
        except Exception as e:
            logger.error(f"Error loading validation store: {e}")

def _save_store():
    try:
        # Use a temporary file to avoid corruption
        temp_path = VALIDATION_STORE_PATH.with_suffix(".tmp")
        with open(temp_path, "w") as f:
            # Use jsonable_encoder to handle dataclasses/pydantic models in the store
            json.dump(jsonable_encoder(_validation_store), f, indent=2)
        os.replace(temp_path, VALIDATION_STORE_PATH)
    except Exception as e:
        logger.error(f"Error saving validation store: {e}")

def _get_val(obj: Any, key: str, default: Any = None) -> Any:
    """Helper to safely get value from object (attribute) or dict (key)."""
    if obj is None:
        return default
    if isinstance(obj, dict):
        return obj.get(key, default)
    return getattr(obj, key, default)

_load_store()

# ============================================================================
# Built-in data sources — always available
# ============================================================================
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


# ============================================================================
# Pydantic models for API
# ============================================================================
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


class ValidationRequest(BaseModel):
    data_source_id: str
    target_path: str
    validation_mode: str = "hybrid"
    custom_rules: Optional[List[ValidationRuleCreate]] = []
    sample_size: Optional[int] = 1000
    full_scan: bool = False
    execution_config: Optional[Dict[str, Any]] = {}
    slice_filters: Optional[Dict[str, Any]] = None  # Key-value pairs for pivot-like slicing
    session_id: Optional[str] = None  # NEW: applied template session
    column_mapping: Optional[Dict[str, str]] = None  # NEW: explicit column mapping


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
    full_scan_used: Optional[bool] = None


class ExportRequest(BaseModel):
    """Export validation results request."""
    validation_id: str
    format: str = "csv"
    include_failed_rows: bool = True
    include_metadata: bool = True


class ExportResponse(BaseModel):
    validation_id: str
    format: str
    file_path: Optional[str] = None
    download_url: Optional[str] = None
    status: str
    message: str


class FixActionRequest(BaseModel):
    """Apply fix action to a validation rule."""
    validation_id: str
    rule_id: str
    action: str = "manual"
    custom_fix_query: Optional[str] = None


class FixActionResponse(BaseModel):
    validation_id: str
    rule_id: str
    action: str
    status: str
    message: str
    fix_query: Optional[str] = None


class LLMHealthResponse(BaseModel):
    status: str
    provider: Optional[str] = None
    model: Optional[str] = None
    response: Optional[str] = None
    error: Optional[str] = None


# ============================================================================
# Data Source Routes
# ============================================================================
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
    limit: int = Query(default=1000, le=10000),
):
    """Get preview data rows for a resource."""
    source = _get_source(source_id)
    try:
        connector = ConnectorFactory.create_connector(
            source["source_type"], source["connection_config"]
        )
        await connector.connect()

        schema = await connector.get_schema(resource_path)
        columns_info = [
            {
                "name": k,
                "type": v.get("type", "unknown"),
                "null_count": v.get("null_count"),
                "unique_count": v.get("unique_count"),
                "null_percent": v.get("null_percent")
            }
            for k, v in schema.get("columns", {}).items()
        ]

        rows = await connector.sample_data(resource_path, sample_size=limit, method="first")

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


# ============================================================================
# Validation Routes
# ============================================================================
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
            "slice_filters": v.get("slice_filters"),
            "error_message": v.get("error_message"),
            "full_scan_used": v.get("full_scan_used", False),
        })
    validations.sort(key=lambda x: x.get("started_at", ""), reverse=True)
    return validations


@router.post("/validate", response_model=ValidationResponse)
async def submit_validation(
    request: ValidationRequest,
    background_tasks: BackgroundTasks,
):
    """Submit a validation request."""
    validation_id = str(uuid.uuid4())

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
        "slice_filters": request.slice_filters,
        "full_scan_used": request.full_scan,
    }

    background_tasks.add_task(run_validation, validation_id, request)
    _save_store()

    return ValidationResponse(
        validation_id=validation_id,
        status="pending",
        message="Validation started",
    )


async def run_validation(validation_id: str, request: ValidationRequest):
    """Run validation in background."""
    try:
        _validation_store[validation_id]["status"] = "running"
        source = _get_source(request.data_source_id)

        # ── Resolve selected columns from applied template session ─────────
        selected_cols: Optional[List[str]] = None
        column_mapping: Dict[str, str] = dict(request.column_mapping or {})

        if request.session_id:
            session = get_applied_session(request.session_id)
            if session:
                selected_cols = session.get("columns") or None
                if session.get("rename_map"):
                    column_mapping.update(session["rename_map"])
                logger.info(
                    f"Template session '{request.session_id}': "
                    f"restricting to {len(selected_cols or [])} columns — {selected_cols}"
                )
            else:
                logger.warning(
                    f"session_id '{request.session_id}' not found in session store "
                    "(expired or invalid). Falling back to all columns."
                )

        data_source_info = DataSourceInfo(
            source_type=source["source_type"],
            connection_config=source["connection_config"],
            target_path=request.target_path,
            full_scan_requested=request.full_scan,
            slice_filters=request.slice_filters,
            selected_columns=selected_cols,
            column_mapping=column_mapping,
        )

        # ── Build custom rules ─────────────────────────────────────────────
        # 1. Rules supplied directly in the request body
        custom_rules = [
            AgentValidationRule(
                id=str(uuid.uuid4()),
                name=rule.name,
                rule_type=rule.rule_type,
                severity=rule.severity,
                target_columns=rule.target_columns,
                config=rule.rule_config,
                expression=rule.expression,
            )
            for rule in (request.custom_rules or [])
        ]

        # 2. Rules from saved rule groups that target this file
        try:
            rule_store = get_rule_group_store()
            group_rules = rule_store.get_rules_for_file(request.target_path)
            if group_rules:
                logger.info(
                    f"Loaded {len(group_rules)} rule(s) from rule groups "
                    f"for target '{request.target_path}'"
                )
            for gr in group_rules:
                custom_rules.append(
                    AgentValidationRule(
                        id=gr.get("id", str(uuid.uuid4())),
                        name=gr.get("rule_name", "group_rule"),
                        rule_type=gr.get("rule_type", "custom_sql"),
                        severity=gr.get("severity", "warning"),
                        target_columns=gr.get("target_columns", []),
                        config={},
                        query=gr.get("query", ""),
                        query_type=gr.get("query_type", "sql"),
                    )
                )
        except Exception as rg_err:
            # Rule group loading is non-critical — log and continue
            logger.warning(f"Failed to load rule groups (non-critical): {rg_err}")

        validation_mode_enum = ValidationMode(request.validation_mode.lower())

        if validation_mode_enum == ValidationMode.SCHEMA_ONLY:
            from app.agents.schema_agent import SchemaValidationAgent
            agent = SchemaValidationAgent()
        elif validation_mode_enum == ValidationMode.BUSINESS_ANALYSIS:
            from app.agents.business_agent import BusinessAnalystAgent
            agent = BusinessAnalystAgent()
        else:
            agent = get_data_quality_agent()

        result = await agent.run(
            validation_id=validation_id,
            validation_mode=validation_mode_enum,
            data_source_info=data_source_info,
            custom_rules=custom_rules,
            execution_config={
                "full_scan": request.full_scan,
                "sample_size": request.sample_size,
                **(request.execution_config or {}),
            },
        )

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
            "full_scan_used": request.full_scan,
        })

        _save_store()
        logger.info(f"Validation {validation_id} completed with score: {result.get('quality_score')}")

    except Exception as e:
        import traceback
        logger.error(f"Validation {validation_id} failed: {str(e)}")
        logger.error(f"Full traceback:\n{traceback.format_exc()}")
        _validation_store[validation_id].update({
            "status": "failed",
            "error_message": str(e),
            "completed_at": datetime.utcnow().isoformat(),
        })
        _save_store()


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
        slice_filters=v.get("slice_filters"),
        data_profile=data_profile,
        result=result,
        full_scan_used=v.get("full_scan_used", False),
    )


@router.get("/validate/{validation_id}/results")
async def get_validation_results(validation_id: str):
    """Get detailed validation results."""
    if validation_id not in _validation_store:
        raise HTTPException(status_code=404, detail="Validation not found")
    result = _validation_store[validation_id].get("result") or {}
    validation_results = result.get("validation_results", []) if result else []
    fix_recommendations = result.get("fix_recommendations", []) if result else []

    serialized = []
    for r in validation_results:
        serialized.append({
            "rule_name": _get_val(r, "rule_name", "unknown"),
            "column_name": _get_val(r, "column_name"),
            "status": _get_val(r, "status", "unknown"),
            "severity": _get_val(r, "severity", "info"),
            "rule_type": _get_val(r, "rule_type", ""),
            "failed_count": _get_val(r, "failed_count", 0),
            "total_count": _get_val(r, "total_count", 0),
            "failure_percentage": _get_val(r, "failure_percentage", 0),
            "failure_examples": _get_val(r, "failure_examples", [])[:5],
            "executed_query": _get_val(r, "executed_query"),
            "data_source": _get_val(r, "data_source", "full_dataset"),
            "check_origin": _get_val(r, "check_origin", "pre_built"),
            "agent_reasoning": _get_val(r, "agent_reasoning"),
            "agent_comprehension": _get_val(r, "agent_comprehension"),
        })

    fixes = []
    for f in fix_recommendations:
        fixes.append({
            "rule_id": _get_val(f, "rule_id", ""),
            "issue_description": _get_val(f, "issue_description", ""),
            "recommended_fix": _get_val(f, "recommended_fix", ""),
            "fix_query": _get_val(f, "fix_query"),
            "estimated_rows_affected": _get_val(f, "estimated_rows_affected", 0),
            "risk_level": _get_val(f, "risk_level", "low"),
            "user_action": _get_val(f, "user_action", "manual"),
        })

    return {
        "validation_id": validation_id,
        "results": serialized,
        "fix_recommendations": fixes,
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
            "full_scan_used": v.get("full_scan_used", False),
        }
    else:
        raise HTTPException(status_code=400, detail=f"Unsupported format: {format}")


# ============================================================================
# Ticketing Routes
# ============================================================================
class TicketRequest(BaseModel):
    rule_names: Optional[List[str]] = None
    rule_id: Optional[str] = None

class NotifyRequest(BaseModel):
    ticket_markdown: str
    assigned_to: str
    rule_names: List[str]

@router.post("/validate/{validation_id}/ticket")
async def create_ticket(validation_id: str, request: TicketRequest):
    """Generate a ticket for one or more failing rules."""
    if validation_id not in _validation_store:
        raise HTTPException(status_code=404, detail="Validation not found")
    
    v = _validation_store[validation_id]
    state = v.get("result") or {}
    
    if not state.get("validation_results"):
        raise HTTPException(status_code=400, detail="No validation results found")
        
    ticketing_agent = TicketingAgent()
    
    # Handle both rule_names (list) and rule_id (string) from frontend
    rule_names = request.rule_names or ([request.rule_id] if request.rule_id else [])
    primary_rule = rule_names[0] if rule_names else "Multiple Rules"
    
    # Try to find rule details in the state
    # Note: _validation_store[validation_id]['result'] might contain 'results' or 'validation_results'
    results = state.get("results") or state.get("validation_results") or []
    rule_details = {}
    failure_examples = []
    
    for r in results:
        r_name = _get_val(r, "rule_name")
        if r_name == primary_rule:
            rule_details = r if isinstance(r, dict) else asdict(r)
            failure_examples = rule_details.get("failure_examples", [])[:5]
            break

    try:
        # Pass necessary context to generate_ticket
        ticket_markdown = await ticketing_agent.generate_ticket(
            rule_name=primary_rule,
            rule_details=rule_details,
            schema=state.get("schema", {}),
            failure_examples=failure_examples
        )
        return {"ticket_markdown": ticket_markdown}
    except Exception as e:
        logger.error(f"Ticketing error: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/users")
async def get_users():
    """Get a mock list of users for ticket assignment."""
    return [
        {"id": "u1", "username": "@data_engineer_1", "role": "Data Engineer"},
        {"id": "u2", "username": "@steward_sarah", "role": "Data Steward"},
        {"id": "u3", "username": "@admin_alex", "role": "Admin"},
        {"id": "u4", "username": "@analyst_jane", "role": "Data Analyst"},
    ]

@router.post("/validate/{validation_id}/notify")
async def send_notification(validation_id: str, request: NotifyRequest):
    """Dispatch the ticket notification to the assigned user."""
    if validation_id not in _validation_store:
        raise HTTPException(status_code=404, detail="Validation not found")
        
    try:
        # In a real app we would send an email or Slack message here.
        # We'll just mock it and save it to the validation state (in-memory).
        v = _validation_store[validation_id]
        if "tickets" not in v:
            v["tickets"] = []
            
        ticket_record = {
            "id": str(uuid.uuid4())[:8],
            "assigned_to": request.assigned_to,
            "rule_names": request.rule_names,
            "created_at": datetime.utcnow().isoformat(),
            "status": "OPEN"
        }
        v["tickets"].append(ticket_record)
        
        return {"status": "success", "message": f"Ticket assigned to {request.assigned_to}", "ticket": ticket_record}
    except Exception as e:
        logger.error(f"Notification dispatch failed: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

# ============================================================================
# Export Routes (NEW - v8)
# ============================================================================
@router.post("/export", response_model=ExportResponse)
async def export_validation_results(request: ExportRequest):
    """Export validation results to CSV, Excel, JSON, or Database."""
    if request.validation_id not in _validation_store:
        raise HTTPException(status_code=404, detail="Validation not found")

    v = _validation_store[request.validation_id]
    result = v.get("result", {})

    if not result:
        raise HTTPException(status_code=400, detail="No results to export")

    valid_formats = ["csv", "excel", "json", "database"]
    if request.format.lower() not in valid_formats:
        raise HTTPException(status_code=400, detail=f"Invalid format. Choose from: {valid_formats}")

    try:
        timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
        filename = f"validation_{request.validation_id}_{timestamp}"

        if request.format.lower() == "csv":
            file_path = await _export_to_csv(result, filename, request)
        elif request.format.lower() == "excel":
            file_path = await _export_to_excel(result, filename, request)
        elif request.format.lower() == "json":
            file_path = await _export_to_json(result, filename, request)
        elif request.format.lower() == "database":
            file_path = await _export_to_database(result, filename, request)
        else:
            raise HTTPException(status_code=400, detail="Unsupported format")

        v["export_result"] = {
            "format": request.format,
            "file_path": str(file_path),
            "exported_at": datetime.utcnow().isoformat(),
        }

        return ExportResponse(
            validation_id=request.validation_id,
            format=request.format,
            file_path=str(file_path),
            download_url=f"/api/export/download/{request.validation_id}",
            status="success",
            message=f"Exported to {request.format.upper()}",
        )
    except Exception as e:
        logger.error(f"Export failed: {e}")
        raise HTTPException(status_code=500, detail=f"Export failed: {str(e)}")


@router.get("/export/download/{validation_id}")
async def download_export(validation_id: str):
    """Download exported file."""
    if validation_id not in _validation_store:
        raise HTTPException(status_code=404, detail="Validation not found")

    v = _validation_store[validation_id]
    export_result = v.get("export_result")

    if not export_result or not export_result.get("file_path"):
        raise HTTPException(status_code=404, detail="No export file found")

    file_path = Path(export_result["file_path"])
    if not file_path.exists():
        raise HTTPException(status_code=404, detail="Export file not found on disk")

    return FileResponse(
        path=str(file_path),
        filename=file_path.name,
        media_type="application/octet-stream",
    )


async def _export_to_csv(result: Dict, filename: str, request: ExportRequest) -> Path:
    """Export results to CSV format."""
    import pandas as pd

    file_path = EXPORT_DIR / f"{filename}.csv"

    summary = result.get("summary_report", {})
    summary_df = pd.DataFrame([summary])
    summary_df.to_csv(file_path, index=False)

    if request.include_failed_rows:
        validation_results = result.get("validation_results", [])
        failed_rows = []
        for vr in validation_results:
            failed_rows.extend(_get_val(vr, "failure_examples", [])[:100])

        if failed_rows:
            rows_df = pd.DataFrame(failed_rows)
            rows_df.to_csv(EXPORT_DIR / f"{filename}_failed_rows.csv", index=False)

    return file_path


async def _export_to_excel(result: Dict, filename: str, request: ExportRequest) -> Path:
    """Export results to Excel format."""
    import pandas as pd

    file_path = EXPORT_DIR / f"{filename}.xlsx"

    with pd.ExcelWriter(file_path) as writer:
        summary = result.get("summary_report", {})
        pd.DataFrame([summary]).to_excel(writer, sheet_name="Summary", index=False)

        validation_results = result.get("validation_results", [])
        if validation_results:
            vr_data = []
            for vr in validation_results:
                vr_data.append({
                    "rule_name": _get_val(vr, "rule_name", ""),
                    "status": _get_val(vr, "status", ""),
                    "severity": _get_val(vr, "severity", ""),
                    "failed_count": _get_val(vr, "failed_count", 0),
                    "total_count": _get_val(vr, "total_count", 0),
                    "failure_percentage": _get_val(vr, "failure_percentage", 0),
                })
            pd.DataFrame(vr_data).to_excel(writer, sheet_name="Validation Results", index=False)

        fix_recommendations = result.get("fix_recommendations", [])
        if fix_recommendations:
            fix_data = []
            for fr in fix_recommendations:
                fix_data.append({
                    "rule_id": _get_val(fr, "rule_id", ""),
                    "issue": _get_val(fr, "issue_description", ""),
                    "fix": _get_val(fr, "recommended_fix", ""),
                    "query": _get_val(fr, "fix_query", ""),
                    "action": _get_val(fr, "user_action", "manual"),
                })
            pd.DataFrame(fix_data).to_excel(writer, sheet_name="Fix Recommendations", index=False)

    return file_path


async def _export_to_json(result: Dict, filename: str, request: ExportRequest) -> Path:
    """Export results to JSON format."""
    import json

    file_path = EXPORT_DIR / f"{filename}.json"

    export_data = {
        "summary_report": result.get("summary_report", {}),
        "validation_results": [
            {
                "rule_name": _get_val(vr, "rule_name", ""),
                "status": _get_val(vr, "status", ""),
                "severity": _get_val(vr, "severity", ""),
                "failed_count": _get_val(vr, "failed_count", 0),
                "total_count": _get_val(vr, "total_count", 0),
                "failure_examples": _get_val(vr, "failure_examples", [])[:50],
            }
            for vr in result.get("validation_results", [])
        ],
        "fix_recommendations": [
            {
                "rule_id": _get_val(fr, "rule_id", ""),
                "issue": _get_val(fr, "issue_description", ""),
                "fix": _get_val(fr, "recommended_fix", ""),
                "query": _get_val(fr, "fix_query", ""),
                "action": _get_val(fr, "user_action", "manual"),
            }
            for fr in result.get("fix_recommendations", [])
        ],
    }

    with open(file_path, 'w') as f:
        json.dump(export_data, f, indent=2, default=str)

    return file_path


async def _export_to_database(result: Dict, filename: str, request: ExportRequest) -> Path:
    """Export results to database (placeholder)."""
    file_path = EXPORT_DIR / f"{filename}_db_export.txt"
    with open(file_path, 'w') as f:
        f.write("Database export - implementation pending")
    return file_path


# ============================================================================
# Quick Fix Routes (NEW - v8)
# ============================================================================
@router.get("/validate/{validation_id}/fixes")
async def get_fix_recommendations(validation_id: str):
    """Get fix recommendations for failed validation rules."""
    if validation_id not in _validation_store:
        raise HTTPException(status_code=404, detail="Validation not found")

    result = _validation_store[validation_id].get("result", {})
    fix_recommendations = result.get("fix_recommendations", [])

    if not fix_recommendations:
        return {"validation_id": validation_id, "fixes": [], "message": "No fix recommendations available"}

    fixes = []
    for f in fix_recommendations:
        fixes.append({
            "rule_id": _get_val(f, "rule_id", ""),
            "rule_name": _get_val(f, "rule_id", "").replace("agent_rule_", "Rule "), # Fallback if no rule_name
            "issue_description": _get_val(f, "issue_description", ""),
            "status": "failed", # Only failed rules get fixes typically
            "severity": _get_val(f, "risk_level", "warning"),
            "failed_count": _get_val(f, "estimated_rows_affected", 0),
            "total_rows": result.get("summary_report", {}).get("records_processed", 0),
            "failure_percentage": (_get_val(f, "estimated_rows_affected", 0) / max(1, result.get("summary_report", {}).get("records_processed", 1))) * 100,
            "suggested_fixes": [
                {
                    "instruction": _get_val(f, "recommended_fix", ""),
                    "label": "Agent Recommended",
                }
            ],
            "executed_query": _get_val(f, "fix_query", None),
        })

    # Also include passed rules for the UI
    validation_results = result.get("validation_results", [])
    for vr in validation_results:
        if _get_val(vr, "status", "") == "passed":
            fixes.append({
                "rule_name": _get_val(vr, "rule_name", ""),
                "status": "passed",
                "severity": "info",
            })
            
    # Add rule_name to failed fixes if available in validation_results
    for fix in fixes:
         if fix["status"] != "passed":
             for vr in validation_results:
                 if _get_val(vr, "rule_id", "") == fix.get("rule_id"):
                     fix["rule_name"] = _get_val(vr, "rule_name", fix.get("rule_name"))
                     fix["failure_examples"] = _get_val(vr, "failure_examples", [])
                     break

    return {
        "validation_id": validation_id, 
        "fixes": fixes, 
        "total_issues": len([f for f in fixes if f["status"] != "passed"]),
        "message": "Fix recommendations retrieved"
    }


class FixInstruction(BaseModel):
    rule_name: str
    instruction: str


class ApplyFixesRequest(BaseModel):
    fix_instructions: List[FixInstruction]
    use_agent: bool = True





@router.post("/validate/{validation_id}/fix")
async def apply_fixes(validation_id: str, request: ApplyFixesRequest):
    """Apply fixes to the dataset based on instructions."""
    if validation_id not in _validation_store:
        raise HTTPException(status_code=404, detail="Validation not found")

    v = _validation_store[validation_id]
    source_id = v.get("data_source_id")
    target_path = v.get("target_path")
    
    if not source_id or not target_path:
        raise HTTPException(status_code=400, detail="Validation missing source information")
        
    source = _get_source(source_id)
    connector = ConnectorFactory.create_connector(
        source["source_type"], source["connection_config"]
    )
    
    try:
        await connector.connect()
        # Fetch data to apply fixes to
        # In a real app we'd paginate or use SQL, but for this demo we'll use pandas on a sample
        sample_data = await connector.sample_data(target_path, sample_size=1000, full_scan=True)
        await connector.disconnect()
        
        import pandas as pd
        df = pd.DataFrame(sample_data)
        original_rows = len(df)
        
        # Simple mock application of fixes if not using actual agent
        # For a full implementation, this would call the LLM to generate pandas code
        # based on the instructions, then `exec()` it safely.
        
        fixed_df = df.copy()
        
        # Mock fix: Just drop rows with nulls for demonstration
        for instr in request.fix_instructions:
            if "drop" in instr.instruction.lower() or "remove" in instr.instruction.lower():
                fixed_df = fixed_df.dropna()
            elif "fill" in instr.instruction.lower() or "replace" in instr.instruction.lower():
                fixed_df = fixed_df.fillna("FIXED")
                
        fixed_rows = len(fixed_df)
        
        # Save to export result instead of modifying the live DB directly
        v["result"]["fixed_data"] = fixed_df.to_dict(orient='records')
        
        return {
            "validation_id": validation_id,
            "status": "success",
            "message": "Fixes applied to staging dataset",
            "original_rows": original_rows,
            "fixed_rows": fixed_rows,
            "rows_removed": original_rows - fixed_rows,
            "columns": list(fixed_df.columns),
            "preview": fixed_df.head(10).to_dict(orient='records')
        }
        
        
    except Exception as e:
        logger.error(f"Error applying fixes: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to apply fixes: {str(e)}")


@router.post("/validate/{validation_id}/apply-fix", response_model=FixActionResponse)
async def apply_fix_action(request: FixActionRequest):
    """Apply fix action to a validation rule (manual/auto/skip)."""
    if request.validation_id not in _validation_store:
        raise HTTPException(status_code=404, detail="Validation not found")

    v = _validation_store[request.validation_id]
    result = v.get("result", {})
    fix_recommendations = result.get("fix_recommendations", [])

    matching_fix = None
    for f in fix_recommendations:
        if _get_val(f, "rule_id", "") == request.rule_id:
            matching_fix = f
            break

    if not matching_fix:
        raise HTTPException(status_code=404, detail=f"Fix recommendation not found for rule {request.rule_id}")

    if request.action == "manual":
        status = "pending_manual"
        message = "Fix query provided for manual execution"
        fix_query = _get_val(matching_fix, "fix_query", None) or request.custom_fix_query
    elif request.action == "auto_agent":
        status = "pending_auto"
        message = "Agent will execute fix automatically"
        fix_query = _get_val(matching_fix, "fix_query", None)
    elif request.action == "skip":
        status = "skipped"
        message = "Fix skipped by user"
        fix_query = None
    else:
        raise HTTPException(status_code=400, detail=f"Invalid action: {request.action}")

    if "fix_actions" not in v:
        v["fix_actions"] = {}
    v["fix_actions"][request.rule_id] = {
        "action": request.action,
        "status": status,
        "applied_at": datetime.utcnow().isoformat(),
        "custom_query": request.custom_fix_query,
    }

    return FixActionResponse(
        validation_id=request.validation_id,
        rule_id=request.rule_id,
        action=request.action,
        status=status,
        message=message,
        fix_query=fix_query,
    )


# ============================================================================
# Rule Management Routes
# ============================================================================
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


# ============================================================================
# AI Routes
# ============================================================================
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


# ============================================================================
# Chatbot Agent Routes (NEW - v10)
# ============================================================================

class ChatRequest(BaseModel):
    message: str
    validation_id: str
    history: Optional[List[Dict[str, Any]]] = None


@router.post("/chat")
async def chat_with_agent(request: ChatRequest):
    """Interact with the Hybrid Chatbot Agent."""
    if request.validation_id not in _validation_store:
        raise HTTPException(status_code=404, detail="Validation session not found")

    v = _validation_store[request.validation_id]
    source_id = v.get("data_source_id")
    target_path = v.get("target_path")
    
    if not source_id or not target_path:
        raise HTTPException(status_code=400, detail="Validation session missing source information")
        
    source = _get_source(source_id)
    source_info = DataSourceInfo(
        source_type=source["source_type"],
        connection_config=source["connection_config"],
        target_path=target_path,
        schema=v.get("result", {}).get("schema"),
        selected_columns=v.get("selected_columns"),
        column_mapping=v.get("column_mapping"),
        slice_filters=v.get("slice_filters"),
    )

    from app.agents.chatbot_agent import ChatbotAgent
    agent = ChatbotAgent()
    
    # Get history from store if not provided
    chat_history = request.history or v.get("chat_history", [])
    
    try:
        result = await agent.run(source_info, request.message, history=chat_history)
        
        # Update store with new history
        v["chat_history"] = result["history"]
        
        return {
            "response": result["response"],
            "history": result["history"],
            "status": result["status"]
        }
    except Exception as e:
        logger.exception("Chatbot interaction failed")
        raise HTTPException(status_code=500, detail=str(e))


# ============================================================================
# LLM Health Check
# ============================================================================
@router.get("/llm/health", response_model=LLMHealthResponse)
async def check_llm_health():
    """Check LLM service health."""
    llm_service = get_llm_service()
    health = await llm_service.check_health()
    return LLMHealthResponse(**health)


# ============================================================================
# File Upload Routes
# ============================================================================
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

# ============================================================================
# Dynamic Filter & Pivot Discovery Routes (NEW - v9)
# ============================================================================

class FilterSelectionItem(BaseModel):
    """A single user filter selection."""
    column: str
    filter_type: str
    selected_values: Optional[List[Any]] = None
    min_value: Optional[Any] = None
    max_value: Optional[Any] = None
    text_pattern: Optional[str] = None
    is_negated: bool = False


class ApplyFiltersRequest(BaseModel):
    """Apply structured filters to a data source."""
    resource_path: str
    filters: List[FilterSelectionItem]
    # Optional: use a template-applied virtual dataset instead of raw file
    template_session_id: Optional[str] = None


class ApplyPivotRequest(BaseModel):
    """Apply pivot operations on (optionally filtered) data."""
    resource_path: str
    dimensions: List[str]
    measures: List[Dict[str, str]]  # [{"column": "x", "aggregation": "sum"}]
    filters: Optional[List[FilterSelectionItem]] = None
    # Optional: use a template-applied virtual dataset instead of raw file
    template_session_id: Optional[str] = None


def _load_df_for_request(
    source: Dict[str, Any],
    resource_path: str,
    template_session_id: Optional[str],
    sample_size: int = 50000,
) -> "pd.DataFrame":
    """
    Synchronous helper used inside async route handlers.
    Returns the virtual df (when a template session is active) OR schedules
    a data load.  NOTE: callers must await the connector themselves; this
    function is a *sync* helper that only resolves session data.
    Returns None if the session path should be used, and the session dict.
    """
    if template_session_id:
        try:
            from app.agents.template_routes import get_applied_session
            session = get_applied_session(template_session_id)
            if session:
                import pandas as pd, json as _json
                raw = session["df_json"]
                records = _json.loads(raw) if isinstance(raw, str) else raw
                return pd.DataFrame(records)
        except Exception as e:
            logger.warning(f"Template session load failed, falling back to raw data: {e}")
    return None   # caller should load from connector


@router.post("/datasources/{source_id}/discover-filters")
async def discover_filters(
    source_id: str,
    resource_path: str = Query(...),
    template_session_id: Optional[str] = Query(default=None),
):
    """
    Run filter & pivot discovery agents on a data source resource.
    When template_session_id is provided, discovery runs on the virtual
    restricted dataset produced by a prior template-apply step.
    """
    source = _get_source(source_id)
    try:
        import pandas as pd
        from app.agents.filter_discovery import TemplateAwareDiscoveryManager
        from app.agents.chart_engine import ChartEngine

        # ── resolve dataframe ─────────────────────────────────────────────
        df = _load_df_for_request(source, resource_path, template_session_id)
        if df is None:
            connector = ConnectorFactory.create_connector(
                source["source_type"], source["connection_config"]
            )
            await connector.connect()
            rows = await connector.sample_data(resource_path, sample_size=50000, method="first")
            await connector.disconnect()
            df = pd.DataFrame(rows)

        if df.empty:
            raise HTTPException(status_code=400, detail="No data found for this resource.")

        manager = TemplateAwareDiscoveryManager()
        result = await manager.discover(df, source_id, resource_path)

        # Generate matplotlib charts
        chart_engine = ChartEngine()
        profiles_raw = [
            {"column_name": p.column_name, "data_type": p.data_type.value,
             "null_percentage": p.null_percentage, "mean_value": p.mean_value,
             "unique_count": p.unique_count}
            for p in manager._last_filter_metadata.column_profiles
        ]
        result["charts"] = {
            "overview": chart_engine.generate_dataset_overview(df, profiles_raw),
            "columns": chart_engine.generate_profile_charts(df, profiles_raw),
        }

        if template_session_id:
            result["template_session_id"] = template_session_id

        logger.info(
            f"Filter discovery completed for {source_id}/{resource_path}: "
            f"{result['filter_metadata']['dataset']['columns']} columns, "
            f"{len(result['charts']['columns'])} charts"
            + (f" [template session: {template_session_id}]" if template_session_id else "")
        )
        return result

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Filter discovery failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/datasources/{source_id}/apply-filters")
async def apply_filters(source_id: str, request: ApplyFiltersRequest):
    """
    Apply structured filters and return filtered data preview.
    When request.template_session_id is set, filtering is restricted to
    the virtual template-matched dataset.
    """
    source = _get_source(source_id)
    try:
        import pandas as pd
        from app.agents.filter_discovery import DynamicFilterExecutor, UserFilterSelection

        # ── resolve dataframe ─────────────────────────────────────────────
        df = _load_df_for_request(source, request.resource_path, request.template_session_id)
        if df is None:
            connector = ConnectorFactory.create_connector(
                source["source_type"], source["connection_config"]
            )
            await connector.connect()
            rows = await connector.sample_data(request.resource_path, sample_size=50000, method="first")
            await connector.disconnect()
            df = pd.DataFrame(rows)

        selections = [
            UserFilterSelection(
                column=f.column,
                filter_type=f.filter_type,
                selected_values=f.selected_values,
                min_value=f.min_value,
                max_value=f.max_value,
                text_pattern=f.text_pattern,
                is_negated=f.is_negated,
            )
            for f in request.filters
        ]

        executor = DynamicFilterExecutor()
        filtered_df, log = executor.apply_filters(df, selections)

        # Generate before/after chart
        filter_chart = None
        try:
            from app.agents.chart_engine import ChartEngine
            chart_engine = ChartEngine()
            filter_cols = [f.column for f in request.filters]
            filter_chart = chart_engine.generate_filtered_chart(df, filtered_df, filter_cols)
        except Exception as chart_err:
            logger.warning(f"Filter chart generation failed: {chart_err}")

        preview_rows = filtered_df.head(200).to_dict(orient="records")
        return {
            "total_rows_before": len(df),
            "total_rows_after": len(filtered_df),
            "preview_rows": preview_rows,
            "execution_log": log,
            "chart": filter_chart,
            "template_session_active": bool(request.template_session_id),
            "available_columns": list(df.columns),
        }

    except Exception as e:
        logger.error(f"Apply filters failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/datasources/{source_id}/apply-pivot")
async def apply_pivot(source_id: str, request: ApplyPivotRequest):
    """Apply pivot operations on (optionally filtered) data."""
    source = _get_source(source_id)
    try:
        import pandas as pd
        from app.agents.filter_discovery import (
            DynamicFilterExecutor, DynamicPivotExecutor,
            UserFilterSelection, UserPivotSelection,
        )

        # ── resolve dataframe (template session or raw) ───────────────────
        df = _load_df_for_request(source, request.resource_path, request.template_session_id)
        if df is None:
            connector = ConnectorFactory.create_connector(
                source["source_type"], source["connection_config"]
            )
            await connector.connect()
            rows = await connector.sample_data(request.resource_path, sample_size=50000, method="first")
            await connector.disconnect()
            df = pd.DataFrame(rows)

        # Apply filters first (if any)
        if request.filters:
            selections = [
                UserFilterSelection(
                    column=f.column,
                    filter_type=f.filter_type,
                    selected_values=f.selected_values,
                    min_value=f.min_value,
                    max_value=f.max_value,
                    text_pattern=f.text_pattern,
                    is_negated=f.is_negated,
                )
                for f in request.filters
            ]
            executor = DynamicFilterExecutor()
            df, _ = executor.apply_filters(df, selections)

        # Apply pivot
        pivot_sel = UserPivotSelection(
            dimensions=request.dimensions,
            measures=request.measures,
        )
        pivot_exec = DynamicPivotExecutor()
        pivoted = pivot_exec.apply_pivot(df, pivot_sel)

        # Generate pivot chart
        pivot_chart = None
        try:
            from app.agents.chart_engine import ChartEngine
            chart_engine = ChartEngine()
            pivot_chart = chart_engine.generate_pivot_chart(
                pivoted, request.dimensions, request.measures
            )
        except Exception as chart_err:
            logger.warning(f"Pivot chart generation failed: {chart_err}")

        return {
            "total_rows_input": len(df),
            "total_rows_output": len(pivoted),
            "columns": list(pivoted.columns),
            "rows": pivoted.head(500).to_dict(orient="records"),
            "chart": pivot_chart,
            "template_session_active": bool(request.template_session_id),
            "available_columns": list(df.columns),
        }

    except Exception as e:
        logger.error(f"Apply pivot failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# ============================================================================
# System Routes
# ============================================================================
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
    return {"source_types": ConnectorFactory.get_supported_types()}

# ============================================================================
# LLM Settings
# ============================================================================
from pydantic import BaseModel
import os

class LLMSettingsUpdate(BaseModel):
    provider: str
    ollama_base_url: str = ""
    ollama_model: str = ""
    lmstudio_base_url: str = ""
    openai_api_key: str = ""
    openai_model: str = ""
    anthropic_api_key: str = ""
    anthropic_model: str = ""
    gemini_api_key: str = ""
    gemini_model: str = ""
    groq_api_keys: str = ""
    groq_model: str = ""
    openrouter_api_keys: str = ""
    openrouter_model: str = ""

@router.get("/settings")
async def get_settings_route():
    """Get current settings from .env file."""
    try:
        from dotenv import dotenv_values
        env_path = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(__file__))), ".env")
        env_vars = dotenv_values(env_path) if os.path.exists(env_path) else {}
        
        return {
            "provider": env_vars.get("LLM_PROVIDER", "ollama"),
            "ollama_base_url": env_vars.get("OLLAMA_BASE_URL", "http://localhost:11434"),
            "ollama_model": env_vars.get("OLLAMA_MODEL", "llama3.2"),
            "lmstudio_base_url": env_vars.get("LMSTUDIO_BASE_URL", "http://localhost:1234/v1"),
            "openai_api_key": env_vars.get("OPENAI_API_KEY", ""),
            "openai_model": env_vars.get("OPENAI_MODEL", "gpt-4"),
            "anthropic_api_key": env_vars.get("ANTHROPIC_API_KEY", ""),
            "anthropic_model": env_vars.get("ANTHROPIC_MODEL", "claude-3-5-sonnet-20241022"),
            "gemini_api_key": env_vars.get("GEMINI_API_KEY", ""),
            "gemini_model": env_vars.get("GEMINI_MODEL", "gemini-2.5-pro"),
            "groq_api_keys": env_vars.get("GROQ_API_KEYS", ""),
            "groq_model": env_vars.get("GROQ_MODEL", "llama-3.2-90b-vision-preview"),
            "openrouter_api_keys": env_vars.get("OPENROUTER_API_KEYS", ""),
            "openrouter_model": env_vars.get("OPENROUTER_MODEL", "deepseek/deepseek-r1"),
        }
    except Exception as e:
        logger.error(f"Failed to fetch settings: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/settings")
async def update_settings(payload: LLMSettingsUpdate):
    """Update settings in the .env file."""
    try:
        from dotenv import set_key
        env_path = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(__file__))), ".env")
        
        # Ensure .env exists
        if not os.path.exists(env_path):
            with open(env_path, "w") as f:
                f.write("")

        set_key(env_path, "LLM_PROVIDER", payload.provider)
        
        if payload.provider == "ollama":
            if payload.ollama_base_url: set_key(env_path, "OLLAMA_BASE_URL", payload.ollama_base_url)
            if payload.ollama_model: set_key(env_path, "OLLAMA_MODEL", payload.ollama_model)
        elif payload.provider == "openai":
            if payload.openai_api_key: set_key(env_path, "OPENAI_API_KEY", payload.openai_api_key)
            if payload.openai_model: set_key(env_path, "OPENAI_MODEL", payload.openai_model)
        elif payload.provider == "anthropic":
            if payload.anthropic_api_key: set_key(env_path, "ANTHROPIC_API_KEY", payload.anthropic_api_key)
            if payload.anthropic_model: set_key(env_path, "ANTHROPIC_MODEL", payload.anthropic_model)
        elif payload.provider == "gemini":
            if payload.gemini_api_key: set_key(env_path, "GEMINI_API_KEY", payload.gemini_api_key)
            if payload.gemini_model: set_key(env_path, "GEMINI_MODEL", payload.gemini_model)
        elif payload.provider == "groq":
            if payload.groq_api_keys: set_key(env_path, "GROQ_API_KEYS", payload.groq_api_keys)
            if payload.groq_model: set_key(env_path, "GROQ_MODEL", payload.groq_model)
        elif payload.provider == "openrouter":
            if payload.openrouter_api_keys: set_key(env_path, "OPENROUTER_API_KEYS", payload.openrouter_api_keys)
            if payload.openrouter_model: set_key(env_path, "OPENROUTER_MODEL", payload.openrouter_model)
            
        settings.LLM_PROVIDER = payload.provider

        return {"status": "success", "message": "Settings saved successfully."}
    except Exception as e:
        logger.error(f"Failed to update settings: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))