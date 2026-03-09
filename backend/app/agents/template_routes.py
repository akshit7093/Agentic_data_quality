"""
Template Routes  –  /api/v1/templates
======================================
Full CRUD for column templates plus two action endpoints:

  POST /templates/{id}/match  → run fuzzy match and return proposals
  POST /templates/{id}/apply  → apply confirmed mappings to a file and
                                 return a restricted dataset definition
                                 (persisted in-session for use by filter/pivot)
"""
from __future__ import annotations

import logging
import uuid
from typing import Any, Dict, List, Optional

import pandas as pd
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from app.agents.template_service import (
    TemplateApplier,
    TemplateMatcher,
    get_template_store,
)

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/api/v1/templates", tags=["templates"])


# ── In-memory session store for applied virtual datasets ─────────────────────
# Key: session_id  →  {"df_json": ..., "columns": [...], "rename_map": {...}}
_applied_sessions: Dict[str, Dict[str, Any]] = {}


def get_applied_session(session_id: str) -> Optional[Dict[str, Any]]:
    return _applied_sessions.get(session_id)


def set_applied_session(session_id: str, payload: Dict[str, Any]):
    _applied_sessions[session_id] = payload


def clear_applied_session(session_id: str):
    _applied_sessions.pop(session_id, None)


def get_session_column_selection(session_id: str) -> Optional[Dict[str, Any]]:
    """Return selected_columns and column_mapping from an applied template session.

    Returns a dict with keys:
      - ``selected_columns``: list[str]  — the renamed/output column names
      - ``column_mapping``:   dict[str, str] — original_name → renamed_name
      - ``source_id``:        str
      - ``resource_path``:    str

    Returns None if the session_id is unknown.
    This is consumed by the validation API route to populate DataSourceInfo
    so the agent only analyses columns the user confirmed during template matching.
    """
    session = _applied_sessions.get(session_id)
    if not session:
        return None
    return {
        "selected_columns": session.get("columns", []),
        "column_mapping": session.get("rename_map", {}),
        "source_id": session.get("source_id", ""),
        "resource_path": session.get("resource_path", ""),
    }


# ══════════════════════════════════════════════════════════════════════════════
# REQUEST / RESPONSE MODELS
# ══════════════════════════════════════════════════════════════════════════════

class TemplateColumnRequest(BaseModel):
    name: str
    dtype_hint: str
    description: str = ""
    required: bool = True
    aliases: List[str] = []


class CreateTemplateRequest(BaseModel):
    name: str
    description: str = ""
    columns: List[TemplateColumnRequest]
    name_similarity_min: float = 0.70
    dtype_match_required: bool = True


class UpdateTemplateRequest(BaseModel):
    name: Optional[str] = None
    description: Optional[str] = None
    columns: Optional[List[TemplateColumnRequest]] = None
    name_similarity_min: Optional[float] = None
    dtype_match_required: Optional[bool] = None
    is_active: Optional[bool] = None


class MatchRequest(BaseModel):
    """Run fuzzy match against a data source file."""
    source_id: str          # data source id (maps to existing source registry)
    resource_path: str      # table name / file path
    name_similarity_min: Optional[float] = None    # override template threshold
    dtype_match_required: Optional[bool] = None    # override template threshold


class ConfirmedMapping(BaseModel):
    template_col: str
    file_col: Optional[str] = None
    output_name: str


class ApplyRequest(BaseModel):
    """Apply confirmed column mappings and produce a virtual restricted dataset."""
    source_id: str
    resource_path: str
    confirmed_mappings: List[ConfirmedMapping]
    extra_columns: List[str] = []     # additional file cols user dragged in


# ══════════════════════════════════════════════════════════════════════════════
# CRUD ENDPOINTS
# ══════════════════════════════════════════════════════════════════════════════

@router.get("")
async def list_templates():
    store = get_template_store()
    return {"templates": store.list_all()}


@router.post("")
async def create_template(req: CreateTemplateRequest):
    store = get_template_store()
    t = store.create(
        name=req.name,
        description=req.description,
        columns=[c.dict() for c in req.columns],
        name_similarity_min=req.name_similarity_min,
        dtype_match_required=req.dtype_match_required,
    )
    return {"template": t.to_dict()}


@router.get("/{template_id}")
async def get_template(template_id: str):
    store = get_template_store()
    t = store.get(template_id)
    if not t:
        raise HTTPException(status_code=404, detail="Template not found")
    return {"template": t.to_dict()}


@router.put("/{template_id}")
async def update_template(template_id: str, req: UpdateTemplateRequest):
    store = get_template_store()
    updates = {k: v for k, v in req.dict().items() if v is not None}
    if "columns" in updates:
        updates["columns"] = [c for c in updates["columns"]]   # keep as dicts
    t = store.update(template_id, **updates)
    if not t:
        raise HTTPException(status_code=404, detail="Template not found")
    return {"template": t.to_dict()}


@router.delete("/{template_id}")
async def delete_template(template_id: str):
    store = get_template_store()
    ok = store.delete(template_id)
    if not ok:
        raise HTTPException(status_code=404, detail="Template not found")
    return {"status": "deleted"}


# ══════════════════════════════════════════════════════════════════════════════
# MATCH  –  fuzzy proposal
# ══════════════════════════════════════════════════════════════════════════════

@router.post("/{template_id}/match")
async def match_template(template_id: str, req: MatchRequest):
    """
    Load the target file, run fuzzy column matching, return proposals.
    The response is serialisable so the frontend can render the drag-and-drop UI.
    """
    store = get_template_store()
    template = store.get(template_id)
    if not template:
        raise HTTPException(status_code=404, detail="Template not found")

    # ── load a sample of the file for dtype inference ────────────────────────
    df = await _load_sample(req.source_id, req.resource_path)

    # ── run matcher ──────────────────────────────────────────────────────────
    matcher = TemplateMatcher()
    report = matcher.match(
        df,
        template,
        name_similarity_min=req.name_similarity_min,
        dtype_match_required=req.dtype_match_required,
    )

    # Serialise to plain dicts for JSON transport
    from dataclasses import asdict
    return {
        "template_id": report.template_id,
        "template_name": report.template_name,
        "file_columns": report.file_columns,
        "file_dtypes": {col: str(df[col].dtype) for col in df.columns},
        "matches": [asdict(m) for m in report.matches],
        "unmatched_file_cols": report.unmatched_file_cols,
        "overall_coverage": report.overall_coverage,
        "thresholds_used": report.thresholds_used,
    }


# ══════════════════════════════════════════════════════════════════════════════
# APPLY  –  confirm mappings and create virtual dataset session
# ══════════════════════════════════════════════════════════════════════════════

@router.post("/{template_id}/apply")
async def apply_template(template_id: str, req: ApplyRequest):
    """
    Confirm user-reviewed mappings and store a restricted virtual dataset.
    Returns a session_id the frontend uses when calling apply-filters/apply-pivot.
    """
    store = get_template_store()
    template = store.get(template_id)
    if not template:
        raise HTTPException(status_code=404, detail="Template not found")

    df = await _load_sample(req.source_id, req.resource_path, full=True)

    mappings = [m.dict() for m in req.confirmed_mappings if m.file_col]
    applier = TemplateApplier()
    virtual_df, rename_map = applier.apply(df, mappings, extra_columns=req.extra_columns)

    session_id = str(uuid.uuid4())
    set_applied_session(session_id, {
        "df_json": virtual_df.to_json(orient="records", date_format="iso"),
        "columns": list(virtual_df.columns),
        "dtypes": {col: str(virtual_df[col].dtype) for col in virtual_df.columns},
        "rename_map": rename_map,
        "source_id": req.source_id,
        "resource_path": req.resource_path,
        "template_id": template_id,
        "template_name": template.name,
        "row_count": len(virtual_df),
    })

    return {
        "session_id": session_id,
        "columns": list(virtual_df.columns),
        "dtypes": {col: str(virtual_df[col].dtype) for col in virtual_df.columns},
        "rename_map": rename_map,
        "row_count": len(virtual_df),
        "preview": virtual_df.head(20).to_dict(orient="records"),
    }


@router.delete("/sessions/{session_id}")
async def clear_session(session_id: str):
    clear_applied_session(session_id)
    return {"status": "cleared"}


# ══════════════════════════════════════════════════════════════════════════════
# INTERNAL HELPER
# ══════════════════════════════════════════════════════════════════════════════

async def _load_sample(source_id: str, resource_path: str, full: bool = False) -> pd.DataFrame:
    """Load data from a registered source for dtype inference."""
    # Import inline to avoid circular dependency
    from app.connectors.factory import ConnectorFactory
    from app.connectors.dataframe_connector import DuckDBFileConnector

    _FILE_SOURCE_TYPES = {"local_file", "csv", "json", "jsonl", "excel", "parquet", "feather", "tsv"}

    # Resolve the source registration
    try:
        from app.api.routes import _get_source  # re-use existing source registry helper
        source = _get_source(source_id)
    except Exception:
        raise HTTPException(status_code=404, detail=f"Data source '{source_id}' not found")

    source_type = source["source_type"]
    conn_cfg = source["connection_config"]

    if str(source_type).lower() in _FILE_SOURCE_TYPES:
        connector = DuckDBFileConnector(conn_cfg)
    else:
        connector = ConnectorFactory.create_connector(source_type, conn_cfg)

    await connector.connect()
    rows = await connector.sample_data(
        resource_path,
        sample_size=500000 if full else 2000,
        method="first",
    )
    await connector.disconnect()

    return pd.DataFrame(rows)