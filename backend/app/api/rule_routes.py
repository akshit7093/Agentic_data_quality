"""
Rule Groups API — CRUD endpoints for managing rule groups.
"""
import logging
from typing import Optional, List
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from app.models.rule_groups import get_rule_group_store

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/api/v1/rules", tags=["rules"])


# ── Request / Response Models ───────────────────────────────

class CreateGroupRequest(BaseModel):
    name: str
    description: str = ""
    target_files: List[str] = []


class UpdateGroupRequest(BaseModel):
    name: Optional[str] = None
    description: Optional[str] = None
    target_files: Optional[List[str]] = None
    is_active: Optional[bool] = None


class AddRuleRequest(BaseModel):
    rule_name: str
    target_file: str = ""
    rule_type: str = "column"
    severity: str = "warning"
    query: str = ""
    query_type: str = "sql"
    description: str = ""
    is_active: bool = True


class UpdateRuleRequest(BaseModel):
    rule_name: Optional[str] = None
    target_file: Optional[str] = None
    rule_type: Optional[str] = None
    severity: Optional[str] = None
    query: Optional[str] = None
    query_type: Optional[str] = None
    description: Optional[str] = None
    is_active: Optional[bool] = None


# ── Group Endpoints ─────────────────────────────────────────

@router.get("/groups")
async def list_groups():
    """List all rule groups with summary info."""
    store = get_rule_group_store()
    return {"groups": store.list_groups()}


@router.post("/groups")
async def create_group(req: CreateGroupRequest):
    """Create a new rule group."""
    store = get_rule_group_store()
    group = store.create_group(
        name=req.name,
        description=req.description,
        target_files=req.target_files,
    )
    return {"group": group}


@router.get("/groups/{group_id}")
async def get_group(group_id: str):
    """Get a group with all its rules."""
    store = get_rule_group_store()
    group = store.get_group(group_id)
    if not group:
        raise HTTPException(status_code=404, detail="Rule group not found")
    return {"group": group}


@router.put("/groups/{group_id}")
async def update_group(group_id: str, req: UpdateGroupRequest):
    """Update group metadata."""
    store = get_rule_group_store()
    updates = {k: v for k, v in req.dict().items() if v is not None}
    group = store.update_group(group_id, **updates)
    if not group:
        raise HTTPException(status_code=404, detail="Rule group not found")
    return {"group": group}


@router.delete("/groups/{group_id}")
async def delete_group(group_id: str):
    """Delete a rule group and all its rules."""
    store = get_rule_group_store()
    deleted = store.delete_group(group_id)
    if not deleted:
        raise HTTPException(status_code=404, detail="Rule group not found")
    return {"status": "deleted"}


# ── Rule Endpoints ──────────────────────────────────────────

@router.post("/groups/{group_id}/rules")
async def add_rule(group_id: str, req: AddRuleRequest):
    """Add a rule to a group."""
    store = get_rule_group_store()
    rule = store.add_rule(group_id, **req.dict())
    if not rule:
        raise HTTPException(status_code=404, detail="Rule group not found")
    return {"rule": rule}


@router.put("/groups/{group_id}/rules/{rule_id}")
async def update_rule(group_id: str, rule_id: str, req: UpdateRuleRequest):
    """Update a rule within a group."""
    store = get_rule_group_store()
    updates = {k: v for k, v in req.dict().items() if v is not None}
    rule = store.update_rule(group_id, rule_id, **updates)
    if not rule:
        raise HTTPException(status_code=404, detail="Rule or group not found")
    return {"rule": rule}


@router.delete("/groups/{group_id}/rules/{rule_id}")
async def delete_rule(group_id: str, rule_id: str):
    """Delete a rule from a group."""
    store = get_rule_group_store()
    deleted = store.delete_rule(group_id, rule_id)
    if not deleted:
        raise HTTPException(status_code=404, detail="Rule or group not found")
    return {"status": "deleted"}


# ── Utility Endpoints ───────────────────────────────────────

@router.get("/for-file/{target_file:path}")
async def get_rules_for_file(target_file: str):
    """Get all active rules from all active groups for a specific file."""
    store = get_rule_group_store()
    rules = store.get_rules_for_file(target_file)
    return {"rules": rules, "count": len(rules)}
