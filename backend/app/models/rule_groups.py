"""
Rule Groups — JSON-file-backed store for organized rule management.

Supports named rule groups (e.g., "BMC Files") with per-file rules.
Groups can be selected during validation for continuous work on the same files.
No PostgreSQL required — data persists to a JSON file on disk.
"""
import json
import uuid
import logging
from datetime import datetime
from pathlib import Path
from typing import Dict, Any, List, Optional
from dataclasses import dataclass, asdict, field

logger = logging.getLogger(__name__)

# Storage path
STORE_DIR = Path(__file__).parent.parent.parent.parent / "data"
STORE_FILE = STORE_DIR / "rule_groups.json"


@dataclass
class GroupRule:
    """A single rule within a group."""
    id: str = field(default_factory=lambda: str(uuid.uuid4())[:8])
    rule_name: str = ""
    target_file: str = ""  # Which file/table this rule applies to
    rule_type: str = "column"  # column, row, table, statistical, custom_sql, custom_pandas
    severity: str = "warning"  # critical, warning, info
    query: str = ""  # SQL or Pandas expression
    query_type: str = "sql"  # sql or pandas
    description: str = ""
    is_active: bool = True
    created_at: str = field(default_factory=lambda: datetime.utcnow().isoformat())


@dataclass
class RuleGroup:
    """A named collection of rules for organized management."""
    id: str = field(default_factory=lambda: str(uuid.uuid4())[:8])
    name: str = ""
    description: str = ""
    target_files: List[str] = field(default_factory=list)  # Files this group applies to
    rules: List[GroupRule] = field(default_factory=list)
    is_active: bool = True
    created_at: str = field(default_factory=lambda: datetime.utcnow().isoformat())
    updated_at: str = field(default_factory=lambda: datetime.utcnow().isoformat())


class RuleGroupStore:
    """JSON-file-backed store for rule groups."""

    def __init__(self, store_path: Optional[Path] = None):
        self.store_path = store_path or STORE_FILE
        self._groups: Dict[str, RuleGroup] = {}
        self._load()

    def _load(self) -> None:
        """Load groups from disk."""
        if self.store_path.exists():
            try:
                data = json.loads(self.store_path.read_text(encoding="utf-8"))
                for gid, gdata in data.items():
                    rules = [GroupRule(**r) for r in gdata.pop("rules", [])]
                    self._groups[gid] = RuleGroup(**gdata, rules=rules)
                logger.info(f"Loaded {len(self._groups)} rule groups from {self.store_path}")
            except Exception as e:
                logger.error(f"Failed to load rule groups: {e}")
                self._groups = {}

    def _save(self) -> None:
        """Persist groups to disk."""
        self.store_path.parent.mkdir(parents=True, exist_ok=True)
        data = {
            gid: {**asdict(g), "rules": [asdict(r) for r in g.rules]}
            for gid, g in self._groups.items()
        }
        self.store_path.write_text(
            json.dumps(data, indent=2, default=str), encoding="utf-8"
        )

    # ── CRUD Operations ─────────────────────────────────────

    def list_groups(self) -> List[Dict[str, Any]]:
        """List all groups with rule counts."""
        return [
            {
                "id": g.id,
                "name": g.name,
                "description": g.description,
                "target_files": g.target_files,
                "rule_count": len(g.rules),
                "active_rules": sum(1 for r in g.rules if r.is_active),
                "is_active": g.is_active,
                "created_at": g.created_at,
                "updated_at": g.updated_at,
            }
            for g in self._groups.values()
        ]

    def get_group(self, group_id: str) -> Optional[Dict[str, Any]]:
        """Get a group with all its rules."""
        g = self._groups.get(group_id)
        if not g:
            return None
        return {
            **asdict(g),
            "rules": [asdict(r) for r in g.rules],
        }

    def create_group(self, name: str, description: str = "", target_files: Optional[List[str]] = None) -> Dict[str, Any]:
        """Create a new rule group."""
        group = RuleGroup(
            name=name,
            description=description,
            target_files=target_files or [],
        )
        self._groups[group.id] = group
        self._save()
        return self.get_group(group.id)

    def update_group(self, group_id: str, **kwargs) -> Optional[Dict[str, Any]]:
        """Update group metadata (not rules)."""
        g = self._groups.get(group_id)
        if not g:
            return None
        for key in ("name", "description", "target_files", "is_active"):
            if key in kwargs:
                setattr(g, key, kwargs[key])
        g.updated_at = datetime.utcnow().isoformat()
        self._save()
        return self.get_group(group_id)

    def delete_group(self, group_id: str) -> bool:
        """Delete a group and all its rules."""
        if group_id in self._groups:
            del self._groups[group_id]
            self._save()
            return True
        return False

    # ── Rule Operations ─────────────────────────────────────

    def add_rule(self, group_id: str, **rule_data) -> Optional[Dict[str, Any]]:
        """Add a rule to a group."""
        g = self._groups.get(group_id)
        if not g:
            return None
        rule = GroupRule(**{k: v for k, v in rule_data.items() if k in GroupRule.__dataclass_fields__})
        g.rules.append(rule)
        g.updated_at = datetime.utcnow().isoformat()
        self._save()
        return asdict(rule)

    def update_rule(self, group_id: str, rule_id: str, **rule_data) -> Optional[Dict[str, Any]]:
        """Update a rule within a group."""
        g = self._groups.get(group_id)
        if not g:
            return None
        for rule in g.rules:
            if rule.id == rule_id:
                for key, val in rule_data.items():
                    if hasattr(rule, key):
                        setattr(rule, key, val)
                g.updated_at = datetime.utcnow().isoformat()
                self._save()
                return asdict(rule)
        return None

    def delete_rule(self, group_id: str, rule_id: str) -> bool:
        """Delete a rule from a group."""
        g = self._groups.get(group_id)
        if not g:
            return False
        original_count = len(g.rules)
        g.rules = [r for r in g.rules if r.id != rule_id]
        if len(g.rules) < original_count:
            g.updated_at = datetime.utcnow().isoformat()
            self._save()
            return True
        return False

    def get_rules_for_file(self, target_file: str) -> List[Dict[str, Any]]:
        """Get all active rules from all active groups that apply to a specific file."""
        results = []
        for g in self._groups.values():
            if not g.is_active:
                continue
            if target_file in g.target_files or not g.target_files:
                for r in g.rules:
                    if r.is_active and (r.target_file == target_file or not r.target_file):
                        results.append({
                            **asdict(r),
                            "group_id": g.id,
                            "group_name": g.name,
                        })
        return results


# Singleton
_store: Optional[RuleGroupStore] = None


def get_rule_group_store() -> RuleGroupStore:
    """Get or create the rule group store singleton."""
    global _store
    if _store is None:
        _store = RuleGroupStore()
    return _store
