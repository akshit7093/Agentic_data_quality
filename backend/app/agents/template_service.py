"""
Template Matching Service
=========================
Manages column templates and performs fuzzy matching between template
column definitions and the columns found in an actual data file.

Architecture:
  - TemplateStore  : JSON-backed CRUD store for templates
  - TemplateMatcher: Fuzzy-matches file columns against a template
  - TemplateApplier: Builds a restricted/renamed DataFrame from confirmed mappings

Configurable thresholds (defaults can be overridden per-template or per-request):
  NAME_SIMILARITY_MIN  : 0.50 – 1.00  (default 0.70)
  DTYPE_MATCH_REQUIRED : bool         (default True)
"""

from __future__ import annotations

import json
import logging
import re
import uuid
from dataclasses import dataclass, field, asdict
from difflib import SequenceMatcher
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd

logger = logging.getLogger(__name__)

# ── Persistence path ──────────────────────────────────────────────────────────
_TEMPLATE_STORE_PATH = Path(__file__).parent.parent.parent / "test_data" / "templates.json"


# ══════════════════════════════════════════════════════════════════════════════
# DATA STRUCTURES
# ══════════════════════════════════════════════════════════════════════════════

@dataclass
class TemplateColumn:
    """A single column definition inside a template."""
    name: str               # canonical column name expected in the file
    dtype_hint: str         # e.g. "int", "float", "str", "datetime", "bool"
    description: str = ""
    required: bool = True
    aliases: List[str] = field(default_factory=list)   # additional accepted names


@dataclass
class DataTemplate:
    """A named template containing an ordered list of expected columns."""
    id: str
    name: str
    description: str
    columns: List[TemplateColumn]
    # Matching thresholds stored with the template
    name_similarity_min: float = 0.70   # 0.50 – 1.00
    dtype_match_required: bool = True   # if True, dtype mismatch disqualifies match
    created_at: str = ""
    updated_at: str = ""
    is_active: bool = True

    # ── serialization helpers ─────────────────────────────────────────────────
    def to_dict(self) -> Dict[str, Any]:
        d = asdict(self)
        return d

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> "DataTemplate":
        cols = [TemplateColumn(**c) for c in d.pop("columns", [])]
        return cls(columns=cols, **d)


@dataclass
class ColumnMatchResult:
    """Result of matching ONE template column against a file."""
    template_col: str           # template column name
    template_dtype: str
    file_col: Optional[str]     # best matching file column (None = no match)
    file_dtype: Optional[str]
    name_score: float           # 0–1
    dtype_match: bool
    overall_score: float        # combined score
    is_confirmed: bool = False  # set to True by user acceptance
    output_name: str = ""       # user-chosen output column name (defaults to template_col)

    def __post_init__(self):
        if not self.output_name:
            self.output_name = self.template_col


@dataclass
class TemplateMatchReport:
    """Full match report for a file against a template."""
    template_id: str
    template_name: str
    file_columns: List[str]
    matches: List[ColumnMatchResult]          # one per template column
    unmatched_file_cols: List[str]            # file cols not matched to any template col
    overall_coverage: float                   # fraction of template cols with a match
    thresholds_used: Dict[str, Any] = field(default_factory=dict)


# ── dtype normalisation map ────────────────────────────────────────────────────
_DTYPE_GROUPS: Dict[str, str] = {
    # integers
    "int8": "int", "int16": "int", "int32": "int", "int64": "int",
    "uint8": "int", "uint16": "int", "uint32": "int", "uint64": "int",
    "integer": "int", "int": "int",
    # floats
    "float16": "float", "float32": "float", "float64": "float",
    "double": "float", "float": "float", "numeric": "float", "decimal": "float",
    # strings / objects
    "object": "str", "string": "str", "str": "str", "varchar": "str",
    "text": "str", "char": "str",
    # boolean
    "bool": "bool", "boolean": "bool",
    # datetime
    "datetime64": "datetime", "datetime64[ns]": "datetime", "datetime": "datetime",
    "date": "datetime", "timestamp": "datetime", "time": "datetime",
    # category
    "category": "str",
}


def _normalise_dtype(dtype_str: str) -> str:
    """Return canonical dtype group for a raw dtype string."""
    key = str(dtype_str).lower().strip()
    # strip pandas ns/tz suffixes like datetime64[ns, UTC]
    key = re.sub(r"\[.*\]", "", key)
    return _DTYPE_GROUPS.get(key, "str")


# ══════════════════════════════════════════════════════════════════════════════
# TEMPLATE STORE  (JSON-backed singleton)
# ══════════════════════════════════════════════════════════════════════════════

class TemplateStore:
    """Persistent JSON-backed CRUD store for DataTemplate objects."""

    def __init__(self, path: Path = _TEMPLATE_STORE_PATH):
        self._path = path
        self._store: Dict[str, DataTemplate] = {}
        self._load()

    # ── persistence ──────────────────────────────────────────────────────────
    def _load(self):
        try:
            self._path.parent.mkdir(parents=True, exist_ok=True)
            if self._path.exists():
                raw = json.loads(self._path.read_text())
                self._store = {tid: DataTemplate.from_dict(t) for tid, t in raw.items()}
                logger.info(f"TemplateStore: loaded {len(self._store)} templates")
        except Exception as e:
            logger.error(f"TemplateStore load error: {e}")

    def _save(self):
        try:
            tmp = self._path.with_suffix(".tmp")
            tmp.write_text(json.dumps({tid: t.to_dict() for tid, t in self._store.items()}, indent=2))
            tmp.replace(self._path)
        except Exception as e:
            logger.error(f"TemplateStore save error: {e}")

    # ── CRUD ─────────────────────────────────────────────────────────────────
    def create(
        self,
        name: str,
        description: str,
        columns: List[Dict[str, Any]],
        name_similarity_min: float = 0.70,
        dtype_match_required: bool = True,
    ) -> DataTemplate:
        from datetime import datetime, timezone
        now = datetime.now(timezone.utc).isoformat()
        tid = str(uuid.uuid4())
        template = DataTemplate(
            id=tid,
            name=name,
            description=description,
            columns=[TemplateColumn(**c) for c in columns],
            name_similarity_min=name_similarity_min,
            dtype_match_required=dtype_match_required,
            created_at=now,
            updated_at=now,
        )
        self._store[tid] = template
        self._save()
        return template

    def get(self, tid: str) -> Optional[DataTemplate]:
        return self._store.get(tid)

    def list_all(self) -> List[Dict[str, Any]]:
        return [t.to_dict() for t in self._store.values()]

    def update(self, tid: str, **kwargs) -> Optional[DataTemplate]:
        from datetime import datetime, timezone
        t = self._store.get(tid)
        if not t:
            return None
        if "columns" in kwargs:
            kwargs["columns"] = [TemplateColumn(**c) for c in kwargs["columns"]]
        for k, v in kwargs.items():
            if hasattr(t, k):
                setattr(t, k, v)
        t.updated_at = datetime.now(timezone.utc).isoformat()
        self._save()
        return t

    def delete(self, tid: str) -> bool:
        if tid not in self._store:
            return False
        del self._store[tid]
        self._save()
        return True


# ── singleton ─────────────────────────────────────────────────────────────────
_template_store: Optional[TemplateStore] = None


def get_template_store() -> TemplateStore:
    global _template_store
    if _template_store is None:
        _template_store = TemplateStore()
    return _template_store


# ══════════════════════════════════════════════════════════════════════════════
# TEMPLATE MATCHER
# ══════════════════════════════════════════════════════════════════════════════

class TemplateMatcher:
    """
    Fuzzy-matches columns from a DataFrame against a DataTemplate.

    Matching strategy (per template column):
    1. Build candidate pool: all file columns not yet consumed.
    2. For each candidate compute:
       - name_score  = max(SequenceMatcher ratio,  alias match)
       - dtype_match = normalised dtypes equal?
    3. overall_score = name_score  (dtype disqualifies when dtype_match_required=True)
    4. Pick candidate with highest overall_score ≥ name_similarity_min.
    """

    def match(
        self,
        df: pd.DataFrame,
        template: DataTemplate,
        name_similarity_min: Optional[float] = None,
        dtype_match_required: Optional[bool] = None,
    ) -> TemplateMatchReport:
        threshold = name_similarity_min if name_similarity_min is not None else template.name_similarity_min
        require_dtype = dtype_match_required if dtype_match_required is not None else template.dtype_match_required

        file_cols = list(df.columns)
        file_dtypes = {col: _normalise_dtype(str(df[col].dtype)) for col in file_cols}
        available = set(file_cols)

        match_results: List[ColumnMatchResult] = []

        for tcol in template.columns:
            target_dtype = _normalise_dtype(tcol.dtype_hint)
            best: Optional[Tuple[str, float, bool]] = None  # (file_col, name_score, dtype_match)

            for fcol in list(available):
                name_score = self._name_similarity(tcol.name, fcol, tcol.aliases)
                dtype_ok = file_dtypes[fcol] == target_dtype

                if require_dtype and not dtype_ok:
                    continue
                if name_score < threshold:
                    continue
                if best is None or name_score > best[1]:
                    best = (fcol, name_score, dtype_ok)

            if best:
                fcol, name_score, dtype_ok = best
                available.discard(fcol)
                overall = name_score
                result = ColumnMatchResult(
                    template_col=tcol.name,
                    template_dtype=tcol.dtype_hint,
                    file_col=fcol,
                    file_dtype=file_dtypes[fcol],
                    name_score=round(name_score, 3),
                    dtype_match=dtype_ok,
                    overall_score=round(overall, 3),
                    is_confirmed=True,   # auto-confirm; user can deselect in UI
                    output_name=tcol.name,
                )
            else:
                result = ColumnMatchResult(
                    template_col=tcol.name,
                    template_dtype=tcol.dtype_hint,
                    file_col=None,
                    file_dtype=None,
                    name_score=0.0,
                    dtype_match=False,
                    overall_score=0.0,
                    is_confirmed=False,
                    output_name=tcol.name,
                )
            match_results.append(result)

        matched_file_cols = {r.file_col for r in match_results if r.file_col}
        unmatched = [c for c in file_cols if c not in matched_file_cols]
        covered = sum(1 for r in match_results if r.file_col) / max(len(template.columns), 1)

        return TemplateMatchReport(
            template_id=template.id,
            template_name=template.name,
            file_columns=file_cols,
            matches=match_results,
            unmatched_file_cols=unmatched,
            overall_coverage=round(covered, 3),
            thresholds_used={
                "name_similarity_min": threshold,
                "dtype_match_required": require_dtype,
            },
        )

    # ── helpers ───────────────────────────────────────────────────────────────
    @staticmethod
    def _name_similarity(template_name: str, file_col: str, aliases: List[str]) -> float:
        """Return best similarity score between template name/aliases and file column."""
        def _score(a: str, b: str) -> float:
            a, b = a.lower().strip(), b.lower().strip()
            # exact match
            if a == b:
                return 1.0
            # normalise separators and compare
            a_norm = re.sub(r"[\s_\-\.]+", "", a)
            b_norm = re.sub(r"[\s_\-\.]+", "", b)
            if a_norm == b_norm:
                return 0.97
            return SequenceMatcher(None, a_norm, b_norm).ratio()

        scores = [_score(template_name, file_col)]
        for alias in aliases:
            scores.append(_score(alias, file_col))
        return max(scores)


# ══════════════════════════════════════════════════════════════════════════════
# TEMPLATE APPLIER
# ══════════════════════════════════════════════════════════════════════════════

class TemplateApplier:
    """
    Takes a confirmed TemplateMatchReport (or a list of confirmed mappings from
    the frontend) and applies it to a DataFrame:

    1. Select only the confirmed file columns.
    2. Rename them to the user-chosen output_name.
    3. Optionally append extra columns the user dragged in.

    Returns a new DataFrame plus a column rename map for traceability.
    """

    def apply(
        self,
        df: pd.DataFrame,
        confirmed_mappings: List[Dict[str, Any]],
        extra_columns: Optional[List[str]] = None,
    ) -> Tuple[pd.DataFrame, Dict[str, str]]:
        """
        confirmed_mappings: list of dicts with keys:
            file_col   : str  — actual column in df
            output_name: str  — desired name in result
        extra_columns: additional file columns to include as-is
        Returns (new_df, rename_map)
        """
        # ── Ensure output names are unique ──
        final_names = []
        name_counts = {}
        select_cols = []
        rename_map = {}
        
        for m in confirmed_mappings:
            fc = m.get("file_col")
            out = m.get("output_name") or fc
            if fc and fc in df.columns:
                # Deduplicate output name
                base_out = out
                if base_out in name_counts:
                    name_counts[base_out] += 1
                    out = f"{base_out}_{name_counts[base_out]}"
                else:
                    name_counts[base_out] = 0
                
                select_cols.append(fc)
                if fc != out:
                    rename_map[fc] = out

        if extra_columns:
            for ec in extra_columns:
                if ec in df.columns and ec not in select_cols:
                    # Deduplicate extra column name if it clashes with an output name
                    out = ec
                    if out in name_counts:
                         name_counts[out] += 1
                         rename_map[ec] = f"{out}_{name_counts[out]}"
                    else:
                         name_counts[out] = 0
                    select_cols.append(ec)

        result = df[select_cols].copy()
        if rename_map:
            result = result.rename(columns=rename_map)

        return result, rename_map
