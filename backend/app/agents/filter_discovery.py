"""
Dynamic Filter & Pivot Discovery System
========================================
Agent-driven filter and pivot discovery for data exploration.

Like Power BI's Advanced Filtering:
1. Analyzes column data types and distributions
2. Determines appropriate filter/pivot options per column
3. Presents options to user for selection
4. Maintains complete traceability of all agent decisions

Architecture:
    Phase 0: Filter Discovery Agent → FilterMetadata
    Phase 0.5: Pivot Discovery Agent → PivotMetadata
    Phase 1: User Selection (via UI)
    Phase 2: Validation Agent (with selected filters/pivots applied)
"""

import json
import logging
import re
import time
from typing import List, Dict, Any, Optional
from datetime import datetime
from dataclasses import dataclass, field
from enum import Enum

import pandas as pd
import numpy as np

logger = logging.getLogger(__name__)


# ==========================================
# DATA STRUCTURES
# ==========================================

class ColumnDataType(Enum):
    """Detected column data types."""
    CATEGORICAL = "categorical"
    NUMERIC_CONTINUOUS = "numeric_continuous"
    NUMERIC_DISCRETE = "numeric_discrete"
    DATETIME = "datetime"
    TEXT_FREEFORM = "text_freeform"
    BOOLEAN = "boolean"
    ID = "identifier"


class FilterType(Enum):
    """Available filter types based on column data type."""
    MULTI_SELECT = "multi_select"
    SINGLE_SELECT = "single_select"
    EXCLUDE = "exclude"
    RANGE_SLIDER = "range_slider"
    RANGE_INPUT = "range_input"
    GREATER_THAN = "greater_than"
    LESS_THAN = "less_than"
    BETWEEN = "between"
    TOP_N = "top_n"
    BOTTOM_N = "bottom_n"
    DATE_RANGE = "date_range"
    DATE_RELATIVE = "date_relative"
    DATE_BEFORE = "date_before"
    DATE_AFTER = "date_after"
    DATE_YEAR = "date_year"
    DATE_MONTH = "date_month"
    DATE_QUARTER = "date_quarter"
    TEXT_CONTAINS = "text_contains"
    TEXT_STARTS_WITH = "text_starts_with"
    TEXT_ENDS_WITH = "text_ends_with"
    TEXT_REGEX = "text_regex"
    TEXT_EXACT = "text_exact"
    TOGGLE = "toggle"
    SEARCH = "search"
    IS_NULL = "is_null"
    IS_NOT_NULL = "is_not_null"
    ADVANCED_EXPRESSION = "advanced_expr"


class PivotType(Enum):
    """Available pivot operation types."""
    SUM = "sum"
    COUNT = "count"
    AVERAGE = "average"
    MIN = "min"
    MAX = "max"
    MEDIAN = "median"
    STDDEV = "stddev"
    GROUP_BY = "group_by"
    PIVOT_TABLE = "pivot_table"
    TIME_SERIES = "time_series"
    ROLLING_AVERAGE = "rolling_average"
    PERCENT_OF_TOTAL = "percent_of_total"
    RANK = "rank"


# ── Column / Filter / Pivot profiles ────────────────────────────

@dataclass
class ColumnProfile:
    """Profile of a single column discovered by the agent."""
    column_name: str
    data_type: ColumnDataType
    original_dtype: str
    total_count: int
    null_count: int
    null_percentage: float
    unique_count: int
    cardinality_percentage: float
    min_value: Optional[Any] = None
    max_value: Optional[Any] = None
    mean_value: Optional[float] = None
    median_value: Optional[float] = None
    std_dev: Optional[float] = None
    top_values: List[Dict[str, Any]] = field(default_factory=list)
    histogram_bins: Optional[List[Dict[str, Any]]] = None
    date_range: Optional[Dict[str, str]] = None
    has_duplicates: bool = False
    is_primary_key_candidate: bool = False
    quality_issues: List[str] = field(default_factory=list)
    agent_reasoning: str = ""


@dataclass
class FilterOption:
    """A single filter option available for a column."""
    filter_type: FilterType
    display_name: str
    description: str
    is_recommended: bool
    recommendation_reason: str
    default_value: Optional[Any] = None
    available_values: Optional[List[Any]] = None
    min_value: Optional[Any] = None
    max_value: Optional[Any] = None
    ui_component: str = "default"
    ui_config: Dict[str, Any] = field(default_factory=dict)
    agent_confidence: float = 1.0
    trace_id: str = ""


@dataclass
class PivotOption:
    """A single pivot option available for a column."""
    pivot_type: PivotType
    display_name: str
    description: str
    is_recommended: bool
    recommendation_reason: str
    can_be_dimension: bool = False
    can_be_measure: bool = False
    compatible_aggregations: List[PivotType] = field(default_factory=list)
    time_granularity: Optional[str] = None
    ui_component: str = "pivot_selector"
    agent_confidence: float = 1.0
    trace_id: str = ""


# ── Metadata containers ────────────────────────────────────────

@dataclass
class FilterMetadata:
    """Complete filter metadata for a dataset."""
    dataset_id: str
    dataset_name: str
    total_columns: int
    total_rows: int
    column_profiles: List[ColumnProfile]
    available_filters: Dict[str, List[FilterOption]]
    recommended_filters: List[Dict[str, Any]]
    discovery_timestamp: str
    discovery_duration_ms: int
    agent_trace: List[Dict[str, Any]]

    def to_ui_format(self) -> Dict[str, Any]:
        # Build dataset summary for KPI cards
        type_counts: Dict[str, int] = {}
        total_nulls = 0
        total_quality_issues = 0
        pk_candidates = []
        for p in self.column_profiles:
            dt = p.data_type.value
            type_counts[dt] = type_counts.get(dt, 0) + 1
            total_nulls += p.null_percentage
            total_quality_issues += len(p.quality_issues)
            if p.is_primary_key_candidate:
                pk_candidates.append(p.column_name)

        avg_null_pct = round(total_nulls / len(self.column_profiles), 2) if self.column_profiles else 0

        return {
            "dataset": {
                "id": self.dataset_id,
                "name": self.dataset_name,
                "columns": self.total_columns,
                "rows": self.total_rows,
            },
            "dataset_summary": {
                "type_breakdown": type_counts,
                "avg_null_percent": avg_null_pct,
                "total_quality_issues": total_quality_issues,
                "primary_key_candidates": pk_candidates,
            },
            "columns": {
                p.column_name: {
                    "type": p.data_type.value,
                    "originalType": p.original_dtype,
                    "stats": {
                        "total": p.total_count,
                        "nulls": p.null_count,
                        "nullPercent": round(p.null_percentage, 2),
                        "unique": p.unique_count,
                        "cardinality": round(p.cardinality_percentage, 2),
                    },
                    "distribution": {
                        "topValues": p.top_values[:10],
                        "min": p.min_value,
                        "max": p.max_value,
                        "mean": p.mean_value,
                        "median": p.median_value,
                    },
                    "quality": {
                        "hasDuplicates": p.has_duplicates,
                        "isPrimaryKey": p.is_primary_key_candidate,
                        "issues": p.quality_issues,
                    },
                    "agentReasoning": p.agent_reasoning,
                }
                for p in self.column_profiles
            },
            "filters": {
                col: [
                    {
                        "type": f.filter_type.value,
                        "name": f.display_name,
                        "description": f.description,
                        "recommended": f.is_recommended,
                        "reason": f.recommendation_reason,
                        "config": {
                            "default": f.default_value,
                            "values": f.available_values,
                            "min": f.min_value,
                            "max": f.max_value,
                        },
                        "ui": {"component": f.ui_component, "config": f.ui_config},
                        "confidence": f.agent_confidence,
                        "traceId": f.trace_id,
                    }
                    for f in filters
                ]
                for col, filters in self.available_filters.items()
            },
            "recommendations": self.recommended_filters,
            "traceability": {
                "timestamp": self.discovery_timestamp,
                "durationMs": self.discovery_duration_ms,
                "trace": self.agent_trace,
            },
        }


@dataclass
class PivotMetadata:
    """Complete pivot metadata for a dataset."""
    dataset_id: str
    dataset_name: str
    available_dimensions: Dict[str, PivotOption]
    available_measures: Dict[str, PivotOption]
    suggested_pivots: List[Dict[str, Any]]
    discovery_timestamp: str
    discovery_duration_ms: int
    agent_trace: List[Dict[str, Any]]

    def to_ui_format(self) -> Dict[str, Any]:
        return {
            "dimensions": {
                col: {
                    "type": p.pivot_type.value,
                    "name": p.display_name,
                    "description": p.description,
                    "recommended": p.is_recommended,
                    "reason": p.recommendation_reason,
                    "timeGranularity": p.time_granularity,
                    "confidence": p.agent_confidence,
                    "traceId": p.trace_id,
                }
                for col, p in self.available_dimensions.items()
            },
            "measures": {
                col: {
                    "type": p.pivot_type.value,
                    "name": p.display_name,
                    "description": p.description,
                    "recommended": p.is_recommended,
                    "reason": p.recommendation_reason,
                    "aggregations": [a.value for a in p.compatible_aggregations],
                    "confidence": p.agent_confidence,
                    "traceId": p.trace_id,
                }
                for col, p in self.available_measures.items()
            },
            "suggestions": self.suggested_pivots,
            "traceability": {
                "timestamp": self.discovery_timestamp,
                "durationMs": self.discovery_duration_ms,
                "trace": self.agent_trace,
            },
        }


# ── User selection dataclasses ──────────────────────────────────

@dataclass
class UserFilterSelection:
    """User's selected filter configuration (sent from UI)."""
    column: str
    filter_type: str  # FilterType value
    selected_values: Optional[List[Any]] = None
    min_value: Optional[Any] = None
    max_value: Optional[Any] = None
    text_pattern: Optional[str] = None
    is_negated: bool = False
    trace_id: str = ""


@dataclass
class UserPivotSelection:
    """User's selected pivot configuration (sent from UI)."""
    dimensions: List[str]
    measures: List[Dict[str, str]]  # [{"column": "x", "aggregation": "sum"}]
    filters: List[UserFilterSelection] = field(default_factory=list)
    pivot_type: str = "pivot_table"
    trace_ids: List[str] = field(default_factory=list)


# ==========================================
# FILTER DISCOVERY AGENT
# ==========================================

class FilterDiscoveryAgent:
    """Profiles all columns and discovers appropriate filter options."""

    def __init__(self):
        self.trace: List[Dict[str, Any]] = []

    async def discover(
        self, df: pd.DataFrame, dataset_id: str, dataset_name: str,
    ) -> FilterMetadata:
        start = time.time()
        self.trace = []
        self._trace("START", f"Filter discovery for {dataset_name} ({len(df)} rows, {len(df.columns)} cols)")

        profiles = self._profile_all_columns(df)
        self._trace("PROFILED", f"Profiled {len(profiles)} columns")

        available_filters = self._determine_all_filters(profiles, df)
        self._trace("FILTERS", f"Determined filters for {len(available_filters)} columns")

        recommendations = self._generate_recommendations(profiles, available_filters)
        duration_ms = int((time.time() - start) * 1000)
        self._trace("COMPLETE", f"Done in {duration_ms}ms")

        return FilterMetadata(
            dataset_id=dataset_id,
            dataset_name=dataset_name,
            total_columns=len(profiles),
            total_rows=len(df),
            column_profiles=profiles,
            available_filters=available_filters,
            recommended_filters=recommendations,
            discovery_timestamp=datetime.utcnow().isoformat(),
            discovery_duration_ms=duration_ms,
            agent_trace=self.trace,
        )

    # ── helpers ─────────────────────────────────────────────────

    def _trace(self, step: str, msg: str):
        self.trace.append({"timestamp": datetime.utcnow().isoformat(), "step": step, "message": msg})
        logger.info(f"[FilterDiscovery] {step}: {msg}")

    def _profile_all_columns(self, df: pd.DataFrame) -> List[ColumnProfile]:
        return [self._profile_column(df, col) for col in df.columns]

    def _profile_column(self, df: pd.DataFrame, column: str) -> ColumnProfile:
        col = df[column]
        total = len(col)

        # Count nulls: include actual nulls AND empty/whitespace strings for object columns
        if col.dtype == object:
            # Treat None, NaN, '', and whitespace-only as "missing"
            is_missing = col.isnull() | col.astype(str).str.strip().eq('')
            nulls = int(is_missing.sum())
            # Clean series: replace empty/whitespace with NaN for accurate stats
            clean_col = col.copy()
            clean_col[is_missing] = np.nan
            uniques = int(clean_col.nunique())  # nunique skips NaN by default
        else:
            nulls = int(col.isnull().sum())
            clean_col = col
            uniques = int(col.nunique())

        profile = ColumnProfile(
            column_name=column,
            data_type=ColumnDataType.TEXT_FREEFORM,  # refined below
            original_dtype=str(col.dtype),
            total_count=total,
            null_count=nulls,
            null_percentage=(nulls / total * 100) if total else 0,
            unique_count=uniques,
            cardinality_percentage=(uniques / total * 100) if total else 0,
            has_duplicates=uniques < (total - nulls),
            is_primary_key_candidate=(uniques == total - nulls and nulls == 0),
        )

        # Numeric stats
        if pd.api.types.is_numeric_dtype(col):
            valid = col.dropna()
            if len(valid):
                profile.min_value = float(valid.min())
                profile.max_value = float(valid.max())
                profile.mean_value = float(valid.mean())
                profile.median_value = float(valid.median())
                profile.std_dev = float(valid.std())
            try:
                hist, bins = np.histogram(valid, bins=min(10, max(1, len(valid))))
                profile.histogram_bins = [
                    {"bin_start": float(bins[i]), "bin_end": float(bins[i + 1]), "count": int(hist[i])}
                    for i in range(len(hist))
                ]
            except Exception:
                pass

        # Datetime stats
        elif pd.api.types.is_datetime64_any_dtype(col):
            valid = col.dropna()
            if len(valid):
                profile.date_range = {"min": str(valid.min()), "max": str(valid.max())}
                profile.min_value = str(valid.min())
                profile.max_value = str(valid.max())

        # String / categorical top values — use clean_col to exclude empties
        else:
            vc = clean_col.value_counts().head(50)
            profile.top_values = [{"value": str(v), "count": int(c)} for v, c in vc.items()]

        # Data type detection (heuristic)
        profile.data_type = self._detect_type(col, profile)

        # Post-detection: for datetime-like string columns, normalize unique count
        # to second-level precision (strip microseconds) to match database COUNT(DISTINCT)
        if profile.data_type == ColumnDataType.DATETIME and col.dtype == object:
            try:
                dt_parsed = pd.to_datetime(clean_col, errors='coerce')
                # Truncate to seconds (removes microsecond variations)
                dt_truncated = dt_parsed.dt.floor('s')
                profile.unique_count = int(dt_truncated.nunique())
                profile.cardinality_percentage = (profile.unique_count / total * 100) if total else 0
            except Exception:
                pass  # keep original count if parsing fails

        profile.agent_reasoning = (
            f"Detected as {profile.data_type.value} "
            f"(dtype={profile.original_dtype}, cardinality={profile.cardinality_percentage:.1f}%)"
        )

        # Quality issues
        if profile.null_percentage > 20:
            profile.quality_issues.append(f"High nulls: {profile.null_percentage:.1f}%")
        if profile.cardinality_percentage == 100 and not profile.is_primary_key_candidate:
            profile.quality_issues.append("100% cardinality but not unique")

        return profile

    def _detect_type(self, col: pd.Series, profile: ColumnProfile) -> ColumnDataType:
        if pd.api.types.is_bool_dtype(col):
            return ColumnDataType.BOOLEAN
        if pd.api.types.is_datetime64_any_dtype(col):
            return ColumnDataType.DATETIME
        if pd.api.types.is_numeric_dtype(col):
            if profile.unique_count <= 20 and profile.cardinality_percentage < 5:
                return ColumnDataType.NUMERIC_DISCRETE
            return ColumnDataType.NUMERIC_CONTINUOUS

        # String heuristics
        col_lower = profile.column_name.lower()
        if profile.is_primary_key_candidate or any(
            kw in col_lower for kw in ("_id", "id", "key", "uuid", "guid", "code")
        ):
            return ColumnDataType.ID

        if profile.cardinality_percentage < 5 or profile.unique_count <= 30:
            return ColumnDataType.CATEGORICAL

        # Check for datetime-like strings
        samples = col.dropna().head(5).astype(str).tolist()
        dt_patterns = [r"\d{4}-\d{2}-\d{2}", r"\d{2}/\d{2}/\d{4}", r"\d{2}-\d{2}-\d{4}"]
        for v in samples:
            for p in dt_patterns:
                if re.match(p, v):
                    return ColumnDataType.DATETIME

        return ColumnDataType.TEXT_FREEFORM

    # ── filter option builders ──────────────────────────────────

    def _determine_all_filters(
        self, profiles: List[ColumnProfile], df: pd.DataFrame
    ) -> Dict[str, List[FilterOption]]:
        return {p.column_name: self._filters_for(p, df[p.column_name]) for p in profiles}

    def _filters_for(self, profile: ColumnProfile, col: pd.Series) -> List[FilterOption]:
        tid = f"{profile.column_name}_{datetime.utcnow().strftime('%Y%m%d%H%M%S')}"
        filters: List[FilterOption] = []

        # Universal: null checks
        filters.append(FilterOption(
            FilterType.IS_NULL, "Is Null/Empty", "Show rows where this column is null",
            False, "Data quality check", ui_component="toggle", trace_id=f"{tid}_null",
        ))
        filters.append(FilterOption(
            FilterType.IS_NOT_NULL, "Is Not Null", "Exclude nulls",
            False, "Exclude nulls", ui_component="toggle", trace_id=f"{tid}_notnull",
        ))

        dt = profile.data_type
        if dt == ColumnDataType.CATEGORICAL:
            filters += self._categorical_filters(profile, col, tid)
        elif dt == ColumnDataType.NUMERIC_CONTINUOUS:
            filters += self._numeric_cont_filters(profile, col, tid)
        elif dt == ColumnDataType.NUMERIC_DISCRETE:
            filters += self._numeric_disc_filters(profile, col, tid)
        elif dt == ColumnDataType.DATETIME:
            filters += self._datetime_filters(profile, col, tid)
        elif dt == ColumnDataType.TEXT_FREEFORM:
            filters += self._text_filters(profile, col, tid)
        elif dt == ColumnDataType.BOOLEAN:
            filters.append(FilterOption(
                FilterType.TOGGLE, "Toggle", "Select True or False", True,
                "Simple boolean toggle", ui_component="boolean_toggle",
                ui_config={"options": ["All", "True", "False"]},
                agent_confidence=1.0, trace_id=f"{tid}_toggle",
            ))
        elif dt == ColumnDataType.ID:
            filters.append(FilterOption(
                FilterType.SEARCH, "Search by ID", "Search for specific IDs", True,
                "IDs are unique — search is most efficient", ui_component="search_input",
                ui_config={"placeholder": "Enter ID..."}, agent_confidence=0.9,
                trace_id=f"{tid}_search",
            ))

        return filters

    def _categorical_filters(self, p: ColumnProfile, col: pd.Series, tid: str) -> List[FilterOption]:
        vals = col.dropna().unique().tolist()
        vals_str = [str(v) for v in vals[:200]]
        return [
            FilterOption(
                FilterType.MULTI_SELECT, "Select Multiple",
                "Select one or more values to include", True,
                "Most flexible for categorical data",
                available_values=vals_str,
                ui_component="multi_select_dropdown",
                ui_config={"showSearch": len(vals) > 10, "showCounts": True},
                agent_confidence=0.95, trace_id=f"{tid}_multi",
            ),
            FilterOption(
                FilterType.SINGLE_SELECT, "Select Single",
                "Filter to a single value", False,
                "Focus on one category",
                available_values=vals_str,
                ui_component="dropdown",
                ui_config={"showSearch": len(vals) > 10},
                agent_confidence=0.9, trace_id=f"{tid}_single",
            ),
            FilterOption(
                FilterType.EXCLUDE, "Exclude Values",
                "Exclude selected values", False,
                "Remove unwanted categories",
                available_values=vals_str,
                ui_component="multi_select_dropdown",
                ui_config={"showSearch": True, "mode": "exclude"},
                agent_confidence=0.85, trace_id=f"{tid}_excl",
            ),
        ]

    def _numeric_cont_filters(self, p: ColumnProfile, col: pd.Series, tid: str) -> List[FilterOption]:
        mn, mx, mean = p.min_value, p.max_value, p.mean_value
        step = (mx - mn) / 100 if mx is not None and mn is not None and mx != mn else 1
        return [
            FilterOption(
                FilterType.RANGE_SLIDER, "Range Slider", "Select range via slider", True,
                "Intuitive for continuous numeric", min_value=mn, max_value=mx,
                default_value=[mn, mx], ui_component="range_slider",
                ui_config={"step": step, "showMinMax": True},
                agent_confidence=0.95, trace_id=f"{tid}_range",
            ),
            FilterOption(
                FilterType.BETWEEN, "Between Values", "Enter min and max manually", False,
                "Precise range selection", min_value=mn, max_value=mx,
                ui_component="range_input", agent_confidence=0.9, trace_id=f"{tid}_between",
            ),
            FilterOption(
                FilterType.GREATER_THAN, "Greater Than", "Values > threshold", False,
                "Find high values", default_value=mean, ui_component="number_input",
                agent_confidence=0.85, trace_id=f"{tid}_gt",
            ),
            FilterOption(
                FilterType.LESS_THAN, "Less Than", "Values < threshold", False,
                "Find low values", default_value=mean, ui_component="number_input",
                agent_confidence=0.85, trace_id=f"{tid}_lt",
            ),
            FilterOption(
                FilterType.TOP_N, "Top N", "Highest N values", False,
                "Quick top analysis", default_value=10, ui_component="number_input",
                ui_config={"min": 1, "max": 1000}, agent_confidence=0.8, trace_id=f"{tid}_topn",
            ),
            FilterOption(
                FilterType.BOTTOM_N, "Bottom N", "Lowest N values", False,
                "Quick bottom analysis", default_value=10, ui_component="number_input",
                ui_config={"min": 1, "max": 1000}, agent_confidence=0.8, trace_id=f"{tid}_botn",
            ),
        ]

    def _numeric_disc_filters(self, p: ColumnProfile, col: pd.Series, tid: str) -> List[FilterOption]:
        vals = sorted(col.dropna().unique().tolist())
        f: List[FilterOption] = []
        if len(vals) <= 50:
            f.append(FilterOption(
                FilterType.MULTI_SELECT, "Select Values",
                "Select discrete values", True,
                f"Only {len(vals)} unique values — dropdown is efficient",
                available_values=[str(v) for v in vals],
                ui_component="multi_select_dropdown",
                ui_config={"showSearch": len(vals) > 20},
                agent_confidence=0.95, trace_id=f"{tid}_multi",
            ))
        f.append(FilterOption(
            FilterType.RANGE_SLIDER, "Range Slider", "Select range", len(vals) > 50,
            "Efficient for high cardinality discrete", min_value=p.min_value, max_value=p.max_value,
            ui_component="range_slider", ui_config={"step": 1, "showMinMax": True},
            agent_confidence=0.9, trace_id=f"{tid}_range",
        ))
        return f

    def _datetime_filters(self, p: ColumnProfile, col: pd.Series, tid: str) -> List[FilterOption]:
        dr = p.date_range or {}
        return [
            FilterOption(
                FilterType.DATE_RANGE, "Date Range", "Select a date range", True,
                "Most intuitive for datetime", min_value=dr.get("min"), max_value=dr.get("max"),
                ui_component="date_range_picker", ui_config={"showPresets": True},
                agent_confidence=0.95, trace_id=f"{tid}_drange",
            ),
            FilterOption(
                FilterType.DATE_RELATIVE, "Relative Date", "Last N days/months/years", True,
                "Great for time-based analysis", ui_component="relative_date_picker",
                ui_config={
                    "options": ["days", "weeks", "months", "quarters", "years"],
                    "presets": ["last_7_days", "last_30_days", "last_90_days", "last_year", "this_year"],
                },
                agent_confidence=0.9, trace_id=f"{tid}_drel",
            ),
            FilterOption(
                FilterType.DATE_YEAR, "By Year", "Filter by year", False,
                "Year-over-year analysis", ui_component="year_picker",
                agent_confidence=0.85, trace_id=f"{tid}_dyear",
            ),
            FilterOption(
                FilterType.DATE_MONTH, "By Month", "Filter by month", False,
                "Seasonal analysis", ui_component="month_picker",
                agent_confidence=0.8, trace_id=f"{tid}_dmonth",
            ),
            FilterOption(
                FilterType.DATE_QUARTER, "By Quarter", "Filter by quarter", False,
                "Quarterly reporting", ui_component="quarter_picker",
                agent_confidence=0.8, trace_id=f"{tid}_dqtr",
            ),
        ]

    def _text_filters(self, p: ColumnProfile, col: pd.Series, tid: str) -> List[FilterOption]:
        return [
            FilterOption(
                FilterType.TEXT_CONTAINS, "Contains", "Substring search", True,
                "Most flexible text search", ui_component="text_input",
                ui_config={"placeholder": "Enter text..."}, agent_confidence=0.9, trace_id=f"{tid}_cont",
            ),
            FilterOption(
                FilterType.TEXT_STARTS_WITH, "Starts With", "Prefix search", False,
                "Prefix-based", ui_component="text_input", agent_confidence=0.85, trace_id=f"{tid}_sw",
            ),
            FilterOption(
                FilterType.TEXT_ENDS_WITH, "Ends With", "Suffix search", False,
                "Suffix-based", ui_component="text_input", agent_confidence=0.85, trace_id=f"{tid}_ew",
            ),
            FilterOption(
                FilterType.TEXT_EXACT, "Exact Match", "Exact text match", False,
                "Precise matching", ui_component="text_input", agent_confidence=0.9, trace_id=f"{tid}_exact",
            ),
            FilterOption(
                FilterType.TEXT_REGEX, "Regex Pattern", "Regex filter", False,
                "Advanced pattern matching", ui_component="text_input",
                ui_config={"placeholder": "Regex...", "validateRegex": True},
                agent_confidence=0.7, trace_id=f"{tid}_regex",
            ),
        ]

    # ── recommendations ─────────────────────────────────────────

    def _generate_recommendations(
        self, profiles: List[ColumnProfile], filters: Dict[str, List[FilterOption]]
    ) -> List[Dict[str, Any]]:
        recs = []
        for p in profiles:
            if p.data_type == ColumnDataType.ID:
                continue
            col_filters = filters.get(p.column_name, [])
            recommended = [f for f in col_filters if f.is_recommended]
            if recommended:
                best = recommended[0]
                recs.append({
                    "column": p.column_name,
                    "data_type": p.data_type.value,
                    "filter_type": best.filter_type.value,
                    "reason": best.recommendation_reason,
                    "confidence": best.agent_confidence,
                    "trace_id": best.trace_id,
                })
        recs.sort(key=lambda x: x["confidence"], reverse=True)
        return recs


# ==========================================
# PIVOT DISCOVERY AGENT
# ==========================================

class PivotDiscoveryAgent:
    """Discovers appropriate pivot/aggregation options."""

    def __init__(self):
        self.trace: List[Dict[str, Any]] = []

    async def discover(
        self, df: pd.DataFrame, filter_metadata: FilterMetadata,
    ) -> PivotMetadata:
        start = time.time()
        self.trace = []
        self._trace("START", f"Pivot discovery for {filter_metadata.dataset_name}")

        dims = self._identify_dimensions(filter_metadata.column_profiles)
        self._trace("DIMS", f"Found {len(dims)} dimensions")

        measures = self._identify_measures(filter_metadata.column_profiles)
        self._trace("MEASURES", f"Found {len(measures)} measures")

        suggestions = self._generate_suggestions(dims, measures)
        duration_ms = int((time.time() - start) * 1000)
        self._trace("COMPLETE", f"Done in {duration_ms}ms")

        return PivotMetadata(
            dataset_id=filter_metadata.dataset_id,
            dataset_name=filter_metadata.dataset_name,
            available_dimensions=dims,
            available_measures=measures,
            suggested_pivots=suggestions,
            discovery_timestamp=datetime.utcnow().isoformat(),
            discovery_duration_ms=duration_ms,
            agent_trace=self.trace,
        )

    def _trace(self, step: str, msg: str):
        self.trace.append({"timestamp": datetime.utcnow().isoformat(), "step": step, "message": msg})
        logger.info(f"[PivotDiscovery] {step}: {msg}")

    def _identify_dimensions(self, profiles: List[ColumnProfile]) -> Dict[str, PivotOption]:
        dims = {}
        dim_keywords = [
            "category", "type", "status", "region", "country", "city", "state",
            "department", "segment", "group", "class", "tier", "channel", "source",
            "medium", "campaign", "brand",
        ]
        for p in profiles:
            reason = ""
            time_gran = None

            if p.data_type == ColumnDataType.CATEGORICAL:
                reason = f"Categorical with {p.unique_count} values — ideal for grouping"
            elif p.data_type == ColumnDataType.BOOLEAN:
                reason = "Boolean groups data into True/False"
            elif p.data_type == ColumnDataType.DATETIME:
                reason = "Datetime enables time-based analysis"
                time_gran = "auto"
            elif p.data_type == ColumnDataType.NUMERIC_DISCRETE and p.cardinality_percentage < 10:
                reason = f"Low-cardinality discrete ({p.unique_count} values)"
            elif any(kw in p.column_name.lower() for kw in dim_keywords):
                reason = "Column name suggests categorical dimension"
            else:
                continue

            dims[p.column_name] = PivotOption(
                pivot_type=PivotType.GROUP_BY,
                display_name=f"Group by {p.column_name}",
                description=f"Group data by {p.column_name}",
                is_recommended=True,
                recommendation_reason=reason,
                can_be_dimension=True,
                time_granularity=time_gran,
                agent_confidence=0.9 if "ideal" in reason.lower() else 0.8,
                trace_id=f"{p.column_name}_dim",
            )
        return dims

    def _identify_measures(self, profiles: List[ColumnProfile]) -> Dict[str, PivotOption]:
        measures = {}
        for p in profiles:
            if p.data_type not in (ColumnDataType.NUMERIC_CONTINUOUS, ColumnDataType.NUMERIC_DISCRETE):
                continue
            cl = p.column_name.lower()
            if any(kw in cl for kw in ("amount", "total", "price", "revenue", "cost", "sales", "value", "income")):
                aggs = [PivotType.SUM, PivotType.AVERAGE, PivotType.MIN, PivotType.MAX, PivotType.MEDIAN]
                reason = "Monetary column — supports all aggregations"
            elif any(kw in cl for kw in ("count", "quantity", "qty", "number", "num")):
                aggs = [PivotType.SUM, PivotType.COUNT, PivotType.AVERAGE]
                reason = "Count-like — sum or average"
            elif any(kw in cl for kw in ("rate", "ratio", "percentage", "percent", "avg")):
                aggs = [PivotType.AVERAGE, PivotType.MIN, PivotType.MAX, PivotType.MEDIAN]
                reason = "Rate/ratio — average is most meaningful"
            elif any(kw in cl for kw in ("id", "_id", "key")):
                aggs = [PivotType.COUNT]
                reason = "ID column — count only"
            else:
                aggs = [PivotType.SUM, PivotType.AVERAGE, PivotType.MIN, PivotType.MAX, PivotType.COUNT,
                        PivotType.MEDIAN, PivotType.STDDEV]
                reason = "Generic numeric — all aggregations"

            measures[p.column_name] = PivotOption(
                pivot_type=PivotType.SUM,
                display_name=f"Aggregate {p.column_name}",
                description=f"Aggregate {p.column_name}",
                is_recommended=True,
                recommendation_reason=reason,
                can_be_measure=True,
                compatible_aggregations=aggs,
                agent_confidence=0.9,
                trace_id=f"{p.column_name}_meas",
            )
        return measures

    def _generate_suggestions(
        self, dims: Dict[str, PivotOption], measures: Dict[str, PivotOption],
    ) -> List[Dict[str, Any]]:
        suggestions: List[Dict[str, Any]] = []
        dl = list(dims.keys())
        ml = list(measures.keys())

        # Single dim × single measure
        for d in dl[:3]:
            for m in ml[:2]:
                mo = measures[m]
                agg = mo.compatible_aggregations[0].value if mo.compatible_aggregations else "sum"
                suggestions.append({
                    "name": f"{m.replace('_', ' ').title()} by {d.replace('_', ' ').title()}",
                    "type": "pivot_table",
                    "dimensions": [d],
                    "measures": [{"column": m, "aggregation": agg}],
                    "reasoning": f"Aggregate {m} grouped by {d}",
                    "confidence": 0.85,
                })

        # Time-series
        time_dims = [d for d, o in dims.items() if o.time_granularity]
        if time_dims and ml:
            suggestions.append({
                "name": "Time Series Analysis",
                "type": "time_series",
                "dimensions": [time_dims[0]],
                "measures": [{"column": m, "aggregation": "sum"} for m in ml[:3]],
                "reasoning": "Track metrics over time",
                "confidence": 0.9,
                "timeGranularityOptions": ["day", "week", "month", "quarter", "year"],
            })

        # Multi-dim
        if len(dl) >= 2 and ml:
            suggestions.append({
                "name": "Multi-Dimension Analysis",
                "type": "pivot_table",
                "dimensions": dl[:2],
                "measures": [{"column": m, "aggregation": "sum"} for m in ml[:2]],
                "reasoning": "Cross-tabulate two dimensions",
                "confidence": 0.8,
            })

        # Top-10
        if dl and ml:
            suggestions.append({
                "name": "Top 10 Analysis",
                "type": "ranking",
                "dimensions": [dl[0]],
                "measures": [{"column": ml[0], "aggregation": "sum"}],
                "orderBy": {"column": ml[0], "direction": "desc"},
                "limit": 10,
                "reasoning": "Quick view of top performers",
                "confidence": 0.85,
            })

        return suggestions[:10]


# ==========================================
# DYNAMIC FILTER EXECUTOR
# ==========================================

class DynamicFilterExecutor:
    """Translates user filter selections to pandas operations."""

    def apply_filters(
        self, df: pd.DataFrame, selections: List[UserFilterSelection],
    ) -> tuple:
        """Returns (filtered_df, execution_log)."""
        result = df.copy()
        log = []

        for sel in selections:
            if sel.column not in result.columns:
                log.append({"column": sel.column, "status": "error", "message": "Column not found"})
                continue

            before = len(result)
            try:
                result = self._apply_one(result, sel)
            except Exception as e:
                log.append({"column": sel.column, "status": "error", "message": str(e)})
                continue

            log.append({
                "column": sel.column,
                "filter_type": sel.filter_type,
                "status": "success",
                "rows_before": before,
                "rows_after": len(result),
                "rows_filtered": before - len(result),
                "trace_id": sel.trace_id,
            })

        return result, log

    def _apply_one(self, df: pd.DataFrame, sel: UserFilterSelection) -> pd.DataFrame:
        col = df[sel.column]
        ft = sel.filter_type

        if ft == "multi_select":
            mask = col.astype(str).isin([str(v) for v in (sel.selected_values or [])])
        elif ft == "single_select":
            mask = col.astype(str) == str(sel.selected_values[0]) if sel.selected_values else pd.Series(False, index=df.index)
        elif ft == "exclude":
            mask = ~col.astype(str).isin([str(v) for v in (sel.selected_values or [])])
        elif ft in ("range_slider", "between"):
            mask = (col >= sel.min_value) & (col <= sel.max_value)
        elif ft == "greater_than":
            mask = col > sel.min_value
        elif ft == "less_than":
            mask = col < sel.max_value
        elif ft == "top_n":
            n = int(sel.min_value or 10)
            mask = col.isin(col.nlargest(n))
        elif ft == "bottom_n":
            n = int(sel.min_value or 10)
            mask = col.isin(col.nsmallest(n))
        elif ft == "date_range":
            dt = pd.to_datetime(col, errors="coerce")
            mask = (dt >= pd.to_datetime(sel.min_value)) & (dt <= pd.to_datetime(sel.max_value))
        elif ft == "date_relative":
            dt = pd.to_datetime(col, errors="coerce")
            rel = sel.text_pattern or "last_30_days"
            now = pd.Timestamp.now()
            presets = {
                "last_7_days": now - pd.Timedelta(days=7),
                "last_30_days": now - pd.Timedelta(days=30),
                "last_90_days": now - pd.Timedelta(days=90),
                "last_year": now - pd.DateOffset(years=1),
                "this_year": pd.Timestamp(year=now.year, month=1, day=1),
            }
            start = presets.get(rel, now - pd.Timedelta(days=30))
            mask = dt >= start
        elif ft == "date_year":
            mask = pd.to_datetime(col, errors="coerce").dt.year == int(sel.min_value)
        elif ft == "date_month":
            mask = pd.to_datetime(col, errors="coerce").dt.month == int(sel.min_value)
        elif ft == "date_quarter":
            mask = pd.to_datetime(col, errors="coerce").dt.quarter == int(sel.min_value)
        elif ft == "date_before":
            mask = pd.to_datetime(col, errors="coerce") < pd.to_datetime(sel.min_value)
        elif ft == "date_after":
            mask = pd.to_datetime(col, errors="coerce") > pd.to_datetime(sel.min_value)
        elif ft == "text_contains":
            mask = col.astype(str).str.contains(sel.text_pattern or "", case=False, na=False)
        elif ft == "text_starts_with":
            mask = col.astype(str).str.startswith(sel.text_pattern or "", na=False)
        elif ft == "text_ends_with":
            mask = col.astype(str).str.endswith(sel.text_pattern or "", na=False)
        elif ft == "text_exact":
            mask = col.astype(str) == (sel.text_pattern or "")
        elif ft == "text_regex":
            mask = col.astype(str).str.match(sel.text_pattern or "", case=False, na=False)
        elif ft == "toggle":
            if sel.selected_values and str(sel.selected_values[0]).lower() == "true":
                mask = col == True
            elif sel.selected_values and str(sel.selected_values[0]).lower() == "false":
                mask = col == False
            else:
                mask = pd.Series(True, index=df.index)
        elif ft == "search":
            mask = col.astype(str).str.contains(sel.text_pattern or "", case=False, na=False)
        elif ft == "is_null":
            mask = col.isnull()
        elif ft == "is_not_null":
            mask = col.notnull()
        else:
            mask = pd.Series(True, index=df.index)

        if sel.is_negated:
            mask = ~mask

        return df[mask]


# ==========================================
# DYNAMIC PIVOT EXECUTOR
# ==========================================

class DynamicPivotExecutor:
    """Executes user-selected pivot operations."""

    def apply_pivot(self, df: pd.DataFrame, selection: UserPivotSelection) -> pd.DataFrame:
        dims = selection.dimensions
        measures = selection.measures
        if not dims or not measures:
            return df

        agg_map = {
            "sum": "sum", "count": "count", "average": "mean",
            "min": "min", "max": "max", "median": "median", "stddev": "std",
        }

        agg_dict = {}
        for m in measures:
            agg_dict[m["column"]] = agg_map.get(m["aggregation"], "sum")

        # Verify columns exist
        valid_dims = [d for d in dims if d in df.columns]
        valid_agg = {k: v for k, v in agg_dict.items() if k in df.columns}

        if not valid_dims or not valid_agg:
            return df

        return df.groupby(valid_dims, dropna=False).agg(valid_agg).reset_index()


# ==========================================
# DISCOVERY MANAGER — main entry point
# ==========================================

class DiscoveryManager:
    """
    Orchestrates filter + pivot discovery.

    Usage:
        1. discover(df) → metadata for UI
        2. UI presents options → user selects
        3. apply_selections(df, selections) → filtered/pivoted data
    """

    def __init__(self):
        self.filter_agent = FilterDiscoveryAgent()
        self.pivot_agent = PivotDiscoveryAgent()
        self.filter_executor = DynamicFilterExecutor()
        self.pivot_executor = DynamicPivotExecutor()
        self._last_filter_metadata: Optional[FilterMetadata] = None

    async def discover(
        self, df: pd.DataFrame, dataset_id: str, dataset_name: str,
    ) -> Dict[str, Any]:
        filter_meta = await self.filter_agent.discover(df, dataset_id, dataset_name)
        self._last_filter_metadata = filter_meta
        pivot_meta = await self.pivot_agent.discover(df, filter_meta)

        return {
            "filter_metadata": filter_meta.to_ui_format(),
            "pivot_metadata": pivot_meta.to_ui_format(),
            "ready_for_user_selection": True,
        }

    def apply_selections(
        self,
        df: pd.DataFrame,
        filter_selections: List[UserFilterSelection],
        pivot_selection: Optional[UserPivotSelection] = None,
    ) -> Dict[str, Any]:
        filtered_df, filter_log = self.filter_executor.apply_filters(df, filter_selections)

        pivot_log = None
        pivoted_df = None
        if pivot_selection:
            pivoted_df = self.pivot_executor.apply_pivot(filtered_df, pivot_selection)
            pivot_log = {
                "dimensions": pivot_selection.dimensions,
                "measures": pivot_selection.measures,
                "rows_before": len(filtered_df),
                "rows_after": len(pivoted_df),
            }

        final_df = pivoted_df if pivoted_df is not None else filtered_df
        return {
            "filtered_data": filtered_df,
            "pivoted_data": pivoted_df,
            "filter_execution_log": filter_log,
            "pivot_execution_log": pivot_log,
            "traceability": {
                "filters_applied": len(filter_selections),
                "pivot_applied": pivot_selection is not None,
                "final_row_count": len(final_df),
            },
        }
