"""Pandas-native tool executor for flat-file data sources.

WHY:
  SQL tools cover most checks, but flat files benefit from pandas-native analysis
  that is either impossible in SQL (mixed types, memory profile, correlation) or
  significantly cleaner to express as pandas operations (IQR outliers, encoding
  checks, column-level statistics).

ARCHITECTURE:
  DataFrameToolExecutor mirrors ValidationToolExecutor's interface so it can be
  used as a drop-in supplement in the agent pipeline:

      if isinstance(connector, DuckDBFileConnector):
          df_executor = DataFrameToolExecutor(connector, resource_path)
          df_result = await df_executor.run_all_profile_tools()

TOOL REGISTRY:
  DF_PROFILE_TOOLS    — always run once per file (table-level profiling)
  DF_COLUMN_TOOLS     — run per-column, keyed by tool_id

Each tool function returns a ToolResult (same dataclass as SQL tools).
"""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)


# ── ToolResult re-export (matches tool_based_agent.ToolResult exactly) ────────

@dataclass
class DFToolResult:
    """Pandas-tool result — identical structure to SQL ToolResult."""
    tool_id:           str
    tool_name:         str
    command_executed:  str
    status:            str          # success | error | warning | skipped
    row_count:         int
    failed_count:      int
    sample_rows:       List[Dict]
    message:           str
    severity:          str          # critical | warning | info
    column_name:       Optional[str] = None
    execution_time_ms: int = 0


# ── Helpers ───────────────────────────────────────────────────────────────────

def _json_safe(val: Any) -> Any:
    if val is None:
        return None
    if isinstance(val, float) and val != val:
        return None
    if isinstance(val, (np.integer,)):
        return int(val)
    if isinstance(val, (np.floating,)):
        return None if np.isnan(val) else float(val)
    if isinstance(val, (np.bool_,)):
        return bool(val)
    if isinstance(val, pd.Timestamp):
        return str(val)
    return val


def _make_result(
    tool_id: str,
    name: str,
    cmd: str,
    failed: int,
    rows: List[Dict],
    msg: str,
    severity: str,
    col: Optional[str],
    elapsed_ms: int,
) -> DFToolResult:
    return DFToolResult(
        tool_id=tool_id,
        tool_name=name,
        command_executed=cmd,
        status="warning" if failed else "success",
        row_count=len(rows),
        failed_count=failed,
        sample_rows=rows[:10],
        message=msg,
        severity=severity,
        column_name=col,
        execution_time_ms=elapsed_ms,
    )


# ═══════════════════════════════════════════════════════════════════════════════
# TABLE-LEVEL PROFILE TOOLS
# ═══════════════════════════════════════════════════════════════════════════════

async def df_shape_profile(df: pd.DataFrame) -> DFToolResult:
    """Report row/column counts and memory usage."""
    t = time.time()
    mem_mb = df.memory_usage(deep=True).sum() / 1_048_576
    rows_dict = [{"rows": len(df), "columns": len(df.columns), "memory_mb": round(mem_mb, 3)}]
    return _make_result(
        "df_shape_profile", "DataFrame Shape & Memory",
        f"df.shape = {df.shape}, memory = {mem_mb:.2f} MB",
        0, rows_dict,
        f"{len(df):,} rows × {len(df.columns)} columns, {mem_mb:.2f} MB in memory",
        "info", None, int((time.time() - t) * 1000),
    )


async def df_dtype_summary(df: pd.DataFrame) -> DFToolResult:
    """Show pandas dtype for every column."""
    t = time.time()
    rows = [{"column": c, "dtype": str(df[c].dtype)} for c in df.columns]
    return _make_result(
        "df_dtype_summary", "Column Data Types",
        "df.dtypes",
        0, rows,
        f"{len(df.columns)} column dtypes catalogued",
        "info", None, int((time.time() - t) * 1000),
    )


async def df_null_heatmap(df: pd.DataFrame) -> DFToolResult:
    """Count nulls per column, flag columns with >5% missing."""
    t = time.time()
    n = len(df)
    rows = []
    critical_cols = []
    for col in df.columns:
        null_n = int(df[col].isna().sum())
        pct = round(null_n / n * 100, 2) if n else 0
        rows.append({"column": col, "null_count": null_n, "null_pct": pct})
        if pct > 5:
            critical_cols.append(col)
    failed = len(critical_cols)
    return _make_result(
        "df_null_heatmap", "Null Heatmap (all columns)",
        "df.isna().sum() / len(df) * 100",
        failed, rows,
        f"{failed} column(s) exceed 5% null threshold: {critical_cols[:5]}",
        "warning" if failed else "info", None, int((time.time() - t) * 1000),
    )


async def df_duplicate_rows(df: pd.DataFrame) -> DFToolResult:
    """Count fully-duplicate rows."""
    t = time.time()
    dup_df = df[df.duplicated(keep=False)]
    n_dup = len(df[df.duplicated()])
    sample = dup_df.head(5).to_dict(orient="records")
    sample = [{k: _json_safe(v) for k, v in r.items()} for r in sample]
    return _make_result(
        "df_duplicate_rows", "Duplicate Rows",
        "df.duplicated().sum()",
        n_dup, sample,
        f"{n_dup} duplicate row(s) found" if n_dup else "No duplicate rows",
        "warning" if n_dup else "info", None, int((time.time() - t) * 1000),
    )


async def df_constant_columns(df: pd.DataFrame) -> DFToolResult:
    """Detect columns with only a single unique value (useless for analysis)."""
    t = time.time()
    const_cols = [c for c in df.columns if df[c].nunique(dropna=False) <= 1]
    rows = [{"column": c, "unique_values": df[c].nunique(dropna=False)} for c in const_cols]
    return _make_result(
        "df_constant_columns", "Constant Columns",
        "[c for c in df.columns if df[c].nunique() <= 1]",
        len(const_cols), rows,
        f"{len(const_cols)} constant column(s): {const_cols}",
        "warning" if const_cols else "info", None, int((time.time() - t) * 1000),
    )


async def df_duplicate_columns(df: pd.DataFrame) -> DFToolResult:
    """Find columns that are exact byte-for-byte duplicates of another column."""
    t = time.time()
    pairs = []
    cols = df.columns.tolist()
    checked: set = set()
    for i, c1 in enumerate(cols):
        for c2 in cols[i + 1:]:
            if c1 in checked or c2 in checked:
                continue
            try:
                if df[c1].equals(df[c2]):
                    pairs.append({"column_a": c1, "column_b": c2})
                    checked.add(c2)
            except Exception:
                pass
    return _make_result(
        "df_duplicate_columns", "Duplicate Columns",
        "column equality check",
        len(pairs), pairs,
        f"{len(pairs)} duplicate column pair(s)" if pairs else "No duplicate columns",
        "warning" if pairs else "info", None, int((time.time() - t) * 1000),
    )


async def df_high_cardinality(df: pd.DataFrame, threshold: float = 0.95) -> DFToolResult:
    """Flag string/object columns where cardinality > threshold (possible accidental ID)."""
    t = time.time()
    n = len(df)
    flagged = []
    for col in df.columns:
        if df[col].dtype == object:
            ratio = df[col].nunique() / n if n else 0
            if ratio > threshold:
                flagged.append({"column": col, "unique_count": int(df[col].nunique()), "cardinality_ratio": round(ratio, 3)})
    return _make_result(
        "df_high_cardinality", f"High-Cardinality String Columns (>{threshold*100:.0f}%)",
        f"nunique / len > {threshold}",
        len(flagged), flagged,
        f"{len(flagged)} high-cardinality column(s) may be IDs or free text",
        "info", None, int((time.time() - t) * 1000),
    )


async def df_numeric_describe(df: pd.DataFrame) -> DFToolResult:
    """Pandas describe() for all numeric columns."""
    t = time.time()
    num_df = df.select_dtypes(include=[np.number])
    if num_df.empty:
        return _make_result(
            "df_numeric_describe", "Numeric Statistics", "df.describe()",
            0, [], "No numeric columns found", "info", None, int((time.time() - t) * 1000),
        )
    desc = num_df.describe().T.reset_index().rename(columns={"index": "column"})
    rows = [{k: _json_safe(v) for k, v in r.items()} for r in desc.to_dict(orient="records")]
    return _make_result(
        "df_numeric_describe", "Numeric Column Statistics", "df.describe()",
        0, rows, f"Statistics for {len(num_df.columns)} numeric column(s)",
        "info", None, int((time.time() - t) * 1000),
    )


async def df_correlation_check(df: pd.DataFrame, threshold: float = 0.95) -> DFToolResult:
    """Find pairs of numeric columns with Pearson correlation above threshold."""
    t = time.time()
    num_df = df.select_dtypes(include=[np.number])
    if num_df.shape[1] < 2:
        return _make_result(
            "df_correlation_check", "Correlation Check", "df.corr()",
            0, [], "Need ≥2 numeric columns for correlation", "info", None, int((time.time() - t) * 1000),
        )
    corr = num_df.corr().abs()
    pairs = []
    cols = corr.columns.tolist()
    for i in range(len(cols)):
        for j in range(i + 1, len(cols)):
            v = corr.iloc[i, j]
            if v >= threshold and not np.isnan(v):
                pairs.append({"col_a": cols[i], "col_b": cols[j], "pearson_r": round(float(v), 4)})
    return _make_result(
        "df_correlation_check", f"High Correlation Pairs (|r| ≥ {threshold})",
        f"df.corr().abs() >= {threshold}",
        len(pairs), pairs,
        f"{len(pairs)} highly correlated pair(s)" if pairs else "No high correlations found",
        "warning" if pairs else "info", None, int((time.time() - t) * 1000),
    )


# ═══════════════════════════════════════════════════════════════════════════════
# COLUMN-LEVEL TOOLS
# ═══════════════════════════════════════════════════════════════════════════════

async def df_outliers_iqr(df: pd.DataFrame, col: str) -> DFToolResult:
    """Flag values outside [Q1 - 1.5*IQR, Q3 + 1.5*IQR] using Tukey fences."""
    t = time.time()
    series = df[col].dropna()
    if not pd.api.types.is_numeric_dtype(series):
        return _make_result(
            "df_outliers_iqr", "IQR Outlier Check", f"IQR on {col}",
            0, [], f"Column '{col}' is not numeric — skipped",
            "info", col, int((time.time() - t) * 1000),
        )
    q1, q3 = series.quantile(0.25), series.quantile(0.75)
    iqr = q3 - q1
    lo, hi = q1 - 1.5 * iqr, q3 + 1.5 * iqr
    mask = (df[col] < lo) | (df[col] > hi)
    outlier_df = df[mask][[col]]
    rows = [{col: _json_safe(v)} for v in outlier_df[col].head(10)]
    return _make_result(
        "df_outliers_iqr", "IQR Outlier Check",
        f"({col} < {lo:.4f}) | ({col} > {hi:.4f})",
        int(mask.sum()), rows,
        f"{int(mask.sum())} outlier(s) in '{col}' (IQR fences [{lo:.4f}, {hi:.4f}])",
        "warning" if mask.sum() else "info", col, int((time.time() - t) * 1000),
    )


async def df_mixed_types(df: pd.DataFrame, col: str) -> DFToolResult:
    """Detect object columns that contain a mix of Python types (str + int + float)."""
    t = time.time()
    series = df[col].dropna()
    if df[col].dtype != object:
        return _make_result(
            "df_mixed_types", "Mixed Type Check", f"type check on {col}",
            0, [], f"Column '{col}' has uniform dtype '{df[col].dtype}' — skipped",
            "info", col, int((time.time() - t) * 1000),
        )
    type_counts: Dict[str, int] = {}
    for v in series.head(500):
        t_name = type(v).__name__
        type_counts[t_name] = type_counts.get(t_name, 0) + 1
    is_mixed = len(type_counts) > 1
    rows = [{"type": k, "count": v} for k, v in sorted(type_counts.items(), key=lambda x: -x[1])]
    return _make_result(
        "df_mixed_types", "Mixed Type Check",
        f"set(type(v) for v in {col}[:500])",
        1 if is_mixed else 0, rows,
        f"Mixed types in '{col}': {type_counts}" if is_mixed else f"'{col}' has uniform type",
        "warning" if is_mixed else "info", col, int((time.time() - t) * 1000),
    )


async def df_date_parse_test(df: pd.DataFrame, col: str) -> DFToolResult:
    """For string columns, test whether pandas can auto-parse them as dates."""
    t = time.time()
    series = df[col].dropna()
    if df[col].dtype != object or len(series) == 0:
        return _make_result(
            "df_date_parse_test", "Date Parseability Test", f"pd.to_datetime({col})",
            0, [], f"Column '{col}' is not an object/string — skipped",
            "info", col, int((time.time() - t) * 1000),
        )
    sample = series.head(100)
    try:
        parsed = pd.to_datetime(sample, infer_datetime_format=True, errors="coerce")
        parse_rate = float(parsed.notna().mean())
    except Exception:
        parse_rate = 0.0
    rows = [{"parse_success_rate": round(parse_rate, 3), "sample_value": str(sample.iloc[0]) if len(sample) else ""}]
    is_parseable = parse_rate >= 0.75
    return _make_result(
        "df_date_parse_test", "Date Parseability Test",
        f"pd.to_datetime({col}, errors='coerce').notna().mean()",
        0, rows,
        f"'{col}' is likely a date column (parse rate {parse_rate:.0%})" if is_parseable else f"'{col}' does not parse as dates ({parse_rate:.0%} success)",
        "info", col, int((time.time() - t) * 1000),
    )


async def df_encoding_issues(df: pd.DataFrame, col: str) -> DFToolResult:
    """Detect non-ASCII / encoding-corrupted characters in string columns."""
    t = time.time()
    series = df[col].dropna()
    if df[col].dtype != object:
        return _make_result(
            "df_encoding_issues", "Encoding Issues Check", f"ascii check on {col}",
            0, [], f"Column '{col}' is not a string — skipped",
            "info", col, int((time.time() - t) * 1000),
        )
    mask = series.astype(str).apply(lambda s: not s.isascii())
    bad_rows = df[mask][[col]].head(10)
    rows = [{col: str(v)} for v in bad_rows[col].tolist()]
    failed = int(mask.sum())
    return _make_result(
        "df_encoding_issues", "Non-ASCII Character Check",
        f"~{col}.str.isascii()",
        failed, rows,
        f"{failed} row(s) contain non-ASCII characters in '{col}'" if failed else f"'{col}' is clean ASCII",
        "info", col, int((time.time() - t) * 1000),
    )


async def df_leading_zeros(df: pd.DataFrame, col: str) -> DFToolResult:
    """Detect numeric-looking strings with leading zeros (postal codes, phone numbers, IDs)."""
    t = time.time()
    series = df[col].dropna()
    if df[col].dtype != object:
        return _make_result(
            "df_leading_zeros", "Leading Zero Check", f"leading zero on {col}",
            0, [], f"Column '{col}' is numeric — no leading-zero risk",
            "info", col, int((time.time() - t) * 1000),
        )
    mask = series.astype(str).str.match(r"^0\d+")
    n = int(mask.sum())
    rows = [{col: str(v)} for v in series[mask].head(5).tolist()]
    return _make_result(
        "df_leading_zeros", "Leading Zeros Check",
        f"{col}.str.match(r'^0\\d+')",
        n, rows,
        f"{n} value(s) in '{col}' have leading zeros — may lose data if cast to int" if n else f"No leading zeros in '{col}'",
        "warning" if n else "info", col, int((time.time() - t) * 1000),
    )


async def df_numeric_as_string(df: pd.DataFrame, col: str) -> DFToolResult:
    """Detect string columns where >80% of values are numeric (should be float/int)."""
    t = time.time()
    series = df[col].dropna()
    if df[col].dtype != object or len(series) == 0:
        return _make_result(
            "df_numeric_as_string", "Numeric-Stored-As-String", f"pd.to_numeric({col})",
            0, [], f"Column '{col}' is not object type — skipped",
            "info", col, int((time.time() - t) * 1000),
        )
    try:
        parsed = pd.to_numeric(series.head(200), errors="coerce")
        rate = float(parsed.notna().mean())
    except Exception:
        rate = 0.0
    rows = [{"numeric_parse_rate": round(rate, 3)}]
    is_issue = rate >= 0.80
    return _make_result(
        "df_numeric_as_string", "Numeric-Stored-As-String Check",
        f"pd.to_numeric({col}, errors='coerce').notna().mean()",
        1 if is_issue else 0, rows,
        f"'{col}' looks numeric ({rate:.0%} parseable) but stored as string" if is_issue else f"'{col}' is not numeric",
        "warning" if is_issue else "info", col, int((time.time() - t) * 1000),
    )


async def df_value_distribution(df: pd.DataFrame, col: str, top_n: int = 15) -> DFToolResult:
    """Top-N value frequency distribution for any column."""
    t = time.time()
    vc = df[col].value_counts(dropna=False).head(top_n)
    rows = [{"value": _json_safe(v), "count": int(c)} for v, c in vc.items()]
    return _make_result(
        "df_value_distribution", f"Top-{top_n} Value Distribution",
        f"{col}.value_counts().head({top_n})",
        0, rows,
        f"Top {min(top_n, len(rows))} value frequencies for '{col}'",
        "info", col, int((time.time() - t) * 1000),
    )


async def df_row_completeness(df: pd.DataFrame, threshold: float = 0.5) -> DFToolResult:
    """Flag rows where more than `threshold` fraction of values are null."""
    t = time.time()
    null_frac = df.isna().mean(axis=1)
    mask = null_frac > threshold
    failed = int(mask.sum())
    sample = df[mask].head(5).to_dict(orient="records")
    sample = [{k: _json_safe(v) for k, v in r.items()} for r in sample]
    return _make_result(
        "df_row_completeness", f"Row Completeness (>{threshold*100:.0f}% null)",
        f"df.isna().mean(axis=1) > {threshold}",
        failed, sample,
        f"{failed} row(s) are more than {threshold*100:.0f}% empty",
        "warning" if failed else "info", None, int((time.time() - t) * 1000),
    )


# ═══════════════════════════════════════════════════════════════════════════════
# TOOL REGISTRY
# ═══════════════════════════════════════════════════════════════════════════════

#: These run once per file (table-level)
DF_PROFILE_TOOLS = [
    df_shape_profile,
    df_dtype_summary,
    df_null_heatmap,
    df_duplicate_rows,
    df_constant_columns,
    df_duplicate_columns,
    df_high_cardinality,
    df_numeric_describe,
    df_correlation_check,
    df_row_completeness,
]

#: Per-column tools — keyed by tool_id for LLM selection
DF_COLUMN_TOOL_MAP: Dict[str, Any] = {
    "df_outliers_iqr":       df_outliers_iqr,
    "df_mixed_types":        df_mixed_types,
    "df_date_parse_test":    df_date_parse_test,
    "df_encoding_issues":    df_encoding_issues,
    "df_leading_zeros":      df_leading_zeros,
    "df_numeric_as_string":  df_numeric_as_string,
    "df_value_distribution": df_value_distribution,
}

#: Human-readable metadata for LLM tool selection prompt
DF_COLUMN_TOOL_DESCRIPTIONS: Dict[str, str] = {
    "df_outliers_iqr":       "Tukey IQR fence outlier detection for numeric columns",
    "df_mixed_types":        "Detect mixed Python types in object columns (str+int+float)",
    "df_date_parse_test":    "Test if a string column can be auto-parsed as dates",
    "df_encoding_issues":    "Find non-ASCII / corrupted characters in string columns",
    "df_leading_zeros":      "Detect leading zeros in numeric-looking strings (zip, phone, ID)",
    "df_numeric_as_string":  "Detect numeric data accidentally stored as strings",
    "df_value_distribution": "Show top-15 value frequencies for any column",
}


# ═══════════════════════════════════════════════════════════════════════════════
# EXECUTOR CLASS
# ═══════════════════════════════════════════════════════════════════════════════

class DataFrameToolExecutor:
    """Executes pandas-native tools against a loaded DataFrame.

    Usage:
        executor = DataFrameToolExecutor(connector, resource_path)
        profile_results = await executor.run_profile_tools()
        col_result = await executor.run_column_tool("df_outliers_iqr", "price")
    """

    def __init__(self, connector, resource_path: str):
        """
        Args:
            connector: A DuckDBFileConnector instance (must have get_dataframe()).
            resource_path: The original resource path / filename.
        """
        self.connector = connector
        self.resource_path = resource_path
        self._df: Optional[pd.DataFrame] = None

    def _get_df(self) -> pd.DataFrame:
        if self._df is None:
            self._df = self.connector.get_dataframe(self.resource_path)
        return self._df

    async def run_profile_tools(self) -> List[DFToolResult]:
        """Run all table-level profile tools and return results."""
        df = self._get_df()
        results: List[DFToolResult] = []
        for fn in DF_PROFILE_TOOLS:
            try:
                r = await fn(df)
                results.append(r)
            except Exception as exc:
                logger.warning(f"[DataFrameToolExecutor] {fn.__name__} failed: {exc}")
                results.append(DFToolResult(
                    tool_id=fn.__name__, tool_name=fn.__name__,
                    command_executed="", status="error", row_count=0, failed_count=0,
                    sample_rows=[], message=str(exc), severity="info",
                ))
        return results

    async def run_column_tool(self, tool_id: str, column: str) -> DFToolResult:
        """Run a single column-level tool."""
        fn = DF_COLUMN_TOOL_MAP.get(tool_id)
        if not fn:
            return DFToolResult(
                tool_id=tool_id, tool_name=tool_id,
                command_executed="", status="error", row_count=0, failed_count=0,
                sample_rows=[], message=f"Unknown DF tool: '{tool_id}'", severity="info",
                column_name=column,
            )
        df = self._get_df()
        try:
            return await fn(df, column)
        except Exception as exc:
            logger.warning(f"[DataFrameToolExecutor] {tool_id}({column}) failed: {exc}")
            return DFToolResult(
                tool_id=tool_id, tool_name=tool_id,
                command_executed="", status="error", row_count=0, failed_count=0,
                sample_rows=[], message=str(exc), severity="info",
                column_name=column,
            )

    async def run_all_column_tools(self, column: str) -> List[DFToolResult]:
        """Run all column-level tools for a given column."""
        results = []
        for tool_id in DF_COLUMN_TOOL_MAP:
            r = await self.run_column_tool(tool_id, column)
            results.append(r)
        return results

    def get_tool_selection_prompt(self) -> str:
        """Return a formatted tool list for LLM prompting."""
        lines = ["PANDAS-NATIVE COLUMN TOOLS (use tool_id):"]
        for tid, desc in DF_COLUMN_TOOL_DESCRIPTIONS.items():
            lines.append(f"  • {tid}: {desc}")
        return "\n".join(lines)
