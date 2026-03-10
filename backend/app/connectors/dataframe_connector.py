"""DuckDB-backed universal file connector.

SUPPORTED FORMATS: CSV · TSV · JSON · JSONL/NDJSON · Excel (xlsx/xls/xlsm) · Parquet · Feather

DESIGN:
  - Exposes the EXACT same async interface as SQL connectors so the entire existing
    tool/agent system (ValidationToolExecutor, _build_rules, etc.) works without changes.
  - File is loaded into pandas once, registered in an in-memory DuckDB instance as a named
    table.  All SQL queries run through DuckDB (fast, columnar, no SQLite type coercion).
  - _rewrite_query() swaps the original filename placeholder with the sanitised DuckDB
    table name, so tools that substitute {table} = "my-file.csv" still work.
  - slice_filters applied as in-memory pandas filter before DuckDB registration when a
    pre-filtered view is needed (e.g. pivot slicing).

REGISTRATION:
    Add this to your ConnectorFactory:
        from app.connectors.dataframe_connector import DuckDBFileConnector
        ConnectorFactory.register("local_file", DuckDBFileConnector)
"""

from __future__ import annotations

import asyncio
import logging
import re
from pathlib import Path
from typing import Any, Dict, List, Optional

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)

# ── Pandas dtype → agent DataType string ──────────────────────────────────────
_DTYPE_MAP: Dict[str, str] = {
    "int8":   "integer", "int16":  "integer", "int32":  "integer", "int64":  "integer",
    "uint8":  "integer", "uint16": "integer", "uint32": "integer", "uint64": "integer",
    "float16":"float",   "float32":"float",   "float64":"float",
    "bool":    "boolean",
    "object":  "string",
    "string":  "string",
    "category":"categorical",
}

# ── File extension → loader method name ───────────────────────────────────────
_LOADERS: Dict[str, str] = {
    ".csv":     "_load_csv",
    ".tsv":     "_load_csv",
    ".txt":     "_load_csv",
    ".json":    "_load_json",
    ".jsonl":   "_load_jsonl",
    ".ndjson":  "_load_jsonl",
    ".xlsx":    "_load_excel",
    ".xls":     "_load_excel",
    ".xlsm":    "_load_excel",
    ".parquet": "_load_parquet",
    ".pq":      "_load_parquet",
    ".feather": "_load_feather",
}

SUPPORTED_EXTENSIONS = set(_LOADERS.keys())


# ── Helpers ────────────────────────────────────────────────────────────────────

def _sanitize_table_name(filename: str) -> str:
    """Derive a valid SQL identifier from a file path / name."""
    stem = Path(filename).stem
    name = re.sub(r"[^a-zA-Z0-9_]", "_", stem)
    if name and name[0].isdigit():
        name = "t_" + name
    name = re.sub(r"_+", "_", name).strip("_")
    return (name or "data_table")[:60]


def _json_safe(val: Any) -> Any:
    """Make a scalar JSON-serialisable (converts numpy types, NaN → None)."""
    if val is None:
        return None
    if isinstance(val, float) and (val != val):   # NaN
        return None
    if isinstance(val, (np.integer,)):
        return int(val)
    if isinstance(val, (np.floating,)):
        return None if np.isnan(val) else float(val)
    if isinstance(val, (np.bool_,)):
        return bool(val)
    if isinstance(val, (np.ndarray,)):
        return val.tolist()
    if isinstance(val, pd.Timestamp):
        return str(val)
    return val


def _safe_rows(df: pd.DataFrame, limit: int = 100) -> List[Dict[str, Any]]:
    """Convert a DataFrame slice to a list of JSON-safe dicts."""
    out = []
    for _, row in df.head(limit).iterrows():
        out.append({k: _json_safe(v) for k, v in row.items()})
    return out


# ── Main Connector ─────────────────────────────────────────────────────────────

class DuckDBFileConnector:
    """Async flat-file connector backed by DuckDB (in-memory)."""

    def __init__(
        self, 
        connection_config: Dict[str, Any],
        selected_columns: Optional[List[str]] = None,
        column_mapping: Optional[Dict[str, str]] = None,
        slice_filters: Optional[Dict[str, Any]] = None
    ):
        self.base_path: str = connection_config.get("base_path", ".")
        self._conn = None                   # duckdb.DuckDBPyConnection
        self._df: Optional[pd.DataFrame] = None
        self.original_name: Optional[str] = None   # raw resource_path as given
        self.table_name: Optional[str] = None       # sanitised DuckDB view name
        self._schema_cache: Optional[Dict] = None

        # ── Scope attributes ──────────────────────────────────────────────────
        self.selected_columns = selected_columns
        self.column_mapping = column_mapping or {}     # original_name → renamed_name
        self.slice_filters = slice_filters or {}

    # ── Lifecycle ──────────────────────────────────────────────────────────────

    async def connect(self, **kwargs) -> None:
        """Prime the connector if resource_path is provided, otherwise no-op (lazy load)."""
        resource_path = kwargs.get("resource_path")
        if resource_path:
            self._ensure_loaded(resource_path)

    async def disconnect(self) -> None:
        """No-op: keep DuckDB connection alive for the duration of the validation run."""
        pass

    def close(self) -> None:
        """Explicitly release DuckDB resources."""
        if self._conn:
            try:
                self._conn.close()
            except Exception:
                pass
            self._conn = None

    # ── Internal loading ───────────────────────────────────────────────────────

    def _resolve_path(self, resource_path: str) -> Path:
        p = Path(resource_path)
        if p.is_absolute() and p.exists():
            return p
        candidate = Path(self.base_path) / resource_path
        if candidate.exists():
            return candidate
        raise FileNotFoundError(
            f"File not found: '{resource_path}' "
            f"(tried absolute + base_path={self.base_path})"
        )

    def _ensure_loaded(self, resource_path: str) -> None:
        """Load the file into DuckDB if not already done for this resource_path."""
        if self._conn is not None and self.original_name == resource_path:
            return  # already loaded

        file_path = self._resolve_path(resource_path)
        ext = file_path.suffix.lower()
        loader_name = _LOADERS.get(ext)
        if not loader_name:
            raise ValueError(
                f"Unsupported file type '{ext}'. "
                f"Supported: {sorted(SUPPORTED_EXTENSIONS)}"
            )

        df: pd.DataFrame = getattr(self, loader_name)(file_path)

        # Strip whitespace from column names
        df.columns = [str(c).strip() for c in df.columns]

        # ── APPLY SESSION SCOPE ───────────────────────────────────────
        # 1. Slice filters (e.g. state='CA')
        if self.slice_filters:
            for col, val in self.slice_filters.items():
                if col in df.columns:
                    df = df[df[col] == val]
            logger.info(f"[DuckDBFileConnector] Applied filters: {self.slice_filters} (rows: {len(df)})")

        # 2. Rename columns
        if self.column_mapping:
            # column_mapping is original_name -> renamed_name
            # subset mapping to only what's actually in df
            existing_mapping = {k: v for k, v in self.column_mapping.items() if k in df.columns}
            if existing_mapping:
                df = df.rename(columns=existing_mapping)
                logger.info(f"[DuckDBFileConnector] Applied rename mapping: {existing_mapping}")

        # 3. Subset columns
        if self.selected_columns:
            # selected_columns are likely the RENAMED names if they came from a template
            available_cols = [c for c in self.selected_columns if c in df.columns]
            if available_cols:
                df = df[available_cols]
                logger.info(f"[DuckDBFileConnector] Subset to {len(available_cols)} selected columns.")

        self._df = df
        self.original_name = resource_path
        self.table_name = _sanitize_table_name(resource_path)
        self._schema_cache = None  # invalidate

        # (Re)create DuckDB connection
        if self._conn:
            try:
                self._conn.close()
            except Exception:
                pass

        import duckdb
        self._conn = duckdb.connect(database=":memory:")
        # Register under sanitised name and a stable alias
        self._conn.register(self.table_name, df)
        self._conn.register("data_table", df)

        logger.info(
            f"[DuckDBFileConnector] Loaded and Registered '{resource_path}' → "
            f"table='{self.table_name}' ({len(df):,} rows × {len(df.columns)} cols)"
        )

    # ── File loaders ──────────────────────────────────────────────────────────

    def _load_csv(self, path: Path) -> pd.DataFrame:
        sep = "\t" if path.suffix.lower() in (".tsv",) else ","
        for enc in ("utf-8", "utf-8-sig", "latin-1", "cp1252"):
            try:
                return pd.read_csv(path, sep=sep, encoding=enc, low_memory=False)
            except UnicodeDecodeError:
                continue
        raise ValueError(f"Could not decode {path} with common encodings")

    def _load_json(self, path: Path) -> pd.DataFrame:
        try:
            return pd.read_json(path)
        except ValueError:
            return pd.read_json(path, lines=True)

    def _load_jsonl(self, path: Path) -> pd.DataFrame:
        return pd.read_json(path, lines=True)

    def _load_excel(self, path: Path) -> pd.DataFrame:
        return pd.read_excel(path, engine="openpyxl" if path.suffix.lower() != ".xls" else "xlrd")

    def _load_parquet(self, path: Path) -> pd.DataFrame:
        return pd.read_parquet(path)

    def _load_feather(self, path: Path) -> pd.DataFrame:
        return pd.read_feather(path)

    # ── Schema ────────────────────────────────────────────────────────────────

    def _infer_col_type(self, series: pd.Series) -> str:
        """Map a pandas Series to a DataType string."""
        dtype_str = str(series.dtype)
        mapped = _DTYPE_MAP.get(dtype_str)
        if mapped:
            # Refine: is it actually a date stored as int epoch?
            if mapped == "integer" and "date" in series.name.lower():
                return "date"
            return mapped
        if "datetime" in dtype_str:
            return "datetime"
        if "float" in dtype_str:
            return "float"
        if "int" in dtype_str:
            return "integer"

        # String heuristics on sample
        if series.dtype == object:
            sample = series.dropna().astype(str).head(50)
            if len(sample) == 0:
                return "string"
            if sample.str.match(r"^[^@\s]+@[^@\s]+\.[^@\s]+$").mean() > 0.75:
                return "email"
            if sample.str.match(r"^\d{4}-\d{2}-\d{2}").mean() > 0.75:
                return "date"
            if sample.str.match(r"^https?://").mean() > 0.75:
                return "url"
            unique_lower = set(sample.str.lower().unique())
            if unique_lower.issubset({"true", "false", "yes", "no", "1", "0", "t", "f"}):
                return "boolean"
            if sample.str.match(r"^\+?[\d\s\-\(\)]{7,20}$").mean() > 0.75:
                return "phone"
            # Categorical heuristic: ≤20 unique values in sample
            if series.nunique() <= 20:
                return "categorical"
            return "string"

        return "string"

    async def get_schema(self, resource_path: str) -> Dict[str, Any]:
        """Return schema dict compatible with agent expectations."""
        self._ensure_loaded(resource_path)

        if self._schema_cache:
            return self._schema_cache

        columns: Dict[str, Any] = {}
        for col in self._df.columns:
            series = self._df[col]
            col_type = self._infer_col_type(series)
            null_count = int(series.isna().sum())
            unique_count = int(series.nunique())
            sample_vals = [
                _json_safe(v) for v in series.dropna().head(5).tolist()
            ]
            columns[col] = {
                "type":         col_type,
                "nullable":     null_count > 0,
                "null_count":   null_count,
                "unique_count": unique_count,
                "sample_values": sample_vals,
                "pandas_dtype": str(series.dtype),
            }

        self._schema_cache = {
            "table_name":    self.table_name,
            "original_name": self.original_name,
            "columns":       columns,
            "row_count":     len(self._df),
            "column_count":  len(self._df.columns),
        }
        return self._schema_cache

    async def get_row_count(self, resource_path: str) -> int:
        """Return total row count (used by preview endpoint)."""
        self._ensure_loaded(resource_path)
        return len(self._df)

    # ── Data sampling ────────────────────────────────────────────────────────

    async def sample_data(
        self,
        resource_path: str,
        sample_size: int = 1000,
        full_scan: bool = False,
        slice_filters: Optional[Dict[str, Any]] = None,
        method: str = "first",
    ) -> List[Dict[str, Any]]:
        """Return a list of row-dicts, optionally filtered."""
        self._ensure_loaded(resource_path)
        df = self._df

        # Apply key=value slice filters
        if slice_filters:
            for col, val in slice_filters.items():
                if col in df.columns:
                    df = df[df[col] == val]

        if full_scan:
            result_df = df
        elif method == "random":
            n = min(sample_size, len(df))
            result_df = df.sample(n=n, random_state=42)
        else:
            result_df = df.head(sample_size)

        return _safe_rows(result_df, limit=len(result_df))

    # ── SQL execution via DuckDB ──────────────────────────────────────────────

    def _rewrite_query(self, query: str) -> str:
        """Replace original filename references with the sanitised DuckDB table name."""
        if not self.original_name or not self.table_name:
            return query

        original = self.original_name

        # Try most-specific patterns first (quoted variants), then bare name
        for pattern in (
            f'"{original}"',
            f"'{original}'",
            f"`{original}`",
            f"[{original}]",
            original,
        ):
            if pattern in query:
                query = query.replace(pattern, self.table_name)
                return query

        # Also handle stem-only (filename without extension)
        stem = Path(original).stem
        sanitised_stem = _sanitize_table_name(original)
        if stem in query and stem != sanitised_stem:
            query = query.replace(stem, self.table_name)

        return query

    async def execute_raw_query(
        self, query: str, query_type: str = "sql"
    ) -> Dict[str, Any]:
        """Execute SQL against the in-memory DuckDB instance."""
        if self._conn is None:
            return {
                "status": "error",
                "error": "Connector not initialised. Call get_schema() or sample_data() first.",
                "row_count": 0,
                "sample_rows": [],
            }

        try:
            rewritten = self._rewrite_query(query.strip())
            # Unescape double-escaped column quotes from LLM output (e.g. \"col\")
            rewritten = rewritten.replace('\\"', '"')

            rel = self._conn.execute(rewritten)
            if rel.description is None:
                # DDL or non-SELECT statement
                return {"status": "success", "row_count": 0, "sample_rows": []}

            col_names = [d[0] for d in rel.description]
            all_rows  = rel.fetchall()

            sample_rows = []
            for row in all_rows[:200]:
                sample_rows.append({
                    k: _json_safe(v) for k, v in zip(col_names, row)
                })

            return {
                "status":      "success",
                "row_count":   len(all_rows),
                "sample_rows": sample_rows,
                "columns":     col_names,
            }

        except Exception as exc:
            logger.warning(
                f"[DuckDBFileConnector] Query error: {exc} | "
                f"query={query[:200]!r}"
            )
            return {
                "status":      "error",
                "error":       str(exc),
                "row_count":   0,
                "sample_rows": [],
            }

    # ── Convenience: expose raw DataFrame for pandas tools ───────────────────

    def get_dataframe(self, resource_path: str) -> pd.DataFrame:
        """Return the loaded DataFrame for pandas-native tools."""
        self._ensure_loaded(resource_path)
        return self._df  # type: ignore[return-value]
