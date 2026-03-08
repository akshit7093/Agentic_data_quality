"""Local file connector for CSV, Excel, Parquet, JSON files.

REWRITE v5 - Updated to support the ReAct Agent architecture.
Includes full_scan support and explicit delegation of query execution
to the ValidationEngine's in-memory sandbox.
"""
import os
import json
import logging
from typing import Dict, Any, List, Optional
from pathlib import Path

import pandas as pd
import pyarrow.parquet as pq

from app.connectors.base import FileConnector

logger = logging.getLogger(__name__)


class LocalFileConnector(FileConnector):
    """Connector for local file system data sources."""
    
    SUPPORTED_FORMATS = ['.csv', '.xlsx', '.xls', '.parquet', '.json', '.jsonl']
    
    def _resolve_files(self, resource_path: str) -> List[str]:
        """Resolve resource_path to a list of data files.
        
        If resource_path points to a directory, recursively find all
        supported data files within it. Otherwise return a single-item list.
        """
        file_path = self._resolve_path(resource_path)
        if os.path.isdir(file_path):
            found = []
            for root, _, files in os.walk(file_path):
                for fname in sorted(files):
                    full_path = os.path.join(root, fname)
                    if Path(fname).suffix.lower() in self.SUPPORTED_FORMATS:
                        # Skip 0-byte placeholder files
                        if os.path.getsize(full_path) > 0:
                            found.append(full_path)
            if not found:
                raise ValueError(
                    f"No supported data files (non-empty) found in directory: {file_path}. "
                    f"Supported formats: {self.SUPPORTED_FORMATS}"
                )
            return found
        return [file_path]
    
    async def _read_multiple_files(
        self,
        file_paths: List[str],
        columns: Optional[List[str]] = None,
        limit: Optional[int] = None,
    ) -> pd.DataFrame:
        """Read and concatenate multiple data files into one DataFrame."""
        frames = []
        remaining = limit
        for fp in file_paths:
            try:
                fmt = self._get_file_format(fp)
                df = await self._read_file(fp, fmt, columns=columns, limit=remaining)
                frames.append(df)
                if remaining is not None:
                    remaining -= len(df)
                    if remaining <= 0:
                        break
            except Exception as e:
                logger.warning(f"Skipping unreadable file {fp}: {e}")
                continue
        if not frames:
            return pd.DataFrame()
        return pd.concat(frames, ignore_index=True)
    
    async def connect(self, **kwargs) -> bool:
        """Verify base path exists."""
        if self.base_path and not os.path.exists(self.base_path):
            raise ValueError(f"Base path does not exist: {self.base_path}")
        self._is_connected = True
        return True
    
    async def disconnect(self) -> None:
        """No-op for file connector."""
        self._is_connected = False
    
    def _get_file_format(self, file_path: str) -> str:
        """Determine file format from extension."""
        ext = Path(file_path).suffix.lower()
        if ext in ['.csv']:
            return 'csv'
        elif ext in ['.xlsx', '.xls']:
            return 'excel'
        elif ext in ['.parquet']:
            return 'parquet'
        elif ext in ['.json']:
            return 'json'
        elif ext in ['.jsonl']:
            return 'jsonl'
        else:
            raise ValueError(f"Unsupported file format: {ext}")
    
    async def list_resources(self, path: Optional[str] = None) -> List[Dict[str, Any]]:
        """List files in directory."""
        search_path = self._resolve_path(path) if path else self.base_path
        
        if not os.path.isdir(search_path):
            return []
        
        resources = []
        for item in os.listdir(search_path):
            item_path = os.path.join(search_path, item)
            ext = Path(item).suffix.lower()
            
            if ext in self.SUPPORTED_FORMATS:
                stat = os.stat(item_path)
                resources.append({
                    "name": item,
                    "path": os.path.relpath(item_path, self.base_path) if self.base_path else item_path,
                    "type": "file",
                    "format": self._get_file_format(item),
                    "size_bytes": stat.st_size,
                    "modified_at": stat.st_mtime,
                })
            elif os.path.isdir(item_path):
                resources.append({
                    "name": item,
                    "path": os.path.relpath(item_path, self.base_path) if self.base_path else item_path,
                    "type": "directory",
                })
        
        return resources
    
    async def get_schema(self, resource_path: str) -> Dict[str, Any]:
        """Get schema from file or directory of files."""
        file_paths = self._resolve_files(resource_path)
        
        # Read a small sample to infer schema
        df = await self._read_multiple_files(file_paths, limit=100)
        
        # Determine format from the first file
        file_format = self._get_file_format(file_paths[0])
        
        # Read the full file directly so the backend can build true missing% and uniques metrics
        # (This is far faster to sum/unique in Pandas than sending JSON rows to the frontend parser)
        try:
            full_df = await self._read_multiple_files(file_paths)
            total_rows = len(full_df)
        except Exception as e:
            logger.warning(f"Could not load full file for schema metrics. Falling back to sample. Error: {e}")
            full_df = df
            total_rows = len(full_df)

        if df.empty:
            logger.warning(f"Inferring schema from empty DataFrame for {resource_path}")
            
        columns = {}
        for col_name, dtype in df.dtypes.items():
            # If we successfully loaded full_df, calculate absolute metrics natively
            if not full_df.empty and col_name in full_df.columns:
                null_count = int(full_df[col_name].isnull().sum())
                unique_count = int(full_df[col_name].nunique(dropna=True))
                null_percent = round((null_count / total_rows * 100)) if total_rows > 0 else 0
            else:
                null_count = 0
                unique_count = 0
                null_percent = 0

            columns[col_name] = {
                "type": str(dtype),
                "pandas_type": str(dtype.name),
                "nullable": bool(df[col_name].isnull().any()) if col_name in df.columns else True,
                "null_count": null_count,
                "unique_count": unique_count,
                "null_percent": null_percent,
            }
        
        return {
            "name": Path(resource_path).name,
            "format": file_format,
            "columns": columns,
            "column_count": len(columns),
        }
    
    async def _read_file(
        self,
        file_path: str,
        file_format: str,
        columns: Optional[List[str]] = None,
        limit: Optional[int] = None,
        offset: Optional[int] = None,
    ) -> pd.DataFrame:
        """Read file into DataFrame."""
        
        if file_format == 'csv':
            logger.info(f"Reading CSV: {file_path} (limit={limit}, offset={offset})")
            try:
                # Try standard utf-8 first
                df = pd.read_csv(
                    file_path,
                    usecols=columns,
                    nrows=limit,
                    skiprows=range(1, offset + 1) if offset else None,
                    low_memory=False,
                )
            except (UnicodeDecodeError, Exception) as e:
                logger.warning(f"Failed to read CSV {file_path} with utf-8: {e}. Trying latin1...")
                # Fallback to latin1 which is common for legacy CSVs
                df = pd.read_csv(
                    file_path,
                    usecols=columns,
                    nrows=limit,
                    skiprows=range(1, offset + 1) if offset else None,
                    low_memory=False,
                    encoding='latin1'
                )
            logger.info(f"Successfully read CSV. Rows: {len(df)}, Columns: {len(df.columns)}")
        
        elif file_format == 'excel':
            df = pd.read_excel(
                file_path,
                usecols=columns,
                nrows=limit,
                skiprows=offset,
            )
        
        elif file_format == 'parquet':
            table = pq.read_table(
                file_path,
                columns=columns,
            )
            df = table.to_pandas()
            if limit:
                df = df.head(limit)
            if offset:
                df = df.iloc[offset:]
        
        elif file_format == 'json':
            with open(file_path, 'r') as f:
                data = json.load(f)
            
            # Handle different JSON structures
            if isinstance(data, list):
                df = pd.DataFrame(data)
            elif isinstance(data, dict):
                # Try to normalize nested JSON
                df = pd.json_normalize(data)
            else:
                raise ValueError(f"Unsupported JSON structure in {file_path}")
            
            if columns:
                df = df[columns]
            if limit:
                df = df.head(limit)
            if offset:
                df = df.iloc[offset:]
        
        elif file_format == 'jsonl':
            df = pd.read_json(file_path, lines=True)
            if columns:
                df = df[columns]
            if limit:
                df = df.head(limit)
            if offset:
                df = df.iloc[offset:]
        
        else:
            raise ValueError(f"Unsupported file format: {file_format}")
        
        return df
    
    async def read_data(
        self,
        resource_path: str,
        columns: Optional[List[str]] = None,
        filters: Optional[Dict[str, Any]] = None,
        limit: Optional[int] = None,
        offset: Optional[int] = None,
    ) -> List[Dict[str, Any]]:
        """Read data from file or directory of files."""
        file_paths = self._resolve_files(resource_path)
        
        df = await self._read_multiple_files(file_paths, columns=columns, limit=limit)
        
        if offset:
            df = df.iloc[offset:]
        
        # Apply filters
        if filters:
            for col, value in filters.items():
                if col in df.columns:
                    df = df[df[col] == value]
        
        # Convert to records
        records = df.replace({pd.NA: None, pd.NaT: None}).to_dict('records')
        
        # Clean NaN values for JSON serialization
        for record in records:
            for key, val in record.items():
                if pd.isna(val):
                    record[key] = None
        
        return records
    
    async def sample_data(
        self,
        resource_path: str,
        sample_size: int = 1000,
        method: str = "random",
        full_scan: bool = False,
    ) -> List[Dict[str, Any]]:
        """Sample data from file or directory of files."""
        if full_scan:
            return await self.read_data(resource_path)
            
        # Get total row count first
        total_rows = await self.get_row_count(resource_path)
        
        if total_rows <= sample_size:
            # Return all data if smaller than sample size
            return await self.read_data(resource_path)
        
        # Read all files into one DataFrame
        file_paths = self._resolve_files(resource_path)
        df = await self._read_multiple_files(file_paths)
        
        if method == "random":
            df = df.sample(n=min(sample_size, len(df)))
        elif method == "first":
            df = df.head(sample_size)
        elif method == "last":
            df = df.tail(sample_size)
        else:
            raise ValueError(f"Unknown sampling method: {method}")
        
        records = df.replace({pd.NA: None, pd.NaT: None}).to_dict('records')
        
        # Clean NaN values
        for record in records:
            for key, val in record.items():
                if pd.isna(val):
                    record[key] = None
        
        return records
    
    async def get_row_count(self, resource_path: str) -> int:
        """Get row count for file or directory of files."""
        file_paths = self._resolve_files(resource_path)
        
        total = 0
        for fp in file_paths:
            fmt = self._get_file_format(fp)
            if fmt == 'csv':
                with open(fp, 'r') as f:
                    total += sum(1 for _ in f) - 1  # Exclude header
            elif fmt == 'parquet':
                metadata = pq.read_metadata(fp)
                total += metadata.num_rows
            else:
                df = await self._read_file(fp, fmt)
                total += len(df)
        
        return total
    
    async def get_metadata(self, resource_path: str) -> Dict[str, Any]:
        """Get file metadata."""
        file_path = self._resolve_path(resource_path)
        stat = os.stat(file_path)
        
        return {
            "path": resource_path,
            "absolute_path": file_path,
            "format": self._get_file_format(file_path),
            "size_bytes": stat.st_size,
            "created_at": stat.st_ctime,
            "modified_at": stat.st_mtime,
            "row_count": await self.get_row_count(resource_path),
        }

    async def execute_raw_query(self, query: str, query_type: str = "sql") -> Dict[str, Any]:
        """
        Local files do not have a native query engine running (unlike PostgreSQL).
        We explicitly raise NotImplementedError so the ValidationEngine intercepts 
        this and runs the query securely in an in-memory SQLite/Pandas sandbox.
        """
        raise NotImplementedError(
            "execute_raw_query is intentionally not implemented for LocalFileConnector. "
            "The ValidationEngine will automatically catch this and use its in-memory "
            "sandbox (SQLite/Pandas) to execute the agent's query against the local file data."
        )