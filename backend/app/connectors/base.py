"""Base connector interface for all data sources.

REWRITE v5 - Added execute_raw_query for ReAct Agent architecture support.
"""
from abc import ABC, abstractmethod
from typing import Dict, Any, List, Optional
from dataclasses import dataclass


@dataclass
class ColumnSchema:
    """Column schema information."""
    name: str
    data_type: str
    nullable: bool = True
    description: Optional[str] = None
    max_length: Optional[int] = None
    precision: Optional[int] = None
    scale: Optional[int] = None


@dataclass
class TableSchema:
    """Table schema information."""
    name: str
    columns: List[ColumnSchema]
    primary_keys: List[str] = None
    description: Optional[str] = None
    
    def __post_init__(self):
        if self.primary_keys is None:
            self.primary_keys = []


class BaseConnector(ABC):
    """Abstract base class for all data connectors."""
    
    def __init__(
        self, 
        connection_config: Dict[str, Any],
        selected_columns: Optional[List[str]] = None,
        column_mapping: Optional[Dict[str, str]] = None,
        slice_filters: Optional[Dict[str, Any]] = None
    ):
        self.connection_config = connection_config
        self.selected_columns = selected_columns
        self.column_mapping = column_mapping or {}
        self.slice_filters = slice_filters or {}
        self._connection = None
        self._is_connected = False
    
    @property
    def is_connected(self) -> bool:
        """Check if connector is connected."""
        return self._is_connected
    
    @abstractmethod
    async def connect(self, **kwargs) -> bool:
        """Establish connection to data source.
        
        Returns:
            True if connection successful, False otherwise.
        """
        pass
    
    @abstractmethod
    async def disconnect(self) -> None:
        """Close connection to data source."""
        pass
    
    @abstractmethod
    async def list_resources(self, path: Optional[str] = None) -> List[Dict[str, Any]]:
        """List available resources (tables, files, datasets).
        
        Args:
            path: Optional path to list resources from.
            
        Returns:
            List of resource metadata.
        """
        pass
    
    @abstractmethod
    async def get_schema(self, resource_path: str) -> Dict[str, Any]:
        """Get schema for a resource.
        
        Args:
            resource_path: Path to the resource (table name, file path, etc.)
            
        Returns:
            Schema dictionary with column information.
        """
        pass
    
    @abstractmethod
    async def read_data(
        self,
        resource_path: str,
        columns: Optional[List[str]] = None,
        filters: Optional[Dict[str, Any]] = None,
        limit: Optional[int] = None,
        offset: Optional[int] = None,
    ) -> List[Dict[str, Any]]:
        """Read data from resource.
        
        Args:
            resource_path: Path to the resource.
            columns: Optional list of columns to select.
            filters: Optional filters to apply.
            limit: Maximum number of rows to return.
            offset: Number of rows to skip.
            
        Returns:
            List of row dictionaries.
        """
        pass
    
    @abstractmethod
    async def sample_data(
        self,
        resource_path: str,
        sample_size: int = 1000,
        method: str = "random",
        full_scan: bool = False,
    ) -> List[Dict[str, Any]]:
        """Sample data from resource.
        
        Args:
            resource_path: Path to the resource.
            sample_size: Number of rows to sample.
            method: Sampling method (random, first, last).
            full_scan: If True, bypasses sampling and returns all data.
            
        Returns:
            List of sampled row dictionaries.
        """
        if full_scan:
            return await self.read_data(resource_path, limit=None)
        pass
    
    @abstractmethod
    async def get_row_count(self, resource_path: str) -> int:
        """Get total row count for resource.
        
        Args:
            resource_path: Path to the resource.
            
        Returns:
            Total number of rows.
        """
        pass
    
    @abstractmethod
    async def get_metadata(self, resource_path: str) -> Dict[str, Any]:
        """Get metadata for resource.
        
        Args:
            resource_path: Path to the resource.
            
        Returns:
            Metadata dictionary.
        """
        pass

    async def execute_raw_query(self, query: str, query_type: str = "sql") -> Dict[str, Any]:
        """Execute a raw query directly against the data source.
        
        This is the native pushdown method for ReAct agents. If a connector does not 
        override this, the system will fall back to in-memory execution via the ValidationEngine.
        
        Args:
            query: The raw query string to execute.
            query_type: The dialect of the query (e.g., 'sql', 'pandas').
            
        Returns:
            Dict containing 'status', 'row_count', 'sample_rows', etc.
        """
        raise NotImplementedError(
            f"execute_raw_query is not natively implemented for {self.__class__.__name__}. "
            "The ValidationEngine will use its in-memory fallback."
        )
    
    async def test_connection(self) -> Dict[str, Any]:
        """Test connection to data source.
        
        Returns:
            Dictionary with connection test results.
        """
        try:
            await self.connect()
            resources = await self.list_resources()
            await self.disconnect()
            
            return {
                "success": True,
                "message": "Connection successful",
                "resources_found": len(resources),
            }
        except Exception as e:
            return {
                "success": False,
                "message": f"Connection failed: {str(e)}",
            }


class FileConnector(BaseConnector):
    """Base class for file-based connectors."""
    
    SUPPORTED_FORMATS = []
    
    def __init__(
        self, 
        connection_config: Dict[str, Any],
        selected_columns: Optional[List[str]] = None,
        column_mapping: Optional[Dict[str, str]] = None,
        slice_filters: Optional[Dict[str, Any]] = None
    ):
        super().__init__(
            connection_config, 
            selected_columns=selected_columns, 
            column_mapping=column_mapping, 
            slice_filters=slice_filters
        )
        self.base_path = connection_config.get('base_path', '')
    
    def _resolve_path(self, resource_path: str) -> str:
        """Resolve relative path to absolute path."""
        import os
        if os.path.isabs(resource_path):
            return resource_path
        return os.path.join(self.base_path, resource_path)