"""Factory for creating data connectors."""
from typing import Dict, Any, Type
from app.connectors.base import BaseConnector
from app.connectors.local_file import LocalFileConnector
from app.connectors.postgresql import PostgreSQLConnector
from app.connectors.sqlite import SQLiteConnector


class ConnectorFactory:
    """Factory for creating data source connectors."""
    
    _connectors: Dict[str, Type[BaseConnector]] = {
        "local_file": LocalFileConnector,
        "csv": LocalFileConnector,
        "excel": LocalFileConnector,
        "parquet": LocalFileConnector,
        "json": LocalFileConnector,
        "postgresql": PostgreSQLConnector,
        "postgres": PostgreSQLConnector,
        "mysql": PostgreSQLConnector,
        "sqlite": SQLiteConnector,
    }

    @classmethod
    def register_connector(cls, source_type: str, connector_class: Type[BaseConnector]) -> None:
        """Register a new connector type.
        
        Args:
            source_type: The source type identifier.
            connector_class: The connector class to register.
        """
        cls._connectors[source_type] = connector_class

    @classmethod
    def create_connector(cls, source_type: str, connection_config: Dict[str, Any]) -> BaseConnector:
        """Create a connector instance.
        
        Args:
            source_type: The type of data source.
            connection_config: Connection configuration.
            
        Returns:
            Connector instance.
            
        Raises:
            ValueError: If source type is not supported.
        """
        source_type = source_type.lower()
        
        if source_type not in cls._connectors:
            raise ValueError(
                f"Unsupported data source type: {source_type}. "
                f"Supported types: {list(cls._connectors.keys())}"
            )
        
        connector_class = cls._connectors[source_type]
        return connector_class(connection_config)

    @classmethod
    def get_supported_types(cls) -> list:
        """Get list of supported source types.
        
        Returns:
            List of supported source type strings.
        """
        return list(cls._connectors.keys())

    @classmethod
    def is_supported(cls, source_type: str) -> bool:
        """Check if a source type is supported.
        
        Args:
            source_type: The source type to check.
            
        Returns:
            True if supported, False otherwise.
        """
        return source_type.lower() in cls._connectors