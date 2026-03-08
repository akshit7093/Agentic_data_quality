"""PostgreSQL and SQL database connector.

REWRITE v5 - Added execute_raw_query for native SQL pushdown 
to support the autonomous ReAct agent architecture.
"""
import logging
from typing import Dict, Any, List, Optional
import asyncio

from sqlalchemy import create_engine, text, inspect, MetaData, Table
from sqlalchemy.engine import Engine
from sqlalchemy.pool import NullPool

from app.connectors.base import BaseConnector

logger = logging.getLogger(__name__)


class PostgreSQLConnector(BaseConnector):
    """Connector for PostgreSQL and compatible databases."""
    
    def __init__(self, connection_config: Dict[str, Any]):
        super().__init__(connection_config)
        self._engine: Optional[Engine] = None
        self._dialect = connection_config.get('dialect', 'postgresql')
    
    def _build_connection_string(self) -> str:
        """Build SQLAlchemy connection string."""
        config = self.connection_config
        
        if 'connection_string' in config:
            return config['connection_string']
        
        # Build from components
        host = config.get('host', 'localhost')
        port = config.get('port', 5432)
        database = config.get('database', config.get('dbname', ''))
        username = config.get('username', config.get('user', ''))
        password = config.get('password', '')
        
        # Handle different dialects
        dialect = self._dialect
        driver = config.get('driver', '')
        
        if driver:
            dialect = f"{dialect}+{driver}"
        
        return f"{dialect}://{username}:{password}@{host}:{port}/{database}"
    
    async def connect(self, **kwargs) -> bool:
        """Establish database connection."""
        try:
            connection_string = self._build_connection_string()
            
            # Create engine with connection pooling
            self._engine = create_engine(
                connection_string,
                poolclass=NullPool,  # Disable pooling for serverless
                echo=self.connection_config.get('echo', False),
            )
            
            # Test connection
            with self._engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            
            self._is_connected = True
            logger.info("Database connection established")
            return True
            
        except Exception as e:
            logger.error(f"Failed to connect to database: {str(e)}")
            raise
    
    async def disconnect(self) -> None:
        """Close database connection."""
        if self._engine:
            self._engine.dispose()
            self._engine = None
        self._is_connected = False
        logger.info("Database connection closed")
    
    async def list_resources(self, path: Optional[str] = None) -> List[Dict[str, Any]]:
        """List tables in database."""
        if not self._engine:
            raise RuntimeError("Not connected to database")
        
        inspector = inspect(self._engine)
        
        resources = []
        for schema_name in inspector.get_schema_names():
            for table_name in inspector.get_table_names(schema=schema_name):
                resources.append({
                    "name": table_name,
                    "schema": schema_name,
                    "path": f"{schema_name}.{table_name}" if schema_name != 'public' else table_name,
                    "type": "table",
                })
        
        return resources
    
    async def get_schema(self, resource_path: str) -> Dict[str, Any]:
        """Get table schema."""
        if not self._engine:
            raise RuntimeError("Not connected to database")
        
        # Parse schema and table name
        if '.' in resource_path:
            schema_name, table_name = resource_path.split('.', 1)
        else:
            schema_name = 'public'
            table_name = resource_path
        
        inspector = inspect(self._engine)
        
        # Get columns
        columns_info = inspector.get_columns(table_name, schema=schema_name)
        
        columns = {}
        for col in columns_info:
            col_name = col['name']
            col_type = col['type']
            
            columns[col_name] = {
                "type": str(col_type),
                "nullable": col.get('nullable', True),
                "default": str(col.get('default')) if col.get('default') else None,
            }
            
            # Add type-specific info
            if hasattr(col_type, 'length') and col_type.length:
                columns[col_name]['max_length'] = col_type.length
            if hasattr(col_type, 'precision') and col_type.precision:
                columns[col_name]['precision'] = col_type.precision
            if hasattr(col_type, 'scale') and col_type.scale:
                columns[col_name]['scale'] = col_type.scale
        
        # Get primary keys
        pk_info = inspector.get_pk_constraint(table_name, schema=schema_name)
        primary_keys = pk_info.get('constrained_columns', [])
        
        # Get foreign keys
        fk_info = inspector.get_foreign_keys(table_name, schema=schema_name)
        foreign_keys = [
            {
                "column": fk['constrained_columns'][0] if fk['constrained_columns'] else None,
                "referenced_table": fk['referred_table'],
                "referenced_column": fk['referred_columns'][0] if fk['referred_columns'] else None,
            }
            for fk in fk_info
        ]
        
        return {
            "name": table_name,
            "schema": schema_name,
            "columns": columns,
            "primary_keys": primary_keys,
            "foreign_keys": foreign_keys,
            "column_count": len(columns),
        }
    
    async def read_data(
        self,
        resource_path: str,
        columns: Optional[List[str]] = None,
        filters: Optional[Dict[str, Any]] = None,
        limit: Optional[int] = None,
        offset: Optional[int] = None,
    ) -> List[Dict[str, Any]]:
        """Read data from table."""
        if not self._engine:
            raise RuntimeError("Not connected to database")
        
        # Parse schema and table name
        if '.' in resource_path:
            schema_name, table_name = resource_path.split('.', 1)
        else:
            schema_name = 'public'
            table_name = resource_path
        
        # Build query
        column_str = ', '.join(f'"{c}"' for c in columns) if columns else '*'
        
        # Properly quote schema and table names
        if schema_name:
            table_ref = f'"{schema_name}"."{table_name}"'
        else:
            table_ref = f'"{table_name}"'
        
        query = f"SELECT {column_str} FROM {table_ref}"
        
        # Add filters
        params = {}
        if filters:
            conditions = []
            for i, (col, val) in enumerate(filters.items()):
                param_name = f"param_{i}"
                conditions.append(f'"{col}" = :{param_name}')
                params[param_name] = val
            query += " WHERE " + " AND ".join(conditions)
        
        # Add pagination
        if limit:
            query += f" LIMIT {limit}"
        if offset:
            query += f" OFFSET {offset}"
        
        # Execute query
        with self._engine.connect() as conn:
            result = conn.execute(text(query), params)
            rows = [dict(row._mapping) for row in result]
        
        return rows
    
    async def sample_data(
        self,
        resource_path: str,
        sample_size: int = 1000,
        method: str = "random",
        full_scan: bool = False,
        slice_filters: Optional[Dict[str, Any]] = None,
    ) -> List[Dict[str, Any]]:
        """Sample data from table."""
        if full_scan:
            return await self.read_data(resource_path, limit=None, filters=slice_filters)
            
        if not self._engine:
            raise RuntimeError("Not connected to database")
        
        # Parse schema and table name
        if '.' in resource_path:
            schema_name, table_name = resource_path.split('.', 1)
        else:
            schema_name = 'public'
            table_name = resource_path
        
        # Properly quote schema and table names
        if schema_name:
            table_ref = f'"{schema_name}"."{table_name}"'
        else:
            table_ref = f'"{table_name}"'
        
        if method == "random":
            # Use database-specific random sampling
            if self._dialect == 'postgresql':
                query = f"SELECT * FROM {table_ref} ORDER BY RANDOM() LIMIT {sample_size}"
            elif self._dialect == 'mysql':
                query = f"SELECT * FROM {table_ref} ORDER BY RAND() LIMIT {sample_size}"
            else:
                query = f"SELECT * FROM {table_ref} TABLESAMPLE SYSTEM ({sample_size} ROWS)"
        
        elif method == "first":
            query = f"SELECT * FROM {table_ref} LIMIT {sample_size}"
        
        elif method == "last":
            # This is tricky - need to know sort order
            query = f"SELECT * FROM {table_ref} ORDER BY ctid DESC LIMIT {sample_size}"
        
        else:
            raise ValueError(f"Unknown sampling method: {method}")
        
        # Add slice filters
        params = {}
        if slice_filters:
            conditions = []
            for i, (col, val) in enumerate(slice_filters.items()):
                param_name = f"slice_{i}"
                conditions.append(f'"{col}" = :{param_name}')
                params[param_name] = val
            
            where_clause = " WHERE " + " AND ".join(conditions)
            # Insert where_clause before any potential ORDER BY or LIMIT
            if " ORDER BY " in query:
                query = query.replace(" ORDER BY ", f"{where_clause} ORDER BY ", 1)
            elif " LIMIT " in query:
                query = query.replace(" LIMIT ", f"{where_clause} LIMIT ", 1)
            elif " TABLESAMPLE " in query:
                # TABLESAMPLE applies to the table before filtering, so we append WHERE after
                query = f"{query} {where_clause}"
            else:
                query += where_clause
        
        # Execute query
        with self._engine.connect() as conn:
            result = conn.execute(text(query), params)
            rows = [dict(row._mapping) for row in result]
        
        return rows
    
    async def get_row_count(self, resource_path: str, slice_filters: Optional[Dict[str, Any]] = None) -> int:
        """Get row count for table."""
        if not self._engine:
            raise RuntimeError("Not connected to database")
        
        # Parse schema and table name
        if '.' in resource_path:
            schema_name, table_name = resource_path.split('.', 1)
        else:
            schema_name = 'public'
            table_name = resource_path
        
        # Properly quote schema and table names
        if schema_name:
            table_ref = f'"{schema_name}"."{table_name}"'
        else:
            table_ref = f'"{table_name}"'
        
        query = f"SELECT COUNT(*) FROM {table_ref}"
        
        params = {}
        if slice_filters:
            conditions = []
            for i, (col, val) in enumerate(slice_filters.items()):
                param_name = f"slice_{i}"
                conditions.append(f'"{col}" = :{param_name}')
                params[param_name] = val
            query += " WHERE " + " AND ".join(conditions)
        
        with self._engine.connect() as conn:
            result = conn.execute(text(query), params)
            count = result.scalar()
        
        return count
    
    async def get_metadata(self, resource_path: str) -> Dict[str, Any]:
        """Get table metadata."""
        if not self._engine:
            raise RuntimeError("Not connected to database")
        
        schema = await self.get_schema(resource_path)
        row_count = await self.get_row_count(resource_path)
        
        return {
            "path": resource_path,
            "name": schema['name'],
            "schema": schema['schema'],
            "column_count": schema['column_count'],
            "row_count": row_count,
            "primary_keys": schema['primary_keys'],
        }

    async def execute_raw_query(self, query: str, query_type: str = "sql", slice_filters: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """
        Natively execute an LLM-generated raw SQL query against the connected database.
        
        Args:
            query: The raw query string to execute.
            query_type: Must be 'sql' for this connector.
            slice_filters: Optional UI filters to enforce as a bounded scope.
            
        Returns:
            Dict containing execution status, row counts, and sample row data.
        """
        if query_type.lower() != "sql":
            return {
                "status": "error",
                "error": f"PostgreSQLConnector only supports 'sql' query_type, received '{query_type}'."
            }

        if not self._engine:
            return {
                "status": "error",
                "error": "Not connected to database."
            }

        try:
            logger.info(f"Natively executing raw SQL query: {query}")
            
            # Subquery injection to enforce UI slice filters
            params = {}
            if slice_filters:
                from sqlalchemy import text
                conditions = []
                for i, (col, val) in enumerate(slice_filters.items()):
                    param_name = f"slice_raw_{i}"
                    conditions.append(f'"{col}" = :{param_name}')
                    params[param_name] = val
                
                where_clause = " AND ".join(conditions)
                
                if "WHERE" in query.upper():
                    upper_query = query.upper()
                    if "ORDER BY" in upper_query:
                        idx = upper_query.index("ORDER BY")
                        query = query[:idx] + f" AND ({where_clause}) " + query[idx:]
                    elif "LIMIT" in upper_query:
                        idx = upper_query.index("LIMIT")
                        query = query[:idx] + f" AND ({where_clause}) " + query[idx:]
                    elif "GROUP BY" in upper_query:
                        idx = upper_query.index("GROUP BY")
                        query = query[:idx] + f" AND ({where_clause}) " + query[idx:]
                    else:
                        query += f" AND ({where_clause})"
                else:
                    upper_query = query.upper()
                    insertion = f" WHERE {where_clause} "
                    if "ORDER BY" in upper_query:
                        idx = upper_query.index("ORDER BY")
                        query = query[:idx] + insertion + query[idx:]
                    elif "LIMIT" in upper_query:
                        idx = upper_query.index("LIMIT")
                        query = query[:idx] + insertion + query[idx:]
                    elif "GROUP BY" in upper_query:
                        idx = upper_query.index("GROUP BY")
                        query = query[:idx] + insertion + query[idx:]
                    else:
                        query += insertion

            with self._engine.connect() as conn:
                # Using a transaction block ensures read-only safety can be managed if needed later
                result = conn.execute(text(query), params)
                
                if result.returns_rows:
                    # Fetch all rows to get the accurate count, but only return a small subset
                    all_rows = [dict(row._mapping) for row in result.fetchall()]
                    row_count = len(all_rows)
                    
                    # Serialize complex types (like datetimes/UUIDs) to string for JSON safety
                    sample_rows = []
                    for row in all_rows[:5]:  # Return top 5 to LLM to save tokens
                        clean_row = {}
                        for k, v in row.items():
                            if v is None:
                                clean_row[k] = None
                            else:
                                clean_row[k] = str(v)
                        sample_rows.append(clean_row)
                        
                    return {
                        "status": "success",
                        "row_count": row_count,
                        "sample_rows": sample_rows
                    }
                else:
                    return {
                        "status": "success",
                        "row_count": result.rowcount,
                        "sample_rows": [],
                        "message": "Query executed successfully but returned no rows."
                    }
                    
        except Exception as e:
            logger.error(f"Native raw query execution failed: {str(e)}")
            return {
                "status": "error",
                "error": str(e)
            }