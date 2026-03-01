"""SQLite database connector.

REWRITE v5 - Added execute_raw_query for native SQL pushdown 
to support the autonomous ReAct agent architecture.
"""
import logging
from typing import Dict, Any, List, Optional
import asyncio
import os

from sqlalchemy import create_engine, text, inspect, MetaData, Table
from sqlalchemy.engine import Engine
from sqlalchemy.pool import NullPool

from app.connectors.base import BaseConnector

logger = logging.getLogger(__name__)


class SQLiteConnector(BaseConnector):
    """Connector for SQLite databases."""

    def __init__(self, connection_config: Dict[str, Any]):
        super().__init__(connection_config)
        self._engine: Optional[Engine] = None
        self._dialect = 'sqlite'

    def _build_connection_string(self) -> str:
        """Build SQLAlchemy connection string."""
        config = self.connection_config

        if 'connection_string' in config:
            return config['connection_string']

        # Get database path from config
        db_path = config.get('database', config.get('db_path', ''))
        
        # If no path provided, try to use default test database
        if not db_path:
            from pathlib import Path
            test_data_dir = Path(__file__).parent.parent.parent.parent / "test_data"
            db_path = str(test_data_dir / "test_database.db")
        
        # Ensure the path is absolute
        if not os.path.isabs(db_path):
            db_path = os.path.abspath(db_path)
        
        return f"sqlite:///{db_path}"

    async def connect(self) -> bool:
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
            logger.info("SQLite database connection established")
            return True

        except Exception as e:
            logger.error(f"Failed to connect to SQLite database: {str(e)}")
            raise

    async def disconnect(self) -> None:
        """Close database connection."""
        if self._engine:
            self._engine.dispose()
            self._engine = None
        self._is_connected = False
        logger.info("SQLite database connection closed")

    async def list_resources(self, path: Optional[str] = None) -> List[Dict[str, Any]]:
        """List tables in database."""
        if not self._engine:
            raise RuntimeError("Not connected to database")

        inspector = inspect(self._engine)

        resources = []
        # For SQLite, there's no schema concept like PostgreSQL, just list tables
        for table_name in inspector.get_table_names():
            resources.append({
                "name": table_name,
                "schema": "",  # SQLite doesn't use schemas
                "path": table_name,
                "type": "table",
            })

        return resources

    async def get_schema(self, resource_path: str) -> Dict[str, Any]:
        """Get table schema."""
        if not self._engine:
            raise RuntimeError("Not connected to database")

        # For SQLite, we don't have schema prefixes like PostgreSQL
        # Just use the table name directly
        table_name = resource_path
        
        # Check if table exists
        inspector = inspect(self._engine)
        if table_name not in inspector.get_table_names():
            raise ValueError(f"Table '{table_name}' does not exist in database")

        # Get columns using SQLAlchemy's inspector
        columns_info = inspector.get_columns(table_name)

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

        # Query exact metrics from SQLite (fast for local files, prevents sampling issues)
        try:
            with self._engine.connect() as conn:
                # 1. Total row count is fast
                total_rows = conn.execute(text(f'SELECT COUNT(*) FROM "{table_name}"')).scalar() or 0
                
                # 2. Get nulls and uniques per column
                for col_name in columns.keys():
                    queries = f'SELECT COUNT("{col_name}"), COUNT(DISTINCT "{col_name}") FROM "{table_name}"'
                    result = conn.execute(text(queries)).fetchone()
                    if result:
                        non_null_count, unique_count = result
                        null_count = total_rows - non_null_count
                        columns[col_name]["null_count"] = null_count
                        columns[col_name]["unique_count"] = unique_count
                        columns[col_name]["null_percent"] = round((null_count / total_rows * 100)) if total_rows > 0 else 0
        except Exception as e:
            logger.warning(f"Could not compute exact column metrics in schema for {table_name}: {e}")

        # Get primary keys
        pk_info = inspector.get_pk_constraint(table_name)
        primary_keys = pk_info.get('constrained_columns', []) if pk_info else []

        # Get foreign keys
        fk_info = inspector.get_foreign_keys(table_name)
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
            "schema": "",  # SQLite doesn't use schemas
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

        # For SQLite, just use the table name directly
        table_name = resource_path

        # Build query
        column_str = ', '.join(f'"{c}"' for c in columns) if columns else '*'
        query = f'SELECT {column_str} FROM "{table_name}"'

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
            print(f"Executing Query: {query} with Params: {params}")
            result = conn.execute(text(query), params)
            rows = [dict(row._mapping) for row in result]
            print(f"Rows fetched: {len(rows)}")

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

        # For SQLite, just use the table name directly
        table_name = resource_path

        if method == "random":
            # Use SQLite's RANDOM() function
            query = f'SELECT * FROM "{table_name}" ORDER BY RANDOM() LIMIT {sample_size}'
        elif method == "first":
            query = f'SELECT * FROM "{table_name}" LIMIT {sample_size}'
        elif method == "last":
            # For last records, we need to order by a unique column if available
            # Since we don't know the structure, we'll just use rowid
            query = f'SELECT * FROM "{table_name}" ORDER BY rowid DESC LIMIT {sample_size}'
        else:
            raise ValueError(f"Unknown sampling method: {method}")

        # Execute query
        params = {}
        if slice_filters:
            conditions = []
            for i, (col, val) in enumerate(slice_filters.items()):
                param_name = f"slice_{i}"
                conditions.append(f'"{col}" = :{param_name}')
                params[param_name] = val
            
            where_clause = " WHERE " + " AND ".join(conditions)
            query = query.replace(f'ROM "{table_name}"', f'ROM "{table_name}" {where_clause}')

        with self._engine.connect() as conn:
            result = conn.execute(text(query), params)
            rows = [dict(row._mapping) for row in result]

        return rows

    async def get_row_count(self, resource_path: str, slice_filters: Optional[Dict[str, Any]] = None) -> int:
        """Get row count for table."""
        if not self._engine:
            raise RuntimeError("Not connected to database")

        # For SQLite, just use the table name directly
        table_name = resource_path

        query = f'SELECT COUNT(*) FROM "{table_name}"'
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
                "error": f"SQLiteConnector only supports 'sql' query_type, received '{query_type}'."
            }

        if not self._engine:
            return {
                "status": "error",
                "error": "Not connected to database."
            }

        try:
            logger.info(f"Natively executing raw SQLite query: {query}")
            
            # Subquery injection to enforce UI slice filters
            params = {}
            if slice_filters:
                # We do a naive replace of the target table name with a subquery
                # For a perfect implementation you'd use a SQL parser (SQLGlot),
                # but for this MVP we find the table name dynamically from the agent state or assume
                # the query is simple enough that replacing the FROM clause works if we know the table.
                # A safer MVP approach: We inject the WHERE clause into the query if it doesn't have one,
                # or append it with AND if it does.
                
                conditions = []
                for i, (col, val) in enumerate(slice_filters.items()):
                    param_name = f"slice_raw_{i}"
                    conditions.append(f'"{col}" = :{param_name}')
                    params[param_name] = val
                
                where_clause = " AND ".join(conditions)
                
                if "WHERE" in query.upper():
                    # Safest simple injection: Replace WHERE with WHERE (original) AND (new)
                    # We'll just append it to the end before ORDER/LIMIT for simple queries
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
                    # No WHERE clause exists
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
                result = conn.execute(text(query), params)
                
                if result.returns_rows:
                    all_rows = [dict(row._mapping) for row in result.fetchall()]
                    row_count = len(all_rows)
                    
                    sample_rows = []
                    for row in all_rows[:5]:  # Return top 5 to LLM
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