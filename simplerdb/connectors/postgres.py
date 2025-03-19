"""
PostgreSQL connector for SimplerDB.
"""

import logging
import time
from typing import Dict, List, Tuple, Union, Any, Optional, Iterator, cast

from ..base import SQLBaseConnector
from ..exceptions import ConnectionError, QueryError, ConfigurationError
from ..utils.sanitization import sanitize_identifier

logger = logging.getLogger(__name__)

class PostgreSQLConnector(SQLBaseConnector):
    """
    Connector for PostgreSQL databases.
    """
    
    def __init__(self, **kwargs):
        """
        Initialize the PostgreSQL connector.
        
        Args:
            host (str): Database host
            port (int): Database port
            dbname (str): Database name
            user (str): Username
            password (str): Password
            sslmode (str): SSL mode
            connect_timeout (int): Connection timeout in seconds
            pool_size (int): Connection pool size (0 for no pooling)
            pool_min_size (int): Minimum pool size
            pool_max_size (int): Maximum pool size
            application_name (str): Application name
            **kwargs: Additional connector-specific parameters
        """
        super().__init__(**kwargs)
        
        # Update configuration with defaults
        self.conf["host"] = kwargs.get("host", "localhost")
        self.conf["port"] = kwargs.get("port", 5432)
        self.conf["dbname"] = kwargs.get("dbname", kwargs.get("db", None))
        self.conf["user"] = kwargs.get("user", None)
        self.conf["password"] = kwargs.get("password", kwargs.get("passwd", None))
        self.conf["sslmode"] = kwargs.get("sslmode", "prefer")
        self.conf["connect_timeout"] = kwargs.get("connect_timeout", 30)
        self.conf["pool_size"] = kwargs.get("pool_size", 0)
        self.conf["pool_min_size"] = kwargs.get("pool_min_size", 1)
        self.conf["pool_max_size"] = kwargs.get("pool_max_size", 10)
        self.conf["application_name"] = kwargs.get("application_name", "SimplerDB")
        
        # Check for required parameters
        required_params = ["dbname", "user", "password"]
        for param in required_params:
            if not self.conf[param]:
                raise ConfigurationError(f"Required parameter '{param}' is missing for PostgreSQL connection")
        
        # Initialize connection and state
        self.conn = None
        self.cur = None
        self._last_query = ""
        self.pool = None
        
        try:
            import psycopg2
            import psycopg2.extras
            import psycopg2.pool
            self.psycopg2 = psycopg2
        except ImportError:
            raise ConfigurationError(
                "psycopg2 is required for PostgreSQL connections. "
                "Install it with: pip install psycopg2-binary"
            )
        
        # Connect to the database
        self.connect()
    
    def connect(self) -> None:
        """Establish a connection to the database."""
        try:
            # Build connection parameters
            conn_params = {
                "host": self.conf["host"],
                "port": self.conf["port"],
                "dbname": self.conf["dbname"],
                "user": self.conf["user"],
                "password": self.conf["password"],
                "sslmode": self.conf["sslmode"],
                "connect_timeout": self.conf["connect_timeout"],
                "application_name": self.conf["application_name"]
            }
            
            # Filter out None values
            conn_params = {k: v for k, v in conn_params.items() if v is not None}
            
            # Set up connection pooling if configured
            if self.conf["pool_size"] > 0:
                self._setup_connection_pool(conn_params)
            else:
                # Create a direct connection
                self.conn = self.psycopg2.connect(**conn_params)
                # Set cursor factory for dict-like row access
                self.cur = self.conn.cursor(cursor_factory=self.psycopg2.extras.DictCursor)
            
            logger.debug(f"Connected to PostgreSQL database: {self.conf['dbname']}")
            
        except Exception as e:
            logger.error(f"PostgreSQL connection failed: {str(e)}")
            raise ConnectionError(f"Failed to connect to PostgreSQL database: {str(e)}") from e
    
    def _setup_connection_pool(self, conn_params: Dict[str, Any]) -> None:
        """Set up a connection pool."""
        try:
            # Create a connection pool
            self.pool = self.psycopg2.pool.ThreadedConnectionPool(
                minconn=self.conf["pool_min_size"],
                maxconn=self.conf["pool_max_size"],
                **conn_params
            )
            
            # Get an initial connection from the pool
            self._get_connection_from_pool()
            
            logger.debug(f"Created PostgreSQL connection pool (min={self.conf['pool_min_size']}, max={self.conf['pool_max_size']})")
            
        except Exception as e:
            logger.error(f"Failed to set up connection pool: {e}")
            # Fall back to a direct connection
            self.pool = None
            self.conn = self.psycopg2.connect(**conn_params)
            self.cur = self.conn.cursor(cursor_factory=self.psycopg2.extras.DictCursor)
    
    def _get_connection_from_pool(self) -> None:
        """Get a connection from the pool."""
        try:
            # Return the current connection to the pool if it exists
            if self.conn and self.pool:
                try:
                    if self.cur:
                        self.cur.close()
                    
                    self.pool.putconn(self.conn)
                except:
                    pass  # Ignore errors when returning a connection
            
            # Get a new connection from the pool
            if self.pool:
                self.conn = self.pool.getconn()
                self.cur = self.conn.cursor(cursor_factory=self.psycopg2.extras.DictCursor)
                logger.debug("Got connection from PostgreSQL pool")
            
        except Exception as e:
            logger.error(f"Failed to get connection from pool: {e}")
            raise ConnectionError(f"Failed to get connection from PostgreSQL pool: {e}") from e
    
    def disconnect(self) -> None:
        """Close the database connection."""
        # Close the current cursor
        if self.cur:
            try:
                self.cur.close()
            except Exception as e:
                logger.warning(f"Error closing cursor: {e}")
            self.cur = None
        
        # Close the current connection
        if self.conn:
            try:
                if self.pool:
                    self.pool.putconn(self.conn)
                else:
                    self.conn.close()
            except Exception as e:
                logger.warning(f"Error closing connection: {e}")
            self.conn = None
        
        # Close the connection pool
        if self.pool:
            try:
                self.pool.closeall()
            except Exception as e:
                logger.warning(f"Error closing connection pool: {e}")
            self.pool = None
        
        logger.debug("PostgreSQL connection(s) closed")
    
    def is_connected(self) -> bool:
        """Check if the database connection is active."""
        if not self.conn or not self.cur:
            return False
        
        try:
            # Check the connection status
            if self.conn.closed:
                return False
            
            # Test the connection with a simple query
            self.cur.execute("SELECT 1")
            return True
        except Exception:
            return False
    
    def execute(self, query: str, params: Optional[Union[List, Tuple, Dict]] = None) -> Any:
        """
        Execute a raw query with parameters.
        
        Args:
            query: SQL query or command string
            params: Parameters for the query
            
        Returns:
            Cursor object
        
        Raises:
            QueryError: If the query execution fails
        """
        # Ensure we have a connection
        if not self.is_connected():
            if self.pool:
                self._get_connection_from_pool()
            else:
                self.connect()
        
        self._last_query = query
        
        # Execute with retry logic
        max_retries = 3
        retry_count = 0
        retry_delay = 0.5  # Initial delay in seconds
        
        while True:
            try:
                if params:
                    self.cur.execute(query, params)
                else:
                    self.cur.execute(query)
                return self.cur
            except Exception as e:
                retry_count += 1
                
                # Check for connection-related errors
                conn_lost = False
                
                # Common PostgreSQL connection error conditions
                if isinstance(e, self.psycopg2.OperationalError) or \
                   isinstance(e, self.psycopg2.InterfaceError) or \
                   "connection" in str(e).lower():
                    conn_lost = True
                
                if conn_lost and retry_count < max_retries:
                    logger.warning(f"Database connection lost. Retrying {retry_count}/{max_retries}...")
                    
                    # Exponential backoff
                    time.sleep(retry_delay)
                    retry_delay *= 2
                    
                    # Reconnect
                    try:
                        if self.pool:
                            self._get_connection_from_pool()
                        else:
                            self.connect()
                    except Exception as conn_error:
                        logger.error(f"Reconnection failed: {conn_error}")
                else:
                    # Log the error
                    logger.error(f"Query execution failed: {e}")
                    logger.debug(f"Query: {query}")
                    if params:
                        logger.debug(f"Parameters: {params}")
                    
                    # Raise a QueryError
                    raise QueryError(f"Query execution failed: {e}") from e
    
    def fetch_one(self) -> Optional[Dict[str, Any]]:
        """Fetch a single row from the result set."""
        if not self.cur:
            return None
            
        try:
            row = self.cur.fetchone()
            if not row:
                return None
                
            # Convert to dictionary (DictCursor should already do this)
            if hasattr(row, 'keys'):
                # If it's already dict-like
                return dict(row)
            else:
                # If it's a regular tuple
                columns = [desc[0] for desc in self.cur.description]
                return dict(zip(columns, row))
        except Exception as e:
            raise QueryError(f"Failed to fetch row: {e}") from e
    
    def fetch_all(self) -> List[Dict[str, Any]]:
        """Fetch all rows from the result set."""
        if not self.cur:
            return []
            
        try:
            rows = self.cur.fetchall()
            if not rows:
                return []
                
            # Convert to list of dictionaries
            result = []
            if hasattr(rows[0], 'keys'):
                # If rows are already dict-like
                for row in rows:
                    result.append(dict(row))
            else:
                # If rows are regular tuples
                columns = [desc[0] for desc in self.cur.description]
                for row in rows:
                    result.append(dict(zip(columns, row)))
            
            return result
        except Exception as e:
            raise QueryError(f"Failed to fetch rows: {e}") from e
    
    def fetch_iter(self, batch_size: int = 1000) -> Iterator[Dict[str, Any]]:
        """Return an iterator for fetching rows in batches."""
        if not self.cur:
            return iter(())  # Empty iterator
            
        try:
            while True:
                rows = self.cur.fetchmany(batch_size)
                if not rows:
                    break
                    
                # Convert to dictionaries
                if hasattr(rows[0], 'keys'):
                    # If rows are already dict-like
                    for row in rows:
                        yield dict(row)
                else:
                    # If rows are regular tuples
                    columns = [desc[0] for desc in self.cur.description]
                    for row in rows:
                        yield dict(zip(columns, row))
        except Exception as e:
            raise QueryError(f"Failed to fetch rows iteratively: {e}") from e
    
    def last_insert_id(self) -> Optional[int]:
        """
        Get the ID of the last inserted row.
        
        Note: This only works if the query included a RETURNING clause.
        """
        if not self.cur or self.cur.rowcount <= 0:
            return None
        
        try:
            # Try to get the ID from the RETURNING clause
            if self.cur.description and self.cur.description[0][0] == 'id':
                row = self.cur.fetchone()
                if row and 'id' in row:
                    return cast(int, row['id'])
            
            return None
        except Exception as e:
            logger.warning(f"Failed to get last insert ID: {e}")
            return None
    
    def last_query(self) -> str:
        """Get the last executed query."""
        return self._last_query
    
    def commit(self) -> None:
        """Commit the current transaction."""
        if not self.conn:
            return
        
        try:
            self.conn.commit()
        except Exception as e:
            raise QueryError(f"Failed to commit transaction: {e}") from e
    
    def rollback(self) -> None:
        """Rollback the current transaction."""
        if not self.conn:
            return
        
        try:
            self.conn.rollback()
        except Exception as e:
            raise QueryError(f"Failed to rollback transaction: {e}") from e
    
    def insert(self, table: str, data: Dict[str, Any]) -> int:
        """
        Insert a single record into the database.
        
        Args:
            table: Table name
            data: Dictionary of field:value pairs
            
        Returns:
            Number of affected rows
        """
        # Build the query
        query, params = self._build_insert_query(table, data)
        
        # Execute the query
        self.execute(query, params)
        
        # Return the number of affected rows
        return self.cur.rowcount
    
    def insert_returning_id(self, table: str, data: Dict[str, Any], id_column: str = 'id') -> Optional[int]:
        """
        Insert a record and return the generated ID.
        
        Args:
            table: Table name
            data: Dictionary of field:value pairs
            id_column: Name of the ID column
            
        Returns:
            Generated ID or None
        """
        # Sanitize identifiers
        safe_table = sanitize_identifier(table)
        safe_id_column = sanitize_identifier(id_column)
        
        # Build the query with RETURNING clause
        query_parts, params = self._build_insert_query(table, data, include_returning=False)
        query = f"{query_parts} RETURNING {safe_id_column}"
        
        # Execute the query
        self.execute(query, params)
        
        # Get the ID from the result
        row = self.fetch_one()
        if row and id_column in row:
            return cast(int, row[id_column])
        
        return None
    
    def insert_batch(self, table: str, data: List[Dict[str, Any]]) -> int:
        """
        Insert multiple records into the database.
        
        Args:
            table: Table name
            data: List of dictionaries with field:value pairs
            
        Returns:
            Number of affected rows
        """
        if not data:
            return 0
        
        # For PostgreSQL, executemany doesn't return affected rows correctly
        # So we'll use a single query with multiple value sets
        
        # Ensure all dictionaries have the same keys
        first_keys = set(data[0].keys())
        for i, item in enumerate(data[1:], 1):
            if set(item.keys()) != first_keys:
                raise ValueError(f"Inconsistent keys in batch insert data at index {i}")
        
        # Build the query
        query, params = self._build_batch_insert_query(table, data)
        
        # Execute the query
        self.execute(query, params)
        
        # Return the number of affected rows
        return self.cur.rowcount
    
    def update(self, table: str, data: Dict[str, Any], where: Optional[Union[str, Tuple, Dict]] = None) -> int:
        """
        Update records in the database.
        
        Args:
            table: Table name
            data: Dictionary of field:value pairs to update
            where: Where condition
            
        Returns:
            Number of affected rows
        """
        # Build the query
        query, params = self._build_update_query(table, data, where)
        
        # Execute the query
        self.execute(query, params)
        
        # Return the number of affected rows
        return self.cur.rowcount
    
    def delete(self, table: str, where: Optional[Union[str, Tuple, Dict]] = None) -> int:
        """
        Delete records from the database.
        
        Args:
            table: Table name
            where: Where condition
            
        Returns:
            Number of affected rows
        """
        # Build the query
        query, params = self._build_delete_query(table, where)
        
        # Execute the query
        self.execute(query, params)
        
        # Return the number of affected rows
        return self.cur.rowcount
    
    def _build_select_query(self, table: str, fields: Union[str, List[str]] = '*', 
                           where: Optional[Union[str, Tuple, Dict]] = None, 
                           order: Optional[Union[str, List]] = None, 
                           limit: Optional[Union[int, Tuple[int, int]]] = None) -> Tuple[str, List]:
        """
        Build a SELECT query.
        
        Args:
            table: Table name
            fields: Field(s) to select
            where: Where condition
            order: Order by clause
            limit: Limit clause
            
        Returns:
            Tuple of (query_string, parameters)
        """
        # Sanitize table name
        safe_table = sanitize_identifier(table)
        
        # Process fields
        if isinstance(fields, str):
            if fields == '*':
                fields_str = '*'
            else:
                fields_str = sanitize_identifier(fields)
        else:
            fields_str = ', '.join(sanitize_identifier(f) for f in fields)
        
        # Start building the query
        query = f"SELECT {fields_str} FROM {safe_table}"
        
        # Process parameters
        params = []
        
        # Process WHERE clause
        if where:
            if isinstance(where, str):
                query += f" WHERE {where}"
            elif isinstance(where, (list, tuple)) and len(where) >= 1:
                query += f" WHERE {where[0]}"
                if len(where) >= 2 and where[1]:
                    params.extend(where[1] if isinstance(where[1], (list, tuple)) else [where[1]])
            elif isinstance(where, dict):
                conditions = []
                for key, value in where.items():
                    conditions.append(f"{sanitize_identifier(key)} = %s")
                    params.append(value)
                query += f" WHERE {' AND '.join(conditions)}"
        
        # Process ORDER BY clause
        if order:
            if isinstance(order, str):
                query += f" ORDER BY {order}"
            elif isinstance(order, (list, tuple)):
                if len(order) >= 1:
                    query += f" ORDER BY {order[0]}"
                    if len(order) >= 2:
                        direction = order[1].upper()
                        if direction in ('ASC', 'DESC'):
                            query += f" {direction}"
        
        # Process LIMIT clause
        if limit is not None:
            if isinstance(limit, int):
                query += f" LIMIT {limit}"
            elif isinstance(limit, (list, tuple)):
                if len(limit) == 1:
                    query += f" LIMIT {limit[0]}"
                elif len(limit) >= 2:
                    query += f" LIMIT {limit[1]} OFFSET {limit[0]}"
        
        return query, params
    
    def _build_insert_query(self, table: str, data: Dict[str, Any], include_returning: bool = True) -> Tuple[str, List]:
        """
        Build an INSERT query.
        
        Args:
            table: Table name
            data: Dictionary of field:value pairs
            include_returning: Whether to include a RETURNING clause
            
        Returns:
            Tuple of (query_string, parameters)
        """
        if not data:
            raise ValueError("No data provided for insert")
        
        # Sanitize table name
        safe_table = sanitize_identifier(table)
        
        # Process columns and values
        columns = [sanitize_identifier(col) for col in data.keys()]
        placeholders = ', '.join(['%s'] * len(data))
        
        # Build the query
        query = f"INSERT INTO {safe_table} ({', '.join(columns)}) VALUES ({placeholders})"
        
        # Parameters are the values from the data dictionary
        params = list(data.values())
        
        return query, params
    
    def _build_batch_insert_query(self, table: str, data: List[Dict[str, Any]]) -> Tuple[str, List]:
        """
        Build a batch INSERT query.
        
        Args:
            table: Table name
            data: List of dictionaries with field:value pairs
            
        Returns:
            Tuple of (query_string, parameters)
        """
        if not data:
            raise ValueError("No data provided for batch insert")
        
        # Sanitize table name
        safe_table = sanitize_identifier(table)
        
        # Process columns (using the first dictionary as a template)
        columns = [sanitize_identifier(col) for col in data[0].keys()]
        
        # Create placeholders for each row
        all_placeholders = []
        all_params = []
        
        for row in data:
            # Add placeholders for this row
            row_placeholders = []
            for col in data[0].keys():
                row_placeholders.append('%s')
                all_params.append(row[col])
            
            # Add this row's placeholders to all placeholders
            all_placeholders.append(f"({', '.join(row_placeholders)})")
        
        # Build the query
        query = f"INSERT INTO {safe_table} ({', '.join(columns)}) VALUES {', '.join(all_placeholders)}"
        
        return query, all_params
    
    def _build_update_query(self, table: str, data: Dict[str, Any], 
                           where: Optional[Union[str, Tuple, Dict]] = None) -> Tuple[str, List]:
        """
        Build an UPDATE query.
        
        Args:
            table: Table name
            data: Dictionary of field:value pairs
            where: Where condition
            
        Returns:
            Tuple of (query_string, parameters)
        """
        if not data:
            raise ValueError("No data provided for update")
        
        # Sanitize table name
        safe_table = sanitize_identifier(table)
        
        # Process SET clause
        set_clauses = []
        params = []
        
        for key, value in data.items():
            set_clauses.append(f"{sanitize_identifier(key)} = %s")
            params.append(value)
        
        # Start building the query
        query = f"UPDATE {safe_table} SET {', '.join(set_clauses)}"
        
        # Process WHERE clause
        if where:
            if isinstance(where, str):
                query += f" WHERE {where}"
            elif isinstance(where, (list, tuple)) and len(where) >= 1:
                query += f" WHERE {where[0]}"
                if len(where) >= 2 and where[1]:
                    params.extend(where[1] if isinstance(where[1], (list, tuple)) else [where[1]])
            elif isinstance(where, dict):
                conditions = []
                for key, value in where.items():
                    conditions.append(f"{sanitize_identifier(key)} = %s")
                    params.append(value)
                query += f" WHERE {' AND '.join(conditions)}"
        
        return query, params
    
    def _build_delete_query(self, table: str, where: Optional[Union[str, Tuple, Dict]] = None) -> Tuple[str, List]:
        """
        Build a DELETE query.
        
        Args:
            table: Table name
            where: Where condition
            
        Returns:
            Tuple of (query_string, parameters)
        """
        # Sanitize table name
        safe_table = sanitize_identifier(table)
        
        # Start building the query
        query = f"DELETE FROM {safe_table}"
        
        # Process parameters
        params = []
        
        # Process WHERE clause
        if where:
            if isinstance(where, str):
                query += f" WHERE {where}"
            elif isinstance(where, (list, tuple)) and len(where) >= 1:
                query += f" WHERE {where[0]}"
                if len(where) >= 2 and where[1]:
                    params.extend(where[1] if isinstance(where[1], (list, tuple)) else [where[1]])
            elif isinstance(where, dict):
                conditions = []
                for key, value in where.items():
                    conditions.append(f"{sanitize_identifier(key)} = %s")
                    params.append(value)
                query += f" WHERE {' AND '.join(conditions)}"
        
        return query, params
    
    def insert_or_update(self, table: str, data: Dict[str, Any], 
                        conflict_columns: Union[str, List[str]]) -> int:
        """
        Insert a row or update it if it exists using ON CONFLICT.
        
        Args:
            table: Table name
            data: Dictionary of field:value pairs
            conflict_columns: Column(s) to check for conflicts
            
        Returns:
            Number of affected rows
        """
        if not data:
            raise ValueError("No data provided for insert or update")
        
        # Sanitize table name
        safe_table = sanitize_identifier(table)
        
        # Process conflict columns
        if isinstance(conflict_columns, str):
            conflict_columns = [conflict_columns]
        
        safe_conflict_columns = [sanitize_identifier(col) for col in conflict_columns]
        
        # Process columns and values for INSERT part
        columns = [sanitize_identifier(col) for col in data.keys()]
        placeholders = ', '.join(['%s'] * len(data))
        
        # Process SET clause for UPDATE part
        update_clauses = []
        
        for key in data.keys():
            if key not in conflict_columns:  # Skip conflict columns in the update part
                update_clauses.append(f"{sanitize_identifier(key)} = EXCLUDED.{sanitize_identifier(key)}")
        
        # Build the query
        query = f"INSERT INTO {safe_table} ({', '.join(columns)}) VALUES ({placeholders}) ON CONFLICT ({', '.join(safe_conflict_columns)})"
        
        if update_clauses:
            query += f" DO UPDATE SET {', '.join(update_clauses)}"
        else:
            query += " DO NOTHING"
        
        # Parameters are the values from the data dictionary
        params = list(data.values())
        
        # Execute the query
        self.execute(query, params)
        
        # Return the number of affected rows
        return self.cur.rowcount
    
    def create_savepoint(self, name: str) -> None:
        """Create a savepoint within the current transaction."""
        self.execute(f"SAVEPOINT {name}")
    
    def release_savepoint(self, name: str) -> None:
        """Release a savepoint within the current transaction."""
        self.execute(f"RELEASE SAVEPOINT {name}")
    
    def rollback_to_savepoint(self, name: str) -> None:
        """Rollback to a savepoint within the current transaction."""
        self.execute(f"ROLLBACK TO SAVEPOINT {name}")
    
    def table_exists(self, table_name: str, schema: str = 'public') -> bool:
        """
        Check if a table exists in the database.
        
        Args:
            table_name: Table name to check
            schema: Schema name
            
        Returns:
            True if the table exists, False otherwise
        """
        try:
            self.execute(
                "SELECT 1 FROM information_schema.tables WHERE table_schema = %s AND table_name = %s",
                [schema, table_name]
            )
            return self.fetch_one() is not None
        except Exception:
            return False
    
    def get_table_columns(self, table_name: str, schema: str = 'public') -> List[Dict[str, Any]]:
        """
        Get column information for a table.
        
        Args:
            table_name: Table name
            schema: Schema name
            
        Returns:
            List of dictionaries with column information
        """
        try:
            self.execute("""
                SELECT 
                    column_name, data_type, udt_name, is_nullable, 
                    column_default, character_maximum_length
                FROM information_schema.columns
                WHERE table_schema = %s AND table_name = %s
                ORDER BY ordinal_position
            """, [schema, table_name])
            
            return self.fetch_all()
        except Exception as e:
            logger.error(f"Failed to get columns for table {table_name}: {e}")
            return []
    
    def execute_procedure(self, procedure_name: str, params: Optional[List[Any]] = None) -> Any:
        """
        Execute a stored procedure.
        
        Args:
            procedure_name: Name of the procedure
            params: Parameters for the procedure
            
        Returns:
            Cursor with the procedure results
        """
        # Build the procedure call
        if params:
            placeholders = ', '.join(['%s'] * len(params))
            query = f"CALL {procedure_name}({placeholders})"
        else:
            query = f"CALL {procedure_name}()"
        
        # Execute the procedure
        return self.execute(query, params)
    
    def execute_function(self, function_name: str, params: Optional[List[Any]] = None) -> Any:
        """
        Execute a function and return its results.
        
        Args:
            function_name: Name of the function
            params: Parameters for the function
            
        Returns:
            Function result
        """
        # Build the function call
        if params:
            placeholders = ', '.join(['%s'] * len(params))
            query = f"SELECT {function_name}({placeholders})"
        else:
            query = f"SELECT {function_name}()"
        
        # Execute the function
        self.execute(query, params)
        
        # Return the result
        return self.fetch_one()