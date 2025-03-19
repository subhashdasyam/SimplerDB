"""
Microsoft SQL Server connector for SimplerDB.
"""

import logging
import time
from typing import Dict, List, Tuple, Union, Any, Optional, Iterator, cast

from ..base import SQLBaseConnector
from ..exceptions import ConnectionError, QueryError, ConfigurationError
from ..utils.sanitization import sanitize_identifier

logger = logging.getLogger(__name__)

class MSSQLConnector(SQLBaseConnector):
    """
    Connector for Microsoft SQL Server databases.
    """
    
    def __init__(self, **kwargs):
        """
        Initialize the MSSQL connector.
        
        Args:
            server (str): SQL Server instance name or address
            database (str): Database name (also accepts 'db' alias)
            user (str): Username (also accepts 'uid', 'username')
            password (str): Password (also accepts 'pwd', 'passwd')
            driver (str): ODBC driver name
            port (int): Port number
            trusted_connection (bool): Use Windows authentication
            encrypt (bool): Use encryption for connection
            connection_timeout (int): Connection timeout in seconds
            login_timeout (int): Login timeout in seconds
            app_name (str): Application name
            autocommit (bool): Whether to autocommit transactions
            pool_size (int): Connection pool size (0 for no pooling)
            **kwargs: Additional connector-specific parameters
        """
        super().__init__(**kwargs)
        
        # Normalize configuration parameters
        self.conf["server"] = kwargs.get("server", kwargs.get("host", "localhost"))
        self.conf["database"] = kwargs.get("database", kwargs.get("db", None))
        self.conf["user"] = kwargs.get("user", kwargs.get("uid", kwargs.get("username", None)))
        self.conf["password"] = kwargs.get("password", kwargs.get("pwd", kwargs.get("passwd", None)))
        self.conf["driver"] = kwargs.get("driver", "{ODBC Driver 17 for SQL Server}")
        self.conf["port"] = kwargs.get("port", 1433)
        self.conf["trusted_connection"] = kwargs.get("trusted_connection", False)
        self.conf["encrypt"] = kwargs.get("encrypt", True)
        self.conf["connection_timeout"] = kwargs.get("connection_timeout", 30)
        self.conf["login_timeout"] = kwargs.get("login_timeout", 15)
        self.conf["app_name"] = kwargs.get("app_name", "SimplerDB")
        self.conf["autocommit"] = kwargs.get("autocommit", False)
        self.conf["pool_size"] = kwargs.get("pool_size", 0)
        
        # Check for required parameters
        if not self.conf["trusted_connection"]:
            required_params = ["database", "user", "password"]
            for param in required_params:
                if not self.conf[param]:
                    raise ConfigurationError(f"Required parameter '{param}' is missing for MSSQL connection")
        else:
            # For Windows authentication, only database is required
            if not self.conf["database"]:
                raise ConfigurationError("Required parameter 'database' is missing for MSSQL connection")
        
        # Initialize connection and state
        self.conn = None
        self.cur = None
        self._last_query = ""
        
        try:
            import pyodbc
            self.pyodbc = pyodbc
        except ImportError:
            raise ConfigurationError(
                "pyodbc is required for MSSQL connections. "
                "Install it with: pip install pyodbc"
            )
        
        # Connect to the database
        self.connect()
    
    def connect(self) -> None:
        """Establish a connection to the database."""
        try:
            # Build connection string
            conn_str_parts = []
            
            # Add server and port
            conn_str_parts.append(f"SERVER={self.conf['server']},{self.conf['port']}")
            
            # Add database
            conn_str_parts.append(f"DATABASE={self.conf['database']}")
            
            # Add authentication details
            if self.conf["trusted_connection"]:
                # Windows authentication
                conn_str_parts.append("Trusted_Connection=yes")
            else:
                # SQL Server authentication
                conn_str_parts.append(f"UID={self.conf['user']}")
                conn_str_parts.append(f"PWD={self.conf['password']}")
            
            # Add driver
            conn_str_parts.append(f"DRIVER={self.conf['driver']}")
            
            # Add additional options
            conn_str_parts.append(f"APP={self.conf['app_name']}")
            conn_str_parts.append(f"Connection Timeout={self.conf['connection_timeout']}")
            conn_str_parts.append(f"Login Timeout={self.conf['login_timeout']}")
            
            # Add encryption option
            if self.conf["encrypt"]:
                conn_str_parts.append("Encrypt=yes")
            else:
                conn_str_parts.append("Encrypt=no")
            
            # Build the final connection string
            conn_str = ";".join(conn_str_parts)
            
            # Create the connection
            self.conn = self.pyodbc.connect(conn_str, autocommit=self.conf["autocommit"])
            
            # Create a cursor
            self.cur = self.conn.cursor()
            
            logger.debug(f"Connected to MSSQL database: {self.conf['database']} at {self.conf['server']}")
            
        except Exception as e:
            logger.error(f"MSSQL connection failed: {str(e)}")
            raise ConnectionError(f"Failed to connect to MSSQL database: {str(e)}") from e
    
    def disconnect(self) -> None:
        """Close the database connection."""
        if self.cur:
            try:
                self.cur.close()
            except Exception as e:
                logger.warning(f"Error closing cursor: {e}")
            self.cur = None
        
        if self.conn:
            try:
                self.conn.close()
            except Exception as e:
                logger.warning(f"Error closing connection: {e}")
            self.conn = None
            
        logger.debug("MSSQL connection closed")
    
    def is_connected(self) -> bool:
        """Check if the database connection is active."""
        if not self.conn or not self.cur:
            return False
        
        try:
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
                
                # Common MSSQL connection error conditions
                conn_lost = False
                error_str = str(e).lower()
                
                if "connection" in error_str or "network" in error_str or "timeout" in error_str:
                    conn_lost = True
                
                if conn_lost and retry_count < max_retries:
                    logger.warning(f"Database connection lost. Retrying {retry_count}/{max_retries}...")
                    
                    # Exponential backoff
                    time.sleep(retry_delay)
                    retry_delay *= 2
                    
                    # Reconnect
                    try:
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
                
            # Convert to dictionary
            columns = [column[0] for column in self.cur.description]
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
            columns = [column[0] for column in self.cur.description]
            return [dict(zip(columns, row)) for row in rows]
        except Exception as e:
            raise QueryError(f"Failed to fetch rows: {e}") from e
    
    def fetch_iter(self, batch_size: int = 1000) -> Iterator[Dict[str, Any]]:
        """Return an iterator for fetching rows in batches."""
        if not self.cur:
            return iter(())  # Empty iterator
            
        try:
            columns = [column[0] for column in self.cur.description]
            
            while True:
                rows = self.cur.fetchmany(batch_size)
                if not rows:
                    break
                    
                for row in rows:
                    yield dict(zip(columns, row))
        except Exception as e:
            raise QueryError(f"Failed to fetch rows iteratively: {e}") from e
    
    def last_insert_id(self) -> Optional[int]:
        """Get the ID of the last inserted row."""
        try:
            # Use SCOPE_IDENTITY() to get the last inserted ID
            self.execute("SELECT SCOPE_IDENTITY() AS id")
            row = self.fetch_one()
            if row and 'id' in row and row['id'] is not None:
                return int(row['id'])
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
        # Insert the record
        self.insert(table, data)
        
        # Get the last insert ID
        return self.last_insert_id()
    
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
        
        # Ensure all dictionaries have the same keys
        first_keys = set(data[0].keys())
        for i, item in enumerate(data[1:], 1):
            if set(item.keys()) != first_keys:
                raise ValueError(f"Inconsistent keys in batch insert data at index {i}")
        
        # In SQL Server, there are several ways to do batch inserts
        # 1. For a small number of records, we can use separate INSERT statements
        # 2. For larger batches, we can use a table valued parameter or bulk insert
        # For simplicity, we'll use separate inserts for now
        
        total_affected = 0
        for item in data:
            query, params = self._build_insert_query(table, item)
            self.execute(query, params)
            total_affected += self.cur.rowcount
        
        return total_affected
    
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
        safe_table = self._sanitize_ms_identifier(table)
        
        # Process fields
        if isinstance(fields, str):
            if fields == '*':
                fields_str = '*'
            else:
                fields_str = self._sanitize_ms_identifier(fields)
        else:
            fields_str = ', '.join(self._sanitize_ms_identifier(f) for f in fields)
        
        # Start building the query
        query = f"SELECT "
        
        # Add limit (TOP clause) if specified
        if isinstance(limit, int) and limit > 0:
            query += f"TOP {limit} "
        elif isinstance(limit, (list, tuple)) and len(limit) >= 1 and limit[0] > 0:
            query += f"TOP {limit[0]} "
        
        # Add fields
        query += f"{fields_str} FROM {safe_table}"
        
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
                    conditions.append(f"{self._sanitize_ms_identifier(key)} = ?")
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
        
        # Process OFFSET/FETCH if limit is a tuple with two values
        if isinstance(limit, (list, tuple)) and len(limit) >= 2 and order:
            offset, fetch = limit
            # SQL Server requires ORDER BY for OFFSET/FETCH
            query += f" OFFSET {offset} ROWS FETCH NEXT {fetch} ROWS ONLY"
        
        return query, params
    
    def _build_insert_query(self, table: str, data: Dict[str, Any]) -> Tuple[str, List]:
        """
        Build an INSERT query.
        
        Args:
            table: Table name
            data: Dictionary of field:value pairs
            
        Returns:
            Tuple of (query_string, parameters)
        """
        if not data:
            raise ValueError("No data provided for insert")
        
        # Sanitize table name
        safe_table = self._sanitize_ms_identifier(table)
        
        # Process columns and values
        columns = [self._sanitize_ms_identifier(col) for col in data.keys()]
        placeholders = ', '.join(['?'] * len(data))
        
        # Build the query
        query = f"INSERT INTO {safe_table} ({', '.join(columns)}) VALUES ({placeholders})"
        
        # Parameters are the values from the data dictionary
        params = list(data.values())
        
        return query, params
    
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
        safe_table = self._sanitize_ms_identifier(table)
        
        # Process SET clause
        set_clauses = []
        params = []
        
        for key, value in data.items():
            set_clauses.append(f"{self._sanitize_ms_identifier(key)} = ?")
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
                    conditions.append(f"{self._sanitize_ms_identifier(key)} = ?")
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
        safe_table = self._sanitize_ms_identifier(table)
        
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
                    conditions.append(f"{self._sanitize_ms_identifier(key)} = ?")
                    params.append(value)
                query += f" WHERE {' AND '.join(conditions)}"
        
        return query, params
    
    def _sanitize_ms_identifier(self, identifier: str) -> str:
        """
        Sanitize a SQL Server identifier (table or column name).
        
        Microsoft SQL Server uses square brackets for quoting identifiers.
        
        Args:
            identifier: The identifier to sanitize
            
        Returns:
            Sanitized identifier
        """
        # Check if identifier is already quoted
        if identifier.startswith('[') and identifier.endswith(']'):
            return identifier
        
        # For identifiers with dot notation (e.g., schema.table)
        if '.' in identifier:
            parts = identifier.split('.')
            return '.'.join([self._sanitize_ms_identifier(part) for part in parts])
        
        # Special case for star
        if identifier == '*':
            return '*'
        
        # Quote the identifier
        return f"[{identifier}]"
    
    def insert_or_update(self, table: str, data: Dict[str, Any], 
                        key_columns: Union[str, List[str]]) -> int:
        """
        Insert a row or update it if it exists (UPSERT operation).
        
        In SQL Server, this is done with MERGE statement.
        
        Args:
            table: Table name
            data: Dictionary of field:value pairs
            key_columns: Primary key column(s)
            
        Returns:
            Number of affected rows
        """
        if not data:
            raise ValueError("No data provided for insert or update")
        
        # Convert key_columns to list if it's a string
        if isinstance(key_columns, str):
            key_columns = [key_columns]
        
        # Ensure all key columns are in the data
        for key in key_columns:
            if key not in data:
                raise ValueError(f"Key column '{key}' not found in data")
        
        # Sanitize table name
        safe_table = self._sanitize_ms_identifier(table)
        
        # Build column lists
        all_columns = list(data.keys())
        non_key_columns = [col for col in all_columns if col not in key_columns]
        
        # If there are no non-key columns, it's just an insert
        if not non_key_columns:
            return self.insert(table, data)
        
        # Build the MERGE statement
        query = f"""
        MERGE INTO {safe_table} AS target
        USING (SELECT {', '.join(['?' + ' AS ' + self._sanitize_ms_identifier(col) for col in all_columns])}) AS source
        ON ({' AND '.join([f'target.{self._sanitize_ms_identifier(col)} = source.{self._sanitize_ms_identifier(col)}' for col in key_columns])})
        WHEN MATCHED THEN
            UPDATE SET {', '.join([f'target.{self._sanitize_ms_identifier(col)} = source.{self._sanitize_ms_identifier(col)}' for col in non_key_columns])}
        WHEN NOT MATCHED THEN
            INSERT ({', '.join([self._sanitize_ms_identifier(col) for col in all_columns])})
            VALUES ({', '.join([f'source.{self._sanitize_ms_identifier(col)}' for col in all_columns])});
        """
        
        # Parameters are the values from the data dictionary
        params = list(data.values())
        
        # Execute the query
        self.execute(query, params)
        
        # Return the number of affected rows
        return self.cur.rowcount
    
    def table_exists(self, table_name: str, schema: str = 'dbo') -> bool:
        """
        Check if a table exists in the database.
        
        Args:
            table_name: Table name to check
            schema: Schema name
            
        Returns:
            True if the table exists, False otherwise
        """
        try:
            self.execute("""
                SELECT 1 
                FROM INFORMATION_SCHEMA.TABLES 
                WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?
            """, [schema, table_name])
            return self.fetch_one() is not None
        except Exception:
            return False
    
    def get_table_columns(self, table_name: str, schema: str = 'dbo') -> List[Dict[str, Any]]:
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
                    c.COLUMN_NAME, c.DATA_TYPE, c.CHARACTER_MAXIMUM_LENGTH,
                    c.NUMERIC_PRECISION, c.NUMERIC_SCALE, c.IS_NULLABLE,
                    c.COLUMN_DEFAULT, 
                    CASE WHEN pk.COLUMN_NAME IS NOT NULL THEN 'YES' ELSE 'NO' END AS IS_PRIMARY_KEY
                FROM INFORMATION_SCHEMA.COLUMNS c
                LEFT JOIN (
                    SELECT ku.TABLE_CATALOG, ku.TABLE_SCHEMA, ku.TABLE_NAME, ku.COLUMN_NAME
                    FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS AS tc
                    JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE AS ku
                    ON tc.CONSTRAINT_TYPE = 'PRIMARY KEY' 
                    AND tc.CONSTRAINT_NAME = ku.CONSTRAINT_NAME
                ) pk
                ON c.TABLE_CATALOG = pk.TABLE_CATALOG
                AND c.TABLE_SCHEMA = pk.TABLE_SCHEMA
                AND c.TABLE_NAME = pk.TABLE_NAME
                AND c.COLUMN_NAME = pk.COLUMN_NAME
                WHERE c.TABLE_SCHEMA = ? AND c.TABLE_NAME = ?
                ORDER BY c.ORDINAL_POSITION
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
            placeholders = ', '.join(['?'] * len(params))
            query = f"EXEC {procedure_name} {placeholders}"
        else:
            query = f"EXEC {procedure_name}"
        
        # Execute the procedure
        return self.execute(query, params)
    
    def create_savepoint(self, name: str) -> None:
        """Create a savepoint within the current transaction."""
        self.execute(f"SAVE TRANSACTION {name}")
    
    def release_savepoint(self, name: str) -> None:
        """
        Release a savepoint within the current transaction.
        
        Note: SQL Server doesn't have an explicit RELEASE SAVEPOINT command.
        Savepoints are automatically released at the end of the transaction.
        """
        # No-op for SQL Server
        pass
    
    def rollback_to_savepoint(self, name: str) -> None:
        """Rollback to a savepoint within the current transaction."""
        self.execute(f"ROLLBACK TRANSACTION {name}")