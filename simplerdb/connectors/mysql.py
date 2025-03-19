"""
MySQL/MariaDB/Percona connector for SimplerDB.
"""

import logging
import re
import time
from typing import Dict, List, Tuple, Union, Any, Optional, Iterator

from ..base import SQLBaseConnector
from ..exceptions import ConnectionError, QueryError, ConfigurationError
from ..utils.sanitization import sanitize_identifier

logger = logging.getLogger(__name__)

class MySQLConnector(SQLBaseConnector):
    """
    Connector for MySQL, MariaDB, and Percona databases.
    """
    
    def __init__(self, **kwargs):
        """
        Initialize the MySQL connector.
        
        Args:
            host (str): Database host
            port (int): Database port
            db (str): Database name
            user (str): Username
            passwd (str): Password
            db_variant (str): 'mysql', 'mariadb', or 'percona'
            charset (str): Character set
            ssl (dict): SSL configuration
            connect_timeout (int): Connection timeout in seconds
            pool_size (int): Connection pool size
            keep_alive (bool): Whether to attempt reconnection on timeout
            autocommit (bool): Whether to autocommit transactions
            **kwargs: Additional connector-specific parameters
        """
        super().__init__(**kwargs)
        
        # Set default configuration
        self.conf["host"] = kwargs.get("host", "localhost")
        self.conf["port"] = kwargs.get("port", 3306)
        self.conf["charset"] = kwargs.get("charset", "utf8mb4")
        self.conf["db_variant"] = kwargs.get("db_variant", "mysql").lower()
        self.conf["keep_alive"] = kwargs.get("keep_alive", False)
        self.conf["autocommit"] = kwargs.get("autocommit", False)
        self.conf["connect_timeout"] = kwargs.get("connect_timeout", 10)
        self.conf["pool_size"] = kwargs.get("pool_size", 0)
        self.conf["ssl"] = kwargs.get("ssl", None)
        
        # Verify required parameters
        required_params = ["db", "user", "passwd"]
        for param in required_params:
            if param not in self.conf:
                raise ConfigurationError(f"Required parameter '{param}' is missing")
        
        # Initialize connection and pool
        self.conn = None
        self.cur = None
        self.pool = None
        self.pool_connections = []
        self._last_query = ""
        
        # Import the appropriate module based on the variant
        self._import_db_module()
        
        # Initialize connection
        self.connect()
    
    def _import_db_module(self):
        """Import the appropriate database module based on db_variant."""
        variant = self.conf["db_variant"]
        
        if variant in ["mysql", "percona"]:
            try:
                import mysql.connector
                self.db_module = mysql.connector
                logger.debug(f"Using mysql.connector for {variant}")
            except ImportError:
                raise ConfigurationError(
                    "Failed to import mysql.connector. "
                    "Install it with: pip install mysql-connector-python"
                )
        elif variant == "mariadb":
            try:
                import mariadb
                self.db_module = mariadb
                logger.debug("Using mariadb connector")
            except ImportError:
                try:
                    # Fall back to MySQL connector
                    import mysql.connector
                    self.db_module = mysql.connector
                    logger.warning(
                        "MariaDB connector not available. Falling back to MySQL connector. "
                        "Install MariaDB connector with: pip install mariadb"
                    )
                except ImportError:
                    raise ConfigurationError(
                        "Failed to import mariadb or mysql.connector. "
                        "Install one with: pip install mariadb or pip install mysql-connector-python"
                    )
        else:
            raise ConfigurationError(f"Unsupported MySQL variant: {variant}")
    
    def connect(self) -> None:
        """Establish a connection to the database."""
        try:
            # Set up connection parameters
            connection_args = {
                'database': self.conf['db'],
                'host': self.conf['host'],
                'port': self.conf['port'],
                'user': self.conf['user'],
                'charset': self.conf['charset'],
                'connection_timeout': self.conf['connect_timeout']
            }
            
            # Password key is different between connectors
            if self.conf["db_variant"] == "mariadb" and hasattr(self.db_module, '__name__') and self.db_module.__name__ == 'mariadb':
                connection_args['password'] = self.conf['passwd']
            else:
                connection_args['passwd'] = self.conf['passwd']
            
            # Add SSL if configured
            if self.conf["ssl"]:
                connection_args['ssl'] = self.conf['ssl']
            
            # Set up connection pooling if configured
            if self.conf["pool_size"] > 0:
                self._setup_connection_pool(connection_args)
            else:
                # Create a direct connection
                self.conn = self.db_module.connect(**connection_args)
                self.cur = self.conn.cursor()
                
                # Handle autocommit
                self._set_autocommit()
            
            logger.debug(f"Connected to {self.conf['db_variant']} database: {self.conf['db']}")
            
        except Exception as e:
            logger.error(f"Database connection failed: {str(e)}")
            raise ConnectionError(f"Failed to connect to {self.conf['db_variant']} database: {str(e)}") from e
    
    def _setup_connection_pool(self, connection_args):
        """Set up a connection pool."""
        # Check if pooling is supported
        if hasattr(self.db_module, 'pooling'):
            try:
                from mysql.connector.pooling import MySQLConnectionPool
                
                pool_args = {
                    'pool_name': 'simplerdb_pool',
                    'pool_size': self.conf["pool_size"],
                    **connection_args
                }
                
                self.pool = MySQLConnectionPool(**pool_args)
                # Get an initial connection
                self._get_connection_from_pool()
                
            except Exception as e:
                logger.error(f"Failed to set up connection pool: {e}")
                # Fall back to a direct connection
                self.conf["pool_size"] = 0
                self.conn = self.db_module.connect(**connection_args)
                self.cur = self.conn.cursor()
                self._set_autocommit()
        else:
            logger.warning(f"Connection pooling not supported by {self.conf['db_variant']} module. Using single connection.")
            self.conf["pool_size"] = 0
            self.conn = self.db_module.connect(**connection_args)
            self.cur = self.conn.cursor()
            self._set_autocommit()
    
    def _get_connection_from_pool(self):
        """Get a connection from the pool."""
        try:
            if self.pool:
                self.conn = self.pool.get_connection()
                self.cur = self.conn.cursor()
                self._set_autocommit()
                
                # Store for cleanup
                self.pool_connections.append(self.conn)
                logger.debug(f"Got connection from pool (active: {len(self.pool_connections)})")
        except Exception as e:
            logger.error(f"Failed to get connection from pool: {e}")
            raise ConnectionError(f"Failed to get connection from pool: {e}") from e
    
    def _set_autocommit(self):
        """Set autocommit mode on the connection."""
        try:
            if self.conf["db_variant"] == "mariadb" and hasattr(self.db_module, '__name__') and self.db_module.__name__ == 'mariadb':
                self.conn.autocommit = self.conf["autocommit"]
            else:
                if hasattr(self.conn, 'autocommit'):
                    self.conn.autocommit = self.conf["autocommit"]
                else:
                    self.conn.set_autocommit(self.conf["autocommit"])
        except Exception as e:
            logger.warning(f"Failed to set autocommit: {e}")
    
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
                self.conn.close()
            except Exception as e:
                logger.warning(f"Error closing connection: {e}")
            self.conn = None
        
        # Close all pooled connections
        for conn in self.pool_connections:
            try:
                conn.close()
            except Exception as e:
                logger.warning(f"Error closing pooled connection: {e}")
        
        self.pool_connections = []
        
        logger.debug("Database connection(s) closed")
    
    def is_connected(self) -> bool:
        """Check if the database connection is active."""
        if not self.conn:
            return False
        
        try:
            # Different methods to check connection based on the module
            if hasattr(self.conn, 'is_connected'):
                return self.conn.is_connected()
            elif hasattr(self.conn, 'ping'):
                self.conn.ping(reconnect=False)
                return True
            elif hasattr(self.conn, 'open'):
                return self.conn.open
            else:
                # As a last resort, try a simple query
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
        max_retries = 3 if self.conf["keep_alive"] else 1
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
                error_code = getattr(e, 'errno', None)
                if error_code is None and hasattr(e, 'args') and len(e.args) > 0:
                    try:
                        error_code = e.args[0]
                    except (IndexError, TypeError):
                        error_code = None
                
                # Common MySQL/MariaDB error codes for connection issues
                connection_error_codes = [2006, 2013, 2055, 4031]  # Server gone, Lost connection, etc.
                
                if self.conf["keep_alive"] and error_code in connection_error_codes and retry_count < max_retries:
                    logger.warning(f"Database connection lost (Error {error_code}). Retrying {retry_count}/{max_retries}...")
                    
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
    
    def last_insert_id(self) -> Optional[int]:
        """Get the ID of the last inserted row."""
        if not self.cur:
            return None
        
        return self.cur.lastrowid
    
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
                    query += f" LIMIT {limit[0]}, {limit[1]}"
        
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
        row_placeholders = []
        params = []
        
        for row in data:
            # Ensure the row has all required columns
            if set(row.keys()) != set(data[0].keys()):
                raise ValueError("All rows in batch insert must have the same columns")
            
            # Add placeholders for this row
            row_placeholders.append('(' + ', '.join(['%s'] * len(row)) + ')')
            
            # Add parameters for this row (in the same order as columns)
            for col in data[0].keys():
                params.append(row[col])
        
        # Build the query
        query = f"INSERT INTO {safe_table} ({', '.join(columns)}) VALUES {', '.join(row_placeholders)}"
        
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
    
    def insert_or_update(self, table: str, data: Dict[str, Any], keys: Union[str, List[str]]) -> int:
        """
        Insert a row or update it if it exists using ON DUPLICATE KEY UPDATE.
        
        Args:
            table: Table name
            data: Dictionary of field:value pairs
            keys: Primary key field name or list of field names
            
        Returns:
            Number of affected rows
        """
        if not data:
            raise ValueError("No data provided for insert or update")
        
        # Sanitize table name
        safe_table = sanitize_identifier(table)
        
        # Convert single key to list
        if isinstance(keys, str):
            keys = [keys]
        
        # Process columns and values for INSERT part
        columns = [sanitize_identifier(col) for col in data.keys()]
        placeholders = ', '.join(['%s'] * len(data))
        
        # Process SET clause for UPDATE part
        update_clauses = []
        for key in data.keys():
            if key not in keys:  # Skip primary key(s) in the update part
                update_clauses.append(f"{sanitize_identifier(key)} = VALUES({sanitize_identifier(key)})")
        
        # Build the query
        query = f"INSERT INTO {safe_table} ({', '.join(columns)}) VALUES ({placeholders}) ON DUPLICATE KEY UPDATE {', '.join(update_clauses)}"
        
        # Parameters are the values from the data dictionary
        params = list(data.values())
        
        # Execute the query
        self.execute(query, params)
        
        # Return the number of affected rows
        return self.cur.rowcount
    
    def update_batch(self, table: str, data: List[Dict[str, Any]], key_field: str) -> int:
        """
        Update multiple records efficiently using CASE statements.
        
        Args:
            table: Table name
            data: List of dictionaries with field:value pairs
            key_field: Field to use as the key for matching records
            
        Returns:
            Number of affected rows
        """
        if not data:
            return 0
        
        # Ensure all dictionaries have the key field
        for i, item in enumerate(data):
            if key_field not in item:
                raise ValueError(f"Key field '{key_field}' missing in update data at index {i}")
        
        # Sanitize table name and key field
        safe_table = sanitize_identifier(table)
        safe_key_field = sanitize_identifier(key_field)
        
        # Get all keys and collect all fields
        all_keys = []
        all_fields = set()
        
        for item in data:
            all_keys.append(item[key_field])
            all_fields.update(item.keys())
        
        # Remove the key field from fields to update
        fields_to_update = [f for f in all_fields if f != key_field]
        
        if not fields_to_update:
            raise ValueError("No fields to update")
        
        # Build the SQL using CASE statements
        sql_parts = []
        params = []
        
        for field in fields_to_update:
            safe_field = sanitize_identifier(field)
            case_parts = [f"{safe_field} = CASE"]
            
            for item in data:
                if field in item:
                    case_parts.append(f"WHEN {safe_key_field} = %s THEN %s")
                    params.append(item[key_field])
                    params.append(item[field])
            
            case_parts.append(f"ELSE {safe_field} END")
            sql_parts.append(" ".join(case_parts))
        
        # Build the complete SQL statement
        placeholders = ', '.join(['%s'] * len(all_keys))
        query = f"UPDATE {safe_table} SET {', '.join(sql_parts)} WHERE {safe_key_field} IN ({placeholders})"
        params.extend(all_keys)
        
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
    
    def table_exists(self, table_name: str) -> bool:
        """
        Check if a table exists in the database.
        
        Args:
            table_name: Table name to check
            
        Returns:
            True if the table exists, False otherwise
        """
        try:
            self.execute(
                "SELECT 1 FROM information_schema.tables WHERE table_schema = %s AND table_name = %s",
                [self.conf['db'], table_name]
            )
            return self.fetch_one() is not None
        except Exception:
            return False
    
    def get_table_columns(self, table_name: str) -> List[Dict[str, Any]]:
        """
        Get column information for a table.
        
        Args:
            table_name: Table name
            
        Returns:
            List of dictionaries with column information
        """
        try:
            self.execute("""
                SELECT 
                    column_name, data_type, column_type, is_nullable, 
                    column_default, extra, column_key
                FROM information_schema.columns
                WHERE table_schema = %s AND table_name = %s
                ORDER BY ordinal_position
            """, [self.conf['db'], table_name])
            
            columns = []
            for row in self.fetch_all():
                columns.append({
                    'name': row['column_name'],
                    'type': row['data_type'],
                    'column_type': row['column_type'],
                    'nullable': row['is_nullable'] == 'YES',
                    'default': row['column_default'],
                    'extra': row['extra'],
                    'key': row['column_key']
                })
            return columns
        except Exception as e:
            logger.error(f"Failed to get columns for table {table_name}: {e}")
            return []