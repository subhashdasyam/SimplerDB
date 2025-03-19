"""
SQLite connector for SimplerDB.
"""

import logging
import os
import sqlite3
import time
from typing import Dict, List, Tuple, Union, Any, Optional, Iterator

from ..base import SQLBaseConnector
from ..exceptions import ConnectionError, QueryError, ConfigurationError
from ..utils.sanitization import sanitize_identifier

logger = logging.getLogger(__name__)

class SQLiteConnector(SQLBaseConnector):
    """
    Connector for SQLite databases.
    """
    
    def __init__(self, **kwargs):
        """
        Initialize the SQLite connector.
        
        Args:
            database (str): Path to the SQLite database file
            isolation_level (str): Transaction isolation level
            detect_types (int): How to detect types (sqlite3 constant)
            timeout (float): Timeout for acquiring a lock
            check_same_thread (bool): Allow access from multiple threads
            cached_statements (int): Number of statements to cache
            uri (bool): Treat the database parameter as a URI
            **kwargs: Additional connector-specific parameters
        """
        super().__init__(**kwargs)
        
        # Required parameters
        if 'database' not in kwargs:
            raise ConfigurationError("Required parameter 'database' is missing")
        
        self.conf['database'] = kwargs['database']
        self.conf['isolation_level'] = kwargs.get('isolation_level', None)
        self.conf['detect_types'] = kwargs.get('detect_types', sqlite3.PARSE_DECLTYPES | sqlite3.PARSE_COLNAMES)
        self.conf['timeout'] = kwargs.get('timeout', 5.0)
        self.conf['check_same_thread'] = kwargs.get('check_same_thread', True)
        self.conf['cached_statements'] = kwargs.get('cached_statements', 100)
        self.conf['uri'] = kwargs.get('uri', False)
        
        # Initialize connection
        self.conn = None
        self.cur = None
        self._last_query = ""
        self._last_id = None
        
        # Connect to the database
        self.connect()
    
    def connect(self) -> None:
        """Establish a connection to the database."""
        try:
            # Ensure the directory exists
            db_dir = os.path.dirname(self.conf['database'])
            if db_dir and not os.path.exists(db_dir):
                os.makedirs(db_dir)
                
            # Create the connection
            self.conn = sqlite3.connect(
                database=self.conf['database'],
                isolation_level=self.conf['isolation_level'],
                detect_types=self.conf['detect_types'],
                timeout=self.conf['timeout'],
                check_same_thread=self.conf['check_same_thread'],
                cached_statements=self.conf['cached_statements'],
                uri=self.conf['uri']
            )
            
            # Enable foreign keys
            self.conn.execute("PRAGMA foreign_keys = ON")
            
            # Row factory for dict-like access
            self.conn.row_factory = sqlite3.Row
            
            # Create a cursor
            self.cur = self.conn.cursor()
            
            logger.debug(f"Connected to SQLite database: {self.conf['database']}")
            
        except Exception as e:
            logger.error(f"SQLite connection failed: {str(e)}")
            raise ConnectionError(f"Failed to connect to SQLite database: {str(e)}") from e
    
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
            
        logger.debug("SQLite connection closed")
    
    def is_connected(self) -> bool:
        """Check if the database connection is active."""
        if not self.conn:
            return False
        
        try:
            # Test the connection with a simple query
            self.conn.execute("SELECT 1").fetchone()
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
        
        # Execute with retry logic for locked database
        max_retries = 3
        retry_count = 0
        retry_delay = 0.1  # Initial delay in seconds
        
        while True:
            try:
                if params:
                    self.cur.execute(query, params)
                else:
                    self.cur.execute(query)
                return self.cur
            except sqlite3.OperationalError as e:
                # Check if it's a database locked error
                if "database is locked" in str(e) and retry_count < max_retries:
                    retry_count += 1
                    logger.warning(f"Database locked. Retrying {retry_count}/{max_retries}...")
                    
                    # Exponential backoff
                    time.sleep(retry_delay)
                    retry_delay *= 2
                else:
                    # Log the error
                    logger.error(f"Query execution failed: {e}")
                    logger.debug(f"Query: {query}")
                    if params:
                        logger.debug(f"Parameters: {params}")
                    
                    # Raise a QueryError
                    raise QueryError(f"Query execution failed: {e}") from e
            except Exception as e:
                # Log the error
                logger.error(f"Query execution failed: {e}")
                logger.debug(f"Query: {query}")
                if params:
                    logger.debug(f"Parameters: {params}")
                
                # Raise a QueryError
                raise QueryError(f"Query execution failed: {e}") from e
    
    def executemany(self, query: str, params_list: List[Union[List, Tuple, Dict]]) -> Any:
        """
        Execute a query multiple times with different parameters.
        
        Args:
            query: SQL query or command string
            params_list: List of parameter sets
            
        Returns:
            Cursor object
        
        Raises:
            QueryError: If the query execution fails
        """
        # Ensure we have a connection
        if not self.is_connected():
            self.connect()
        
        self._last_query = query
        
        try:
            self.cur.executemany(query, params_list)
            return self.cur
        except Exception as e:
            # Log the error
            logger.error(f"Query execution failed: {e}")
            logger.debug(f"Query: {query}")
            logger.debug(f"Parameter sets: {len(params_list)}")
            
            # Raise a QueryError
            raise QueryError(f"Query execution failed: {e}") from e
    
    def executescript(self, sql_script: str) -> Any:
        """
        Execute a SQL script.
        
        Args:
            sql_script: SQL script to execute
            
        Returns:
            Cursor object
        
        Raises:
            QueryError: If the script execution fails
        """
        # Ensure we have a connection
        if not self.is_connected():
            self.connect()
        
        self._last_query = sql_script
        
        try:
            self.cur.executescript(sql_script)
            return self.cur
        except Exception as e:
            # Log the error
            logger.error(f"Script execution failed: {e}")
            
            # Raise a QueryError
            raise QueryError(f"Script execution failed: {e}") from e
    
    def last_insert_id(self) -> Optional[int]:
        """Get the ID of the last inserted row."""
        if not self.conn:
            return None
        
        return self.conn.execute("SELECT last_insert_rowid()").fetchone()[0]
    
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
        
        # Build the query for a single row
        column_names = [sanitize_identifier(key) for key in data[0].keys()]
        placeholders = ', '.join(['?'] * len(data[0]))
        query = f"INSERT INTO {sanitize_identifier(table)} ({', '.join(column_names)}) VALUES ({placeholders})"
        
        # Prepare parameter sets
        param_sets = []
        for row in data:
            # Ensure the values are in the same order as the columns
            param_sets.append([row[key] for key in data[0].keys()])
        
        # Execute the query with executemany
        self.executemany(query, param_sets)
        
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
                    conditions.append(f"{sanitize_identifier(key)} = ?")
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
        safe_table = sanitize_identifier(table)
        
        # Process SET clause
        set_clauses = []
        params = []
        
        for key, value in data.items():
            set_clauses.append(f"{sanitize_identifier(key)} = ?")
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
                    conditions.append(f"{sanitize_identifier(key)} = ?")
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
                    conditions.append(f"{sanitize_identifier(key)} = ?")
                    params.append(value)
                query += f" WHERE {' AND '.join(conditions)}"
        
        return query, params
    
    def insert_or_replace(self, table: str, data: Dict[str, Any]) -> int:
        """
        Insert a row or replace it if it exists.
        
        Args:
            table: Table name
            data: Dictionary of field:value pairs
            
        Returns:
            Number of affected rows
        """
        if not data:
            raise ValueError("No data provided for insert or replace")
        
        # Sanitize table name
        safe_table = sanitize_identifier(table)
        
        # Process columns and values
        columns = [sanitize_identifier(col) for col in data.keys()]
        placeholders = ', '.join(['?'] * len(data))
        
        # Build the query
        query = f"INSERT OR REPLACE INTO {safe_table} ({', '.join(columns)}) VALUES ({placeholders})"
        
        # Parameters are the values from the data dictionary
        params = list(data.values())
        
        # Execute the query
        self.execute(query, params)
        
        # Return the number of affected rows
        return self.cur.rowcount
    
    def begin_transaction(self) -> None:
        """Begin a transaction."""
        self.execute("BEGIN TRANSACTION")
    
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
                "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?",
                [table_name]
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
            self.execute(f"PRAGMA table_info({sanitize_identifier(table_name)})")
            
            columns = []
            for row in self.fetch_all():
                columns.append({
                    'cid': row['cid'],
                    'name': row['name'],
                    'type': row['type'],
                    'notnull': bool(row['notnull']),
                    'default': row['dflt_value'],
                    'pk': bool(row['pk'])
                })
            return columns
        except Exception as e:
            logger.error(f"Failed to get columns for table {table_name}: {e}")
            return []