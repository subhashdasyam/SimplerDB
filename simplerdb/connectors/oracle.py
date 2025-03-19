"""
Oracle database connector for SimplerDB.
"""

import logging
import time
import re
from typing import Dict, List, Tuple, Union, Any, Optional, Iterator, cast

from ..base import SQLBaseConnector
from ..exceptions import ConnectionError, QueryError, ConfigurationError
from ..utils.sanitization import sanitize_identifier

logger = logging.getLogger(__name__)

class OracleConnector(SQLBaseConnector):
    """
    Connector for Oracle databases.
    """
    
    def __init__(self, **kwargs):
        """
        Initialize the Oracle connector.
        
        Args:
            dsn (str): Data Source Name (e.g., 'localhost:1521/orcl')
            user (str): Username
            password (str): Password
            service_name (str): Service name
            sid (str): SID
            host (str): Hostname
            port (int): Port number
            encoding (str): Character encoding
            nencoding (str): National character encoding
            mode (int): Session mode
            events (bool): Whether to enable subscription to events
            threaded (bool): Whether to use threaded mode
            min (int): Minimum connections in the pool
            max (int): Maximum connections in the pool
            increment (int): Connection pool increment
            autocommit (bool): Whether to autocommit transactions
            **kwargs: Additional connector-specific parameters
        """
        super().__init__(**kwargs)
        
        # Set default configuration
        self.conf["user"] = kwargs.get("user", kwargs.get("username", None))
        self.conf["password"] = kwargs.get("password", kwargs.get("passwd", None))
        self.conf["dsn"] = kwargs.get("dsn", None)
        self.conf["service_name"] = kwargs.get("service_name", None)
        self.conf["sid"] = kwargs.get("sid", None)
        self.conf["host"] = kwargs.get("host", "localhost")
        self.conf["port"] = kwargs.get("port", 1521)
        self.conf["encoding"] = kwargs.get("encoding", "UTF-8")
        self.conf["nencoding"] = kwargs.get("nencoding", "UTF-8")
        self.conf["mode"] = kwargs.get("mode", None)
        self.conf["events"] = kwargs.get("events", False)
        self.conf["threaded"] = kwargs.get("threaded", False)
        self.conf["min"] = kwargs.get("min", 1)
        self.conf["max"] = kwargs.get("max", 2)
        self.conf["increment"] = kwargs.get("increment", 1)
        self.conf["autocommit"] = kwargs.get("autocommit", False)
        
        # Check if we have enough connection information
        if not self.conf["dsn"] and not (self.conf["host"] and (self.conf["service_name"] or self.conf["sid"])):
            raise ConfigurationError("Either 'dsn' or 'host' and ('service_name' or 'sid') must be provided for Oracle connection")
        
        # Check for required parameters
        required_params = ["user", "password"]
        for param in required_params:
            if not self.conf[param]:
                raise ConfigurationError(f"Required parameter '{param}' is missing for Oracle connection")
        
        # Initialize connection and state
        self.conn = None
        self.cur = None
        self._last_query = ""
        self.pool = None
        
        try:
            import cx_Oracle
            self.cx_Oracle = cx_Oracle
        except ImportError:
            raise ConfigurationError(
                "cx_Oracle is required for Oracle connections. "
                "Install it with: pip install cx_Oracle"
            )
        
        # Connect to the database
        self.connect()
    
    def connect(self) -> None:
        """Establish a connection to the database."""
        try:
            # Set NLS_LANG environment variable
            import os
            os.environ["NLS_LANG"] = f".{self.conf['encoding']}"
            
            # Determine DSN
            dsn = self.conf["dsn"]
            if not dsn:
                # Build DSN from host, port, and service_name/sid
                if self.conf["service_name"]:
                    dsn = self.cx_Oracle.makedsn(
                        self.conf["host"],
                        self.conf["port"],
                        service_name=self.conf["service_name"]
                    )
                else:
                    dsn = self.cx_Oracle.makedsn(
                        self.conf["host"],
                        self.conf["port"],
                        sid=self.conf["sid"]
                    )
            
            # Determine if we should use connection pooling
            if self.conf["min"] > 1 or self.conf["max"] > 1:
                # Create a connection pool
                self.pool = self.cx_Oracle.SessionPool(
                    user=self.conf["user"],
                    password=self.conf["password"],
                    dsn=dsn,
                    min=self.conf["min"],
                    max=self.conf["max"],
                    increment=self.conf["increment"],
                    encoding=self.conf["encoding"],
                    nencoding=self.conf["nencoding"],
                    threaded=self.conf["threaded"],
                    events=self.conf["events"]
                )
                
                # Get a connection from the pool
                self.conn = self.pool.acquire()
            else:
                # Create a direct connection
                self.conn = self.cx_Oracle.connect(
                    user=self.conf["user"],
                    password=self.conf["password"],
                    dsn=dsn,
                    encoding=self.conf["encoding"],
                    nencoding=self.conf["nencoding"],
                    mode=self.conf["mode"],
                    threaded=self.conf["threaded"],
                    events=self.conf["events"]
                )
            
            # Set autocommit
            self.conn.autocommit = self.conf["autocommit"]
            
            # Create a cursor
            self.cur = self.conn.cursor()
            
            logger.debug(f"Connected to Oracle database as {self.conf['user']}@{dsn}")
            
        except Exception as e:
            logger.error(f"Oracle connection failed: {str(e)}")
            raise ConnectionError(f"Failed to connect to Oracle database: {str(e)}") from e
    
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
                if self.pool:
                    self.pool.release(self.conn)
                else:
                    self.conn.close()
            except Exception as e:
                logger.warning(f"Error closing connection: {e}")
            self.conn = None
        
        if self.pool:
            try:
                self.pool.close()
            except Exception as e:
                logger.warning(f"Error closing connection pool: {e}")
            self.pool = None
            
        logger.debug("Oracle connection closed")
    
    def is_connected(self) -> bool:
        """Check if the database connection is active."""
        if not self.conn or not self.cur:
            return False
        
        try:
            # Test the connection with a simple query
            self.cur.execute("SELECT 1 FROM DUAL")
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
        
        # Oracle uses named parameters with :param_name syntax
        # Convert ? placeholders to :n style if params is a list or tuple
        if params and not isinstance(params, dict) and '?' in query:
            param_index = 1
            while '?' in query:
                query = query.replace('?', f':{param_index}', 1)
                param_index += 1
            
            # Convert list/tuple to dict
            if isinstance(params, (list, tuple)):
                params_dict = {}
                for i, value in enumerate(params, 1):
                    params_dict[str(i)] = value
                params = params_dict
        
        while True:
            try:
                if params:
                    self.cur.execute(query, params)
                else:
                    self.cur.execute(query)
                return self.cur
            except Exception as e:
                retry_count += 1
                
                # Common Oracle connection error conditions
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
        
        # Oracle uses named parameters with :param_name syntax
        # For executemany, we need consistent parameter format
        # If the first params is a dict, all must be dicts
        # If the first params is a list/tuple, all must be lists/tuples
        
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
    
    def fetch_one(self) -> Optional[Dict[str, Any]]:
        """Fetch a single row from the result set."""
        if not self.cur:
            return None
            
        try:
            row = self.cur.fetchone()
            if not row:
                return None
                
            # Convert to dictionary
            columns = [d[0].lower() for d in self.cur.description]
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
            columns = [d[0].lower() for d in self.cur.description]
            return [dict(zip(columns, row)) for row in rows]
        except Exception as e:
            raise QueryError(f"Failed to fetch rows: {e}") from e
    
    def fetch_iter(self, batch_size: int = 1000) -> Iterator[Dict[str, Any]]:
        """Return an iterator for fetching rows in batches."""
        if not self.cur:
            return iter(())  # Empty iterator
            
        try:
            columns = [d[0].lower() for d in self.cur.description]
            
            while True:
                rows = self.cur.fetchmany(batch_size)
                if not rows:
                    break
                    
                for row in rows:
                    yield dict(zip(columns, row))
        except Exception as e:
            raise QueryError(f"Failed to fetch rows iteratively: {e}") from e
    
    def last_insert_id(self) -> Optional[int]:
        """
        Get the ID of the last inserted row.
        
        For Oracle, this requires using a RETURNING clause in the INSERT statement
        or querying the sequence directly.
        """
        # This is a wrapper to maintain interface consistency
        # But Oracle doesn't support this directly - use insert_returning_id instead
        logger.warning("last_insert_id() is not directly supported in Oracle. Use insert_returning_id() instead.")
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
    
    def insert_returning_id(self, table: str, data: Dict[str, Any], id_column: str = 'id', 
                           sequence_name: Optional[str] = None) -> Optional[int]:
        """
        Insert a record and return the generated ID.
        
        Args:
            table: Table name
            data: Dictionary of field:value pairs
            id_column: Name of the ID column
            sequence_name: Name of the sequence to use (optional)
            
        Returns:
            Generated ID or None
        """
        # Sanitize identifiers
        safe_table = self._sanitize_oracle_identifier(table)
        safe_id_column = self._sanitize_oracle_identifier(id_column)
        
        # If id_column is in data, use its value
        if id_column in data:
            # Regular insert
            self.insert(table, data)
            return data[id_column]
        
        # If sequence_name is provided, use it to get the next value
        if sequence_name:
            safe_sequence = self._sanitize_oracle_identifier(sequence_name)
            
            # Get the next value from the sequence
            self.execute(f"SELECT {safe_sequence}.NEXTVAL FROM DUAL")
            row = self.fetch_one()
            
            if row:
                id_value = list(row.values())[0]
                # Add the ID to the data
                data[id_column] = id_value
                # Insert with the ID
                self.insert(table, data)
                return id_value
        
        # Try to use RETURNING clause
        # Process columns and values
        columns = [self._sanitize_oracle_identifier(col) for col in data.keys()]
        placeholders = []
        bind_params = {}
        
        for i, col in enumerate(data.keys(), 1):
            param_name = f"p{i}"
            placeholders.append(f":{param_name}")
            bind_params[param_name] = data[col]
        
        # Build the query with RETURNING clause
        query = f"""
        INSERT INTO {safe_table} ({', '.join(columns)})
        VALUES ({', '.join(placeholders)})
        RETURNING {safe_id_column} INTO :out_id
        """
        
        # Add output parameter
        out_val = self.cx_Oracle.Number()
        bind_params["out_id"] = out_val
        
        # Execute the query
        self.execute(query, bind_params)
        self.commit()
        
        # Return the generated ID
        return int(out_val.getvalue())
    
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
        
        # In Oracle, there are several ways to do batch inserts
        # 1. Multiple INSERT statements
        # 2. INSERT ALL statement
        # 3. Using executemany with bind variables
        
        # For simplicity and efficiency, we'll use executemany
        
        # Sanitize table name
        safe_table = self._sanitize_oracle_identifier(table)
        
        # Build column and placeholder lists
        columns = [self._sanitize_oracle_identifier(col) for col in data[0].keys()]
        placeholders = []
        
        for i, _ in enumerate(data[0].keys(), 1):
            placeholders.append(f":{i}")
        
        # Build the query
        query = f"INSERT INTO {safe_table} ({', '.join(columns)}) VALUES ({', '.join(placeholders)})"
        
        # Convert data to list of tuples for executemany
        params_list = []
        for item in data:
            params_list.append(tuple(item.values()))
        
        # Execute the query
        self.executemany(query, params_list)
        
        # Return the number of affected rows
        return len(data)  # Oracle doesn't report actual rows affected in executemany
    
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
                           limit: Optional[Union[int, Tuple[int, int]]] = None) -> Tuple[str, Dict]:
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
        safe_table = self._sanitize_oracle_identifier(table)
        
        # Process fields
        if isinstance(fields, str):
            if fields == '*':
                fields_str = '*'
            else:
                fields_str = self._sanitize_oracle_identifier(fields)
        else:
            fields_str = ', '.join(self._sanitize_oracle_identifier(f) for f in fields)
        
        # For Oracle, we need to use ROWNUM or ROW_NUMBER() for pagination
        # If limit is specified, we'll use ROW_NUMBER() for more flexibility
        if limit is not None:
            # Determine the row range
            if isinstance(limit, int):
                start_row = 1
                end_row = limit
            elif isinstance(limit, (list, tuple)):
                if len(limit) == 1:
                    start_row = 1
                    end_row = limit[0]
                else:
                    start_row = limit[0] + 1  # Convert from 0-based to 1-based
                    end_row = start_row + limit[1] - 1
            
            # Build a query with ROW_NUMBER()
            inner_query = f"SELECT {fields_str} FROM {safe_table}"
            
            # Process WHERE clause for inner query
            params = {}
            if where:
                inner_where, inner_params = self._process_where_clause(where)
                if inner_where:
                    inner_query += f" WHERE {inner_where}"
                    params.update(inner_params)
            
            # Process ORDER BY for inner query
            order_clause = ""
            if order:
                if isinstance(order, str):
                    order_clause = f" ORDER BY {order}"
                elif isinstance(order, (list, tuple)):
                    if len(order) >= 1:
                        order_clause = f" ORDER BY {order[0]}"
                        if len(order) >= 2:
                            direction = order[1].upper()
                            if direction in ('ASC', 'DESC'):
                                order_clause += f" {direction}"
            
            # Build the final query with pagination
            query = f"""
            SELECT *
            FROM (
                SELECT a.*, ROW_NUMBER() OVER({order_clause or 'ORDER BY ROWID'}) AS rn
                FROM ({inner_query}) a
            )
            WHERE rn BETWEEN :start_row AND :end_row
            """
            
            # Add pagination parameters
            params['start_row'] = start_row
            params['end_row'] = end_row
            
            return query, params
        else:
            # Standard query without pagination
            query = f"SELECT {fields_str} FROM {safe_table}"
            
            # Process WHERE clause
            params = {}
            if where:
                where_clause, where_params = self._process_where_clause(where)
                if where_clause:
                    query += f" WHERE {where_clause}"
                    params.update(where_params)
            
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
            
            return query, params
    
    def _process_where_clause(self, where: Union[str, Tuple, Dict]) -> Tuple[str, Dict]:
        """
        Process the WHERE clause and convert it to Oracle syntax.
        
        Args:
            where: Where condition
            
        Returns:
            Tuple of (where_clause, parameters)
        """
        where_clause = ""
        params = {}
        
        if isinstance(where, str):
            # Direct string, might need to convert ? to :param style
            param_index = 1
            while '?' in where:
                where = where.replace('?', f':p{param_index}', 1)
                param_index += 1
            where_clause = where
        elif isinstance(where, (list, tuple)) and len(where) >= 1:
            # Tuple with condition and parameters
            condition = where[0]
            
            # Convert ? to :param style
            param_index = 1
            while '?' in condition:
                condition = condition.replace('?', f':p{param_index}', 1)
                param_index += 1
            
            where_clause = condition
            
            # Add parameters
            if len(where) >= 2 and where[1]:
                if isinstance(where[1], (list, tuple)):
                    for i, value in enumerate(where[1], 1):
                        params[f'p{i}'] = value
                else:
                    params['p1'] = where[1]
        elif isinstance(where, dict):
            # Dictionary of field:value pairs
            conditions = []
            for i, (key, value) in enumerate(where.items(), 1):
                param_name = f'p{i}'
                conditions.append(f"{self._sanitize_oracle_identifier(key)} = :{param_name}")
                params[param_name] = value
            
            where_clause = ' AND '.join(conditions)
        
        return where_clause, params
    
    def _build_insert_query(self, table: str, data: Dict[str, Any]) -> Tuple[str, Dict]:
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
        safe_table = self._sanitize_oracle_identifier(table)
        
        # Process columns and values
        columns = [self._sanitize_oracle_identifier(col) for col in data.keys()]
        placeholders = []
        params = {}
        
        for i, (key, value) in enumerate(data.items(), 1):
            param_name = f'p{i}'
            placeholders.append(f":{param_name}")
            params[param_name] = value
        
        # Build the query
        query = f"INSERT INTO {safe_table} ({', '.join(columns)}) VALUES ({', '.join(placeholders)})"
        
        return query, params
    
    def _build_update_query(self, table: str, data: Dict[str, Any], 
                           where: Optional[Union[str, Tuple, Dict]] = None) -> Tuple[str, Dict]:
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
        safe_table = self._sanitize_oracle_identifier(table)
        
        # Process SET clause
        set_clauses = []
        params = {}
        
        for i, (key, value) in enumerate(data.items(), 1):
            param_name = f'p{i}'
            set_clauses.append(f"{self._sanitize_oracle_identifier(key)} = :{param_name}")
            params[param_name] = value
        
        # Start building the query
        query = f"UPDATE {safe_table} SET {', '.join(set_clauses)}"
        
        # Process WHERE clause
        if where:
            where_clause, where_params = self._process_where_clause(where)
            if where_clause:
                query += f" WHERE {where_clause}"
                params.update(where_params)
        
        return query, params
    
    def _build_delete_query(self, table: str, where: Optional[Union[str, Tuple, Dict]] = None) -> Tuple[str, Dict]:
        """
        Build a DELETE query.
        
        Args:
            table: Table name
            where: Where condition
            
        Returns:
            Tuple of (query_string, parameters)
        """
        # Sanitize table name
        safe_table = self._sanitize_oracle_identifier(table)
        
        # Start building the query
        query = f"DELETE FROM {safe_table}"
        
        # Process WHERE clause
        params = {}
        if where:
            where_clause, where_params = self._process_where_clause(where)
            if where_clause:
                query += f" WHERE {where_clause}"
                params.update(where_params)
        
        return query, params
    
    def _sanitize_oracle_identifier(self, identifier: str) -> str:
        """
        Sanitize an Oracle identifier (table or column name).
        
        Oracle uses double quotes for quoting identifiers.
        
        Args:
            identifier: The identifier to sanitize
            
        Returns:
            Sanitized identifier
        """
        # Check if identifier is already quoted
        if identifier.startswith('"') and identifier.endswith('"'):
            return identifier
        
        # For identifiers with dot notation (e.g., schema.table)
        if '.' in identifier:
            parts = identifier.split('.')
            return '.'.join([self._sanitize_oracle_identifier(part) for part in parts])
        
        # Special case for star
        if identifier == '*':
            return '*'
        
        # Quote the identifier if it needs quoting
        # Oracle identifiers can be up to 30 characters, contain letters, numbers, _, $, and #
        pattern = re.compile(r'^[a-zA-Z][a-zA-Z0-9_$#]{0,29}$')
        if not pattern.match(identifier) or identifier.upper() in self._oracle_reserved_words:
            return f'"{identifier}"'
        
        # By default, Oracle stores identifiers in uppercase
        # To preserve case sensitivity, we always quote
        return f'"{identifier}"'
    
    # List of Oracle reserved words
    _oracle_reserved_words = {
        'ACCESS', 'ADD', 'ALL', 'ALTER', 'AND', 'ANY', 'AS', 'ASC', 'AUDIT', 
        'BETWEEN', 'BY', 'CHAR', 'CHECK', 'CLUSTER', 'COLUMN', 'COMMENT', 'COMPRESS', 
        'CONNECT', 'CREATE', 'CURRENT', 'DATE', 'DECIMAL', 'DEFAULT', 'DELETE', 
        'DESC', 'DISTINCT', 'DROP', 'ELSE', 'EXCLUSIVE', 'EXISTS', 'FILE', 'FLOAT', 
        'FOR', 'FROM', 'GRANT', 'GROUP', 'HAVING', 'IDENTIFIED', 'IMMEDIATE', 'IN', 
        'INCREMENT', 'INDEX', 'INITIAL', 'INSERT', 'INTEGER', 'INTERSECT', 'INTO', 
        'IS', 'LEVEL', 'LIKE', 'LOCK', 'LONG', 'MAXEXTENTS', 'MINUS', 'MLSLABEL', 
        'MODE', 'MODIFY', 'NOAUDIT', 'NOCOMPRESS', 'NOT', 'NOWAIT', 'NULL', 'NUMBER', 
        'OF', 'OFFLINE', 'ON', 'ONLINE', 'OPTION', 'OR', 'ORDER', 'PCTFREE', 'PRIOR', 
        'PRIVILEGES', 'PUBLIC', 'RAW', 'RENAME', 'RESOURCE', 'REVOKE', 'ROW', 'ROWID', 
        'ROWNUM', 'ROWS', 'SELECT', 'SESSION', 'SET', 'SHARE', 'SIZE', 'SMALLINT', 
        'START', 'SUCCESSFUL', 'SYNONYM', 'SYSDATE', 'TABLE', 'THEN', 'TO', 'TRIGGER', 
        'UID', 'UNION', 'UNIQUE', 'UPDATE', 'USER', 'VALIDATE', 'VALUES', 'VARCHAR', 
        'VARCHAR2', 'VIEW', 'WHENEVER', 'WHERE', 'WITH'
    }
    
    def merge_insert_update(self, table: str, data: Dict[str, Any], 
                           key_columns: Union[str, List[str]]) -> int:
        """
        Insert a row or update it if it exists (UPSERT operation).
        
        In Oracle, this is done with MERGE statement.
        
        Args:
            table: Table name
            data: Dictionary of field:value pairs
            key_columns: Primary key column(s)
            
        Returns:
            Number of affected rows
        """
        if not data:
            raise ValueError("No data provided for merge")
        
        # Convert key_columns to list if it's a string
        if isinstance(key_columns, str):
            key_columns = [key_columns]
        
        # Ensure all key columns are in the data
        for key in key_columns:
            if key not in data:
                raise ValueError(f"Key column '{key}' not found in data")
        
        # Sanitize table name
        safe_table = self._sanitize_oracle_identifier(table)
        
        # Separate data into key and non-key columns
        key_data = {k: data[k] for k in key_columns}
        non_key_data = {k: v for k, v in data.items() if k not in key_columns}
        
        # If there are no non-key columns, it's just an insert
        if not non_key_data:
            return self.insert(table, data)
        
        # Build the MERGE statement
        params = {}
        
        # Build the condition part
        join_conditions = []
        for i, key in enumerate(key_columns, 1):
            param_name = f'k{i}'
            safe_key = self._sanitize_oracle_identifier(key)
            join_conditions.append(f"t.{safe_key} = :{param_name}")
            params[param_name] = key_data[key]
        
        # Build the update part
        update_clauses = []
        for i, (key, value) in enumerate(non_key_data.items(), 1):
            param_name = f'u{i}'
            safe_key = self._sanitize_oracle_identifier(key)
            update_clauses.append(f"t.{safe_key} = :{param_name}")
            params[param_name] = value
        
        # Build the insert part
        all_columns = list(data.keys())
        insert_columns = [self._sanitize_oracle_identifier(col) for col in all_columns]
        insert_values = []
        
        for i, (key, value) in enumerate(data.items(), 1):
            param_name = f'i{i}'
            insert_values.append(f":{param_name}")
            params[param_name] = value
        
        # Combine into the complete MERGE statement
        query = f"""
        MERGE INTO {safe_table} t
        USING DUAL ON ({' AND '.join(join_conditions)})
        WHEN MATCHED THEN
            UPDATE SET {', '.join(update_clauses)}
        WHEN NOT MATCHED THEN
            INSERT ({', '.join(insert_columns)})
            VALUES ({', '.join(insert_values)})
        """
        
        # Execute the query
        self.execute(query, params)
        
        # Return the number of affected rows
        return self.cur.rowcount
    
    def create_savepoint(self, name: str) -> None:
        """Create a savepoint within the current transaction."""
        self.execute(f"SAVEPOINT {name}")
    
    def release_savepoint(self, name: str) -> None:
        """
        Release a savepoint within the current transaction.
        
        Note: Oracle doesn't have an explicit RELEASE SAVEPOINT command.
        Savepoints are automatically released at the end of the transaction.
        """
        # No-op for Oracle
        pass
    
    def rollback_to_savepoint(self, name: str) -> None:
        """Rollback to a savepoint within the current transaction."""
        self.execute(f"ROLLBACK TO SAVEPOINT {name}")
    
    def table_exists(self, table_name: str, owner: Optional[str] = None) -> bool:
        """
        Check if a table exists in the database.
        
        Args:
            table_name: Table name to check
            owner: Schema name
            
        Returns:
            True if the table exists, False otherwise
        """
        try:
            params = {'table_name': table_name.upper()}
            query = "SELECT 1 FROM ALL_TABLES WHERE TABLE_NAME = :table_name"
            
            if owner:
                query += " AND OWNER = :owner"
                params['owner'] = owner.upper()
            
            self.execute(query, params)
            return self.fetch_one() is not None
        except Exception:
            return False
    
    def get_table_columns(self, table_name: str, owner: Optional[str] = None) -> List[Dict[str, Any]]:
        """
        Get column information for a table.
        
        Args:
            table_name: Table name
            owner: Schema name
            
        Returns:
            List of dictionaries with column information
        """
        try:
            params = {'table_name': table_name.upper()}
            query = """
                SELECT 
                    COLUMN_NAME, DATA_TYPE, DATA_LENGTH, DATA_PRECISION,
                    DATA_SCALE, NULLABLE, DATA_DEFAULT, 
                    COLUMN_ID
                FROM ALL_TAB_COLUMNS
                WHERE TABLE_NAME = :table_name
            """
            
            if owner:
                query += " AND OWNER = :owner"
                params['owner'] = owner.upper()
            
            query += " ORDER BY COLUMN_ID"
            
            self.execute(query, params)
            return self.fetch_all()
        except Exception as e:
            logger.error(f"Failed to get columns for table {table_name}: {e}")
            return []
    
    def execute_procedure(self, procedure_name: str, params: Optional[Dict[str, Any]] = None) -> Any:
        """
        Execute a stored procedure.
        
        Args:
            procedure_name: Name of the procedure
            params: Dictionary of parameters {name: value}
            
        Returns:
            Cursor with the procedure results
        """
        if not params:
            # Simple procedure call without parameters
            query = f"BEGIN {procedure_name}; END;"
            return self.execute(query)
        
        # Build parameter list
        param_list = []
        bind_params = {}
        
        for name, value in params.items():
            param_list.append(f":{name}")
            bind_params[name] = value
        
        # Build the procedure call
        query = f"BEGIN {procedure_name}({', '.join(param_list)}); END;"
        
        # Execute the procedure
        return self.execute(query, bind_params)
    
    def get_sequence_next_val(self, sequence_name: str) -> int:
        """
        Get the next value from a sequence.
        
        Args:
            sequence_name: Name of the sequence
            
        Returns:
            Next value from the sequence
        """
        safe_sequence = self._sanitize_oracle_identifier(sequence_name)
        self.execute(f"SELECT {safe_sequence}.NEXTVAL FROM DUAL")
        row = self.fetch_one()
        
        if row:
            return int(list(row.values())[0])
        
        raise QueryError(f"Failed to get next value from sequence {sequence_name}")