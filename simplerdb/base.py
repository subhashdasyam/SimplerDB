"""
Base classes for SimplerDB database connectors.
"""

from abc import ABC, abstractmethod
from typing import Dict, List, Tuple, Union, Any, Optional, Iterator
import logging

from .exceptions import SimplerDBError, ConnectionError, QueryError
from .transaction import TransactionContext

logger = logging.getLogger(__name__)

class DBInterface(ABC):
    """
    Abstract base class for all database connectors.
    
    This defines the common interface that all database connectors must implement.
    """
    
    @abstractmethod
    def __init__(self, **kwargs):
        """Initialize the database connection"""
        self.conn = None
        self.cur = None
        self.conf = kwargs
        
    @abstractmethod
    def connect(self) -> None:
        """Establish a connection to the database"""
        pass
        
    @abstractmethod
    def disconnect(self) -> None:
        """Close the database connection"""
        pass
        
    @abstractmethod
    def is_connected(self) -> bool:
        """Check if the database connection is active"""
        pass
        
    @abstractmethod
    def execute(self, query: str, params: Optional[Union[List, Tuple, Dict]] = None) -> Any:
        """
        Execute a raw query with parameters
        
        Args:
            query: SQL query or command string
            params: Parameters for the query
            
        Returns:
            Cursor or cursor-like object
        """
        pass
        
    @abstractmethod
    def fetch_one(self) -> Optional[Dict[str, Any]]:
        """
        Fetch a single row from the last executed query
        
        Returns:
            A dictionary representing a single row or None
        """
        pass
        
    @abstractmethod
    def fetch_all(self) -> List[Dict[str, Any]]:
        """
        Fetch all rows from the last executed query
        
        Returns:
            A list of dictionaries, each representing a row
        """
        pass
        
    @abstractmethod
    def fetch_iter(self, batch_size: int = 1000) -> Iterator[Dict[str, Any]]:
        """
        Return an iterator for fetching rows in batches
        
        Args:
            batch_size: Number of rows to fetch at a time
            
        Returns:
            An iterator that yields rows as dictionaries
        """
        pass
        
    @abstractmethod
    def commit(self) -> None:
        """Commit the current transaction"""
        pass
        
    @abstractmethod
    def rollback(self) -> None:
        """Rollback the current transaction"""
        pass
        
    @abstractmethod
    def last_insert_id(self) -> Optional[int]:
        """
        Get the ID of the last inserted row
        
        Returns:
            The last insert ID or None if not applicable
        """
        pass
        
    @abstractmethod
    def last_query(self) -> str:
        """
        Get the last executed query
        
        Returns:
            The SQL string of the last executed query
        """
        pass
    
    @abstractmethod
    def get_one(self, table: str, fields: Union[str, List[str]] = '*', 
               where: Optional[Union[str, Tuple, Dict]] = None, 
               order: Optional[Union[str, List]] = None, 
               limit: Optional[Union[int, Tuple[int, int]]] = 1) -> Optional[Dict[str, Any]]:
        """
        Get a single record from the database
        
        Args:
            table: Table name
            fields: Field(s) to select
            where: Where condition
            order: Order by clause
            limit: Limit clause
            
        Returns:
            A dictionary representing a single row or None
        """
        pass
        
    @abstractmethod
    def get_all(self, table: str, fields: Union[str, List[str]] = '*', 
               where: Optional[Union[str, Tuple, Dict]] = None, 
               order: Optional[Union[str, List]] = None, 
               limit: Optional[Union[int, Tuple[int, int]]] = None) -> List[Dict[str, Any]]:
        """
        Get multiple records from the database
        
        Args:
            table: Table name
            fields: Field(s) to select
            where: Where condition
            order: Order by clause
            limit: Limit clause
            
        Returns:
            A list of dictionaries, each representing a row
        """
        pass
        
    @abstractmethod
    def insert(self, table: str, data: Dict[str, Any]) -> int:
        """
        Insert a single record into the database
        
        Args:
            table: Table name
            data: Dictionary of field:value pairs
            
        Returns:
            Number of affected rows
        """
        pass
        
    @abstractmethod
    def insert_batch(self, table: str, data: List[Dict[str, Any]]) -> int:
        """
        Insert multiple records into the database
        
        Args:
            table: Table name
            data: List of dictionaries with field:value pairs
            
        Returns:
            Number of affected rows
        """
        pass
        
    @abstractmethod
    def update(self, table: str, data: Dict[str, Any], 
              where: Optional[Union[str, Tuple, Dict]] = None) -> int:
        """
        Update records in the database
        
        Args:
            table: Table name
            data: Dictionary of field:value pairs to update
            where: Where condition
            
        Returns:
            Number of affected rows
        """
        pass
        
    @abstractmethod
    def delete(self, table: str, where: Optional[Union[str, Tuple, Dict]] = None) -> int:
        """
        Delete records from the database
        
        Args:
            table: Table name
            where: Where condition
            
        Returns:
            Number of affected rows
        """
        pass
        
    def transaction(self) -> TransactionContext:
        """
        Return a transaction context manager
        
        Returns:
            A context manager for transaction handling
        """
        return TransactionContext(self)
        
    def __enter__(self):
        """Context manager entry point"""
        return self
        
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit point"""
        if exc_type is None:
            try:
                self.commit()
            except Exception as e:
                logger.warning(f"Failed to commit on context exit: {e}")
        else:
            try:
                self.rollback()
            except Exception as e:
                logger.warning(f"Failed to rollback on context exit: {e}")
        
        self.disconnect()


class SQLBaseConnector(DBInterface):
    """
    Base class for SQL-based database connectors.
    
    Implements common functionality for SQL databases.
    """
    
    def execute(self, query: str, params: Optional[Union[List, Tuple, Dict]] = None) -> Any:
        """Execute a raw query"""
        if not self.is_connected():
            self.connect()
            
        try:
            if params:
                self.cur.execute(query, params)
            else:
                self.cur.execute(query)
            return self.cur
        except Exception as e:
            raise QueryError(f"Query execution failed: {e}") from e
            
    def fetch_one(self) -> Optional[Dict[str, Any]]:
        """Fetch a single row from the result set"""
        if not self.cur:
            return None
            
        try:
            row = self.cur.fetchone()
            if not row:
                return None
                
            # Convert to dictionary
            columns = [desc[0] for desc in self.cur.description]
            return dict(zip(columns, row))
        except Exception as e:
            raise QueryError(f"Failed to fetch row: {e}") from e
            
    def fetch_all(self) -> List[Dict[str, Any]]:
        """Fetch all rows from the result set"""
        if not self.cur:
            return []
            
        try:
            rows = self.cur.fetchall()
            if not rows:
                return []
                
            # Convert to list of dictionaries
            columns = [desc[0] for desc in self.cur.description]
            return [dict(zip(columns, row)) for row in rows]
        except Exception as e:
            raise QueryError(f"Failed to fetch rows: {e}") from e
    
    def fetch_iter(self, batch_size: int = 1000) -> Iterator[Dict[str, Any]]:
        """Return an iterator for fetching rows in batches"""
        if not self.cur:
            return iter(())  # Empty iterator
            
        try:
            columns = [desc[0] for desc in self.cur.description]
            while True:
                rows = self.cur.fetchmany(batch_size)
                if not rows:
                    break
                    
                for row in rows:
                    yield dict(zip(columns, row))
        except Exception as e:
            raise QueryError(f"Failed to fetch rows iteratively: {e}") from e
            
    def get_one(self, table: str, fields: Union[str, List[str]] = '*', 
               where: Optional[Union[str, Tuple, Dict]] = None, 
               order: Optional[Union[str, List]] = None, 
               limit: Optional[Union[int, Tuple[int, int]]] = 1) -> Optional[Dict[str, Any]]:
        """Get a single record from the database"""
        # Build the query
        query, params = self._build_select_query(table, fields, where, order, limit)
        
        # Execute the query
        self.execute(query, params)
        
        # Return the first row
        return self.fetch_one()
        
    def get_all(self, table: str, fields: Union[str, List[str]] = '*', 
               where: Optional[Union[str, Tuple, Dict]] = None, 
               order: Optional[Union[str, List]] = None, 
               limit: Optional[Union[int, Tuple[int, int]]] = None) -> List[Dict[str, Any]]:
        """Get multiple records from the database"""
        # Build the query
        query, params = self._build_select_query(table, fields, where, order, limit)
        
        # Execute the query
        self.execute(query, params)
        
        # Return all rows
        return self.fetch_all()
        
    def _build_select_query(self, table: str, fields: Union[str, List[str]] = '*', 
                           where: Optional[Union[str, Tuple, Dict]] = None, 
                           order: Optional[Union[str, List]] = None, 
                           limit: Optional[Union[int, Tuple[int, int]]] = None) -> Tuple[str, List]:
        """
        Build a SELECT query
        
        Args:
            table: Table name
            fields: Field(s) to select
            where: Where condition
            order: Order by clause
            limit: Limit clause
            
        Returns:
            Tuple of (query_string, parameters)
        """
        # Implemented in specific connector classes
        raise NotImplementedError("_build_select_query must be implemented by subclasses")
        
    def _build_insert_query(self, table: str, data: Dict[str, Any]) -> Tuple[str, List]:
        """
        Build an INSERT query
        
        Args:
            table: Table name
            data: Dictionary of field:value pairs
            
        Returns:
            Tuple of (query_string, parameters)
        """
        # Implemented in specific connector classes
        raise NotImplementedError("_build_insert_query must be implemented by subclasses")
        
    def _build_update_query(self, table: str, data: Dict[str, Any], 
                           where: Optional[Union[str, Tuple, Dict]] = None) -> Tuple[str, List]:
        """
        Build an UPDATE query
        
        Args:
            table: Table name
            data: Dictionary of field:value pairs
            where: Where condition
            
        Returns:
            Tuple of (query_string, parameters)
        """
        # Implemented in specific connector classes
        raise NotImplementedError("_build_update_query must be implemented by subclasses")
        
    def _build_delete_query(self, table: str, where: Optional[Union[str, Tuple, Dict]] = None) -> Tuple[str, List]:
        """
        Build a DELETE query
        
        Args:
            table: Table name
            where: Where condition
            
        Returns:
            Tuple of (query_string, parameters)
        """
        # Implemented in specific connector classes
        raise NotImplementedError("_build_delete_query must be implemented by subclasses")


class NoSQLBaseConnector(DBInterface):
    """
    Base class for NoSQL database connectors.
    
    Implements common functionality for NoSQL databases.
    """
    
    def get_one(self, table: str, fields: Union[str, List[str]] = '*', 
               where: Optional[Union[str, Tuple, Dict]] = None, 
               order: Optional[Union[str, List]] = None, 
               limit: Optional[Union[int, Tuple[int, int]]] = 1) -> Optional[Dict[str, Any]]:
        """
        Get a single record from the database
        
        For NoSQL databases, 'table' might represent a collection, key prefix,
        or other container concept depending on the database type.
        """
        # Implemented in specific connector classes for each NoSQL DB
        raise NotImplementedError("get_one must be implemented by subclasses")
        
    def get_all(self, table: str, fields: Union[str, List[str]] = '*', 
               where: Optional[Union[str, Tuple, Dict]] = None, 
               order: Optional[Union[str, List]] = None, 
               limit: Optional[Union[int, Tuple[int, int]]] = None) -> List[Dict[str, Any]]:
        """
        Get multiple records from the database
        
        For NoSQL databases, 'table' might represent a collection, key prefix,
        or other container concept depending on the database type.
        """
        # Implemented in specific connector classes for each NoSQL DB
        raise NotImplementedError("get_all must be implemented by subclasses")