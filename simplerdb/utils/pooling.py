"""
Connection pooling utilities for SimplerDB.
"""

import logging
import threading
import time
import queue
from typing import Dict, Any, List, Callable, Optional, Type, TypeVar, Generic

logger = logging.getLogger(__name__)

T = TypeVar('T')

class ConnectionPool(Generic[T]):
    """
    A generic connection pool implementation.
    
    This provides a reusable connection pool that works with any type of connection.
    """
    
    def __init__(
        self,
        create_connection: Callable[[], T],
        close_connection: Callable[[T], None],
        validate_connection: Callable[[T], bool],
        min_size: int = 1,
        max_size: int = 10,
        max_idle_time: float = 60.0,
        max_lifetime: float = 3600.0,
        acquire_timeout: float = 10.0
    ):
        """
        Initialize the connection pool.
        
        Args:
            create_connection: Function to create a new connection
            close_connection: Function to close a connection
            validate_connection: Function to validate a connection is still usable
            min_size: Minimum number of connections in the pool
            max_size: Maximum number of connections in the pool
            max_idle_time: Maximum time a connection can be idle before being closed (seconds)
            max_lifetime: Maximum lifetime of a connection before being closed (seconds)
            acquire_timeout: Timeout when acquiring a connection (seconds)
        """
        self.create_connection = create_connection
        self.close_connection = close_connection
        self.validate_connection = validate_connection
        self.min_size = min_size
        self.max_size = max_size
        self.max_idle_time = max_idle_time
        self.max_lifetime = max_lifetime
        self.acquire_timeout = acquire_timeout
        
        # Connection pool
        self.pool = queue.Queue(maxsize=max_size)
        
        # Tracking connection metadata
        self.connection_metadata: Dict[int, Dict[str, Any]] = {}
        
        # Synchronization
        self.lock = threading.RLock()
        self._closed = False
        
        # Stats
        self.created_connections = 0
        self.closed_connections = 0
        self.active_connections = 0
        self.peak_connections = 0
        
        # Initialize the pool with min_size connections
        self._initialize_pool()
        
        # Start the maintenance thread
        self.maintenance_thread = threading.Thread(
            target=self._maintenance_worker,
            daemon=True,
            name="ConnectionPool-Maintenance"
        )
        self.maintenance_thread.start()
    
    def _initialize_pool(self) -> None:
        """Initialize the pool with min_size connections."""
        for _ in range(self.min_size):
            try:
                conn = self._create_connection()
                self.pool.put(conn)
            except Exception as e:
                logger.error(f"Failed to initialize connection in pool: {e}")
    
    def _create_connection(self) -> T:
        """
        Create a new connection and track its metadata.
        
        Returns:
            A new connection
        """
        with self.lock:
            conn = self.create_connection()
            conn_id = id(conn)
            
            # Track connection metadata
            self.connection_metadata[conn_id] = {
                "created_at": time.time(),
                "last_used_at": time.time(),
                "in_use": False
            }
            
            # Update stats
            self.created_connections += 1
            self.active_connections += 1
            self.peak_connections = max(self.peak_connections, self.active_connections)
            
            return conn
    
    def _close_connection(self, conn: T) -> None:
        """
        Close a connection and remove its metadata.
        
        Args:
            conn: Connection to close
        """
        with self.lock:
            try:
                self.close_connection(conn)
            except Exception as e:
                logger.warning(f"Error closing connection: {e}")
            
            # Remove connection metadata
            conn_id = id(conn)
            if conn_id in self.connection_metadata:
                del self.connection_metadata[conn_id]
            
            # Update stats
            self.closed_connections += 1
            self.active_connections -= 1
    
    def acquire(self) -> T:
        """
        Acquire a connection from the pool.
        
        Returns:
            A pooled connection
            
        Raises:
            QueueEmpty: If no connection is available within the timeout
        """
        if self._closed:
            raise ValueError("Connection pool is closed")
        
        # Try to get a connection from the pool
        start_time = time.time()
        timeout = self.acquire_timeout
        
        while True:
            try:
                # Get a connection from the pool
                conn = self.pool.get(timeout=timeout)
                
                # Check if connection is valid
                conn_id = id(conn)
                if conn_id in self.connection_metadata:
                    # Update metadata
                    with self.lock:
                        metadata = self.connection_metadata[conn_id]
                        metadata["last_used_at"] = time.time()
                        metadata["in_use"] = True
                    
                    # Validate connection
                    if self.validate_connection(conn):
                        return conn
                    else:
                        # Connection is not valid, close it and create a new one
                        logger.debug("Connection validation failed, creating a new connection")
                        self._close_connection(conn)
                        conn = self._create_connection()
                        
                        # Update metadata
                        with self.lock:
                            conn_id = id(conn)
                            metadata = self.connection_metadata[conn_id]
                            metadata["in_use"] = True
                        
                        return conn
                else:
                    # Connection metadata not found (shouldn't happen)
                    logger.warning("Connection metadata not found, creating a new connection")
                    self._close_connection(conn)
                    conn = self._create_connection()
                    
                    # Update metadata
                    with self.lock:
                        conn_id = id(conn)
                        metadata = self.connection_metadata[conn_id]
                        metadata["in_use"] = True
                    
                    return conn
            
            except queue.Empty:
                # No connection available in the pool, try to create a new one
                with self.lock:
                    if self.active_connections < self.max_size:
                        conn = self._create_connection()
                        
                        # Update metadata
                        conn_id = id(conn)
                        metadata = self.connection_metadata[conn_id]
                        metadata["in_use"] = True
                        
                        return conn
                
                # Pool is at max size, wait for a connection to be released
                elapsed = time.time() - start_time
                if elapsed >= self.acquire_timeout:
                    raise ValueError(f"Timeout waiting for connection after {self.acquire_timeout} seconds")
                
                # Adjust timeout for next iteration
                timeout = max(0.1, self.acquire_timeout - elapsed)
    
    def release(self, conn: T) -> None:
        """
        Release a connection back to the pool.
        
        Args:
            conn: Connection to release
        """
        if self._closed:
            # Pool is closed, just close the connection
            self._close_connection(conn)
            return
        
        conn_id = id(conn)
        if conn_id in self.connection_metadata:
            # Update metadata
            with self.lock:
                metadata = self.connection_metadata[conn_id]
                metadata["last_used_at"] = time.time()
                metadata["in_use"] = False
            
            try:
                # Put the connection back in the pool
                self.pool.put_nowait(conn)
            except queue.Full:
                # Pool is full, close the connection
                self._close_connection(conn)
        else:
            # Connection not in metadata (shouldn't happen)
            logger.warning("Connection metadata not found on release, closing")
            self._close_connection(conn)
    
    def _maintenance_worker(self) -> None:
        """Maintenance worker thread to manage pool size and close idle connections."""
        while not self._closed:
            try:
                self._maintenance_task()
            except Exception as e:
                logger.error(f"Error in connection pool maintenance: {e}")
            
            # Sleep for 5 seconds
            time.sleep(5)
    
    def _maintenance_task(self) -> None:
        """
        Maintenance task to manage pool size and close idle connections.
        
        This will:
        1. Close connections that have been idle for too long
        2. Close connections that have exceeded their maximum lifetime
        3. Ensure the pool has at least min_size connections
        """
        now = time.time()
        to_close: List[T] = []
        in_use = 0
        
        # Check all connections
        with self.lock:
            for conn_id, metadata in list(self.connection_metadata.items()):
                # Skip connections that are in use
                if metadata["in_use"]:
                    in_use += 1
                    continue
                
                # Check if connection has exceeded its max lifetime
                if now - metadata["created_at"] > self.max_lifetime:
                    # Find the connection object
                    conn = self._find_connection_by_id(conn_id)
                    if conn:
                        to_close.append(conn)
                        continue
                
                # Check if connection has been idle for too long
                if now - metadata["last_used_at"] > self.max_idle_time:
                    # Only close idle connections if we have more than min_size
                    if self.active_connections - len(to_close) > self.min_size:
                        conn = self._find_connection_by_id(conn_id)
                        if conn:
                            to_close.append(conn)
        
        # Close connections that need to be closed
        for conn in to_close:
            try:
                # Remove from pool
                try:
                    with self.pool.mutex:
                        self.pool.queue.remove(conn)
                except ValueError:
                    pass
                
                # Close the connection
                self._close_connection(conn)
            except Exception as e:
                logger.warning(f"Error closing connection in maintenance: {e}")
        
        # Ensure we have min_size connections
        with self.lock:
            pool_size = self.active_connections
            to_create = max(0, self.min_size - pool_size)
        
        # Create new connections if needed
        for _ in range(to_create):
            try:
                conn = self._create_connection()
                try:
                    self.pool.put_nowait(conn)
                except queue.Full:
                    self._close_connection(conn)
            except Exception as e:
                logger.error(f"Failed to create connection in maintenance: {e}")
    
    def _find_connection_by_id(self, conn_id: int) -> Optional[T]:
        """
        Find a connection in the pool by its ID.
        
        Args:
            conn_id: Connection ID to find
            
        Returns:
            Connection object or None if not found
        """
        # Get connections from the queue without removing them
        with self.pool.mutex:
            for conn in self.pool.queue:
                if id(conn) == conn_id:
                    return conn
        
        return None
    
    def close(self) -> None:
        """Close the pool and all connections."""
        if self._closed:
            return
        
        with self.lock:
            self._closed = True
        
        # Drain the pool and close all connections
        while True:
            try:
                conn = self.pool.get_nowait()
                self._close_connection(conn)
            except queue.Empty:
                break
        
        # Find any in-use connections
        with self.lock:
            for conn_id, metadata in list(self.connection_metadata.items()):
                if metadata["in_use"]:
                    logger.warning(f"Connection {conn_id} still in use during pool shutdown")
        
        logger.info("Connection pool closed")
    
    def stats(self) -> Dict[str, Any]:
        """
        Get statistics about the connection pool.
        
        Returns:
            Dictionary of pool statistics
        """
        with self.lock:
            in_use = sum(1 for metadata in self.connection_metadata.values() if metadata["in_use"])
            
            return {
                "min_size": self.min_size,
                "max_size": self.max_size,
                "current_size": self.active_connections,
                "in_use": in_use,
                "available": self.active_connections - in_use,
                "created_total": self.created_connections,
                "closed_total": self.closed_connections,
                "peak_connections": self.peak_connections
            }


# Factory function to create a connection pool for a specific connector
def create_connection_pool(
    connector_class: Type[Any],
    connector_kwargs: Dict[str, Any],
    pool_kwargs: Optional[Dict[str, Any]] = None
) -> ConnectionPool:
    """
    Create a connection pool for a specific database connector.
    
    Args:
        connector_class: Database connector class
        connector_kwargs: Arguments to pass to the connector
        pool_kwargs: Connection pool configuration
        
    Returns:
        A connection pool for the connector
    """
    # Set default pool kwargs
    if pool_kwargs is None:
        pool_kwargs = {}
    
    # Set default values
    pool_kwargs.setdefault("min_size", 1)
    pool_kwargs.setdefault("max_size", 10)
    
    # Create functions for the pool
    def create_connection():
        connector = connector_class(**connector_kwargs)
        return connector
    
    def close_connection(connector):
        connector.disconnect()
    
    def validate_connection(connector):
        return connector.is_connected()
    
    # Create the pool
    pool = ConnectionPool(
        create_connection,
        close_connection,
        validate_connection,
        **pool_kwargs
    )
    
    return pool