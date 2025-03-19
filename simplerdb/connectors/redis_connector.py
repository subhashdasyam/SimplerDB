"""
Redis connector for SimplerDB.
"""

import json
import logging
import time
from typing import Dict, List, Tuple, Union, Any, Optional, Iterator, cast

from ..base import NoSQLBaseConnector
from ..exceptions import ConnectionError, QueryError, ConfigurationError, FeatureNotSupportedError

logger = logging.getLogger(__name__)

class RedisConnector(NoSQLBaseConnector):
    """
    Connector for Redis databases.
    """
    
    def __init__(self, **kwargs):
        """
        Initialize the Redis connector.
        
        Args:
            host (str): Redis host
            port (int): Redis port
            db (int): Redis database number
            password (str): Redis password
            username (str): Redis username (for Redis 6+)
            decode_responses (bool): Whether to decode byte responses to strings
            socket_timeout (float): Socket timeout in seconds
            socket_connect_timeout (float): Socket connect timeout in seconds
            client_name (str): Client name
            health_check_interval (float): Health check interval in seconds
            retry_on_timeout (bool): Whether to retry on timeout
            retry_on_error (list): List of errors to retry on
            **kwargs: Additional connector-specific parameters
        """
        super().__init__(**kwargs)
        
        # Set default configuration
        self.conf["host"] = kwargs.get("host", "localhost")
        self.conf["port"] = kwargs.get("port", 6379)
        self.conf["db"] = kwargs.get("db", 0)
        self.conf["password"] = kwargs.get("password", kwargs.get("passwd", None))
        self.conf["username"] = kwargs.get("username", None)
        self.conf["decode_responses"] = kwargs.get("decode_responses", True)
        self.conf["socket_timeout"] = kwargs.get("socket_timeout", 5.0)
        self.conf["socket_connect_timeout"] = kwargs.get("socket_connect_timeout", 5.0)
        self.conf["client_name"] = kwargs.get("client_name", "SimplerDB")
        self.conf["health_check_interval"] = kwargs.get("health_check_interval", 30.0)
        self.conf["retry_on_timeout"] = kwargs.get("retry_on_timeout", True)
        self.conf["retry_on_error"] = kwargs.get("retry_on_error", [])
        self.conf["max_connections"] = kwargs.get("max_connections", None)
        self.conf["use_connection_pool"] = kwargs.get("use_connection_pool", True)
        
        # Initialize connection and redis object
        self.conn = None
        self.redis = None
        self._last_query = ""
        
        try:
            import redis
            self.redis_module = redis
        except ImportError:
            raise ConfigurationError(
                "redis is required for Redis connections. "
                "Install it with: pip install redis"
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
                "db": self.conf["db"],
                "decode_responses": self.conf["decode_responses"],
                "socket_timeout": self.conf["socket_timeout"],
                "socket_connect_timeout": self.conf["socket_connect_timeout"],
                "client_name": self.conf["client_name"],
                "health_check_interval": self.conf["health_check_interval"],
                "retry_on_timeout": self.conf["retry_on_timeout"],
            }
            
            # Add optional parameters
            if self.conf["password"]:
                conn_params["password"] = self.conf["password"]
            
            if self.conf["username"]:
                conn_params["username"] = self.conf["username"]
            
            if self.conf["retry_on_error"]:
                conn_params["retry_on_error"] = self.conf["retry_on_error"]
            
            if self.conf["use_connection_pool"]:
                # Use connection pool
                pool = self.redis_module.ConnectionPool(
                    **conn_params,
                    max_connections=self.conf["max_connections"]
                )
                self.redis = self.redis_module.Redis(connection_pool=pool)
            else:
                # Direct connection
                self.redis = self.redis_module.Redis(**conn_params)
            
            # Test the connection
            self.redis.ping()
            
            logger.debug(f"Connected to Redis database {self.conf['db']} at {self.conf['host']}:{self.conf['port']}")
            
        except Exception as e:
            logger.error(f"Redis connection failed: {str(e)}")
            raise ConnectionError(f"Failed to connect to Redis database: {str(e)}") from e
    
    def disconnect(self) -> None:
        """Close the database connection."""
        if self.redis:
            try:
                # Close client connections
                self.redis.close()
                # If using connection pool, disconnect all connections in the pool
                if hasattr(self.redis, 'connection_pool'):
                    self.redis.connection_pool.disconnect()
            except Exception as e:
                logger.warning(f"Error closing Redis connection: {e}")
            
            self.redis = None
            
        logger.debug("Redis connection closed")
    
    def is_connected(self) -> bool:
        """Check if the database connection is active."""
        if not self.redis:
            return False
        
        try:
            # Test the connection with a ping
            self.redis.ping()
            return True
        except Exception:
            return False
    
    def execute(self, query: str, params: Optional[Union[List, Tuple, Dict]] = None) -> Any:
        """
        Execute a Redis command.
        
        For Redis, this is a pass-through to execute raw Redis commands.
        
        Args:
            query: Redis command as a string
            params: Command parameters
            
        Returns:
            Redis command result
            
        Raises:
            QueryError: If the command execution fails
        """
        self._last_query = query
        
        # Ensure we're connected
        if not self.is_connected():
            self.connect()
        
        try:
            # Parse the command and any parameters
            command_parts = query.split()
            command = command_parts[0].lower()
            
            # Add any parameters from the query string
            args = command_parts[1:] if len(command_parts) > 1 else []
            
            # Add any params passed as arguments
            if params:
                if isinstance(params, (list, tuple)):
                    args.extend(params)
                elif isinstance(params, dict):
                    # For dict params, alternate keys and values
                    for k, v in params.items():
                        args.extend([k, v])
            
            # Execute the command
            if hasattr(self.redis, command):
                redis_command = getattr(self.redis, command)
                if callable(redis_command):
                    return redis_command(*args)
                return redis_command
            else:
                # Fall back to generic command execution
                return self.redis.execute_command(command, *args)
            
        except Exception as e:
            # Log the error
            logger.error(f"Redis command execution failed: {e}")
            logger.debug(f"Command: {query}")
            if params:
                logger.debug(f"Parameters: {params}")
            
            # Raise a QueryError
            raise QueryError(f"Redis command execution failed: {e}") from e
    
    def fetch_one(self) -> Optional[Dict[str, Any]]:
        """Not applicable for Redis."""
        raise FeatureNotSupportedError("fetch_one() is not applicable for Redis. Use get() instead.")
    
    def fetch_all(self) -> List[Dict[str, Any]]:
        """Not applicable for Redis."""
        raise FeatureNotSupportedError("fetch_all() is not applicable for Redis. Use specific Redis commands instead.")
    
    def fetch_iter(self, batch_size: int = 1000) -> Iterator[Dict[str, Any]]:
        """Not applicable for Redis."""
        raise FeatureNotSupportedError("fetch_iter() is not applicable for Redis.")
    
    def last_insert_id(self) -> Optional[int]:
        """Not applicable for Redis."""
        raise FeatureNotSupportedError("last_insert_id() is not applicable for Redis.")
    
    def last_query(self) -> str:
        """Get the last executed command."""
        return self._last_query
    
    def commit(self) -> None:
        """Not applicable for Redis as it's not transactional by default."""
        # Redis MULTI/EXEC transactions are handled directly with those commands
        logger.debug("No-op: Redis does not use traditional commit")
    
    def rollback(self) -> None:
        """Not applicable for Redis as it's not transactional by default."""
        # Redis MULTI/EXEC transactions are handled directly with those commands
        logger.debug("No-op: Redis does not use traditional rollback")
    
    def get(self, key: str) -> Any:
        """
        Get a value by key.
        
        Args:
            key: Redis key
            
        Returns:
            Value associated with the key or None
        """
        # Ensure we're connected
        if not self.is_connected():
            self.connect()
        
        try:
            return self.redis.get(key)
        except Exception as e:
            logger.error(f"Failed to get key {key}: {e}")
            raise QueryError(f"Failed to get key {key}: {e}") from e
    
    def set(self, key: str, value: Any, ex: Optional[int] = None, 
           px: Optional[int] = None, nx: bool = False, xx: bool = False) -> bool:
        """
        Set a key-value pair.
        
        Args:
            key: Redis key
            value: Value to set
            ex: Expire time in seconds
            px: Expire time in milliseconds
            nx: Only set if key does not exist
            xx: Only set if key exists
            
        Returns:
            True if successful
        """
        # Ensure we're connected
        if not self.is_connected():
            self.connect()
        
        try:
            return bool(self.redis.set(key, value, ex=ex, px=px, nx=nx, xx=xx))
        except Exception as e:
            logger.error(f"Failed to set key {key}: {e}")
            raise QueryError(f"Failed to set key {key}: {e}") from e
    
    def delete(self, *keys: str) -> int:
        """
        Delete one or more keys.
        
        Args:
            *keys: Keys to delete
            
        Returns:
            Number of keys deleted
        """
        # Ensure we're connected
        if not self.is_connected():
            self.connect()
        
        try:
            return self.redis.delete(*keys)
        except Exception as e:
            logger.error(f"Failed to delete keys: {e}")
            raise QueryError(f"Failed to delete keys: {e}") from e
    
    def exists(self, *keys: str) -> int:
        """
        Check if one or more keys exist.
        
        Args:
            *keys: Keys to check
            
        Returns:
            Number of keys that exist
        """
        # Ensure we're connected
        if not self.is_connected():
            self.connect()
        
        try:
            return self.redis.exists(*keys)
        except Exception as e:
            logger.error(f"Failed to check if keys exist: {e}")
            raise QueryError(f"Failed to check if keys exist: {e}") from e
    
    def expire(self, key: str, seconds: int) -> bool:
        """
        Set a key's time to live in seconds.
        
        Args:
            key: Redis key
            seconds: Time to live in seconds
            
        Returns:
            True if successful
        """
        # Ensure we're connected
        if not self.is_connected():
            self.connect()
        
        try:
            return bool(self.redis.expire(key, seconds))
        except Exception as e:
            logger.error(f"Failed to set expiry for key {key}: {e}")
            raise QueryError(f"Failed to set expiry for key {key}: {e}") from e
    
    def ttl(self, key: str) -> int:
        """
        Get the time to live for a key in seconds.
        
        Args:
            key: Redis key
            
        Returns:
            TTL in seconds, -1 if no expiry, -2 if key does not exist
        """
        # Ensure we're connected
        if not self.is_connected():
            self.connect()
        
        try:
            return self.redis.ttl(key)
        except Exception as e:
            logger.error(f"Failed to get TTL for key {key}: {e}")
            raise QueryError(f"Failed to get TTL for key {key}: {e}") from e
    
    def keys(self, pattern: str = '*') -> List[str]:
        """
        Find all keys matching a pattern.
        
        WARNING: This command should not be used in production as it may block Redis.
        
        Args:
            pattern: Key pattern to match
            
        Returns:
            List of matching keys
        """
        # Ensure we're connected
        if not self.is_connected():
            self.connect()
        
        try:
            return self.redis.keys(pattern)
        except Exception as e:
            logger.error(f"Failed to get keys with pattern {pattern}: {e}")
            raise QueryError(f"Failed to get keys with pattern {pattern}: {e}") from e
    
    def scan(self, cursor: int = 0, match: Optional[str] = None, 
            count: Optional[int] = None) -> Tuple[int, List[str]]:
        """
        Incrementally iterate over keys.
        
        Args:
            cursor: Cursor position
            match: Pattern to match
            count: Number of elements to return per call
            
        Returns:
            Tuple of (next_cursor, keys)
        """
        # Ensure we're connected
        if not self.is_connected():
            self.connect()
        
        try:
            return self.redis.scan(cursor=cursor, match=match, count=count)
        except Exception as e:
            logger.error(f"Failed to scan keys: {e}")
            raise QueryError(f"Failed to scan keys: {e}") from e
    
    def scan_iter(self, match: Optional[str] = None, count: Optional[int] = None) -> Iterator[str]:
        """
        Incrementally iterate over keys as an iterator.
        
        Args:
            match: Pattern to match
            count: Number of elements to return per call
            
        Returns:
            Iterator over keys
        """
        # Ensure we're connected
        if not self.is_connected():
            self.connect()
        
        try:
            return self.redis.scan_iter(match=match, count=count)
        except Exception as e:
            logger.error(f"Failed to create scan iterator: {e}")
            raise QueryError(f"Failed to create scan iterator: {e}") from e
    
    # Hash operations
    
    def hset(self, name: str, key: str, value: Any) -> int:
        """
        Set a hash field to a value.
        
        Args:
            name: Hash name
            key: Field name
            value: Field value
            
        Returns:
            1 if field is new, 0 if field existed
        """
        # Ensure we're connected
        if not self.is_connected():
            self.connect()
        
        try:
            return self.redis.hset(name, key, value)
        except Exception as e:
            logger.error(f"Failed to set hash field {key} in {name}: {e}")
            raise QueryError(f"Failed to set hash field {key} in {name}: {e}") from e
    
    def hget(self, name: str, key: str) -> Any:
        """
        Get the value of a hash field.
        
        Args:
            name: Hash name
            key: Field name
            
        Returns:
            Field value or None
        """
        # Ensure we're connected
        if not self.is_connected():
            self.connect()
        
        try:
            return self.redis.hget(name, key)
        except Exception as e:
            logger.error(f"Failed to get hash field {key} from {name}: {e}")
            raise QueryError(f"Failed to get hash field {key} from {name}: {e}") from e
    
    def hgetall(self, name: str) -> Dict[str, Any]:
        """
        Get all fields and values in a hash.
        
        Args:
            name: Hash name
            
        Returns:
            Dictionary of field-value pairs
        """
        # Ensure we're connected
        if not self.is_connected():
            self.connect()
        
        try:
            return self.redis.hgetall(name)
        except Exception as e:
            logger.error(f"Failed to get all fields from hash {name}: {e}")
            raise QueryError(f"Failed to get all fields from hash {name}: {e}") from e
    
    def hdel(self, name: str, *keys: str) -> int:
        """
        Delete one or more hash fields.
        
        Args:
            name: Hash name
            *keys: Field names to delete
            
        Returns:
            Number of fields deleted
        """
        # Ensure we're connected
        if not self.is_connected():
            self.connect()
        
        try:
            return self.redis.hdel(name, *keys)
        except Exception as e:
            logger.error(f"Failed to delete hash fields from {name}: {e}")
            raise QueryError(f"Failed to delete hash fields from {name}: {e}") from e
    
    def hexists(self, name: str, key: str) -> bool:
        """
        Check if a hash field exists.
        
        Args:
            name: Hash name
            key: Field name
            
        Returns:
            True if the field exists
        """
        # Ensure we're connected
        if not self.is_connected():
            self.connect()
        
        try:
            return bool(self.redis.hexists(name, key))
        except Exception as e:
            logger.error(f"Failed to check if hash field {key} exists in {name}: {e}")
            raise QueryError(f"Failed to check if hash field {key} exists in {name}: {e}") from e
    
    # List operations
    
    def lpush(self, name: str, *values: Any) -> int:
        """
        Prepend values to a list.
        
        Args:
            name: List name
            *values: Values to prepend
            
        Returns:
            Length of the list after operation
        """
        # Ensure we're connected
        if not self.is_connected():
            self.connect()
        
        try:
            return self.redis.lpush(name, *values)
        except Exception as e:
            logger.error(f"Failed to push values to list {name}: {e}")
            raise QueryError(f"Failed to push values to list {name}: {e}") from e
    
    def rpush(self, name: str, *values: Any) -> int:
        """
        Append values to a list.
        
        Args:
            name: List name
            *values: Values to append
            
        Returns:
            Length of the list after operation
        """
        # Ensure we're connected
        if not self.is_connected():
            self.connect()
        
        try:
            return self.redis.rpush(name, *values)
        except Exception as e:
            logger.error(f"Failed to push values to list {name}: {e}")
            raise QueryError(f"Failed to push values to list {name}: {e}") from e
    
    def lrange(self, name: str, start: int, end: int) -> List[Any]:
        """
        Get a range of elements from a list.
        
        Args:
            name: List name
            start: Start index
            end: End index
            
        Returns:
            List of elements
        """
        # Ensure we're connected
        if not self.is_connected():
            self.connect()
        
        try:
            return self.redis.lrange(name, start, end)
        except Exception as e:
            logger.error(f"Failed to get range from list {name}: {e}")
            raise QueryError(f"Failed to get range from list {name}: {e}") from e
    
    # Set operations
    
    def sadd(self, name: str, *values: Any) -> int:
        """
        Add members to a set.
        
        Args:
            name: Set name
            *values: Values to add
            
        Returns:
            Number of members added
        """
        # Ensure we're connected
        if not self.is_connected():
            self.connect()
        
        try:
            return self.redis.sadd(name, *values)
        except Exception as e:
            logger.error(f"Failed to add values to set {name}: {e}")
            raise QueryError(f"Failed to add values to set {name}: {e}") from e
    
    def smembers(self, name: str) -> set:
        """
        Get all members of a set.
        
        Args:
            name: Set name
            
        Returns:
            Set of members
        """
        # Ensure we're connected
        if not self.is_connected():
            self.connect()
        
        try:
            return self.redis.smembers(name)
        except Exception as e:
            logger.error(f"Failed to get members from set {name}: {e}")
            raise QueryError(f"Failed to get members from set {name}: {e}") from e
    
    def srem(self, name: str, *values: Any) -> int:
        """
        Remove members from a set.
        
        Args:
            name: Set name
            *values: Values to remove
            
        Returns:
            Number of members removed
        """
        # Ensure we're connected
        if not self.is_connected():
            self.connect()
        
        try:
            return self.redis.srem(name, *values)
        except Exception as e:
            logger.error(f"Failed to remove values from set {name}: {e}")
            raise QueryError(f"Failed to remove values from set {name}: {e}") from e
    
    # Sorted set operations
    
    def zadd(self, name: str, mapping: Dict[Any, float]) -> int:
        """
        Add members to a sorted set.
        
        Args:
            name: Sorted set name
            mapping: Dictionary mapping members to scores
            
        Returns:
            Number of members added
        """
        # Ensure we're connected
        if not self.is_connected():
            self.connect()
        
        try:
            return self.redis.zadd(name, mapping)
        except Exception as e:
            logger.error(f"Failed to add values to sorted set {name}: {e}")
            raise QueryError(f"Failed to add values to sorted set {name}: {e}") from e
    
    def zrange(self, name: str, start: int, end: int, withscores: bool = False) -> List[Any]:
        """
        Get a range of members from a sorted set.
        
        Args:
            name: Sorted set name
            start: Start index
            end: End index
            withscores: Whether to include scores
            
        Returns:
            List of members or member-score pairs
        """
        # Ensure we're connected
        if not self.is_connected():
            self.connect()
        
        try:
            return self.redis.zrange(name, start, end, withscores=withscores)
        except Exception as e:
            logger.error(f"Failed to get range from sorted set {name}: {e}")
            raise QueryError(f"Failed to get range from sorted set {name}: {e}") from e
    
    # JSON operations (requires RedisJSON module)
    
    def json_set(self, name: str, path: str, obj: Any, nx: bool = False, xx: bool = False) -> bool:
        """
        Set JSON value at path in key.
        
        Args:
            name: Key name
            path: JSON path
            obj: Object to set
            nx: Only set if key does not exist
            xx: Only set if key exists
            
        Returns:
            True if successful
        """
        # Ensure we're connected
        if not self.is_connected():
            self.connect()
        
        try:
            # Check if JSON module is available
            if not hasattr(self.redis, 'json'):
                raise FeatureNotSupportedError("RedisJSON module not available")
            
            # Convert object to JSON string
            json_str = json.dumps(obj)
            
            # Set JSON
            return bool(self.redis.json().set(name, path, json_str, nx=nx, xx=xx))
        except FeatureNotSupportedError:
            raise
        except Exception as e:
            logger.error(f"Failed to set JSON at {path} in {name}: {e}")
            raise QueryError(f"Failed to set JSON at {path} in {name}: {e}") from e
    
    def json_get(self, name: str, path: str = '.') -> Any:
        """
        Get JSON value at path in key.
        
        Args:
            name: Key name
            path: JSON path
            
        Returns:
            JSON value
        """
        # Ensure we're connected
        if not self.is_connected():
            self.connect()
        
        try:
            # Check if JSON module is available
            if not hasattr(self.redis, 'json'):
                raise FeatureNotSupportedError("RedisJSON module not available")
            
            # Get JSON
            result = self.redis.json().get(name, path)
            
            # Parse JSON string if needed
            if isinstance(result, str):
                try:
                    return json.loads(result)
                except json.JSONDecodeError:
                    return result
            
            return result
        except FeatureNotSupportedError:
            raise
        except Exception as e:
            logger.error(f"Failed to get JSON at {path} from {name}: {e}")
            raise QueryError(f"Failed to get JSON at {path} from {name}: {e}") from e
    
    # SimplerDB interface implementations
    
    def get_one(self, table: str, fields: Union[str, List[str]] = '*', 
               where: Optional[Union[str, Tuple, Dict]] = None, 
               order: Optional[Union[str, List]] = None, 
               limit: Optional[Union[int, Tuple[int, int]]] = 1) -> Optional[Dict[str, Any]]:
        """
        Get a single record from Redis.
        
        For Redis, 'table' is treated as a key or key prefix.
        
        Args:
            table: Key or key prefix
            fields: Field(s) to select (hash fields or JSON paths)
            where: Not used for Redis
            order: Not used for Redis
            limit: Not used for Redis
            
        Returns:
            Dictionary with the data
        """
        # Determine the key type
        key_type = self.redis.type(table)
        
        if key_type == b'hash' or key_type == 'hash':
            # For hash, return the hash fields
            if fields == '*':
                return self.hgetall(table)
            else:
                if isinstance(fields, str):
                    fields = [fields]
                
                result = {}
                for field in fields:
                    value = self.hget(table, field)
                    if value is not None:
                        result[field] = value
                
                return result if result else None
                
        elif key_type == b'string' or key_type == 'string':
            # For string, try to parse as JSON
            try:
                value = self.get(table)
                if value:
                    try:
                        # Try to parse as JSON
                        data = json.loads(value)
                        
                        # Filter fields if needed
                        if fields != '*' and isinstance(data, dict):
                            if isinstance(fields, str):
                                fields = [fields]
                            
                            result = {}
                            for field in fields:
                                if field in data:
                                    result[field] = data[field]
                            
                            return result if result else None
                        
                        return data
                    except json.JSONDecodeError:
                        # Not JSON, return as is
                        return {'value': value}
            except Exception as e:
                logger.error(f"Failed to get key {table}: {e}")
                raise QueryError(f"Failed to get key {table}: {e}") from e
                
        elif key_type == b'ReJSON-RL' or key_type == 'ReJSON-RL':
            # For RedisJSON, use json_get
            try:
                if fields == '*':
                    return self.json_get(table)
                else:
                    if isinstance(fields, str):
                        fields = [fields]
                    
                    result = {}
                    for field in fields:
                        path = f'.{field}'
                        value = self.json_get(table, path)
                        if value is not None:
                            result[field] = value
                    
                    return result if result else None
            except Exception as e:
                logger.error(f"Failed to get JSON key {table}: {e}")
                raise QueryError(f"Failed to get JSON key {table}: {e}") from e
                
        return None
    
    def get_all(self, table: str, fields: Union[str, List[str]] = '*', 
               where: Optional[Union[str, Tuple, Dict]] = None, 
               order: Optional[Union[str, List]] = None, 
               limit: Optional[Union[int, Tuple[int, int]]] = None) -> List[Dict[str, Any]]:
        """
        Get multiple records from Redis.
        
        For Redis, 'table' is treated as a key pattern.
        
        Args:
            table: Key pattern
            fields: Field(s) to select (hash fields or JSON paths)
            where: Not used for Redis
            order: Not used for Redis
            limit: Maximum number of records to return
            
        Returns:
            List of dictionaries with the data
        """
        # Safely scan for keys matching the pattern
        try:
            # Get keys matching the pattern
            matching_keys = list(self.scan_iter(match=table))
            
            # Apply limit if specified
            if isinstance(limit, int) and limit > 0:
                matching_keys = matching_keys[:limit]
            elif isinstance(limit, (list, tuple)) and len(limit) >= 2:
                start, count = limit
                matching_keys = matching_keys[start:start+count]
            
            results = []
            
            # Process each key
            for key in matching_keys:
                result = self.get_one(key, fields)
                if result:
                    # Add the key to the result
                    result['_key'] = key
                    results.append(result)
            
            return results
            
        except Exception as e:
            logger.error(f"Failed to get keys matching pattern {table}: {e}")
            raise QueryError(f"Failed to get keys matching pattern {table}: {e}") from e
    
    def insert(self, table: str, data: Dict[str, Any]) -> int:
        """
        Insert a record into Redis.
        
        For Redis, this sets a key with a value.
        
        Args:
            table: Key name
            data: Data to insert
            
        Returns:
            1 if successful, 0 otherwise
        """
        try:
            # Determine how to store the data based on its structure and size
            if len(data) == 1 and 'value' in data:
                # Simple key-value
                result = self.set(table, data['value'])
            elif len(data) > 1:
                # Use a hash for multiple fields
                for key, value in data.items():
                    self.hset(table, key, value)
                result = True
            else:
                # Default to storing as JSON
                result = self.set(table, json.dumps(data))
            
            return 1 if result else 0
        except Exception as e:
            logger.error(f"Failed to insert data for key {table}: {e}")
            raise QueryError(f"Failed to insert data for key {table}: {e}") from e
    
    def update(self, table: str, data: Dict[str, Any], 
              where: Optional[Union[str, Tuple, Dict]] = None) -> int:
        """
        Update a record in Redis.
        
        For Redis, this updates fields in a hash or key.
        
        Args:
            table: Key name
            data: Data to update
            where: Not used for Redis
            
        Returns:
            1 if successful, 0 otherwise
        """
        try:
            # Check if key exists
            if not self.exists(table):
                return 0
            
            # Determine key type
            key_type = self.redis.type(table)
            
            if key_type == b'hash' or key_type == 'hash':
                # Update hash fields
                for key, value in data.items():
                    self.hset(table, key, value)
                return 1
            elif key_type == b'string' or key_type == 'string':
                # For string, try to parse as JSON and update
                value = self.get(table)
                if value:
                    try:
                        # Try to parse as JSON
                        existing_data = json.loads(value)
                        if isinstance(existing_data, dict):
                            # Update the dictionary
                            existing_data.update(data)
                            # Store back
                            self.set(table, json.dumps(existing_data))
                            return 1
                    except json.JSONDecodeError:
                        # Not JSON, replace the value
                        if len(data) == 1 and 'value' in data:
                            self.set(table, data['value'])
                            return 1
            elif key_type == b'ReJSON-RL' or key_type == 'ReJSON-RL':
                # For RedisJSON, update fields
                for key, value in data.items():
                    path = f'.{key}'
                    self.json_set(table, path, value)
                return 1
            
            return 0
        except Exception as e:
            logger.error(f"Failed to update data for key {table}: {e}")
            raise QueryError(f"Failed to update data for key {table}: {e}") from e
    
    def delete(self, table: str, where: Optional[Union[str, Tuple, Dict]] = None) -> int:
        """
        Delete records from Redis.
        
        For Redis, this deletes keys matching a pattern.
        
        Args:
            table: Key or pattern
            where: Not used for Redis
            
        Returns:
            Number of keys deleted
        """
        try:
            if '*' in table or '?' in table or '[' in table:
                # Pattern - get matching keys and delete them
                keys = list(self.scan_iter(match=table))
                if keys:
                    return self.redis.delete(*keys)
                return 0
            else:
                # Single key
                return self.redis.delete(table)
        except Exception as e:
            logger.error(f"Failed to delete key(s) {table}: {e}")
            raise QueryError(f"Failed to delete key(s) {table}: {e}") from e