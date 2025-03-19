"""
Exceptions for SimplerDB.
"""

class SimplerDBError(Exception):
    """Base exception for all SimplerDB errors."""
    pass


class ConnectionError(SimplerDBError):
    """Error raised when a database connection cannot be established or is lost."""
    pass


class QueryError(SimplerDBError):
    """Error raised when a query fails to execute."""
    pass


class TransactionError(SimplerDBError):
    """Error raised when a transaction operation (commit, rollback) fails."""
    pass


class ConfigurationError(SimplerDBError):
    """Error raised when there is an issue with the database configuration."""
    pass


class PoolError(SimplerDBError):
    """Error raised when there is an issue with the connection pool."""
    pass


class DatabaseTypeError(SimplerDBError):
    """Error raised when an unsupported database type is specified."""
    pass


class FeatureNotSupportedError(SimplerDBError):
    """Error raised when a feature is not supported by the database type."""
    pass


class AuthenticationError(ConnectionError):
    """Error raised when authentication to the database fails."""
    pass


class TimeoutError(ConnectionError):
    """Error raised when a database operation times out."""
    pass


class SanitizationError(SimplerDBError):
    """Error raised when input sanitization fails."""
    pass


class MigrationError(SimplerDBError):
    """Error raised when a database migration fails."""
    pass


class SchemaError(SimplerDBError):
    """Error raised when there's an issue with the database schema."""
    pass


class DataValidationError(SimplerDBError):
    """Error raised when data validation fails."""
    pass