"""
SimplerDB: A unified interface for multiple database systems.

Supported databases:
- MySQL/MariaDB/Percona
- SQLite
- PostgreSQL (with pgvector support)
- Microsoft SQL Server
- Oracle
- Redis
- ChromaDB

Author: Subhash Dasyam, March 2025
License: GPLv2
"""

from .factory import create_db
from .base import DBInterface
from .transaction import TransactionContext
from .exceptions import (
    SimplerDBError,
    ConnectionError,
    QueryError,
    PoolError,
    TransactionError,
    ConfigurationError
)

__version__ = "1.0.0"
__author__ = "Subhash Dasyam"

# Simplified import for most common databases
from .connectors.mysql import MySQLConnector
from .connectors.sqlite import SQLiteConnector
from .connectors.postgres import PostgreSQLConnector
from .connectors.mssql import MSSQLConnector
from .connectors.oracle import OracleConnector
from .connectors.redis_connector import RedisConnector
from .connectors.chromadb import ChromaDBConnector

# Main function to create a database connection
def connect(db_type, **kwargs):
    """
    Create a new database connection.
    
    Args:
        db_type (str): Type of database to connect to. Options are:
                      'mysql', 'mariadb', 'percona', 'sqlite', 'postgres', 
                      'pgvector', 'mssql', 'oracle', 'redis', 'chromadb'
        **kwargs: Connection parameters specific to the database type
        
    Returns:
        DBInterface: A database connector instance
        
    Raises:
        ConfigurationError: If the database type is not supported or configuration is invalid
    """
    return create_db(db_type, **kwargs)