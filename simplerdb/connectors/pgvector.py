"""
pgvector connector for PostgreSQL vector database extension.
"""

import logging
from typing import Dict, List, Tuple, Union, Any, Optional, Iterator, cast

from .postgres import PostgreSQLConnector
from ..exceptions import ConfigurationError, QueryError, FeatureNotSupportedError
from ..utils.sanitization import sanitize_identifier

logger = logging.getLogger(__name__)

class PGVectorConnector(PostgreSQLConnector):
    """
    Connector for PostgreSQL with pgvector extension for vector similarity search.
    
    Extends the PostgreSQL connector with vector-specific operations.
    """
    
    def __init__(self, **kwargs):
        """
        Initialize the pgvector connector.
        
        Args:
            **kwargs: Same parameters as PostgreSQLConnector
        """
        super().__init__(**kwargs)
        
        # Verify pgvector extension is available and install if needed
        try:
            # Check if pgvector extension is already installed
            self.execute("SELECT 1 FROM pg_extension WHERE extname = 'vector'")
            is_installed = self.fetch_one() is not None
            
            if not is_installed and kwargs.get("auto_install_extension", True):
                try:
                    logger.info("Installing pgvector extension...")
                    self.execute("CREATE EXTENSION IF NOT EXISTS vector")
                    self.commit()
                    logger.info("pgvector extension installed successfully")
                except Exception as e:
                    logger.error(f"Failed to install pgvector extension: {e}")
                    raise ConfigurationError(f"Failed to install pgvector extension: {e}") from e
        except Exception as e:
            logger.error(f"Error checking pgvector extension: {e}")
            raise ConfigurationError(f"Error checking pgvector extension: {e}") from e
    
    def create_vector_column(self, table: str, column: str, dimensions: int) -> bool:
        """
        Add a vector column to an existing table.
        
        Args:
            table: Table name
            column: Column name
            dimensions: Vector dimensions
            
        Returns:
            True if successful
        """
        safe_table = sanitize_identifier(table)
        safe_column = sanitize_identifier(column)
        
        try:
            self.execute(f"ALTER TABLE {safe_table} ADD COLUMN {safe_column} vector({dimensions})")
            self.commit()
            return True
        except Exception as e:
            logger.error(f"Failed to create vector column: {e}")
            raise QueryError(f"Failed to create vector column: {e}") from e
    
    def create_vector_index(self, table: str, column: str, index_type: str = 'ivfflat', 
                           index_name: Optional[str] = None, options: Optional[Dict[str, Any]] = None) -> bool:
        """
        Create an index on a vector column.
        
        Args:
            table: Table name
            column: Column name
            index_type: Type of index ('ivfflat', 'hnsw', or 'ivfflat_cosine')
            index_name: Custom index name (optional)
            options: Additional index options
            
        Returns:
            True if successful
        """
        safe_table = sanitize_identifier(table)
        safe_column = sanitize_identifier(column)
        
        # Determine the index method based on the type
        if index_type == 'ivfflat':
            index_method = 'vector_ivfflat_ops'
        elif index_type == 'hnsw':
            index_method = 'vector_hnsw_ops'
        elif index_type == 'ivfflat_cosine':
            index_method = 'vector_cosine_ops'
        else:
            raise ValueError(f"Unsupported index type: {index_type}")
        
        # Generate an index name if not provided
        if not index_name:
            index_name = f"{table}_{column}_{index_type}_idx"
        
        safe_index_name = sanitize_identifier(index_name)
        
        # Build the index options string
        options_str = ""
        if options:
            options_list = []
            for key, value in options.items():
                options_list.append(f"{key} = {value}")
            if options_list:
                options_str = f" WITH ({', '.join(options_list)})"
        
        try:
            query = f"CREATE INDEX {safe_index_name} ON {safe_table} USING {index_type}({safe_column} {index_method}){options_str}"
            self.execute(query)
            self.commit()
            return True
        except Exception as e:
            logger.error(f"Failed to create vector index: {e}")
            raise QueryError(f"Failed to create vector index: {e}") from e
    
    def similarity_search(self, table: str, vector_column: str, query_vector: List[float], 
                         limit: int = 10, where: Optional[Union[str, Tuple, Dict]] = None, 
                         similarity_measure: str = 'l2') -> List[Dict[str, Any]]:
        """
        Perform a vector similarity search.
        
        Args:
            table: Table name
            vector_column: Vector column name
            query_vector: Vector to search for (list of floats)
            limit: Maximum number of results
            where: Additional WHERE conditions
            similarity_measure: Similarity measure ('l2', 'cosine', or 'inner')
            
        Returns:
            List of matching records with similarity scores
        """
        safe_table = sanitize_identifier(table)
        safe_vector_column = sanitize_identifier(vector_column)
        
        # Convert the query vector to a PG vector string
        vector_str = self._convert_to_pg_vector(query_vector)
        
        # Choose the similarity operator based on the measure
        if similarity_measure == 'l2':
            sim_operator = '<->'  # L2 distance (smaller is better)
        elif similarity_measure == 'cosine':
            sim_operator = '<=>'  # Cosine distance (smaller is better)
        elif similarity_measure == 'inner':
            sim_operator = '<#>'  # Negative inner product (smaller is better)
        else:
            raise ValueError(f"Unsupported similarity measure: {similarity_measure}")
        
        # Build the query
        query = f"SELECT *, ({safe_vector_column} {sim_operator} '{vector_str}') AS similarity_score FROM {safe_table}"
        
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
        
        # Add ORDER BY and LIMIT clauses
        query += f" ORDER BY similarity_score LIMIT {limit}"
        
        # Execute the query
        self.execute(query, params)
        
        # Return the results
        return self.fetch_all()
    
    def k_nearest_neighbors(self, table: str, vector_column: str, query_vector: List[float], 
                            k: int = 10, where: Optional[Union[str, Tuple, Dict]] = None,
                            similarity_measure: str = 'l2') -> List[Dict[str, Any]]:
        """
        Find k-nearest neighbors to a vector.
        
        A more explicit alias for similarity_search.
        
        Args:
            table: Table name
            vector_column: Vector column name
            query_vector: Vector to search for (list of floats)
            k: Number of neighbors to return
            where: Additional WHERE conditions
            similarity_measure: Similarity measure ('l2', 'cosine', or 'inner')
            
        Returns:
            List of k-nearest neighbors with similarity scores
        """
        return self.similarity_search(
            table=table,
            vector_column=vector_column,
            query_vector=query_vector,
            limit=k,
            where=where,
            similarity_measure=similarity_measure
        )
    
    def batch_insert_vectors(self, table: str, data: List[Dict[str, Any]], 
                            vector_columns: Union[str, List[str]]) -> int:
        """
        Insert multiple records with vector data.
        
        Args:
            table: Table name
            data: List of dictionaries with field:value pairs
            vector_columns: Vector column name(s)
            
        Returns:
            Number of affected rows
        """
        if not data:
            return 0
        
        # Convert vector column(s) to a list
        if isinstance(vector_columns, str):
            vector_columns = [vector_columns]
        
        # Process vector data - convert Python lists to PG vector format
        for item in data:
            for col in vector_columns:
                if col in item and isinstance(item[col], list):
                    item[col] = self._convert_to_pg_vector(item[col])
        
        # Use the standard batch insert method
        return self.insert_batch(table, data)
    
    def _convert_to_pg_vector(self, vector: List[float]) -> str:
        """
        Convert a Python list to a PostgreSQL vector literal.
        
        Args:
            vector: List of float values
            
        Returns:
            String representation for PostgreSQL
        """
        return f"[{','.join(str(x) for x in vector)}]"
    
    def vector_dot_product(self, vector1: List[float], vector2: List[float]) -> float:
        """
        Calculate the dot product between two vectors.
        
        Args:
            vector1: First vector
            vector2: Second vector
            
        Returns:
            Dot product value
        """
        vec1_str = self._convert_to_pg_vector(vector1)
        vec2_str = self._convert_to_pg_vector(vector2)
        
        self.execute(f"SELECT '{vec1_str}'::vector <#> '{vec2_str}'::vector")
        result = self.fetch_one()
        
        if result:
            # The <#> operator returns negative inner product, so we negate it
            return -float(list(result.values())[0])
        
        return 0.0
    
    def vector_cosine_similarity(self, vector1: List[float], vector2: List[float]) -> float:
        """
        Calculate the cosine similarity between two vectors.
        
        Args:
            vector1: First vector
            vector2: Second vector
            
        Returns:
            Cosine similarity value (1.0 is most similar)
        """
        vec1_str = self._convert_to_pg_vector(vector1)
        vec2_str = self._convert_to_pg_vector(vector2)
        
        self.execute(f"SELECT 1 - ('{vec1_str}'::vector <=> '{vec2_str}'::vector)")
        result = self.fetch_one()
        
        if result:
            return float(list(result.values())[0])
        
        return 0.0
    
    def vector_l2_distance(self, vector1: List[float], vector2: List[float]) -> float:
        """
        Calculate the L2 (Euclidean) distance between two vectors.
        
        Args:
            vector1: First vector
            vector2: Second vector
            
        Returns:
            L2 distance value (0.0 is most similar)
        """
        vec1_str = self._convert_to_pg_vector(vector1)
        vec2_str = self._convert_to_pg_vector(vector2)
        
        self.execute(f"SELECT '{vec1_str}'::vector <-> '{vec2_str}'::vector")
        result = self.fetch_one()
        
        if result:
            return float(list(result.values())[0])
        
        return 0.0
    
    def create_vector_table(self, table: str, vector_column: str, dimensions: int, 
                           extra_columns: Optional[Dict[str, str]] = None,
                           primary_key: str = 'id') -> bool:
        """
        Create a new table with a vector column.
        
        Args:
            table: Table name
            vector_column: Vector column name
            dimensions: Vector dimensions
            extra_columns: Dictionary of column name to type for additional columns
            primary_key: Primary key column name
            
        Returns:
            True if successful
        """
        safe_table = sanitize_identifier(table)
        safe_vector_column = sanitize_identifier(vector_column)
        safe_pk = sanitize_identifier(primary_key)
        
        # Start building the CREATE TABLE statement
        create_table_sql = f"CREATE TABLE {safe_table} ({safe_pk} SERIAL PRIMARY KEY, "
        
        # Add the vector column
        create_table_sql += f"{safe_vector_column} vector({dimensions})"
        
        # Add extra columns if provided
        if extra_columns:
            for col_name, col_type in extra_columns.items():
                safe_col = sanitize_identifier(col_name)
                create_table_sql += f", {safe_col} {col_type}"
        
        # Close the statement
        create_table_sql += ")"
        
        try:
            self.execute(create_table_sql)
            self.commit()
            return True
        except Exception as e:
            logger.error(f"Failed to create vector table: {e}")
            raise QueryError(f"Failed to create vector table: {e}") from e