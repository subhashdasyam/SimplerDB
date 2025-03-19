"""
ChromaDB connector for SimplerDB.
"""

import logging
import json
import uuid
from typing import Dict, List, Tuple, Union, Any, Optional, Iterator, cast

from ..base import NoSQLBaseConnector
from ..exceptions import ConnectionError, QueryError, ConfigurationError, FeatureNotSupportedError

logger = logging.getLogger(__name__)

class ChromaDBConnector(NoSQLBaseConnector):
    """
    Connector for ChromaDB, an open-source vector database.
    """
    
    def __init__(self, **kwargs):
        """
        Initialize the ChromaDB connector.
        
        Args:
            path (str): Path to the persistence directory
            host (str): Host for client connection
            port (int): Port for client connection
            ssl (bool): Whether to use SSL
            headers (dict): Headers for client connection
            settings (dict): ChromaDB settings
            collection_name (str): Default collection name
            persist (bool): Whether to persist the database
            **kwargs: Additional connector-specific parameters
        """
        super().__init__(**kwargs)
        
        # Set default configuration
        self.conf["path"] = kwargs.get("path", "./chroma_db")
        self.conf["host"] = kwargs.get("host", None)
        self.conf["port"] = kwargs.get("port", None)
        self.conf["ssl"] = kwargs.get("ssl", False)
        self.conf["headers"] = kwargs.get("headers", None)
        self.conf["settings"] = kwargs.get("settings", {})
        self.conf["collection_name"] = kwargs.get("collection_name", "default")
        self.conf["persist"] = kwargs.get("persist", True)
        
        # Initialize connection and state
        self.conn = None
        self.client = None
        self.collection = None
        self._last_query = ""
        
        try:
            import chromadb
            self.chromadb = chromadb
        except ImportError:
            raise ConfigurationError(
                "chromadb is required for ChromaDB connections. "
                "Install it with: pip install chromadb"
            )
        
        # Connect to the database
        self.connect()
    
    def connect(self) -> None:
        """Establish a connection to the database."""
        try:
            # Determine client type
            if self.conf["host"] and self.conf["port"]:
                # HTTP client
                self.client = self.chromadb.HttpClient(
                    host=self.conf["host"],
                    port=self.conf["port"],
                    ssl=self.conf["ssl"],
                    headers=self.conf["headers"]
                )
            else:
                # Persistent client
                self.client = self.chromadb.PersistentClient(
                    path=self.conf["path"],
                    settings=self.conf["settings"]
                )
            
            # Get or create the default collection
            self.collection = self.get_or_create_collection(self.conf["collection_name"])
            
            logger.debug(f"Connected to ChromaDB using {type(self.client).__name__}")
            
        except Exception as e:
            logger.error(f"ChromaDB connection failed: {str(e)}")
            raise ConnectionError(f"Failed to connect to ChromaDB: {str(e)}") from e
    
    def disconnect(self) -> None:
        """Close the database connection."""
        # ChromaDB doesn't have an explicit close method
        # But we can reset our connection objects
        self.collection = None
        self.client = None
        
        logger.debug("ChromaDB connection reset")
    
    def is_connected(self) -> bool:
        """Check if the database connection is active."""
        if not self.client or not self.collection:
            return False
        
        try:
            # Test the connection by getting collection count
            self.collection.count()
            return True
        except Exception:
            return False
    
    def execute(self, query: str, params: Optional[Union[List, Tuple, Dict]] = None) -> Any:
        """
        Execute a 'query' in ChromaDB.
        
        For ChromaDB, this is an abstract method that delegates to the appropriate
        ChromaDB client method based on the query 'command'.
        
        Args:
            query: String representation of ChromaDB operation
            params: Parameters for the operation
            
        Returns:
            Result of the operation
            
        Raises:
            QueryError: If the operation fails
        """
        self._last_query = query
        
        # Ensure we're connected
        if not self.is_connected():
            self.connect()
        
        try:
            # Parse the query command
            if not query:
                raise QueryError("Empty query string")
            
            # Split the query into command and arguments
            parts = query.strip().lower().split()
            command = parts[0]
            
            # Convert params to dict if it's None
            if params is None:
                params = {}
            elif not isinstance(params, dict):
                raise QueryError("Parameters must be a dictionary for ChromaDB operations")
            
            # Dispatch to the appropriate method
            if command == "add":
                return self._execute_add(params)
            elif command == "get":
                return self._execute_get(params)
            elif command == "query":
                return self._execute_query(params)
            elif command == "update":
                return self._execute_update(params)
            elif command == "upsert":
                return self._execute_upsert(params)
            elif command == "delete":
                return self._execute_delete(params)
            else:
                raise QueryError(f"Unsupported ChromaDB command: {command}")
            
        except Exception as e:
            logger.error(f"ChromaDB operation failed: {e}")
            logger.debug(f"Query: {query}")
            if params:
                logger.debug(f"Parameters: {params}")
            
            raise QueryError(f"ChromaDB operation failed: {e}") from e
    
    def _execute_add(self, params: Dict[str, Any]) -> Dict[str, Any]:
        """Execute an ADD operation in ChromaDB."""
        # Check required parameters
        if "embeddings" not in params and "documents" not in params:
            raise QueryError("Either 'embeddings' or 'documents' must be provided for ADD operation")
        
        # Get the collection
        collection = self._get_collection(params)
        
        # Set IDs if not provided
        ids = params.get("ids")
        if not ids:
            count = len(params.get("embeddings", params.get("documents", [])))
            ids = [str(uuid.uuid4()) for _ in range(count)]
        
        # Execute the add operation
        collection.add(
            ids=ids,
            embeddings=params.get("embeddings"),
            documents=params.get("documents"),
            metadatas=params.get("metadatas")
        )
        
        return {"success": True, "ids": ids}
    
    def _execute_get(self, params: Dict[str, Any]) -> Dict[str, Any]:
        """Execute a GET operation in ChromaDB."""
        # Get the collection
        collection = self._get_collection(params)
        
        # Execute the get operation
        return collection.get(
            ids=params.get("ids"),
            where=params.get("where"),
            where_document=params.get("where_document"),
            limit=params.get("limit"),
            offset=params.get("offset"),
            include=params.get("include")
        )
    
    def _execute_query(self, params: Dict[str, Any]) -> Dict[str, Any]:
        """Execute a QUERY operation in ChromaDB."""
        # Check required parameters
        if "query_embeddings" not in params and "query_texts" not in params:
            raise QueryError("Either 'query_embeddings' or 'query_texts' must be provided for QUERY operation")
        
        # Get the collection
        collection = self._get_collection(params)
        
        # Execute the query operation
        return collection.query(
            query_embeddings=params.get("query_embeddings"),
            query_texts=params.get("query_texts"),
            n_results=params.get("n_results", 10),
            where=params.get("where"),
            where_document=params.get("where_document"),
            include=params.get("include")
        )
    
    def _execute_update(self, params: Dict[str, Any]) -> Dict[str, Any]:
        """Execute an UPDATE operation in ChromaDB."""
        # Check required parameters
        if "ids" not in params:
            raise QueryError("'ids' must be provided for UPDATE operation")
        
        # Get the collection
        collection = self._get_collection(params)
        
        # Execute the update operation
        collection.update(
            ids=params["ids"],
            embeddings=params.get("embeddings"),
            documents=params.get("documents"),
            metadatas=params.get("metadatas")
        )
        
        return {"success": True}
    
    def _execute_upsert(self, params: Dict[str, Any]) -> Dict[str, Any]:
        """Execute an UPSERT operation in ChromaDB."""
        # Check required parameters
        if "ids" not in params:
            raise QueryError("'ids' must be provided for UPSERT operation")
        if "embeddings" not in params and "documents" not in params:
            raise QueryError("Either 'embeddings' or 'documents' must be provided for UPSERT operation")
        
        # Get the collection
        collection = self._get_collection(params)
        
        # Execute the upsert operation
        collection.upsert(
            ids=params["ids"],
            embeddings=params.get("embeddings"),
            documents=params.get("documents"),
            metadatas=params.get("metadatas")
        )
        
        return {"success": True}
    
    def _execute_delete(self, params: Dict[str, Any]) -> Dict[str, Any]:
        """Execute a DELETE operation in ChromaDB."""
        # Get the collection
        collection = self._get_collection(params)
        
        # Execute the delete operation
        collection.delete(
            ids=params.get("ids"),
            where=params.get("where"),
            where_document=params.get("where_document")
        )
        
        return {"success": True}
    
    def _get_collection(self, params: Dict[str, Any]) -> Any:
        """
        Get the collection to operate on.
        
        If collection_name is specified in params, use that collection.
        Otherwise, use the default collection.
        """
        collection_name = params.pop("collection_name", None)
        if collection_name and collection_name != self.conf["collection_name"]:
            return self.get_or_create_collection(collection_name)
        
        return self.collection
    
    def fetch_one(self) -> Optional[Dict[str, Any]]:
        """Not applicable for ChromaDB."""
        raise FeatureNotSupportedError("fetch_one() is not applicable for ChromaDB.")
    
    def fetch_all(self) -> List[Dict[str, Any]]:
        """Not applicable for ChromaDB."""
        raise FeatureNotSupportedError("fetch_all() is not applicable for ChromaDB.")
    
    def fetch_iter(self, batch_size: int = 1000) -> Iterator[Dict[str, Any]]:
        """Not applicable for ChromaDB."""
        raise FeatureNotSupportedError("fetch_iter() is not applicable for ChromaDB.")
    
    def last_insert_id(self) -> Optional[int]:
        """Not applicable for ChromaDB."""
        raise FeatureNotSupportedError("last_insert_id() is not applicable for ChromaDB.")
    
    def last_query(self) -> str:
        """Get the last executed query."""
        return self._last_query
    
    def commit(self) -> None:
        """
        ChromaDB uses an explicit persistence model.
        
        If persist=True was set during initialization, data is automatically persisted.
        """
        # No-op for ChromaDB
        pass
    
    def rollback(self) -> None:
        """
        ChromaDB doesn't support traditional transactions.
        """
        # No-op for ChromaDB
        pass
    
    def get_or_create_collection(self, name: str, embedding_function=None, 
                               metadata: Optional[Dict[str, Any]] = None) -> Any:
        """
        Get or create a ChromaDB collection.
        
        Args:
            name: Collection name
            embedding_function: Function to convert documents to embeddings
            metadata: Collection metadata
            
        Returns:
            ChromaDB collection
        """
        try:
            # Try to get existing collection
            return self.client.get_collection(name=name, embedding_function=embedding_function)
        except Exception:
            # Create a new collection
            return self.client.create_collection(
                name=name,
                embedding_function=embedding_function,
                metadata=metadata
            )
    
    def delete_collection(self, name: str) -> bool:
        """
        Delete a ChromaDB collection.
        
        Args:
            name: Collection name
            
        Returns:
            True if successful
        """
        try:
            self.client.delete_collection(name=name)
            # If the deleted collection was the current one, reset it
            if self.collection and self.collection.name == name:
                self.collection = self.get_or_create_collection(self.conf["collection_name"])
            return True
        except Exception as e:
            logger.error(f"Failed to delete collection {name}: {e}")
            return False
    
    def list_collections(self) -> List[Dict[str, Any]]:
        """
        List all collections.
        
        Returns:
            List of collection information
        """
        try:
            collections = self.client.list_collections()
            return [{"name": c.name, "metadata": c.metadata} for c in collections]
        except Exception as e:
            logger.error(f"Failed to list collections: {e}")
            return []
    
    def count(self, collection_name: Optional[str] = None) -> int:
        """
        Get the number of items in a collection.
        
        Args:
            collection_name: Collection name (or None for default collection)
            
        Returns:
            Number of items in the collection
        """
        try:
            if collection_name:
                collection = self.get_or_create_collection(collection_name)
                return collection.count()
            
            return self.collection.count()
        except Exception as e:
            logger.error(f"Failed to get count for collection: {e}")
            return 0
    
    def get_nearest_neighbors(self, query_embedding: List[float], n_results: int = 10, 
                             collection_name: Optional[str] = None, 
                             where: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """
        Find nearest neighbors to a query embedding.
        
        Args:
            query_embedding: Vector to search for
            n_results: Number of results to return
            collection_name: Collection name (or None for default collection)
            where: Filter condition
            
        Returns:
            Query results
        """
        params = {
            "query_embeddings": [query_embedding],
            "n_results": n_results
        }
        
        if collection_name:
            params["collection_name"] = collection_name
        
        if where:
            params["where"] = where
        
        return self._execute_query(params)
    
    def get_nearest_neighbors_by_text(self, query_text: str, n_results: int = 10, 
                                    collection_name: Optional[str] = None, 
                                    where: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """
        Find nearest neighbors to a query text.
        
        Args:
            query_text: Text to search for
            n_results: Number of results to return
            collection_name: Collection name (or None for default collection)
            where: Filter condition
            
        Returns:
            Query results
        """
        params = {
            "query_texts": [query_text],
            "n_results": n_results
        }
        
        if collection_name:
            params["collection_name"] = collection_name
        
        if where:
            params["where"] = where
        
        return self._execute_query(params)
    
    def add_documents(self, documents: List[str], ids: Optional[List[str]] = None, 
                     metadatas: Optional[List[Dict[str, Any]]] = None, 
                     collection_name: Optional[str] = None) -> Dict[str, Any]:
        """
        Add documents to a collection.
        
        Args:
            documents: List of document texts
            ids: List of IDs (or None for auto-generation)
            metadatas: List of metadata dictionaries
            collection_name: Collection name (or None for default collection)
            
        Returns:
            Result of the operation
        """
        params = {
            "documents": documents
        }
        
        if ids:
            params["ids"] = ids
        
        if metadatas:
            params["metadatas"] = metadatas
        
        if collection_name:
            params["collection_name"] = collection_name
        
        return self._execute_add(params)
    
    def add_embeddings(self, embeddings: List[List[float]], ids: Optional[List[str]] = None, 
                      metadatas: Optional[List[Dict[str, Any]]] = None, 
                      documents: Optional[List[str]] = None, 
                      collection_name: Optional[str] = None) -> Dict[str, Any]:
        """
        Add embeddings to a collection.
        
        Args:
            embeddings: List of embedding vectors
            ids: List of IDs (or None for auto-generation)
            metadatas: List of metadata dictionaries
            documents: List of document texts
            collection_name: Collection name (or None for default collection)
            
        Returns:
            Result of the operation
        """
        params = {
            "embeddings": embeddings
        }
        
        if ids:
            params["ids"] = ids
        
        if metadatas:
            params["metadatas"] = metadatas
        
        if documents:
            params["documents"] = documents
        
        if collection_name:
            params["collection_name"] = collection_name
        
        return self._execute_add(params)
    
    def get_documents(self, ids: Optional[List[str]] = None, 
                     where: Optional[Dict[str, Any]] = None, 
                     limit: Optional[int] = None, 
                     collection_name: Optional[str] = None) -> Dict[str, Any]:
        """
        Get documents from a collection.
        
        Args:
            ids: List of IDs to retrieve
            where: Filter condition
            limit: Maximum number of results
            collection_name: Collection name (or None for default collection)
            
        Returns:
            Documents matching the criteria
        """
        params = {
            "include": ["documents", "metadatas", "embeddings"]
        }
        
        if ids:
            params["ids"] = ids
        
        if where:
            params["where"] = where
        
        if limit:
            params["limit"] = limit
        
        if collection_name:
            params["collection_name"] = collection_name
        
        return self._execute_get(params)
    
    # SimplerDB interface implementations
    
    def get_one(self, table: str, fields: Union[str, List[str]] = '*', 
               where: Optional[Union[str, Tuple, Dict]] = None, 
               order: Optional[Union[str, List]] = None, 
               limit: Optional[Union[int, Tuple[int, int]]] = 1) -> Optional[Dict[str, Any]]:
        """
        Get a single record from ChromaDB.
        
        For ChromaDB, 'table' is treated as the collection name.
        
        Args:
            table: Collection name
            fields: List of fields to include
            where: Filter condition
            order: Not used for ChromaDB
            limit: Not used for ChromaDB
            
        Returns:
            Single document or None
        """
        # Process fields
        include = []
        if fields == '*':
            include = ["documents", "metadatas", "embeddings"]
        else:
            if isinstance(fields, str):
                fields = [fields]
            
            # Map SimplerDB field names to ChromaDB field names
            field_mapping = {
                "id": "ids",
                "ids": "ids",
                "document": "documents",
                "documents": "documents",
                "metadata": "metadatas",
                "metadatas": "metadatas",
                "embedding": "embeddings",
                "embeddings": "embeddings"
            }
            
            for field in fields:
                mapped_field = field_mapping.get(field.lower())
                if mapped_field and mapped_field not in include:
                    include.append(mapped_field)
        
        # Process where condition
        chroma_where = None
        if isinstance(where, dict):
            chroma_where = where
        elif isinstance(where, (list, tuple)) and len(where) >= 2:
            # Try to convert SimplerDB where format to ChromaDB where
            # This is very limited and only works for simple cases
            if isinstance(where[1], dict):
                chroma_where = where[1]
        
        # Get documents
        params = {
            "collection_name": table,
            "include": include,
            "limit": 1
        }
        
        if chroma_where:
            params["where"] = chroma_where
        
        result = self._execute_get(params)
        
        # Check if we got results
        if not result or not result.get("ids") or len(result["ids"]) == 0:
            return None
        
        # Convert to a single document
        document = {}
        for key, values in result.items():
            if values and len(values) > 0:
                document[key] = values[0]
        
        return document
    
    def get_all(self, table: str, fields: Union[str, List[str]] = '*', 
               where: Optional[Union[str, Tuple, Dict]] = None, 
               order: Optional[Union[str, List]] = None, 
               limit: Optional[Union[int, Tuple[int, int]]] = None) -> List[Dict[str, Any]]:
        """
        Get multiple records from ChromaDB.
        
        For ChromaDB, 'table' is treated as the collection name.
        
        Args:
            table: Collection name
            fields: List of fields to include
            where: Filter condition
            order: Not used for ChromaDB
            limit: Maximum number of results
            
        Returns:
            List of documents
        """
        # Process fields
        include = []
        if fields == '*':
            include = ["documents", "metadatas", "embeddings"]
        else:
            if isinstance(fields, str):
                fields = [fields]
            
            # Map SimplerDB field names to ChromaDB field names
            field_mapping = {
                "id": "ids",
                "ids": "ids",
                "document": "documents",
                "documents": "documents",
                "metadata": "metadatas",
                "metadatas": "metadatas",
                "embedding": "embeddings",
                "embeddings": "embeddings"
            }
            
            for field in fields:
                mapped_field = field_mapping.get(field.lower())
                if mapped_field and mapped_field not in include:
                    include.append(mapped_field)
        
        # Process where condition
        chroma_where = None
        if isinstance(where, dict):
            chroma_where = where
        elif isinstance(where, (list, tuple)) and len(where) >= 2:
            # Try to convert SimplerDB where format to ChromaDB where
            # This is very limited and only works for simple cases
            if isinstance(where[1], dict):
                chroma_where = where[1]
        
        # Process limit
        chroma_limit = None
        if isinstance(limit, int):
            chroma_limit = limit
        elif isinstance(limit, (list, tuple)) and len(limit) >= 1:
            chroma_limit = limit[0]
        
        # Get documents
        params = {
            "collection_name": table,
            "include": include
        }
        
        if chroma_where:
            params["where"] = chroma_where
        
        if chroma_limit:
            params["limit"] = chroma_limit
        
        result = self._execute_get(params)
        
        # Check if we got results
        if not result or not result.get("ids") or len(result["ids"]) == 0:
            return []
        
        # Convert to a list of documents
        documents = []
        for i in range(len(result["ids"])):
            document = {}
            for key, values in result.items():
                if values and len(values) > i:
                    document[key] = values[i]
            documents.append(document)
        
        return documents
    
    def insert(self, table: str, data: Dict[str, Any]) -> int:
        """
        Insert a record into ChromaDB.
        
        For ChromaDB, 'table' is treated as the collection name.
        
        Args:
            table: Collection name
            data: Data to insert
            
        Returns:
            1 if successful
        """
        # Extract fields from data
        ids = data.get("id", data.get("ids"))
        if ids and not isinstance(ids, list):
            ids = [ids]
        
        document = data.get("document", data.get("documents"))
        if document and not isinstance(document, list):
            document = [document]
        
        embedding = data.get("embedding", data.get("embeddings"))
        if embedding and not isinstance(embedding, list):
            # Check if it's already a nested list
            if isinstance(embedding[0], (list, tuple)):
                embedding = embedding
            else:
                embedding = [embedding]
        
        metadata = data.get("metadata", data.get("metadatas"))
        if metadata and not isinstance(metadata, list):
            metadata = [metadata]
        
        # Build parameters
        params = {
            "collection_name": table
        }
        
        if ids:
            params["ids"] = ids
        
        if document:
            params["documents"] = document
        
        if embedding:
            params["embeddings"] = embedding
        
        if metadata:
            params["metadatas"] = metadata
        
        # Execute add operation
        self._execute_add(params)
        
        return 1
    
    def insert_batch(self, table: str, data: List[Dict[str, Any]]) -> int:
        """
        Insert multiple records into ChromaDB.
        
        For ChromaDB, 'table' is treated as the collection name.
        
        Args:
            table: Collection name
            data: List of data dictionaries
            
        Returns:
            Number of records inserted
        """
        # Extract fields from data
        ids = []
        documents = []
        embeddings = []
        metadatas = []
        
        # Process each record
        for record in data:
            # Extract ID
            record_id = record.get("id", record.get("ids"))
            if not record_id:
                record_id = str(uuid.uuid4())
            ids.append(record_id)
            
            # Extract document
            document = record.get("document", record.get("documents"))
            if document is not None:
                documents.append(document)
            else:
                documents.append(None)
            
            # Extract embedding
            embedding = record.get("embedding", record.get("embeddings"))
            if embedding is not None:
                embeddings.append(embedding)
            else:
                embeddings.append(None)
            
            # Extract metadata
            metadata = record.get("metadata", record.get("metadatas"))
            if metadata is not None:
                metadatas.append(metadata)
            else:
                metadatas.append(None)
        
        # Build parameters
        params = {
            "collection_name": table,
            "ids": ids
        }
        
        # Only include non-empty lists
        if any(doc is not None for doc in documents):
            params["documents"] = [doc for doc in documents if doc is not None]
        
        if any(emb is not None for emb in embeddings):
            params["embeddings"] = [emb for emb in embeddings if emb is not None]
        
        if any(meta is not None for meta in metadatas):
            params["metadatas"] = [meta for meta in metadatas if meta is not None]
        
        # Execute add operation
        self._execute_add(params)
        
        return len(ids)
    
    def update(self, table: str, data: Dict[str, Any], where: Optional[Union[str, Tuple, Dict]] = None) -> int:
        """
        Update records in ChromaDB.
        
        For ChromaDB, 'table' is treated as the collection name.
        
        Args:
            table: Collection name
            data: Data to update
            where: Filter condition
            
        Returns:
            Number of records updated
        """
        # For ChromaDB, we need IDs to update records
        # If 'where' contains IDs, use those, otherwise get IDs that match the condition
        ids = None
        
        # Extract IDs from data
        if "id" in data or "ids" in data:
            ids = data.get("id", data.get("ids"))
            if ids and not isinstance(ids, list):
                ids = [ids]
        
        # If we don't have IDs, try to get them from where condition
        if not ids and where:
            if isinstance(where, dict) and "id" in where:
                ids = where["id"]
                if not isinstance(ids, list):
                    ids = [ids]
            elif isinstance(where, (list, tuple)) and len(where) >= 2:
                if isinstance(where[1], dict) and "id" in where[1]:
                    ids = where[1]["id"]
                    if not isinstance(ids, list):
                        ids = [ids]
        
        # If we still don't have IDs, get them by querying with the where condition
        if not ids and where:
            chroma_where = None
            if isinstance(where, dict):
                chroma_where = where
            elif isinstance(where, (list, tuple)) and len(where) >= 2:
                if isinstance(where[1], dict):
                    chroma_where = where[1]
            
            if chroma_where:
                params = {
                    "collection_name": table,
                    "include": ["ids"],
                    "where": chroma_where
                }
                
                result = self._execute_get(params)
                if result and result.get("ids"):
                    ids = result["ids"]
        
        # If we still don't have IDs, we can't update
        if not ids:
            return 0
        
        # Extract fields from data
        document = data.get("document", data.get("documents"))
        if document and not isinstance(document, list):
            document = [document]
        
        embedding = data.get("embedding", data.get("embeddings"))
        if embedding and not isinstance(embedding, list):
            # Check if it's already a nested list
            if isinstance(embedding[0], (list, tuple)):
                embedding = embedding
            else:
                embedding = [embedding]
        
        metadata = data.get("metadata", data.get("metadatas"))
        if metadata and not isinstance(metadata, list):
            metadata = [metadata]
        
        # Build parameters
        params = {
            "collection_name": table,
            "ids": ids
        }
        
        if document:
            params["documents"] = document
        
        if embedding:
            params["embeddings"] = embedding
        
        if metadata:
            params["metadatas"] = metadata
        
        # Execute update operation
        self._execute_update(params)
        
        return len(ids)
    
    def delete(self, table: str, where: Optional[Union[str, Tuple, Dict]] = None) -> int:
        """
        Delete records from ChromaDB.
        
        For ChromaDB, 'table' is treated as the collection name.
        
        Args:
            table: Collection name
            where: Filter condition
            
        Returns:
            Number of records deleted
        """
        # For ChromaDB, we can delete by IDs or by condition
        ids = None
        chroma_where = None
        
        # Extract IDs and where condition
        if where:
            if isinstance(where, dict):
                if "id" in where or "ids" in where:
                    ids = where.get("id", where.get("ids"))
                    if ids and not isinstance(ids, list):
                        ids = [ids]
                else:
                    chroma_where = where
            elif isinstance(where, (list, tuple)) and len(where) >= 2:
                if isinstance(where[1], dict):
                    if "id" in where[1] or "ids" in where[1]:
                        ids = where[1].get("id", where[1].get("ids"))
                        if ids and not isinstance(ids, list):
                            ids = [ids]
                    else:
                        chroma_where = where[1]
        
        # If we don't have any condition, get all documents
        if not ids and not chroma_where:
            # First, count how many documents we have
            count = self.count(table)
            if count == 0:
                return 0
            
            # Get all IDs
            params = {
                "collection_name": table,
                "include": ["ids"]
            }
            
            result = self._execute_get(params)
            if result and result.get("ids"):
                ids = result["ids"]
            else:
                return 0
        
        # Build parameters
        params = {
            "collection_name": table
        }
        
        if ids:
            params["ids"] = ids
        
        if chroma_where:
            params["where"] = chroma_where
        
        # Execute delete operation
        self._execute_delete(params)
        
        return len(ids) if ids else 0