# SimplerDB

A simple, unified interface for multiple database systems in Python 3.8+.

Developed by: Subhash Dasyam, March 2025

## Features

- **Single API for multiple databases**: Work with different databases using a consistent interface
- **Supported databases**:
  - MySQL, MariaDB, Percona
  - SQLite
  - PostgreSQL (with pgvector support)
  - Microsoft SQL Server
  - Oracle
  - Redis
  - ChromaDB
- **Security-focused**: Protection against SQL injection including table/column name sanitization
- **Connection management**: Pooling, automatic reconnection, and retry mechanisms
- **Transaction support**: Context managers for easy transaction handling
- **Vector database support**: Built-in vector operations for PostgreSQL with pgvector
- **Type hints**: Full type annotation support for better IDE integration
- **Python 3.8+** compatibility

## Installation

### Basic Installation

```bash
pip install simplerdb
```

### With Specific Database Support

```bash
# MySQL/MariaDB support
pip install simplerdb[mysql]

# PostgreSQL support
pip install simplerdb[postgres]

# PostgreSQL with vector support
pip install simplerdb[pgvector]

# Microsoft SQL Server support
pip install simplerdb[mssql]

# Oracle support
pip install simplerdb[oracle]

# Redis support
pip install simplerdb[redis]

# ChromaDB support
pip install simplerdb[chromadb]

# All databases
pip install simplerdb[all]
```

## Quick Start

### Creating Database Connections

```python
from simplerdb import connect

# MySQL connection
mysql_db = connect("mysql", 
                   host="localhost", 
                   db="mydatabase", 
                   user="username", 
                   passwd="password")

# SQLite connection
sqlite_db = connect("sqlite", 
                    database="/path/to/database.db")

# PostgreSQL connection
postgres_db = connect("postgres", 
                      host="localhost", 
                      dbname="mydatabase", 
                      user="username", 
                      password="password")

# Redis connection
redis_db = connect("redis", 
                   host="localhost", 
                   port=6379, 
                   db=0, 
                   decode_responses=True)
```

### Basic CRUD Operations

```python
# Insert a record
db.insert("users", {
    "username": "john_doe",
    "email": "john@example.com",
    "age": 30
})

# Get a single record
user = db.get_one("users", ["username", "email"], {"username": "john_doe"})
print(f"User: {user['username']}, Email: {user['email']}")

# Get multiple records
active_users = db.get_all(
    "users", 
    ["id", "username", "email"], 
    ("status = %s", ["active"]), 
    ["created_at", "DESC"], 
    [0, 10]  # LIMIT 0, 10
)

# Update records
db.update(
    "users",
    {"status": "inactive", "updated_at": "2025-03-19"},
    {"username": "john_doe"}
)

# Delete records
db.delete("users", {"username": "john_doe"})
```

### Using Transactions

```python
# Using context manager for transactions
with db.transaction():
    db.insert("orders", {"user_id": 42, "total": 99.99})
    order_id = db.last_insert_id()
    db.insert("order_items", {"order_id": order_id, "product_id": 101, "quantity": 2})
    # Automatically commits on success, rolls back on exception
```

### Vector Operations with pgvector

```python
from simplerdb import connect

# Connect to PostgreSQL with pgvector extension
db = connect("pgvector", 
             host="localhost", 
             dbname="vectordb", 
             user="username", 
             password="password")

# Create a table with a vector column
db.create_vector_table(
    "documents",
    "embedding",
    dimensions=384,
    extra_columns={
        "title": "VARCHAR(255) NOT NULL",
        "content": "TEXT",
        "metadata": "JSONB"
    }
)

# Insert a record with a vector
db.insert("documents", {
    "title": "Sample Document",
    "content": "This is a sample document for vector search",
    "embedding": [0.1, 0.2, 0.3, ...],  # 384-dimensional vector
    "metadata": {"source": "example", "tags": ["sample", "demo"]}
})

# Perform a similarity search
query_vector = [0.15, 0.25, 0.35, ...]  # Your query vector
results = db.similarity_search(
    "documents",
    "embedding",
    query_vector,
    limit=5,
    where={"metadata->>'source'": "example"},
    similarity_measure="cosine"
)

# Process results
for doc in results:
    print(f"Document: {doc['title']}, Similarity: {doc['similarity_score']}")
```

### Working with Redis

```python
from simplerdb import connect

# Connect to Redis
db = connect("redis", 
             host="localhost", 
             port=6379, 
             db=0, 
             decode_responses=True)

# Set a simple key-value
db.set("user:1001", json.dumps({
    "username": "john_doe",
    "email": "john@example.com",
    "age": 30
}))

# Get a value
user_data = json.loads(db.get("user:1001"))

# Hash operations
db.hset("user:1002", "username", "jane_doe")
db.hset("user:1002", "email", "jane@example.com")
db.hset("user:1002", "age", 28)

# Get all hash fields
user = db.hgetall("user:1002")

# Using SimplerDB-style interface
db.insert("user:1003", {
    "username": "alice",
    "email": "alice@example.com",
    "age": 35
})

# Get multiple users by pattern
users = db.get_all("user:*")
```

## Database-Specific Features

Each database connector provides specialized methods and features beyond the common interface.

### MySQL/MariaDB Specific

```python
# Batch update with optimized SQL
db.update_batch("products", [
    {"id": 1, "price": 19.99, "stock": 100},
    {"id": 2, "price": 29.99, "stock": 50},
    {"id": 3, "price": 39.99, "stock": 75}
], "id")

# Insert or update (UPSERT)
db.insert_or_update("products", 
                    {"id": 4, "name": "New Product", "price": 49.99}, 
                    "id")
```

### PostgreSQL Specific

```python
# Execute a stored procedure
db.execute_procedure("update_inventory", [101, 10])

# Execute a function and get result
total = db.execute_function("calculate_order_total", [1001])

# JSON operations
db.execute("UPDATE users SET preferences = preferences || %s::jsonb WHERE id = %s",
           [json.dumps({"theme": "dark"}), 42])
```

### SQLite Specific

```python
# Execute a SQL script
db.executescript("""
    BEGIN TRANSACTION;
    CREATE TABLE IF NOT EXISTS users (id INTEGER PRIMARY KEY, name TEXT);
    INSERT INTO users (name) VALUES ('Alice');
    INSERT INTO users (name) VALUES ('Bob');
    COMMIT;
""")

# Use pragmas
db.execute("PRAGMA journal_mode = WAL")
db.execute("PRAGMA synchronous = NORMAL")
```

### Redis Specific

```python
# List operations
db.lpush("recent_users", "user:1001", "user:1002")
recent_users = db.lrange("recent_users", 0, -1)

# Set operations
db.sadd("active_users", "user:1001", "user:1003")
active_users = db.smembers("active_users")

# Sorted set operations
db.zadd("user_scores", {"user:1001": 95, "user:1002": 88, "user:1003": 92})
top_users = db.zrange("user_scores", 0, 2, withscores=True)
```

## Advanced Usage

### Connection Pooling

```python
# MySQL with connection pooling
db = connect("mysql", 
             host="localhost", 
             db="mydatabase", 
             user="username", 
             passwd="password",
             pool_size=10)

# PostgreSQL with connection pooling
db = connect("postgres", 
             host="localhost", 
             dbname="mydatabase", 
             user="username", 
             password="password",
             pool_min_size=3,
             pool_max_size=20)
```

### Query Retry Mechanism

```python
from simplerdb import connect

db = connect("mysql", 
             host="localhost", 
             db="mydatabase", 
             user="username", 
             passwd="password")

# Execute with retry for transient failures
try:
    result = db.execute(
        "SELECT * FROM large_table WHERE complex_condition = %s", 
        ["value"], 
        max_retries=3, 
        retry_delay=1.0  # seconds
    )
except Exception as e:
    print(f"Query failed after retries: {e}")
```

### Using Multiple Databases

```python
from simplerdb import connect

# Connect to different databases
mysql_db = connect("mysql", 
                   host="localhost", 
                   db="app_data", 
                   user="username", 
                   passwd="password")

redis_db = connect("redis", 
                   host="localhost", 
                   port=6379, 
                   db=0, 
                   decode_responses=True)

vector_db = connect("pgvector", 
                    host="localhost", 
                    dbname="embeddings", 
                    user="username", 
                    password="password")

# Use each database for its strengths
user = mysql_db.get_one("users", ["id", "username", "email"], {"id": 42})

redis_db.set(f"user:{user['id']}:last_login", datetime.now().isoformat())
redis_db.sadd("active_users", str(user['id']))

document_embeddings = vector_db.similarity_search(
    "documents", 
    "embedding", 
    query_vector,
    where={"user_id": user['id']},
    limit=5
)
```

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## License

This project is licensed under the GNU GPLv2 License.