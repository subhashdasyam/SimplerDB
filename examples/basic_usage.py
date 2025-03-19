#!/usr/bin/env python3
"""
Basic usage examples for SimplerDB.

This script demonstrates how to use SimplerDB with various database types.
"""

import os
import sys
import logging

# Add the parent directory to the Python path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from simplerdb import connect
from simplerdb.utils.logging import configure_logger

# Configure logging
configure_logger(level=logging.INFO)
logger = logging.getLogger("simplerdb")

def test_sqlite():
    """Test SQLite connection."""
    print("\n===== Testing SQLite Connection =====")
    
    # Connect to an in-memory SQLite database
    db = connect("sqlite", database=":memory:")
    
    # Create a test table
    db.execute("""
    CREATE TABLE users (
        id INTEGER PRIMARY KEY,
        username TEXT NOT NULL,
        email TEXT NOT NULL,
        age INTEGER
    )
    """)
    
    # Insert some data
    db.insert("users", {
        "username": "john_doe",
        "email": "john@example.com",
        "age": 30
    })
    
    db.insert("users", {
        "username": "jane_doe",
        "email": "jane@example.com",
        "age": 28
    })
    
    # Insert multiple records
    db.insert_batch("users", [
        {
            "username": "alice",
            "email": "alice@example.com",
            "age": 35
        },
        {
            "username": "bob",
            "email": "bob@example.com",
            "age": 42
        }
    ])
    
    # Query the data
    user = db.get_one("users", ["username", "email"], {"username": "john_doe"})
    print(f"User: {user['username']}, Email: {user['email']}")
    
    # Query multiple users
    users = db.get_all("users", ["id", "username", "age"], ("age > ?", [30]))
    print("\nUsers over 30:")
    for user in users:
        print(f"ID: {user['id']}, Username: {user['username']}, Age: {user['age']}")
    
    # Update a record
    db.update("users", {"age": 31}, {"username": "john_doe"})
    print("\nAfter update:")
    user = db.get_one("users", ["username", "age"], {"username": "john_doe"})
    print(f"User: {user['username']}, Age: {user['age']}")
    
    # Delete a record
    db.delete("users", {"username": "bob"})
    
    # Count the remaining users
    count = db.execute("SELECT COUNT(*) FROM users").fetchone()[0]
    print(f"\nRemaining users: {count}")
    
    # Close the connection
    db.disconnect()
    print("SQLite test completed successfully!")

def test_transaction():
    """Test transaction support."""
    print("\n===== Testing Transaction Support =====")
    
    # Connect to an in-memory SQLite database
    db = connect("sqlite", database=":memory:")
    
    # Create a test table
    db.execute("""
    CREATE TABLE accounts (
        id INTEGER PRIMARY KEY,
        name TEXT NOT NULL,
        balance REAL NOT NULL
    )
    """)
    
    # Insert initial data
    db.insert_batch("accounts", [
        {"name": "Alice", "balance": 1000.0},
        {"name": "Bob", "balance": 500.0}
    ])
    
    # Using transaction context manager
    print("\nTransfer $200 from Alice to Bob:")
    try:
        with db.transaction():
            # Deduct from Alice's account
            db.update("accounts", {"balance": db.execute("SELECT balance - 200 FROM accounts WHERE name = 'Alice'").fetchone()[0]}, {"name": "Alice"})
            
            # If Alice's balance goes negative, we'll get an error and rollback
            alice_balance = db.execute("SELECT balance FROM accounts WHERE name = 'Alice'").fetchone()[0]
            if alice_balance < 0:
                raise ValueError("Insufficient funds!")
            
            # Add to Bob's account
            db.update("accounts", {"balance": db.execute("SELECT balance + 200 FROM accounts WHERE name = 'Bob'").fetchone()[0]}, {"name": "Bob"})
            
        print("Transaction committed successfully")
    except Exception as e:
        print(f"Transaction failed: {e}")
    
    # Check the balances
    alice = db.get_one("accounts", ["name", "balance"], {"name": "Alice"})
    bob = db.get_one("accounts", ["name", "balance"], {"name": "Bob"})
    
    print(f"Alice's balance: ${alice['balance']}")
    print(f"Bob's balance: ${bob['balance']}")
    
    # Close the connection
    db.disconnect()
    print("Transaction test completed successfully!")

if __name__ == "__main__":
    try:
        # Run the SQLite test
        test_sqlite()
        
        # Run the transaction test
        test_transaction()
        
        print("\nAll tests completed successfully!")
    except Exception as e:
        logger.error(f"Test failed: {e}", exc_info=True)
        sys.exit(1)