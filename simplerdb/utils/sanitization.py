"""
Sanitization utilities for SimplerDB.
"""

import re
from typing import Union, List, Dict, Any

from ..exceptions import SanitizationError

def sanitize_identifier(identifier: str) -> str:
    """
    Sanitize an SQL identifier (table or column name).
    
    This prevents SQL injection via table/column names.
    
    Args:
        identifier: The table or column name to sanitize
        
    Returns:
        Properly sanitized and quoted identifier
        
    Raises:
        SanitizationError: If the identifier contains invalid characters
    """
    # Check if identifier is already backtick-quoted
    if identifier.startswith('`') and identifier.endswith('`'):
        return identifier
    
    # Handle special case - star selector
    if identifier == '*':
        return '*'
    
    # For identifiers with dot notation (e.g., table.column)
    if '.' in identifier:
        parts = identifier.split('.')
        return '.'.join([sanitize_identifier(part) for part in parts])
    
    # Validate identifier against a whitelist pattern
    # Only allows alphanumeric characters, underscores and dollars
    pattern = re.compile(r'^[a-zA-Z0-9_$]+$')
    if not pattern.match(identifier):
        raise SanitizationError(f"Invalid identifier: {identifier}. "
                               "Identifiers must contain only letters, numbers, underscores, and dollar signs.")
    
    # Properly quote the identifier
    return f"`{identifier}`"

def sanitize_like_pattern(pattern: str) -> str:
    """
    Sanitize a pattern for LIKE clauses.
    
    This escapes special characters in LIKE patterns.
    
    Args:
        pattern: The LIKE pattern to sanitize
        
    Returns:
        Sanitized LIKE pattern
    """
    # Escape special characters: %, _, [, ], ^, -, \
    pattern = pattern.replace('\\', '\\\\')
    pattern = pattern.replace('%', '\\%')
    pattern = pattern.replace('_', '\\_')
    pattern = pattern.replace('[', '\\[')
    pattern = pattern.replace(']', '\\]')
    pattern = pattern.replace('^', '\\^')
    pattern = pattern.replace('-', '\\-')
    
    return pattern

def sanitize_order_by(field: str, direction: str = 'ASC') -> str:
    """
    Sanitize an ORDER BY clause.
    
    Args:
        field: Field name to sort by
        direction: Sort direction ('ASC' or 'DESC')
        
    Returns:
        Sanitized ORDER BY clause
        
    Raises:
        SanitizationError: If the direction is invalid
    """
    safe_field = sanitize_identifier(field)
    
    # Validate direction
    if direction.upper() not in ('ASC', 'DESC'):
        raise SanitizationError(f"Invalid sort direction: {direction}. Must be 'ASC' or 'DESC'.")
    
    return f"{safe_field} {direction.upper()}"

def sanitize_limit(limit: Union[int, List[int], tuple]) -> str:
    """
    Sanitize a LIMIT clause.
    
    Args:
        limit: Limit value or [offset, limit] tuple
        
    Returns:
        Sanitized LIMIT clause
        
    Raises:
        SanitizationError: If the limit values are invalid
    """
    if isinstance(limit, int):
        if limit < 0:
            raise SanitizationError(f"Invalid limit value: {limit}. Must be a non-negative integer.")
        return f"LIMIT {limit}"
    
    elif isinstance(limit, (list, tuple)):
        if len(limit) == 1:
            if limit[0] < 0:
                raise SanitizationError(f"Invalid limit value: {limit[0]}. Must be a non-negative integer.")
            return f"LIMIT {limit[0]}"
        
        elif len(limit) >= 2:
            if limit[0] < 0:
                raise SanitizationError(f"Invalid offset value: {limit[0]}. Must be a non-negative integer.")
            if limit[1] < 0:
                raise SanitizationError(f"Invalid limit value: {limit[1]}. Must be a non-negative integer.")
            return f"LIMIT {limit[0]}, {limit[1]}"
    
    raise SanitizationError(f"Invalid limit format: {limit}. "
                          "Must be an integer or a list/tuple of [offset, limit].")

def sanitize_table_name(table_name: str) -> str:
    """
    Sanitize a table name.
    
    Args:
        table_name: Table name to sanitize
        
    Returns:
        Sanitized table name
        
    Raises:
        SanitizationError: If the table name contains invalid characters
    """
    return sanitize_identifier(table_name)

def sanitize_column_name(column_name: str) -> str:
    """
    Sanitize a column name.
    
    Args:
        column_name: Column name to sanitize
        
    Returns:
        Sanitized column name
        
    Raises:
        SanitizationError: If the column name contains invalid characters
    """
    return sanitize_identifier(column_name)

def sanitize_value(value: Any) -> Any:
    """
    Ensure a value is safe for database operations.
    
    This function doesn't actually modify the value, as that should be
    handled by parameterized queries, but it performs basic validation.
    
    Args:
        value: Value to check
        
    Returns:
        The same value, if valid
        
    Raises:
        SanitizationError: If the value is of an unsupported type
    """
    # Check for basic supported types
    if value is None or isinstance(value, (bool, int, float, str, bytes, bytearray)):
        return value
    
    # Check for supported container types
    if isinstance(value, (list, tuple, dict)):
        # For JSON serialization in databases that support it
        return value
    
    # If we get here, it's an unsupported type
    raise SanitizationError(f"Unsupported value type: {type(value).__name__}. "
                          "Values must be None, bool, int, float, str, bytes, or containers.")

def validate_data_dict(data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Validate a dictionary of data for database operations.
    
    Args:
        data: Dictionary of field:value pairs
        
    Returns:
        The same dictionary, with sanitized keys
        
    Raises:
        SanitizationError: If the dictionary contains invalid keys or values
    """
    if not data:
        raise SanitizationError("Empty data dictionary provided")
    
    # Check each key and value
    sanitized = {}
    for key, value in data.items():
        # Sanitize the key (but strip the backticks for the actual dict key)
        safe_key = sanitize_identifier(key).strip('`')
        # Check the value
        safe_value = sanitize_value(value)
        sanitized[safe_key] = safe_value
    
    return sanitized