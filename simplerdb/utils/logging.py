"""
Logging utilities for SimplerDB.
"""

import logging
import sys
import os
from typing import Optional, Dict, Any, Union

# Default logging format
DEFAULT_LOG_FORMAT = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"

def configure_logger(
    name: str = "simplerdb",
    level: Union[int, str] = logging.INFO,
    log_format: str = DEFAULT_LOG_FORMAT,
    log_file: Optional[str] = None,
    log_to_console: bool = True,
    log_to_file: bool = False,
    rotate_logs: bool = False,
    max_bytes: int = 10485760,  # 10MB
    backup_count: int = 5,
    propagate: bool = True,
    additional_handlers: Optional[Dict[str, Any]] = None
) -> logging.Logger:
    """
    Configure a logger with the given parameters.
    
    Args:
        name: Logger name
        level: Logging level
        log_format: Format string for log messages
        log_file: Path to log file (if log_to_file is True)
        log_to_console: Whether to log to console
        log_to_file: Whether to log to file
        rotate_logs: Whether to rotate log files
        max_bytes: Maximum size of each log file
        backup_count: Number of backup log files to keep
        propagate: Whether to propagate to parent loggers
        additional_handlers: Additional logging handlers
        
    Returns:
        Configured logger
    """
    # Get the logger
    logger = logging.getLogger(name)
    
    # Set the logging level
    if isinstance(level, str):
        level = getattr(logging, level.upper())
    logger.setLevel(level)
    
    # Create a formatter
    formatter = logging.Formatter(log_format)
    
    # Configure console logging
    if log_to_console:
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setFormatter(formatter)
        logger.addHandler(console_handler)
    
    # Configure file logging
    if log_to_file and log_file:
        # Create directory if it doesn't exist
        log_dir = os.path.dirname(log_file)
        if log_dir and not os.path.exists(log_dir):
            os.makedirs(log_dir)
            
        if rotate_logs:
            # Use rotating file handler
            from logging.handlers import RotatingFileHandler
            file_handler = RotatingFileHandler(
                log_file,
                maxBytes=max_bytes,
                backupCount=backup_count
            )
        else:
            # Use regular file handler
            file_handler = logging.FileHandler(log_file)
            
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)
    
    # Add additional handlers
    if additional_handlers:
        for handler_name, handler_config in additional_handlers.items():
            try:
                handler_type = handler_config.pop("type", None)
                if handler_type:
                    handler_class = getattr(logging.handlers, handler_type, None)
                    if handler_class:
                        handler = handler_class(**handler_config)
                        handler.setFormatter(formatter)
                        logger.addHandler(handler)
            except Exception as e:
                sys.stderr.write(f"Error configuring handler {handler_name}: {str(e)}\n")
    
    # Set propagation
    logger.propagate = propagate
    
    return logger


def get_logger(name: str = "simplerdb") -> logging.Logger:
    """
    Get a logger with the given name.
    
    If the logger doesn't exist, it will be created with default settings.
    
    Args:
        name: Logger name
        
    Returns:
        Logger instance
    """
    logger = logging.getLogger(name)
    
    # If logger has no handlers and is not configured, configure it with defaults
    if not logger.handlers and not logger.parent.handlers:
        configure_logger(name)
        
    return logger