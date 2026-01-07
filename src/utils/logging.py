

"""
Logging utility module for consistent logging across the seed data generation pipeline.
This module provides centralized logging configuration, log formatting, and utility
functions for logging at different levels with consistent formatting and context.

The utility is designed to be:
- Configurable: Adjustable log levels, formats, and output destinations
- Context-aware: Includes timestamps, module names, and execution context
- Performance-conscious: Optimized for high-volume logging without performance impact
- Observable: Supports both console and file logging with rotation
- Extensible: Easy to add custom log handlers and formatting
"""

import logging
import sys
import json
import time
from datetime import datetime
from pathlib import Path
from typing import Dict, Any, Optional, Union
import traceback

def setup_logging(config: Dict[str, Any] = None):
    """
    Set up logging configuration for the application.
    
    Args:
        config: Configuration dictionary with logging settings
    """
    config = config or {}
    
    # Get log level from config or environment
    log_level_str = config.get('log_level', 'INFO').upper()
    log_level = getattr(logging, log_level_str, logging.INFO)
    
    # Get log format
    log_format = config.get('log_format', '%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    
    # Get log file path
    log_file = config.get('log_file')
    if log_file:
        log_file = Path(log_file)
        log_file.parent.mkdir(parents=True, exist_ok=True)
    
    # Create formatter
    formatter = logging.Formatter(log_format)
    
    # Create root logger
    root_logger = logging.getLogger()
    root_logger.setLevel(log_level)
    
    # Clear existing handlers
    root_logger.handlers = []
    
    # Add console handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(formatter)
    root_logger.addHandler(console_handler)
    
    # Add file handler if configured
    if log_file:
        try:
            file_handler = logging.FileHandler(log_file)
            file_handler.setFormatter(formatter)
            root_logger.addHandler(file_handler)
            logging.info(f"Logging to file: {log_file}")
        except Exception as e:
            logging.warning(f"Failed to set up file logging: {e}")
    
    # Set up specific loggers
    loggers_to_configure = [
        'src',  # All source code
        'src.scrapers', 'src.generators', 'src.utils', 'src.models', 'src.prompts',
        'openai',  # LLM library
        'requests',  # HTTP requests
        'sqlite3',  # Database
        'faker'  # Fake data generation
    ]
    
    for logger_name in loggers_to_configure:
        logger = logging.getLogger(logger_name)
        logger.setLevel(log_level)
        # Don't propagate to root logger to avoid duplicate logs
        logger.propagate = False
        
        # Add handlers to specific loggers
        if not logger.handlers:
            if log_file:
                file_handler = logging.FileHandler(log_file)
                file_handler.setFormatter(formatter)
                logger.addHandler(file_handler)
            
            console_handler = logging.StreamHandler(sys.stdout)
            console_handler.setFormatter(formatter)
            logger.addHandler(console_handler)
    
    logging.info("Logging setup complete")
    logging.info(f"Log level: {log_level_str}")
    if log_file:
        logging.info(f"Log file: {log_file}")

def get_logger(name: str) -> logging.Logger:
    """
    Get a configured logger for a specific module or component.
    
    Args:
        name: Name of the logger (typically __name__)
        
    Returns:
        Configured logger instance
    """
    return logging.getLogger(name)

def log_function_call(func):
    """
    Decorator to log function calls with timing and arguments.
    
    Usage:
        @log_function_call
        def my_function(arg1, arg2):
            pass
            
    Args:
        func: Function to decorate
        
    Returns:
        Wrapped function with logging
    """
    logger = get_logger(func.__module__)
    
    def wrapper(*args, **kwargs):
        start_time = time.time()
        func_name = func.__name__
        
        try:
            # Log function call with sanitized arguments
            sanitized_args = _sanitize_log_args(args, kwargs)
            logger.debug(f"Calling {func_name} with args: {sanitized_args}")
            
            # Execute function
            result = func(*args, **kwargs)
            
            # Calculate execution time
            execution_time = time.time() - start_time
            
            # Log successful execution
            logger.debug(f"Function {func_name} completed successfully in {execution_time:.3f}s")
            
            return result
            
        except Exception as e:
            # Calculate execution time even on failure
            execution_time = time.time() - start_time
            
            # Log error with traceback
            logger.error(f"Function {func_name} failed after {execution_time:.3f}s: {str(e)}")
            logger.error(traceback.format_exc())
            raise
    
    return wrapper

def _sanitize_log_args(args, kwargs) -> Dict[str, Any]:
    """
    Sanitize arguments for logging to avoid sensitive data exposure.
    
    Args:
        args: Function positional arguments
        kwargs: Function keyword arguments
        
    Returns:
        Sanitized dictionary of arguments
    """
    sanitized = {
        'args': [],
        'kwargs': {}
    }
    
    # Sanitize positional arguments
    for arg in args:
        if isinstance(arg, (str, int, float, bool, type(None))):
            sanitized['args'].append(arg)
        elif isinstance(arg, (list, tuple, set)):
            sanitized['args'].append(f"<{type(arg).__name__}>")
        elif isinstance(arg, dict):
            sanitized['args'].append("<dict>")
        else:
            sanitized['args'].append(f"<{type(arg).__name__}>")
    
    # Sanitize keyword arguments
    sensitive_keywords = ['password', 'token', 'api_key', 'secret', 'credential']
    
    for key, value in kwargs.items():
        if any(sensitive in key.lower() for sensitive in sensitive_keywords):
            sanitized['kwargs'][key] = "****"  # Mask sensitive values
        elif isinstance(value, (str, int, float, bool, type(None))):
            sanitized['kwargs'][key] = value
        elif isinstance(value, (list, tuple, set)):
            sanitized['kwargs'][key] = f"<{type(value).__name__}>"
        elif isinstance(value, dict):
            sanitized['kwargs'][key] = "<dict>"
        else:
            sanitized['kwargs'][key] = f"<{type(value).__name__}>"
    
    return sanitized

def log_database_operation(operation: str, query: str, params: Any = None, duration: float = None):
    """
    Log database operations with timing and query information.
    
    Args:
        operation: Type of operation (SELECT, INSERT, UPDATE, DELETE)
        query: SQL query (sanitized)
        params: Query parameters (sanitized)
        duration: Execution duration in seconds
    """
    logger = get_logger('src.utils.database')
    
    # Sanitize query for logging (remove actual data)
    sanitized_query = _sanitize_sql_query(query)
    
    if duration is not None:
        logger.debug(f"Database {operation}: {sanitized_query} | Duration: {duration:.3f}s")
    else:
        logger.debug(f"Database {operation}: {sanitized_query}")

def _sanitize_sql_query(query: str) -> str:
    """
    Sanitize SQL query for logging by removing sensitive data and limiting length.
    
    Args:
        query: SQL query string
        
    Returns:
        Sanitized query string
    """
    # Remove actual values from INSERT/UPDATE statements
    if 'INSERT' in query or 'UPDATE' in query:
        # Replace VALUES clause with placeholder
        import re
        query = re.sub(r'VALUES\s*\(.*?\)', 'VALUES (<values>)', query, flags=re.IGNORECASE | re.DOTALL)
        query = re.sub(r'SET\s*.*?WHERE', 'SET <fields> WHERE', query, flags=re.IGNORECASE | re.DOTALL)
    
    # Limit query length
    max_length = 200
    if len(query) > max_length:
        return query[:max_length] + "..."
    
    return query

def log_llm_request(model: str, prompt_length: int, response_length: int = None, duration: float = None):
    """
    Log LLM API requests for monitoring and cost tracking.
    
    Args:
        model: LLM model name
        prompt_length: Length of prompt in tokens/characters
        response_length: Length of response in tokens/characters
        duration: Request duration in seconds
    """
    logger = get_logger('src.utils.llm')
    
    if duration is not None and response_length is not(None):
        logger.debug(f"LLM Request: {model} | Prompt: {prompt_length} chars | Response: {response_length} chars | Duration: {duration:.3f}s")
    else:
        logger.debug(f"LLM Request: {model} | Prompt: {prompt_length} chars")

def log_validation_result(entity_type: str, entity_id: Any, is_valid: bool, issues: list = None):
    """
    Log validation results for data quality monitoring.
    
    Args:
        entity_type: Type of entity being validated (task, project, user, etc.)
        entity_id: Entity identifier
        is_valid: Validation result
        issues: List of validation issues if any
    """
    logger = get_logger('src.utils.validation')
    
    if is_valid:
        logger.debug(f"Validation passed for {entity_type} {entity_id}")
    else:
        logger.warning(f"Validation failed for {entity_type} {entity_id}")
        if issues:
            for issue in issues:
                logger.warning(f"  - {issue}")

def log_progress(iteration: int, total: int, description: str = "Processing"):
    """
    Log progress for long-running operations.
    
    Args:
        iteration: Current iteration number
        total: Total number of iterations
        description: Description of the operation
    """
    if total <= 0:
        return
    
    # Log every 10% or at the end
    progress_percent = (iteration / total) * 100
    if iteration % max(1, total // 10) == 0 or iteration == total:
        logger = get_logger('progress')
        logger.info(f"{description}: {iteration}/{total} ({progress_percent:.1f}%)")

def get_log_stats() -> Dict[str, Any]:
    """
    Get logging statistics and metrics.
    
    Returns:
        Dictionary with logging metrics
    """
    return {
        'timestamp': datetime.now().isoformat(),
        'log_level': logging.getLogger().level,
        'handlers_count': len(logging.getLogger().handlers),
        'loggers_count': len(logging.Logger.manager.loggerDict)
    }

# Example usage and testing
if __name__ == "__main__":
    # Test configuration
    test_config = {
        'log_level': 'DEBUG',
        'log_format': '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        'log_file': 'data/logs/test_log.log'
    }
    
    try:
        print("=== Logging Utility Testing ===\n")
        
        # Set up logging
        setup_logging(test_config)
        logger = get_logger(__name__)
        
        # Test basic logging
        print("Basic Logging Test:")
        logger.debug("This is a debug message")
        logger.info("This is an info message")
        logger.warning("This is a warning message")
        logger.error("This is an error message")
        
        # Test function decorator
        print("\nFunction Decorator Test:")
        
        @log_function_call
        def test_function(x: int, y: str, password: str = None):
            """Test function with logging decorator"""
            print(f"  Inside test_function: x={x}, y={y}, password={password}")
            if x < 0:
                raise ValueError("x cannot be negative")
            return x * len(y)
        
        # Test successful call
        print("  Testing successful function call:")
        result1 = test_function(5, "hello", password="secret123")
        print(f"  Result: {result1}")
        
        # Test failing call
        print("\n  Testing failing function call:")
        try:
            result2 = test_function(-1, "world")
        except ValueError as e:
            print(f"  Caught expected error: {e}")
        
        # Test database logging
        print("\nDatabase Operation Logging Test:")
        log_database_operation(
            operation="SELECT",
            query="SELECT * FROM users WHERE email = 'user@example.com'",
            duration=0.023
        )
        
        log_database_operation(
            operation="INSERT",
            query="INSERT INTO tasks (name, description) VALUES ('Task 1', 'Description')",
            duration=0.015
        )
        
        # Test LLM logging
        print("\nLLM Request Logging Test:")
        log_llm_request(
            model="gpt-4-turbo",
            prompt_length=150,
            response_length=300,
            duration=1.25
        )
        
        # Test validation logging
        print("\nValidation Logging Test:")
        log_validation_result(
            entity_type="task",
            entity_id=123,
            is_valid=True
        )
        
        log_validation_result(
            entity_type="project",
            entity_id=456,
            is_valid=False,
            issues=[
                "Project start date after end date",
                "Missing required field: description"
            ]
        )
        
        # Test progress logging
        print("\nProgress Logging Test:")
        for i in range(1, 11):
            log_progress(i, 10, "Generating test data")
        
        # Test log stats
        print("\nLog Statistics Test:")
        stats = get_log_stats()
        print(f"  Log level: {stats['log_level']}")
        print(f"  Handlers count: {stats['handlers_count']}")
        print(f"  Loggers count: {stats['loggers_count']}")
        
        print("\n✅ All logging utility tests completed successfully!")
        
    except Exception as e:
        print(f"Logging test error: {str(e)}")
        import traceback
        traceback.print_exc()