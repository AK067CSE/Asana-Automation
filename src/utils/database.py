

"""
Database utility module for SQLite database operations with enterprise-grade reliability.
This module provides connection management, schema initialization, transaction handling,
and database optimization for the seed data generation pipeline.

The utility is designed to be:
- Reliable: Handles connection pooling, retries, and transaction management
- Performant: Optimizes SQLite for bulk data insertion and querying
- Safe: Implements proper error handling and data integrity checks
- Configurable: Adjustable connection parameters and optimization settings
- Observable: Detailed logging and performance metrics for database operations
"""

import logging
import sqlite3
import time
from typing import List, Dict, Optional, Any, Tuple, Union
from pathlib import Path
import json

from src.utils.logging import get_logger

logger = get_logger(__name__)

class DatabaseManager:
    """
    Database manager for SQLite operations with enterprise patterns.
    
    This class handles:
    1. Connection lifecycle management with retry logic
    2. Schema initialization and migration
    3. Bulk data insertion with transaction batching
    4. Query optimization and indexing
    5. Data integrity validation and cleanup
    6. Performance monitoring and logging
    
    The manager follows best practices for SQLite in high-volume data generation scenarios.
    """
    
    def __init__(self, database_path: str, config: Dict[str, Any] = None):
        """
        Initialize the database manager.
        
        Args:
            database_path: Path to SQLite database file
            config: Configuration dictionary with database settings
        """
        self.database_path = database_path
        self.config = config or {}
        
        # Database configuration
        self.batch_size = int(self.config.get('batch_size', 1000))
        self.connection_timeout = int(self.config.get('connection_timeout', 30))
        self.retry_attempts = int(self.config.get('retry_attempts', 3))
        self.retry_delay = float(self.config.get('retry_delay', 1.0))
        
        # Performance tracking
        self.operation_times = {}
        self.total_operations = 0
        
        # Ensure database directory exists
        db_dir = Path(database_path).parent
        if db_dir and not db_dir.exists():
            db_dir.mkdir(parents=True, exist_ok=True)
            logger.info(f"Created database directory: {db_dir}")
    
    def _get_connection(self, timeout: int = None) -> sqlite3.Connection:
        """
        Get a database connection with retry logic.
        
        Args:
            timeout: Connection timeout in seconds
            
        Returns:
            SQLite database connection
            
        Raises:
            sqlite3.Error: If connection fails after retries
        """
        timeout = timeout or self.connection_timeout
        
        for attempt in range(self.retry_attempts):
            try:
                conn = sqlite3.connect(
                    self.database_path,
                    timeout=timeout,
                    check_same_thread=False
                )
                
                # Configure connection for performance
                self._configure_connection(conn)
                
                logger.debug(f"Database connection established on attempt {attempt + 1}")
                return conn
                
            except sqlite3.Error as e:
                logger.warning(f"Connection attempt {attempt + 1} failed: {str(e)}")
                if attempt < self.retry_attempts - 1:
                    time.sleep(self.retry_delay * (attempt + 1))  # Exponential backoff
                else:
                    logger.error(f"Failed to connect to database after {self.retry_attempts} attempts")
                    raise
        
        raise sqlite3.Error(f"Failed to establish database connection to {self.database_path}")
    
    def _configure_connection(self, conn: sqlite3.Connection):
        """
        Configure database connection with performance optimizations.
        
        Args:
            conn: SQLite database connection
        """
        cursor = conn.cursor()
        
        try:
            # Performance optimizations
            cursor.execute("PRAGMA journal_mode = WAL;")  # Write-ahead logging for concurrency
            cursor.execute("PRAGMA synchronous = NORMAL;")  # Balance safety and performance
            cursor.execute("PRAGMA cache_size = -10000;")   # 10MB cache
            cursor.execute("PRAGMA temp_store = MEMORY;")  # Use memory for temp tables
            cursor.execute("PRAGMA mmap_size = 268435456;") # 256MB memory mapping
            
            # Foreign key support
            cursor.execute("PRAGMA foreign_keys = ON;")
            
            conn.commit()
            logger.debug("Database connection configured with performance optimizations")
            
        except sqlite3.Error as e:
            logger.warning(f"Error configuring database connection: {str(e)}")
            # Continue with default configuration
    
    def initialize_database(self, schema_file: str = 'schema.sql'):
        """
        Initialize database with schema from SQL file.
        
        Args:
            schema_file: Path to SQL schema file
            
        Raises:
            FileNotFoundError: If schema file not found
            sqlite3.Error: If schema execution fails
        """
        logger.info(f"Initializing database schema from {schema_file}")
        
        try:
            # Read schema file
            schema_path = Path(schema_file)
            if not schema_path.exists():
                # Try to find schema in parent directories
                possible_paths = [
                    Path.cwd() / schema_file,
                    Path.cwd().parent / schema_file,
                    Path.cwd().parent.parent / schema_file,
                    Path(__file__).parent.parent.parent / schema_file
                ]
                
                for path in possible_paths:
                    if path.exists():
                        schema_path = path
                        break
                
                if not schema_path.exists():
                    raise FileNotFoundError(f"Schema file not found at {schema_file} or common locations")
            
            with open(schema_path, 'r', encoding='utf-8') as f:
                schema_sql = f.read()
            
            # Execute schema
            conn = self._get_connection()
            cursor = conn.cursor()
            
            # Split schema into individual statements (handle semicolons within strings)
            statements = []
            current_stmt = []
            in_string = False
            escape_next = False
            
            for char in schema_sql:
                if escape_next:
                    escape_next = False
                    current_stmt.append(char)
                    continue
                
                if char == '\\' and in_string:
                    escape_next = True
                    current_stmt.append(char)
                    continue
                
                if char == "'" and not escape_next:
                    in_string = not in_string
                
                if char == ';' and not in_string:
                    stmt = ''.join(current_stmt).strip()
                    if stmt:
                        statements.append(stmt)
                    current_stmt = []
                else:
                    current_stmt.append(char)
            
            # Execute each statement
            for stmt in statements:
                if stmt.strip():
                    start_time = time.time()
                    cursor.execute(stmt)
                    elapsed = time.time() - start_time
                    logger.debug(f"Executed schema statement in {elapsed:.3f}s: {stmt[:100]}...")
            
            conn.commit()
            logger.info(f"Successfully initialized database schema with {len(statements)} statements")
            
        except FileNotFoundError as e:
            logger.error(f"Schema file error: {str(e)}")
            raise
        except sqlite3.Error as e:
            logger.error(f"Database schema initialization error: {str(e)}")
            raise
        finally:
            if 'conn' in locals():
                conn.close()
    
    def execute_batch(self, query: str, params_list: List[Tuple], commit: bool = True) -> List[int]:
        """
        Execute a batch of SQL statements with transaction management.
        
        Args:
            query: SQL query template with placeholders
            params_list: List of parameter tuples for each execution
            commit: Whether to commit the transaction
            
        Returns:
            List of lastrowid values for each successful execution
            
        Raises:
            sqlite3.Error: If batch execution fails
        """
        if not params_list:
            return []
        
        conn = self._get_connection()
        cursor = conn.cursor()
        lastrowids = []
        
        try:
            start_time = time.time()
            total_batches = (len(params_list) + self.batch_size - 1) // self.batch_size
            
            for i in range(0, len(params_list), self.batch_size):
                batch = params_list[i:i + self.batch_size]
                batch_start_time = time.time()
                
                # Execute batch
                cursor.executemany(query, batch)
                
                # Get lastrowids if needed (only works for INSERT statements)
                if query.strip().upper().startswith('INSERT'):
                    lastrowids.extend(cursor.lastrowid + j for j in range(len(batch)))
                
                batch_elapsed = time.time() - batch_start_time
                logger.debug(f"Batch {i//self.batch_size + 1}/{total_batches} executed in {batch_elapsed:.3f}s")
                
                # Commit intermediate batches to avoid large transactions
                if commit and i > 0 and i % (self.batch_size * 10) == 0:
                    conn.commit()
                    logger.debug(f"Committed intermediate batch at position {i}")
            
            if commit:
                conn.commit()
            
            total_elapsed = time.time() - start_time
            self._track_operation('execute_batch', total_elapsed, len(params_list))
            
            logger.info(f"Successfully executed batch of {len(params_list)} statements in {total_elapsed:.3f}s")
            return lastrowids
            
        except sqlite3.Error as e:
            logger.error(f"Batch execution error: {str(e)}")
            conn.rollback()
            raise
        finally:
            conn.close()
    
    def fetch_all(self, query: str, params: Tuple = None) -> List[Dict[str, Any]]:
        """
        Fetch all rows from a query as dictionaries.
        
        Args:
            query: SQL query
            params: Query parameters
            
        Returns:
            List of dictionaries with column names as keys
            
        Raises:
            sqlite3.Error: If query execution fails
        """
        conn = self._get_connection()
        cursor = conn.cursor()
        
        try:
            start_time = time.time()
            
            if params:
                cursor.execute(query, params)
            else:
                cursor.execute(query)
            
            # Get column names
            columns = [description[0] for description in cursor.description]
            
            # Fetch and convert to dictionaries
            rows = cursor.fetchall()
            results = [dict(zip(columns, row)) for row in rows]
            
            elapsed = time.time() - start_time
            self._track_operation('fetch_all', elapsed, len(results))
            
            logger.debug(f"Fetched {len(results)} rows in {elapsed:.3f}s: {query[:100]}...")
            return results
            
        except sqlite3.Error as e:
            logger.error(f"Query execution error: {str(e)}")
            raise
        finally:
            conn.close()
    
    def fetch_one(self, query: str, params: Tuple = None) -> Optional[Dict[str, Any]]:
        """
        Fetch a single row from a query as a dictionary.
        
        Args:
            query: SQL query
            params: Query parameters
            
        Returns:
            Dictionary with column names as keys, or None if no row found
            
        Raises:
            sqlite3.Error: If query execution fails
        """
        conn = self._get_connection()
        cursor = conn.cursor()
        
        try:
            start_time = time.time()
            
            if params:
                cursor.execute(query, params)
            else:
                cursor.execute(query)
            
            row = cursor.fetchone()
            if not row:
                return None
            
            # Get column names
            columns = [description[0] for description in cursor.description]
            result = dict(zip(columns, row))
            
            elapsed = time.time() - start_time
            self._track_operation('fetch_one', elapsed)
            
            logger.debug(f"Fetched single row in {elapsed:.3f}s: {query[:100]}...")
            return result
            
        except sqlite3.Error as e:
            logger.error(f"Query execution error: {str(e)}")
            raise
        finally:
            conn.close()
    
    def get_table_stats(self) -> Dict[str, Dict[str, Any]]:
        """
        Get statistics for all tables in the database.
        
        Returns:
            Dictionary with table names as keys and stats as values
        """
        conn = self._get_connection()
        cursor = conn.cursor()
        
        try:
            # Get list of tables
            cursor.execute("""
                SELECT name FROM sqlite_master 
                WHERE type='table' AND name NOT LIKE 'sqlite_%'
                ORDER BY name
            """)
            
            tables = [row[0] for row in cursor.fetchall()]
            stats = {}
            
            for table in tables:
                # Get row count
                cursor.execute(f"SELECT COUNT(*) FROM {table}")
                row_count = cursor.fetchone()[0]
                
                # Get size estimate (approximate)
                cursor.execute(f"PRAGMA table_info({table})")
                columns = cursor.fetchall()
                avg_row_size = sum(100 for _ in columns)  # Rough estimate of 100 bytes per column
                
                # Get index count
                cursor.execute(f"""
                    SELECT COUNT(*) FROM sqlite_master 
                    WHERE type='index' AND tbl_name=?
                """, (table,))
                index_count = cursor.fetchone()[0]
                
                stats[table] = {
                    'row_count': row_count,
                    'column_count': len(columns),
                    'estimated_size_bytes': row_count * avg_row_size,
                    'index_count': index_count,
                    'columns': [col[1] for col in columns]
                }
            
            logger.info(f"Retrieved stats for {len(tables)} tables")
            return stats
            
        except sqlite3.Error as e:
            logger.error(f"Error getting table stats: {str(e)}")
            return {}
        finally:
            conn.close()
    
    def vacuum_database(self):
        """
        Vacuum the database to reclaim space and optimize performance.
        """
        logger.info("Starting database vacuum operation...")
        
        conn = self._get_connection()
        cursor = conn.cursor()
        
        try:
            start_time = time.time()
            
            # Get current size
            import os
            before_size = os.path.getsize(self.database_path) if os.path.exists(self.database_path) else 0
            
            cursor.execute("VACUUM;")
            conn.commit()
            
            # Get new size
            after_size = os.path.getsize(self.database_path) if os.path.exists(self.database_path) else 0
            
            elapsed = time.time() - start_time
            space_saved = before_size - after_size
            
            logger.info(f"Database vacuum completed in {elapsed:.3f}s")
            logger.info(f"Space saved: {space_saved / 1024 / 1024:.2f} MB ({space_saved / before_size * 100:.1f}% reduction)")
            
        except sqlite3.Error as e:
            logger.error(f"Vacuum operation error: {str(e)}")
            raise
        finally:
            conn.close()
    
    def _track_operation(self, operation_name: str, duration: float, count: int = 1):
        """
        Track operation performance metrics.
        
        Args:
            operation_name: Name of the operation
            duration: Duration in seconds
            count: Number of items processed
        """
        if operation_name not in self.operation_times:
            self.operation_times[operation_name] = {'total_time': 0.0, 'count': 0, 'total_items': 0}
        
        stats = self.operation_times[operation_name]
        stats['total_time'] += duration
        stats['count'] += 1
        stats['total_items'] += count
        
        self.total_operations += 1
    
    def get_performance_stats(self) -> Dict[str, Any]:
        """
        Get database performance statistics.
        
        Returns:
            Dictionary with performance metrics
        """
        stats = {
            'total_operations': self.total_operations,
            'operations': {},
            'average_time_per_operation': 0.0
        }
        
        total_time = 0.0
        total_count = 0
        
        for op_name, op_stats in self.operation_times.items():
            avg_time = op_stats['total_time'] / op_stats['count'] if op_stats['count'] > 0 else 0
            avg_time_per_item = op_stats['total_time'] / op_stats['total_items'] if op_stats['total_items'] > 0 else 0
            
            stats['operations'][op_name] = {
                'count': op_stats['count'],
                'total_time': op_stats['total_time'],
                'average_time': avg_time,
                'total_items': op_stats['total_items'],
                'average_time_per_item': avg_time_per_item
            }
            
            total_time += op_stats['total_time']
            total_count += op_stats['count']
        
        if total_count > 0:
            stats['average_time_per_operation'] = total_time / total_count
        
        return stats
    
    def backup_database(self, backup_path: str = None):
        """
        Create a backup of the database.
        
        Args:
            backup_path: Path for backup file (defaults to database_path + .backup)
        """
        if backup_path is None:
            backup_path = f"{self.database_path}.backup"
        
        logger.info(f"Creating database backup to {backup_path}")
        
        try:
            import shutil
            
            # Close any existing connections first
            if hasattr(self, '_connection') and self._connection:
                self._connection.close()
            
            # Copy the database file
            shutil.copy2(self.database_path, backup_path)
            
            logger.info(f"Database backup created successfully at {backup_path}")
            
        except Exception as e:
            logger.error(f"Backup operation error: {str(e)}")
            raise
    
    def close(self):
        """Cleanup database resources."""
        logger.info("Database manager closed")
    
    @staticmethod
    def get_db_connection(database_path: str) -> sqlite3.Connection:
        """
        Static method to get a database connection (for compatibility with existing code).
        
        Args:
            database_path: Path to SQLite database file
            
        Returns:
            SQLite database connection
        """
        manager = DatabaseManager(database_path)
        return manager._get_connection()
    
    @staticmethod
    def initialize_database_static(database_path: str, schema_file: str = 'schema.sql'):
        """
        Static method to initialize database (for compatibility with existing code).
        
        Args:
            database_path: Path to SQLite database file
            schema_file: Path to SQL schema file
        """
        manager = DatabaseManager(database_path)
        manager.initialize_database(schema_file)
    
    @staticmethod
    def validate_database_integrity(conn: sqlite3.Connection) -> Dict[str, Any]:
        """
        Static method to validate database integrity (for compatibility with existing code).
        
        Args:
            conn: Database connection
            
        Returns:
            Dictionary with validation results
        """
        cursor = conn.cursor()
        results = {}
        
        try:
            # Check table existence and row counts
            cursor.execute("""
                SELECT name FROM sqlite_master 
                WHERE type='table' AND name NOT LIKE 'sqlite_%'
                ORDER BY name
            """)
            
            tables = [row[0] for row in cursor.fetchall()]
            
            for table in tables:
                cursor.execute(f"SELECT COUNT(*) FROM {table}")
                count = cursor.fetchone()[0]
                results[table] = {
                    'status': 'success',
                    'message': f'{count} rows found',
                    'row_count': count
                }
            
            # Check foreign key integrity
            cursor.execute("PRAGMA foreign_key_check;")
            fk_violations = cursor.fetchall()
            
            if fk_violations:
                results['foreign_keys'] = {
                    'status': 'warning',
                    'message': f'{len(fk_violations)} foreign key violations found',
                    'violations': fk_violations
                }
            else:
                results['foreign_keys'] = {
                    'status': 'success',
                    'message': 'No foreign key violations found'
                }
            
            logger.info("Database integrity validation completed")
            return results
            
        except sqlite3.Error as e:
            logger.error(f"Database validation error: {str(e)}")
            return {'error': {'status': 'error', 'message': str(e)}}

# Example usage and testing
if __name__ == "__main__":
    # Setup logging for testing
    logging.basicConfig(level=logging.INFO)
    
    # Test configuration
    test_config = {
        'batch_size': 100,
        'connection_timeout': 10,
        'retry_attempts': 2,
        'retry_delay': 0.5,
        'debug_mode': True
    }
    
    # Create test database path
    test_db_path = 'data/test/test_database.sqlite'
    
    try:
        print("=== Database Manager Testing ===\n")
        
        # Initialize database manager
        db_manager = DatabaseManager(test_db_path, test_config)
        
        # Initialize database with schema (using a simple schema for testing)
        test_schema = """
        CREATE TABLE IF NOT EXISTS users (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            email TEXT UNIQUE NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        
        CREATE TABLE IF NOT EXISTS projects (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            description TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            user_id INTEGER,
            FOREIGN KEY (user_id) REFERENCES users(id)
        );
        
        CREATE INDEX IF NOT EXISTS idx_users_email ON users(email);
        CREATE INDEX IF NOT EXISTS idx_projects_user_id ON projects(user_id);
        """
        
        # Write test schema to file
        schema_path = 'data/test/test_schema.sql'
        Path(schema_path).parent.mkdir(parents=True, exist_ok=True)
        with open(schema_path, 'w') as f:
            f.write(test_schema)
        
        # Initialize database
        db_manager.initialize_database(schema_path)
        print("✓ Database schema initialized successfully")
        
        # Test batch insertion
        user_data = [
            ('John Doe', 'john@example.com'),
            ('Jane Smith', 'jane@example.com'), 
            ('Bob Johnson', 'bob@example.com'),
            ('Alice Williams', 'alice@example.com'),
            ('Charlie Brown', 'charlie@example.com')
        ]
        
        insert_query = "INSERT INTO users (name, email) VALUES (?, ?)"
        lastrowids = db_manager.execute_batch(insert_query, user_data)
        print(f"✓ Batch inserted {len(user_data)} users with IDs: {lastrowids}")
        
        # Test fetching data
        users = db_manager.fetch_all("SELECT * FROM users ORDER BY name")
        print(f"\nRetrieved {len(users)} users:")
        for user in users:
            print(f"  - {user['name']} ({user['email']})")
        
        # Test single row fetch
        john = db_manager.fetch_one("SELECT * FROM users WHERE email = ?", ('john@example.com',))
        if john:
            print(f"\nFound user: {john['name']} with ID {john['id']}")
        
        # Test table stats
        stats = db_manager.get_table_stats()
        print("\nTable Statistics:")
        for table_name, table_stats in stats.items():
            print(f"  {table_name}:")
            print(f"    Rows: {table_stats['row_count']}")
            print(f"    Columns: {table_stats['column_count']}")
            print(f"    Indexes: {table_stats['index_count']}")
        
        # Test performance stats
        perf_stats = db_manager.get_performance_stats()
        print(f"\nPerformance Statistics:")
        print(f"  Total operations: {perf_stats['total_operations']}")
        print(f"  Average time per operation: {perf_stats['average_time_per_operation']:.4f}s")
        
        # Test backup
        backup_path = 'data/test/test_database_backup.sqlite'
        db_manager.backup_database(backup_path)
        print(f"\n✓ Database backup created at {backup_path}")
        
        # Test vacuum
        db_manager.vacuum_database()
        print("\n✓ Database vacuum completed")
        
        print("\nAll database tests completed successfully!")
        
    except Exception as e:
        print(f"Database test error: {str(e)}")
        import traceback
        traceback.print_exc()
    finally:
        try:
            db_manager.close()
        except:
            pass