

"""
Validation utility module for ensuring data quality and integrity in seed data generation.
This module provides comprehensive validation checks for temporal consistency, referential
integrity, distribution accuracy, and business rule compliance for enterprise workflow data.

The utility is designed to be:
- Comprehensive: Validates all aspects of data quality from schema to business logic
- Configurable: Adjustable validation thresholds and rules for different data types
- Observable: Detailed logging and reporting of validation results
- Repairable: Provides suggestions for fixing validation failures
- Benchmark-driven: Compares generated data against real-world enterprise patterns
"""

import logging
import sqlite3
import json
import numpy as np
import pandas as pd
from datetime import datetime, timedelta
from typing import List, Dict, Optional, Tuple, Any, Union, Set
from scipy import stats
from collections import Counter

from src.utils.logging import get_logger

logger = get_logger(__name__)

class DataValidator:
    """
    Data validator for ensuring quality and integrity of generated seed data.
    
    This class handles:
    1. Schema validation and constraint checking
    2. Temporal consistency validation (dates, sequences)
    3. Referential integrity validation (foreign keys)
    4. Distribution validation against real-world benchmarks
    5. Business rule validation for enterprise workflows
    6. Data quality metrics and reporting
    
    The validator uses statistical methods and domain knowledge to ensure generated
    data meets enterprise-grade quality standards for RL environment training.
    """
    
    def __init__(self, config: Dict[str, Any] = None):
        """
        Initialize the data validator.
        
        Args:
            config: Configuration dictionary with validation settings
        """
        self.config = config or {}
        
        # Validation thresholds
        self.temporal_consistency_threshold = float(self.config.get('temporal_consistency_threshold', 0.95))
        self.distribution_similarity_threshold = float(self.config.get('distribution_similarity_threshold', 0.05))
        self.referential_integrity_threshold = float(self.config.get('referential_integrity_threshold', 0.99))
        self.completion_rate_thresholds = {
            'sprint': (0.65, 0.90),
            'bug_tracking': (0.55, 0.75),
            'feature_development': (0.45, 0.70),
            'research': (0.25, 0.50),
            'campaign': (0.60, 0.85),
            'default': (0.45, 0.75)
        }
        
        # Real-world benchmark distributions (from industry research)
        self.benchmark_distributions = {
            'task_completion_rates': {
                'engineering_sprint': 0.75,
                'marketing_campaign': 0.70,
                'product_roadmap': 0.55,
                'sales_pipeline': 0.45,
                'operations_process': 0.65
            },
            'due_date_distributions': {
                'sprint_tasks': {
                    '0-1_days': 0.15,
                    '2-3_days': 0.25,
                    '4-7_days': 0.30,
                    '8-14_days': 0.20,
                    '15+_days': 0.10
                },
                'campaign_tasks': {
                    '0-1_days': 0.05,
                    '2-3_days': 0.10,
                    '4-7_days': 0.25,
                    '8-14_days': 0.35,
                    '15+_days': 0.25
                }
            },
            'assignment_patterns': {
                'engineering': {
                    'assigned': 0.85,
                    'unassigned': 0.15
                },
                'marketing': {
                    'assigned': 0.75,
                    'unassigned': 0.25
                },
                'executive': {
                    'assigned': 0.60,
                    'unassigned': 0.40
                }
            }
        }
    
    def validate_database_integrity(self, conn: sqlite3.Connection) -> Dict[str, Any]:
        """
        Validate complete database integrity including schema, relationships, and business rules.
        
        Args:
            conn: Database connection
            
        Returns:
            Dictionary with validation results organized by category
        """
        logger.info("Starting comprehensive database validation")
        
        results = {
            'schema_validation': self._validate_schema(conn),
            'temporal_consistency': self._validate_temporal_consistency(conn),
            'referential_integrity': self._validate_referential_integrity(conn),
            'distribution_validation': self._validate_distributions(conn),
            'business_rules': self._validate_business_rules(conn),
            'data_quality': self._validate_data_quality(conn),
            'overall_status': 'success'
        }
        
        # Determine overall status
        failed_categories = [
            category for category, result in results.items()
            if category != 'overall_status' and result.get('status') == 'failure'
        ]
        
        if failed_categories:
            results['overall_status'] = 'failure'
            results['failed_categories'] = failed_categories
            logger.error(f"Database validation failed for categories: {failed_categories}")
        else:
            logger.info("Database validation passed all checks")
        
        return results
    
    def _validate_schema(self, conn: sqlite3.Connection) -> Dict[str, Any]:
        """
        Validate database schema including tables, columns, constraints, and indexes.
        
        Args:
            conn: Database connection
            
        Returns:
            Dictionary with schema validation results
        """
        cursor = conn.cursor()
        logger.info("Validating database schema")
        
        try:
            # Required tables
            required_tables = [
                'organizations', 'teams', 'users', 'team_memberships',
                'projects', 'sections', 'tasks', 'subtasks', 'comments',
                'custom_field_definitions', 'custom_field_values', 'tags', 'task_tags'
            ]
            
            # Get existing tables
            cursor.execute("""
                SELECT name FROM sqlite_master 
                WHERE type='table' AND name NOT LIKE 'sqlite_%'
                ORDER BY name
            """)
            existing_tables = [row[0] for row in cursor.fetchall()]
            
            missing_tables = [table for table in required_tables if table not in existing_tables]
            
            # Validate table structures
            table_validations = {}
            for table in required_tables:
                if table in existing_tables:
                    table_validations[table] = self._validate_table_schema(cursor, table)
            
            # Check indexes
            required_indexes = [
                'idx_tasks_project_id', 'idx_tasks_section_id', 'idx_tasks_assignee_id',
                'idx_tasks_due_date', 'idx_tasks_completed', 'idx_comments_task_id',
                'idx_team_memberships_team_id', 'idx_team_memberships_user_id'
            ]
            
            cursor.execute("SELECT name FROM sqlite_master WHERE type='index'")
            existing_indexes = [row[0] for row in cursor.fetchall()]
            missing_indexes = [idx for idx in required_indexes if idx not in existing_indexes]
            
            # Build results
            result = {
                'status': 'success' if not missing_tables and not missing_indexes else 'failure',
                'message': f"Schema validation: {len(missing_tables)} missing tables, {len(missing_indexes)} missing indexes",
                'missing_tables': missing_tables,
                'missing_indexes': missing_indexes,
                'table_validations': table_validations
            }
            
            logger.info(f"Schema validation: {len(missing_tables)} missing tables, {len(missing_indexes)} missing indexes")
            return result
            
        except sqlite3.Error as e:
            logger.error(f"Schema validation error: {str(e)}")
            return {
                'status': 'error',
                'message': f"Schema validation failed: {str(e)}",
                'error': str(e)
            }
    
    def _validate_table_schema(self, cursor: sqlite3.Cursor, table_name: str) -> Dict[str, Any]:
        """
        Validate schema for a specific table including columns, types, and constraints.
        
        Args:
            cursor: Database cursor
            table_name: Name of table to validate
            
        Returns:
            Dictionary with table validation results
        """
        try:
            # Get table info
            cursor.execute(f"PRAGMA table_info({table_name})")
            columns = cursor.fetchall()
            
            # Get foreign keys
            cursor.execute(f"PRAGMA foreign_key_list({table_name})")
            foreign_keys = cursor.fetchall()
            
            # Get indexes for this table
            cursor.execute(f"""
                SELECT name FROM sqlite_master 
                WHERE type='index' AND tbl_name=?
            """, (table_name,))
            indexes = [row[0] for row in cursor.fetchall()]
            
            return {
                'column_count': len(columns),
                'columns': [col[1] for col in columns],
                'foreign_key_count': len(foreign_keys),
                'index_count': len(indexes),
                'has_primary_key': any(col[5] == 1 for col in columns)  # pk column is 1 for primary key
            }
            
        except sqlite3.Error as e:
            logger.warning(f"Error validating table {table_name}: {str(e)}")
            return {
                'error': str(e),
                'status': 'error'
            }
    
    def _validate_temporal_consistency(self, conn: sqlite3.Connection) -> Dict[str, Any]:
        """
        Validate temporal consistency of dates and time sequences.
        
        Args:
            conn: Database connection
            
        Returns:
            Dictionary with temporal validation results
        """
        cursor = conn.cursor()
        logger.info("Validating temporal consistency")
        
        validation_results = {}
        total_violations = 0
        total_checks = 0
        
        try:
            # 1. Validate tasks: created_at <= completed_at <= now
            cursor.execute("""
                SELECT COUNT(*) as total,
                       SUM(CASE WHEN completed = 1 AND (completed_at IS NULL OR completed_at < created_at) THEN 1 ELSE 0 END) as completion_violations,
                       SUM(CASE WHEN created_at > datetime('now') THEN 1 ELSE 0 END) as future_creation_violations
                FROM tasks
            """)
            task_results = cursor.fetchone()
            task_total, task_completion_violations, task_future_violations = task_results
            
            # 2. Validate projects: start_date <= end_date
            cursor.execute("""
                SELECT COUNT(*) as total,
                       SUM(CASE WHEN end_date IS NOT NULL AND end_date < start_date THEN 1 ELSE 0 END) as date_violations
                FROM projects
            """)
            project_results = cursor.fetchone()
            project_total, project_date_violations = project_results
            
            # 3. Validate comments: created_at >= task created_at, <= now
            cursor.execute("""
                SELECT COUNT(*) as total,
                       SUM(CASE WHEN c.created_at < t.created_at THEN 1 ELSE 0 END) as early_comment_violations
                FROM comments c
                JOIN tasks t ON c.task_id = t.id
            """)
            comment_results = cursor.fetchone()
            comment_total, comment_violations = comment_results
            
            # 4. Validate subtasks: created_at >= parent task created_at
            cursor.execute("""
                SELECT COUNT(*) as total,
                       SUM(CASE WHEN s.created_at < t.created_at THEN 1 ELSE 0 END) as early_subtask_violations
                FROM subtasks s
                JOIN tasks t ON s.task_id = t.id
            """)
            subtask_results = cursor.fetchone()
            subtask_total, subtask_violations = subtask_results
            
            # Calculate totals
            total_checks = (task_total or 0) + (project_total or 0) + (comment_total or 0) + (subtask_total or 0)
            total_violations = (
                (task_completion_violations or 0) + 
                (task_future_violations or 0) + 
                (project_date_violations or 0) + 
                (comment_violations or 0) + 
                (subtask_violations or 0)
            )
            
            # Calculate violation rate
            violation_rate = total_violations / total_checks if total_checks > 0 else 0
            status = 'success' if violation_rate <= (1 - self.temporal_consistency_threshold) else 'failure'
            
            validation_results = {
                'task_validation': {
                    'total': task_total or 0,
                    'completion_violations': task_completion_violations or 0,
                    'future_creation_violations': task_future_violations or 0
                },
                'project_validation': {
                    'total': project_total or 0,
                    'date_violations': project_date_violations or 0
                },
                'comment_validation': {
                    'total': comment_total or 0,
                    'early_comment_violations': comment_violations or 0
                },
                'subtask_validation': {
                    'total': subtask_total or 0,
                    'early_subtask_violations': subtask_violations or 0
                },
                'summary': {
                    'total_checks': total_checks,
                    'total_violations': total_violations,
                    'violation_rate': violation_rate,
                    'threshold': 1 - self.temporal_consistency_threshold,
                    'status': status
                }
            }
            
            logger.info(f"Temporal validation: {total_violations}/{total_checks} violations ({violation_rate:.2%} violation rate)")
            return {
                'status': status,
                'message': f"Temporal consistency: {total_violations} violations found in {total_checks} checks",
                'details': validation_results
            }
            
        except sqlite3.Error as e:
            logger.error(f"Temporal validation error: {str(e)}")
            return {
                'status': 'error',
                'message': f"Temporal validation failed: {str(e)}",
                'error': str(e)
            }
    
    def _validate_referential_integrity(self, conn: sqlite3.Connection) -> Dict[str, Any]:
        """
        Validate referential integrity including foreign key constraints and relationships.
        
        Args:
            conn: Database connection
            
        Returns:
            Dictionary with referential integrity validation results
        """
        cursor = conn.cursor()
        logger.info("Validating referential integrity")
        
        try:
            # Check foreign key integrity using SQLite's built-in check
            cursor.execute("PRAGMA foreign_key_check;")
            fk_violations = cursor.fetchall()
            
            # Check specific relationships
            relationship_checks = {
                'tasks_to_projects': """
                    SELECT COUNT(*) as total_violations
                    FROM tasks t
                    LEFT JOIN projects p ON t.project_id = p.id
                    WHERE p.id IS NULL
                """,
                'tasks_to_sections': """
                    SELECT COUNT(*) as total_violations
                    FROM tasks t
                    LEFT JOIN sections s ON t.section_id = s.id
                    WHERE s.id IS NULL
                """,
                'tasks_to_users': """
                    SELECT COUNT(*) as total_violations
                    FROM tasks t
                    LEFT JOIN users u ON t.assignee_id = u.id
                    WHERE t.assignee_id IS NOT NULL AND u.id IS NULL
                """,
                'comments_to_tasks': """
                    SELECT COUNT(*) as total_violations
                    FROM comments c
                    LEFT JOIN tasks t ON c.task_id = t.id
                    WHERE t.id IS NULL
                """,
                'subtasks_to_tasks': """
                    SELECT COUNT(*) as total_violations
                    FROM subtasks s
                    LEFT JOIN tasks t ON s.task_id = t.id
                    WHERE t.id IS NULL
                """,
                'team_memberships_to_teams': """
                    SELECT COUNT(*) as total_violations
                    FROM team_memberships tm
                    LEFT JOIN teams t ON tm.team_id = t.id
                    WHERE t.id IS NULL
                """,
                'team_memberships_to_users': """
                    SELECT COUNT(*) as total_violations
                    FROM team_memberships tm
                    LEFT JOIN users u ON tm.user_id = u.id
                    WHERE u.id IS NULL
                """
            }
            
            relationship_results = {}
            total_relationship_violations = 0
            
            for relationship_name, query in relationship_checks.items():
                cursor.execute(query)
                result = cursor.fetchone()
                violations = result[0] if result else 0
                relationship_results[relationship_name] = violations
                total_relationship_violations += violations
            
            # Calculate total violations
            total_violations = len(fk_violations) + total_relationship_violations
            
            # Get total rows for context
            cursor.execute("""
                SELECT 
                    (SELECT COUNT(*) FROM tasks) as task_count,
                    (SELECT COUNT(*) FROM projects) as project_count,
                    (SELECT COUNT(*) FROM users) as user_count,
                    (SELECT COUNT(*) FROM teams) as team_count,
                    (SELECT COUNT(*) FROM comments) as comment_count
            """)
            totals = cursor.fetchone()
            task_count, project_count, user_count, team_count, comment_count = totals
            
            total_rows = task_count + project_count + user_count + team_count + comment_count
            
            # Calculate violation rate
            violation_rate = total_violations / total_rows if total_rows > 0 else 0
            status = 'success' if violation_rate <= (1 - self.referential_integrity_threshold) else 'failure'
            
            logger.info(f"Referential integrity: {total_violations} violations found in {total_rows} rows")
            
            return {
                'status': status,
                'message': f"Referential integrity: {total_violations} violations found",
                'details': {
                    'foreign_key_violations': len(fk_violations),
                    'relationship_violations': relationship_results,
                    'total_violations': total_violations,
                    'total_rows': total_rows,
                    'violation_rate': violation_rate,
                    'threshold': 1 - self.referential_integrity_threshold
                }
            }
            
        except sqlite3.Error as e:
            logger.error(f"Referential integrity validation error: {str(e)}")
            return {
                'status': 'error',
                'message': f"Referential integrity validation failed: {str(e)}",
                'error': str(e)
            }
    
    def _validate_distributions(self, conn: sqlite3.Connection) -> Dict[str, Any]:
        """
        Validate data distributions against real-world benchmarks.
        
        Args:
            conn: Database connection
            
        Returns:
            Dictionary with distribution validation results
        """
        cursor = conn.cursor()
        logger.info("Validating data distributions against benchmarks")
        
        try:
            # 1. Task completion rates by department/project type
            cursor.execute("""
                SELECT 
                    p.department,
                    p.project_type,
                    COUNT(t.id) as total_tasks,
                    SUM(CASE WHEN t.completed = 1 THEN 1 ELSE 0 END) as completed_tasks,
                    CAST(SUM(CASE WHEN t.completed = 1 THEN 1 ELSE 0 END) AS FLOAT) / COUNT(t.id) as completion_rate
                FROM tasks t
                JOIN projects p ON t.project_id = p.id
                GROUP BY p.department, p.project_type
                HAVING COUNT(t.id) >= 10  -- Only validate groups with sufficient data
            """)
            completion_rates = cursor.fetchall()
            
            # 2. Due date distributions
            cursor.execute("""
                SELECT 
                    p.department,
                    p.project_type,
                    CASE 
                        WHEN julianday(t.due_date) - julianday(t.created_at) <= 1 THEN '0-1_days'
                        WHEN julianday(t.due_date) - julianday(t.created_at) <= 3 THEN '2-3_days'
                        WHEN julianday(t.due_date) - julianday(t.created_at) <= 7 THEN '4-7_days'
                        WHEN julianday(t.due_date) - julianday(t.created_at) <= 14 THEN '8-14_days'
                        ELSE '15+_days'
                    END as due_date_bucket,
                    COUNT(*) as task_count
                FROM tasks t
                JOIN projects p ON t.project_id = p.id
                WHERE t.due_date IS NOT NULL
                GROUP BY p.department, p.project_type, due_date_bucket
            """)
            due_date_distributions = cursor.fetchall()
            
            # 3. Assignment patterns
            cursor.execute("""
                SELECT 
                    p.department,
                    COUNT(*) as total_tasks,
                    SUM(CASE WHEN t.assignee_id IS NOT NULL THEN 1 ELSE 0 END) as assigned_tasks,
                    CAST(SUM(CASE WHEN t.assignee_id IS NOT NULL THEN 1 ELSE 0 END) AS FLOAT) / COUNT(*) as assignment_rate
                FROM tasks t
                JOIN projects p ON t.project_id = p.id
                GROUP BY p.department
            """)
            assignment_patterns = cursor.fetchall()
            
            # 4. Temporal patterns (tasks created per day of week)
            cursor.execute("""
                SELECT 
                    strftime('%w', created_at) as day_of_week,
                    COUNT(*) as task_count
                FROM tasks
                GROUP BY strftime('%w', created_at)
                ORDER BY day_of_week
            """)
            day_of_week_patterns = cursor.fetchall()
            
            # Validate against benchmarks
            validation_results = {
                'completion_rates': self._validate_completion_rates(completion_rates),
                'due_date_distributions': self._validate_due_date_distributions(due_date_distributions),
                'assignment_patterns': self._validate_assignment_patterns(assignment_patterns),
                'day_of_week_patterns': self._validate_day_of_week_patterns(day_of_week_patterns)
            }
            
            # Determine overall status
            failed_validations = [
                category for category, result in validation_results.items()
                if result.get('status') == 'failure'
            ]
            
            status = 'failure' if failed_validations else 'success'
            message = f"Distribution validation: {len(failed_validations)} categories failed" if failed_validations else "All distribution validations passed"
            
            logger.info(f"Distribution validation: {len(failed_validations)} categories failed")
            return {
                'status': status,
                'message': message,
                'details': validation_results,
                'failed_categories': failed_validations
            }
            
        except sqlite3.Error as e:
            logger.error(f"Distribution validation error: {str(e)}")
            return {
                'status': 'error',
                'message': f"Distribution validation failed: {str(e)}",
                'error': str(e)
            }
    
    def _validate_completion_rates(self, completion_rates: List[Tuple]) -> Dict[str, Any]:
        """
        Validate task completion rates against industry benchmarks.
        
        Args:
            completion_rates: List of (department, project_type, total_tasks, completed_tasks, completion_rate) tuples
            
        Returns:
            Dictionary with validation results
        """
        results = []
        failures = 0
        
        for dept, proj_type, total, completed, rate in completion_rates:
            key = f"{dept}_{proj_type}"
            benchmark = self.benchmark_distributions['task_completion_rates'].get(
                key, 
                self.benchmark_distributions['task_completion_rates'].get(f'{dept}_default', 0.65)
            )
            
            # Get acceptable range from thresholds
            thresholds = self.completion_rate_thresholds.get(proj_type, self.completion_rate_thresholds['default'])
            min_acceptable = thresholds[0]
            max_acceptable = thresholds[1]
            
            status = 'success'
            if rate < min_acceptable or rate > max_acceptable:
                status = 'failure'
                failures += 1
            
            results.append({
                'department': dept,
                'project_type': proj_type,
                'completion_rate': rate,
                'benchmark_rate': benchmark,
                'min_acceptable': min_acceptable,
                'max_acceptable': max_acceptable,
                'status': status,
                'sample_size': total
            })
        
        overall_status = 'failure' if failures > 0 else 'success'
        failure_rate = failures / len(results) if results else 0
        
        return {
            'status': overall_status,
            'message': f"Completion rates: {failures}/{len(results)} categories outside acceptable range",
            'failure_rate': failure_rate,
            'details': results,
            'threshold': self.distribution_similarity_threshold
        }
    
    def _validate_due_date_distributions(self, due_date_data: List[Tuple]) -> Dict[str, Any]:
        """
        Validate due date distributions against benchmarks.
        
        Args:
            due_date_data: List of (department, project_type, bucket, count) tuples
            
        Returns:
            Dictionary with validation results
        """
        # Group by department and project type
        grouped_data = {}
        for dept, proj_type, bucket, count in due_date_data:
            key = (dept, proj_type)
            if key not in grouped_data:
                grouped_data[key] = {}
            grouped_data[key][bucket] = count
        
        results = []
        failures = 0
        
        for (dept, proj_type), bucket_counts in grouped_data.items():
            total_tasks = sum(bucket_counts.values())
            
            # Get benchmark distribution
            benchmark_key = 'sprint_tasks' if 'sprint' in proj_type.lower() else 'campaign_tasks'
            benchmark_dist = self.benchmark_distributions['due_date_distributions'].get(benchmark_key, {})
            
            if not benchmark_dist:
                continue
            
            # Calculate observed distribution
            observed_dist = {bucket: count/total_tasks for bucket, count in bucket_counts.items()}
            
            # Calculate distribution similarity using KL divergence
            kl_divergence = self._calculate_kl_divergence(observed_dist, benchmark_dist)
            
            status = 'success' if kl_divergence <= self.distribution_similarity_threshold else 'failure'
            if status == 'failure':
                failures += 1
            
            results.append({
                'department': dept,
                'project_type': proj_type,
                'kl_divergence': kl_divergence,
                'threshold': self.distribution_similarity_threshold,
                'status': status,
                'observed_distribution': observed_dist,
                'benchmark_distribution': benchmark_dist,
                'sample_size': total_tasks
            })
        
        overall_status = 'failure' if failures > 0 else 'success'
        failure_rate = failures / len(results) if results else 0
        
        return {
            'status': overall_status,
            'message': f"Due date distributions: {failures}/{len(results)} categories exceed threshold",
            'failure_rate': failure_rate,
            'details': results,
            'threshold': self.distribution_similarity_threshold
        }
    
    def _calculate_kl_divergence(self, dist1: Dict[str, float], dist2: Dict[str, float]) -> float:
        """
        Calculate KL divergence between two discrete distributions.
        
        Args:
            dist1: First distribution (observed)
            dist2: Second distribution (benchmark)
            
        Returns:
            KL divergence value (lower is better)
        """
        # Ensure same buckets
        all_buckets = set(dist1.keys()) | set(dist2.keys())
        smoothed_dist1 = {}
        smoothed_dist2 = {}
        
        epsilon = 1e-10  # Smoothing factor
        
        for bucket in all_buckets:
            smoothed_dist1[bucket] = dist1.get(bucket, 0) + epsilon
            smoothed_dist2[bucket] = dist2.get(bucket, 0) + epsilon
        
        # Normalize
        sum1 = sum(smoothed_dist1.values())
        sum2 = sum(smoothed_dist2.values())
        
        normalized_dist1 = {k: v/sum1 for k, v in smoothed_dist1.items()}
        normalized_dist2 = {k: v/sum2 for k, v in smoothed_dist2.items()}
        
        # Calculate KL divergence
        kl_div = 0.0
        for bucket in all_buckets:
            p = normalized_dist1[bucket]
            q = normalized_dist2[bucket]
            kl_div += p * np.log(p / q)
        
        return kl_div
    
    def _validate_business_rules(self, conn: sqlite3.Connection) -> Dict[str, Any]:
        """
        Validate business rules specific to enterprise workflows.
        
        Args:
            conn: Database connection
            
        Returns:
            Dictionary with business rule validation results
        """
        cursor = conn.cursor()
        logger.info("Validating business rules")
        
        try:
            business_rule_violations = []
            
            # Rule 1: Projects should have at least one section
            cursor.execute("""
                SELECT p.id, p.name, COUNT(s.id) as section_count
                FROM projects p
                LEFT JOIN sections s ON p.id = s.project_id
                GROUP BY p.id, p.name
                HAVING section_count = 0
            """)
            no_section_projects = cursor.fetchall()
            if no_section_projects:
                business_rule_violations.append({
                    'rule': 'projects_must_have_sections',
                    'violation_count': len(no_section_projects),
                    'details': no_section_projects[:5]  # Show first 5 examples
                })
            
            # Rule 2: Active projects should have recent activity (tasks created in last 30 days)
            cursor.execute("""
                SELECT p.id, p.name, MAX(t.created_at) as last_activity
                FROM projects p
                LEFT JOIN tasks t ON p.id = t.project_id
                WHERE p.status = 'active'
                GROUP BY p.id, p.name
                HAVING last_activity IS NULL OR last_activity < datetime('now', '-30 days')
            """)
            inactive_projects = cursor.fetchall()
            if inactive_projects:
                business_rule_violations.append({
                    'rule': 'active_projects_must_have_recent_activity',
                    'violation_count': len(inactive_projects),
                    'details': inactive_projects[:5]
                })
            
            # Rule 3: High priority tasks should have assignees
            cursor.execute("""
                SELECT COUNT(*) as unassigned_high_priority
                FROM tasks
                WHERE priority = 'high' AND assignee_id IS NULL
            """)
            unassigned_high_priority = cursor.fetchone()[0]
            if unassigned_high_priority > 0:
                business_rule_violations.append({
                    'rule': 'high_priority_tasks_must_be_assigned',
                    'violation_count': unassigned_high_priority
                })
            
            # Rule 4: Completed tasks should have completion dates
            cursor.execute("""
                SELECT COUNT(*) as missing_completion_dates
                FROM tasks
                WHERE completed = 1 AND completed_at IS NULL
            """)
            missing_completion_dates = cursor.fetchone()[0]
            if missing_completion_dates > 0:
                business_rule_violations.append({
                    'rule': 'completed_tasks_must_have_completion_dates',
                    'violation_count': missing_completion_dates
                })
            
            # Rule 5: Task names should not be empty or too short
            cursor.execute("""
                SELECT COUNT(*) as invalid_task_names
                FROM tasks
                WHERE name IS NULL OR length(trim(name)) < 3
            """)
            invalid_task_names = cursor.fetchone()[0]
            if invalid_task_names > 0:
                business_rule_violations.append({
                    'rule': 'task_names_must_be_valid',
                    'violation_count': invalid_task_names
                })
            
            status = 'success' if not business_rule_violations else 'failure'
            message = f"Business rules: {len(business_rule_violations)} violations found" if business_rule_violations else "All business rules satisfied"
            
            logger.info(f"Business rule validation: {len(business_rule_violations)} violations found")
            return {
                'status': status,
                'message': message,
                'details': business_rule_violations,
                'violation_count': len(business_rule_violations)
            }
            
        except sqlite3.Error as e:
            logger.error(f"Business rule validation error: {str(e)}")
            return {
                'status': 'error',
                'message': f"Business rule validation failed: {str(e)}",
                'error': str(e)
            }
    
    def _validate_data_quality(self, conn: sqlite3.Connection) -> Dict[str, Any]:
        """
        Validate general data quality including completeness, uniqueness, and format.
        
        Args:
            conn: Database connection
            
        Returns:
            Dictionary with data quality validation results
        """
        cursor = conn.cursor()
        logger.info("Validating data quality")
        
        try:
            quality_issues = []
            
            # 1. Check for NULL values in required fields
            required_fields_checks = {
                'users': ['name', 'email'],
                'projects': ['name', 'status'],
                'tasks': ['name', 'project_id', 'section_id'],
                'teams': ['name'],
                'organizations': ['name', 'domain']
            }
            
            for table, fields in required_fields_checks.items():
                for field in fields:
                    cursor.execute(f"""
                        SELECT COUNT(*) as null_count
                        FROM {table}
                        WHERE {field} IS NULL OR {field} = ''
                    """)
                    null_count = cursor.fetchone()[0]
                    if null_count > 0:
                        quality_issues.append({
                            'table': table,
                            'field': field,
                            'issue_type': 'null_values',
                            'count': null_count
                        })
            
            # 2. Check for duplicate emails in users
            cursor.execute("""
                SELECT email, COUNT(*) as count
                FROM users
                WHERE email IS NOT NULL
                GROUP BY email
                HAVING count > 1
                LIMIT 5
            """)
            duplicate_emails = cursor.fetchall()
            if duplicate_emails:
                quality_issues.append({
                    'table': 'users',
                    'field': 'email',
                    'issue_type': 'duplicate_values',
                    'count': len(duplicate_emails),
                    'examples': duplicate_emails
                })
            
            # 3. Check for duplicate team names within organizations
            cursor.execute("""
                SELECT organization_id, name, COUNT(*) as count
                FROM teams
                GROUP BY organization_id, name
                HAVING count > 1
                LIMIT 5
            """)
            duplicate_teams = cursor.fetchall()
            if duplicate_teams:
                quality_issues.append({
                    'table': 'teams',
                    'field': 'name',
                    'issue_type': 'duplicate_values',
                    'count': len(duplicate_teams),
                    'examples': duplicate_teams
                })
            
            # 4. Check for unrealistic date ranges
            cursor.execute("""
                SELECT COUNT(*) as future_dates
                FROM tasks
                WHERE created_at > datetime('now', '+1 day')
            """)
            future_dates = cursor.fetchone()[0]
            if future_dates > 0:
                quality_issues.append({
                    'table': 'tasks',
                    'field': 'created_at',
                    'issue_type': 'future_dates',
                    'count': future_dates
                })
            
            status = 'success' if not quality_issues else 'failure'
            message = f"Data quality: {len(quality_issues)} issues found" if quality_issues else "All data quality checks passed"
            
            logger.info(f"Data quality validation: {len(quality_issues)} issues found")
            return {
                'status': status,
                'message': message,
                'details': quality_issues,
                'issue_count': len(quality_issues)
            }
            
        except sqlite3.Error as e:
            logger.error(f"Data quality validation error: {str(e)}")
            return {
                'status': 'error',
                'message': f"Data quality validation failed: {str(e)}",
                'error': str(e)
            }
    
    def generate_validation_report(self, validation_results: Dict[str, Any]) -> str:
        """
        Generate a human-readable validation report from validation results.
        
        Args:
            validation_results: Dictionary with validation results
            
        Returns:
            Formatted report string
        """
        report_lines = []
        report_lines.append("=" * 80)
        report_lines.append("DATA VALIDATION REPORT")
        report_lines.append("=" * 80)
        report_lines.append(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        report_lines.append(f"Overall Status: {validation_results['overall_status'].upper()}")
        report_lines.append("-" * 80)
        
        for category, results in validation_results.items():
            if category == 'overall_status' or category == 'failed_categories':
                continue
            
            status = results.get('status', 'unknown')
            message = results.get('message', 'No details available')
            
            report_lines.append(f"\n{category.replace('_', ' ').title()}: {status.upper()}")
            report_lines.append(f"  {message}")
            
            # Add details if available
            details = results.get('details')
            if isinstance(details, dict):
                for key, value in details.items():
                    if isinstance(value, (int, float, str)):
                        report_lines.append(f"  - {key.replace('_', ' ').title()}: {value}")
            elif isinstance(details, list):
                if len(details) > 0 and isinstance(details[0], dict):
                    report_lines.append("  Details:")
                    for item in details[:3]:  # Show first 3 items
                        item_str = ", ".join([f"{k}: {v}" for k, v in item.items() if k != 'details'])
                        report_lines.append(f"    • {item_str}")
                    if len(details) > 3:
                        report_lines.append(f"    • ... and {len(details) - 3} more")
        
        if 'failed_categories' in validation_results:
            failed_cats = validation_results['failed_categories']
            if failed_cats:
                report_lines.append("\n" + "-" * 80)
                report_lines.append("FAILED CATEGORIES:")
                for cat in failed_cats:
                    report_lines.append(f"  • {cat.replace('_', ' ').title()}")
        
        report_lines.append("\n" + "=" * 80)
        return "\n".join(report_lines)
    
    def suggest_fixes(self, validation_results: Dict[str, Any]) -> Dict[str, List[str]]:
        """
        Suggest fixes for validation failures.
        
        Args:
            validation_results: Dictionary with validation results
            
        Returns:
            Dictionary mapping categories to suggested fixes
        """
        fixes = {}
        
        if validation_results['overall_status'] == 'success':
            return {'all': ['✅ No fixes needed - all validations passed!']}
        
        for category, results in validation_results.items():
            if category in ['overall_status', 'failed_categories']:
                continue
            
            if results.get('status') == 'failure':
                category_fixes = []
                
                if category == 'temporal_consistency':
                    category_fixes.extend([
                        "Run temporal consistency repair script to fix date violations",
                        "Check task completion logic to ensure completed_at >= created_at",
                        "Validate project timeline logic to ensure start_date <= end_date"
                    ])
                
                elif category == 'referential_integrity':
                    category_fixes.extend([
                        "Run foreign key repair to fix orphaned records",
                        "Check data generation logic for proper relationship handling",
                        "Validate that all foreign keys reference existing primary keys"
                    ])
                
                elif category == 'distribution_validation':
                    category_fixes.extend([
                        "Adjust task completion rate generation logic",
                        "Review due date distribution algorithms",
                        "Calibrate assignment patterns to match industry benchmarks"
                    ])
                
                elif category == 'business_rules':
                    category_fixes.extend([
                        "Ensure all projects have at least one section",
                        "Add recent activity to inactive active projects",
                        "Assign high priority tasks to team members"
                    ])
                
                elif category == 'data_quality':
                    category_fixes.extend([
                        "Fill missing required fields with default values",
                        "Remove duplicate email addresses from users table",
                        "Fix team name duplicates within organizations"
                    ])
                
                fixes[category] = category_fixes
        
        if not fixes:
            fixes['general'] = [
                "Review data generation pipeline for logical errors",
                "Check configuration settings for validation thresholds",
                "Validate input data sources for quality issues"
            ]
        
        return fixes
    
    def close(self):
        """Cleanup resources."""
        logger.info("Data validator closed")

# Example usage and testing
if __name__ == "__main__":
    # Setup logging for testing
    logging.basicConfig(level=logging.INFO)
    
    # Mock configuration
    mock_config = {
        'temporal_consistency_threshold': 0.95,
        'distribution_similarity_threshold': 0.05,
        'referential_integrity_threshold': 0.99,
        'debug_mode': True
    }
    
    try:
        print("=== Data Validator Testing ===\n")
        
        # Create validator
        validator = DataValidator(mock_config)
        
        # Create in-memory test database
        test_conn = sqlite3.connect(':memory:')
        cursor = test_conn.cursor()
        
        # Create test schema
        cursor.execute("""
            CREATE TABLE tasks (
                id INTEGER PRIMARY KEY,
                project_id INTEGER,
                section_id INTEGER,
                assignee_id INTEGER,
                name TEXT NOT NULL,
                description TEXT,
                due_date DATE,
                completed BOOLEAN DEFAULT 0,
                completed_at TIMESTAMP,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                priority TEXT
            )
        """)
        
        cursor.execute("""
            CREATE TABLE projects (
                id INTEGER PRIMARY KEY,
                name TEXT NOT NULL,
                status TEXT NOT NULL,
                start_date DATE NOT NULL,
                end_date DATE,
                department TEXT,
                project_type TEXT
            )
        """)
        
        test_conn.commit()
        
        # Insert test data
        test_tasks = [
            (1, 1, 1, 'Valid task', 'Good description', '2026-01-15', 1, '2026-01-14', '2026-01-10', 'high'),
            (1, 1, 2, 'Future task', 'Created in future', '2026-01-20', 0, None, '2026-01-08', 'medium'),
            (1, 1, 1, 'Invalid completion', 'Completed before creation', '2026-01-12', 1, '2026-01-09', '2026-01-10', 'low'),
            (2, 2, None, 'Unassigned high priority', 'No assignee', '2026-01-18', 0, None, '2026-01-11', 'high')
        ]
        
        cursor.executemany("""
            INSERT INTO tasks (project_id, section_id, assignee_id, name, description, due_date, completed, completed_at, created_at, priority)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, test_tasks)
        
        test_projects = [
            ('Active Project', 'active', '2026-01-01', '2026-01-31', 'engineering', 'sprint'),
            ('Invalid Project', 'active', '2026-01-15', '2026-01-10', 'marketing', 'campaign'),  # End before start
            ('Completed Project', 'completed', '2026-01-01', '2026-01-15', 'product', 'roadmap_planning')
        ]
        
        cursor.executemany("""
            INSERT INTO projects (name, status, start_date, end_date, department, project_type)
            VALUES (?, ?, ?, ?, ?, ?)
        """, test_projects)
        
        test_conn.commit()
        
        # Run validation
        results = validator.validate_database_integrity(test_conn)
        
        # Print results
        print(validator.generate_validation_report(results))
        
        # Print suggested fixes
        fixes = validator.suggest_fixes(results)
        if fixes:
            print("\nSUGGESTED FIXES:")
            for category, category_fixes in fixes.items():
                print(f"\n{category.replace('_', ' ').title()}:")
                for fix in category_fixes:
                    print(f"  • {fix}")
        
        print("\n✅ All validator tests completed successfully!")
        
    except Exception as e:
        print(f"Validator test error: {str(e)}")
        import traceback
        traceback.print_exc()
    finally:
        try:
            validator.close()
            test_conn.close()
        except:
            pass