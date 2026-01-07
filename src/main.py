# src/main.py

"""
Main entry point for the Asana RL Environment Seed Data Generator.
This script orchestrates the entire data generation process, from initialization
to database population, ensuring temporal consistency and relational integrity.

The generator creates realistic enterprise workflow data for a B2B SaaS company
with 5,000-10,000 employees, spanning 6 months of historical data.
"""

import logging
import os
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, Any

import sys
import os
# Add the project root to Python path
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, project_root)

from dotenv import load_dotenv
import sqlite3

from src.utils.database import DatabaseManager
from src.utils.logging import setup_logging
from src.utils.validation import DataValidator
from src.utils.temporal import TemporalGenerator
from src.models.organization import OrganizationConfig
from src.models.base import TimeRange
from src.generators.users import UserGenerator
from src.generators.projects import ProjectGenerator
from src.generators.tasks import TaskGenerator
from src.generators.comments import CommentGenerator

def load_configuration() -> Dict[str, Any]:
    """
    Load and validate configuration from environment variables.
    
    Returns:
        Dict[str, Any]: Configuration dictionary with validated values
    """
    load_dotenv()
    
    config = {
        # Database configuration
        'database_path': os.getenv('DATABASE_PATH', 'output/asana_simulation.sqlite'),
        
        # Company simulation parameters
        'company_size_min': int(os.getenv('COMPANY_SIZE_MIN', '5000')),
        'company_size_max': int(os.getenv('COMPANY_SIZE_MAX', '10000')),
        'num_organizations': int(os.getenv('NUM_ORGANIZATIONS', '1')),
        
        # Temporal simulation parameters
        'simulation_start_date': os.getenv('SIMULATION_START_DATE', '2025-07-01'),
        'simulation_end_date': os.getenv('SIMULATION_END_DATE', '2026-01-07'),
        'simulation_timezone': os.getenv('SIMULATION_TIMEZONE', 'America/Los_Angeles'),
        
        # Data generation parameters
        'num_teams_per_org_min': int(os.getenv('NUM_TEAMS_PER_ORGANIZATION_MIN', '5')),
        'num_teams_per_org_max': int(os.getenv('NUM_TEAMS_PER_ORGANIZATION_MAX', '15')),
        'num_users_per_team_min': int(os.getenv('NUM_USERS_PER_TEAM_MIN', '3')),
        'num_users_per_team_max': int(os.getenv('NUM_USERS_PER_TEAM_MAX', '15')),
        'num_projects_per_team_min': int(os.getenv('NUM_PROJECTS_PER_TEAM_MIN', '2')),
        'num_projects_per_team_max': int(os.getenv('NUM_PROJECTS_PER_TEAM_MAX', '8')),
        'num_tasks_per_project_min': int(os.getenv('NUM_TASKS_PER_PROJECT_MIN', '20')),
        'num_tasks_per_project_max': int(os.getenv('NUM_TASKS_PER_PROJECT_MAX', '100')),
        
        # LLM configuration
        'openai_api_key': os.getenv('OPENAI_API_KEY'),
        'openai_model': os.getenv('OPENAI_MODEL', 'gpt-4-turbo'),
        'openai_temperature': float(os.getenv('OPENAI_TEMPERATURE', '0.7')),
        
        # Performance configuration
        'batch_size': int(os.getenv('BATCH_SIZE', '1000')),
        'parallel_workers': int(os.getenv('PARALLEL_WORKERS', '4')),
        
        # Quality control
        'validation_enabled': os.getenv('VALIDATION_ENABLED', 'true').lower() == 'true',
        'debug_mode': os.getenv('DEBUG_MODE', 'false').lower() == 'true',
    }
    
    # Validate critical configuration
    if not config['openai_api_key']:
        raise ValueError("OPENAI_API_KEY is required in .env file")
    
    if config['company_size_min'] > config['company_size_max']:
        raise ValueError("COMPANY_SIZE_MIN cannot be greater than COMPANY_SIZE_MAX")
    
    return config

def ensure_output_directory(database_path: str) -> None:
    """
    Ensure the output directory exists for the database file.
    
    Args:
        database_path: Path to the database file
    """
    output_dir = os.path.dirname(database_path)
    if output_dir and not os.path.exists(output_dir):
        os.makedirs(output_dir, exist_ok=True)
        logging.info(f"Created output directory: {output_dir}")

def generate_seed_data(config: Dict[str, Any]) -> None:
    """
    Main orchestration function for generating seed data.
    
    Args:
        config: Configuration dictionary
    """
    start_time = time.time()
    logging.info("Starting seed data generation...")
    logging.info(f"Configuration loaded: {config}")
    
    # Initialize database
    db_manager = DatabaseManager(config['database_path'], config)
    db_manager.initialize_database('schema.sql')
    db_conn = db_manager._get_connection()
    
    try:
        # Get simulation time range
        temporal_generator = TemporalGenerator(config)
        time_range_dict = temporal_generator.get_simulation_time_range(
            config['simulation_start_date'],
            config['simulation_end_date']
        )
        time_range = TimeRange(
            start_date=time_range_dict['start_date'],
            end_date=time_range_dict['end_date']
        )
        
        # Create organization configuration
        org_config = OrganizationConfig(
            name="Acme Corporation",
            domain="acme.corp",
            size_min=config['company_size_min'],
            size_max=config['company_size_max'],
            num_teams_range=(config['num_teams_per_org_min'], config['num_teams_per_org_max']),
            num_users_per_team_range=(config['num_users_per_team_min'], config['num_users_per_team_max']),
            num_projects_per_team_range=(config['num_projects_per_team_min'], config['num_projects_per_team_max']),
            num_tasks_per_project_range=(config['num_tasks_per_project_min'], config['num_tasks_per_project_max']),
            time_range=time_range
        )
        
        # Initialize generators
        user_generator = UserGenerator(db_conn, config, org_config)
        project_generator = ProjectGenerator(db_conn, config, org_config)
        task_generator = TaskGenerator(db_conn, config, org_config)
        comment_generator = CommentGenerator(db_conn, config, org_config)
        
        logging.info("Starting data generation pipeline...")
        
        # Generate organizations and users
        logging.info("Generating organizations and users...")
        organizations = user_generator.generate_organizations(config['num_organizations'])
        
        # Extract teams and memberships from organizations
        logging.info("Extracting teams and memberships from organizations...")
        teams = []
        team_memberships = []
        for org in organizations:
            teams.extend(org['teams'])
            team_memberships.extend(org['memberships'])
        
        # Generate projects and sections
        logging.info("Generating projects and sections...")
        projects = project_generator.generate_projects_for_teams(teams, [])
        
        # Insert projects to get IDs
        projects = project_generator.insert_projects(projects)
        
        # Generate sections with correct project IDs
        sections = project_generator.generate_sections_for_projects(projects)
        sections = project_generator.insert_sections(sections)
        
        # Generate tasks and subtasks
        logging.info("Generating tasks and subtasks...")
        tasks = task_generator.generate_tasks_for_projects(projects, sections, team_memberships, [], [])
        tasks = task_generator.insert_tasks(tasks)
        subtasks = task_generator.generate_subtasks_for_tasks(tasks, [])
        subtasks = task_generator.insert_subtasks(subtasks)
        
        # Generate comments
        logging.info("Generating comments...")
        comments = comment_generator.generate_comments_for_tasks(tasks, team_memberships, [], projects)
        
        # Generate custom fields and tags (simplified for this example)
        logging.info("Generating custom fields and tags...")
        # This would be implemented in subsequent steps
        
        # Commit all changes
        db_conn.commit()
        logging.info("All data committed to database")
        
        # Run validation if enabled
        if config['validation_enabled']:
            logging.info("Running database validation...")
            validator = DataValidator(config)
            validation_results = validator.validate_database_integrity(db_conn)
            for table, result in validation_results.items():
                if isinstance(result, dict):
                    logging.info(f"{table}: {result['status']} - {result['message']}")
                    if not result['status']:
                        logging.warning(f"Validation failed for {table}: {result['message']}")
                else:
                    logging.info(f"{table}: {result}")
        
        elapsed_time = time.time() - start_time
        logging.info(f"Seed data generation completed successfully in {elapsed_time:.2f} seconds")
        logging.info(f"Database created at: {config['database_path']}")
        
    except Exception as e:
        logging.error(f"Error during data generation: {str(e)}", exc_info=True)
        db_conn.rollback()
        raise
    finally:
        db_conn.close()

def main() -> None:
    """
    Main entry point for the application.
    """
    try:
        # Setup logging first
        setup_logging()
        
        # Load configuration
        config = load_configuration()
        
        # Ensure output directory exists
        ensure_output_directory(config['database_path'])
        
        # Generate seed data
        generate_seed_data(config)
        
    except Exception as e:
        logging.critical(f"Application failed: {str(e)}", exc_info=True)
        raise SystemExit(1)

if __name__ == "__main__":
    main()