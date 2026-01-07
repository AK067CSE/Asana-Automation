

"""
Organization model module for representing organizational structures and configurations.
This module defines data models for organizations, teams, and their configurations
with validation logic and business rules for enterprise workflow generation.

The models are designed to be:
- Type-safe: Strict type checking with validation
- Configurable: Adjustable parameters for different organization sizes and structures
- Extensible: Base classes that can be inherited and extended
- Validatable: Built-in validation logic for business rules
- Serializable: Easy JSON/database serialization
"""

import logging
import random
from datetime import datetime, timedelta
from typing import Optional, Dict, List, Any, Tuple, TypeVar, Generic, Type, Union
from enum import Enum, auto
from dataclasses import dataclass, field, asdict
import json

from src.models.base import BaseModel, ValidationError, ValidationLevel, TimeRange
from src.utils.logging import get_logger

logger = get_logger(__name__)

class OrganizationSize(Enum):
    """Organization size categories."""
    SMALL = auto()      # < 100 employees
    MEDIUM = auto()     # 100-1000 employees  
    LARGE = auto()      # 1000-5000 employees
    ENTERPRISE = auto() # 5000+ employees

class OrganizationType(Enum):
    """Organization type categories."""
    B2B_SAAS = auto()           # Business-to-Business Software as a Service
    ENTERPRISE_SOFTWARE = auto() # Enterprise software company
    FINTECH = auto()            # Financial technology
    ECOMMERCE = auto()          # E-commerce platform
    HEALTHCARE = auto()         # Healthcare technology
    EDUCATION = auto()          # Education technology

@dataclass
class OrganizationConfig(BaseModel):
    """
    Organization configuration model for seed data generation.
    
    This model defines the parameters and structure for generating realistic
    organization data including size, team structures, and temporal ranges.
    """
    name: str
    domain: str
    size_min: int
    size_max: int
    industry: str = "b2b_saas"
    num_teams_range: Tuple[int, int] = field(default_factory=lambda: (5, 15))
    num_users_per_team_range: Tuple[int, int] = field(default_factory=lambda: (3, 15))
    num_projects_per_team_range: Tuple[int, int] = field(default_factory=lambda: (2, 8))
    num_tasks_per_project_range: Tuple[int, int] = field(default_factory=lambda: (20, 100))
    time_range: TimeRange = field(default_factory=lambda: TimeRange(
        start_date=datetime(2025, 7, 1),
        end_date=datetime(2026, 1, 7)
    ))
    
    def _validate_fields(self):
        """Validate individual fields."""
        if not self.name or len(self.name) < 2:
            raise ValidationError("Organization name must be at least 2 characters", "name")
        
        if not self.domain or '@' in self.domain:
            raise ValidationError("Invalid domain format", "domain")
        
        if self.size_min <= 0 or self.size_max <= 0:
            raise ValidationError("Organization size must be positive", "size_min/size_max")
        
        if self.size_min > self.size_max:
            raise ValidationError(f"size_min ({self.size_min}) cannot be greater than size_max ({self.size_max})", "size_min/size_max")
        
        if self.num_teams_range[0] <= 0 or self.num_teams_range[1] <= 0:
            raise ValidationError("Number of teams must be positive", "num_teams_range")
        
        if self.num_teams_range[0] > self.num_teams_range[1]:
            raise ValidationError(f"num_teams_range min ({self.num_teams_range[0]}) cannot be greater than max ({self.num_teams_range[1]})", "num_teams_range")
    
    def _validate_business_rules(self):
        """Validate business rules."""
        # Validate organization size makes sense for B2B SaaS
        org_size = (self.size_min + self.size_max) // 2
        if self.industry == "b2b_saas" and org_size < 1000:
            logger.warning(f"B2B SaaS organization size {org_size} seems small. Consider 1000+ employees for realistic B2B SaaS simulation.")
        
        # Validate time range duration
        duration_days = (self.time_range.end_date - self.time_range.start_date).days
        if duration_days < 30:
            raise ValidationError(f"Time range duration ({duration_days} days) is too short. Minimum 30 days recommended.", "time_range")
        if duration_days > 365:
            logger.warning(f"Time range duration ({duration_days} days) is longer than 1 year. This may impact performance.")
    
    def get_organization_size(self) -> OrganizationSize:
        """Get organization size category based on employee count."""
        avg_size = (self.size_min + self.size_max) // 2
        if avg_size < 100:
            return OrganizationSize.SMALL
        elif avg_size < 1000:
            return OrganizationSize.MEDIUM
        elif avg_size < 5000:
            return OrganizationSize.LARGE
        else:
            return OrganizationSize.ENTERPRISE
    
    def get_estimated_teams(self) -> int:
        """Get estimated number of teams based on organization size."""
        avg_size = (self.size_min + self.size_max) // 2
        size_category = self.get_organization_size()
        
        if size_category == OrganizationSize.SMALL:
            return min(5, max(2, avg_size // 20))
        elif size_category == OrganizationSize.MEDIUM:
            return min(15, max(5, avg_size // 50))
        elif size_category == OrganizationSize.LARGE:
            return min(30, max(10, avg_size // 100))
        else:  # ENTERPRISE
            return min(50, max(15, avg_size // 200))
    
    def get_estimated_users_per_team(self) -> Tuple[int, int]:
        """Get estimated users per team based on organization size."""
        size_category = self.get_organization_size()
        
        if size_category == OrganizationSize.SMALL:
            return (3, 8)
        elif size_category == OrganizationSize.MEDIUM:
            return (5, 12)
        elif size_category == OrganizationSize.LARGE:
            return (8, 15)
        else:  # ENTERPRISE
            return (10, 20)
    
    def __str__(self):
        return f"OrganizationConfig(name='{self.name}', domain='{self.domain}', size={self.size_min}-{self.size_max}, industry='{self.industry}')"

@dataclass
class DepartmentConfig(BaseModel):
    """
    Department configuration model for organizational departments.
    
    This model defines department-specific parameters and structures for
    generating realistic department data within an organization.
    """
    name: str
    description: str
    head_count_range: Tuple[int, int]
    team_structure: Dict[str, Dict[str, Any]] = field(default_factory=dict)
    priority_level: int = 1  # 1 = highest priority
    
    def _validate_fields(self):
        """Validate individual fields."""
        if not self.name or len(self.name) < 2:
            raise ValidationError("Department name must be at least 2 characters", "name")
        
        if self.head_count_range[0] <= 0 or self.head_count_range[1] <= 0:
            raise ValidationError("Head count must be positive", "head_count_range")
        
        if self.head_count_range[0] > self.head_count_range[1]:
            raise ValidationError(f"head_count_range min ({self.head_count_range[0]}) cannot be greater than max ({self.head_count_range[1]})", "head_count_range")
        
        if self.priority_level < 1 or self.priority_level > 10:
            raise ValidationError("Priority level must be between 1 and 10", "priority_level")
    
    def _validate_business_rules(self):
        """Validate business rules."""
        avg_head_count = (self.head_count_range[0] + self.head_count_range[1]) // 2
        if avg_head_count > 500 and self.priority_level < 3:
            logger.warning(f"Large department ({avg_head_count} employees) with low priority level ({self.priority_level}) may not be realistic")
    
    def get_team_types(self) -> List[str]:
        """Get team types available in this department."""
        return list(self.team_structure.keys())
    
    def get_team_structure_for_type(self, team_type: str) -> Dict[str, Any]:
        """Get team structure configuration for a specific team type."""
        return self.team_structure.get(team_type, {})
    
    def __str__(self):
        return f"DepartmentConfig(name='{self.name}', head_count={self.head_count_range[0]}-{self.head_count_range[1]}, priority={self.priority_level})"

@dataclass
class TeamStructureConfig(BaseModel):
    """
    Team structure configuration model for organizational teams.
    
    This model defines team-specific parameters and structures for
    generating realistic team data within departments.
    """
    team_type: str
    size_range: Tuple[int, int]
    role_distribution: Dict[str, float]
    reporting_structure: List[str] = field(default_factory=list)
    specializations: List[str] = field(default_factory=list)
    
    def _validate_fields(self):
        """Validate individual fields."""
        if not self.team_type or len(self.team_type) < 2:
            raise ValidationError("Team type must be at least 2 characters", "team_type")
        
        if self.size_range[0] <= 0 or self.size_range[1] <= 0:
            raise ValidationError("Team size must be positive", "size_range")
        
        if self.size_range[0] > self.size_range[1]:
            raise ValidationError(f"size_range min ({self.size_range[0]}) cannot be greater than max ({self.size_range[1]})", "size_range")
        
        # Validate role distribution sums to approximately 1.0
        total = sum(self.role_distribution.values())
        if abs(total - 1.0) > 0.01:  # Allow small floating point errors
            raise ValidationError(f"Role distribution must sum to 1.0, got {total:.4f}", "role_distribution")
    
    def _validate_business_rules(self):
        """Validate business rules."""
        avg_size = (self.size_range[0] + self.size_range[1]) // 2
        if avg_size < 3 and 'manager' in self.role_distribution:
            logger.warning(f"Small team ({avg_size} members) with manager role may not be realistic")
    
    def get_role_for_experience(self, experience_level: str) -> str:
        """Get appropriate role based on experience level."""
        # This is a simplified mapping - real implementation would be more complex
        role_mapping = {
            'junior': ['junior developer', 'associate', 'intern', 'specialist'],
            'mid': ['developer', 'engineer', 'analyst', 'designer', 'product manager'],
            'senior': ['senior developer', 'senior engineer', 'lead', 'architect', 'manager'],
            'executive': ['director', 'vp', 'cto', 'head of department']
        }
        
        possible_roles = role_mapping.get(experience_level.lower(), list(self.role_distribution.keys()))
        return random.choice(possible_roles)
    
    def __str__(self):
        return f"TeamStructureConfig(type='{self.team_type}', size={self.size_range[0]}-{self.size_range[1]}, roles={len(self.role_distribution)})"

class OrganizationBuilder:
    """
    Builder class for creating organization configurations with sensible defaults.
    
    This class provides a fluent interface for constructing organization configurations
    with validation and reasonable defaults based on industry patterns.
    """
    
    def __init__(self):
        self.config = OrganizationConfig(
            name="Acme Corporation",
            domain="acme.corp",
            size_min=5000,
            size_max=10000,
            industry="b2b_saas",
            num_teams_range=(5, 15),
            num_users_per_team_range=(3, 15),
            num_projects_per_team_range=(2, 8),
            num_tasks_per_project_range=(20, 100),
            time_range=TimeRange(
                start_date=datetime(2025, 7, 1),
                end_date=datetime(2026, 1, 7)
            )
        )
    
    def with_name(self, name: str) -> 'OrganizationBuilder':
        """Set organization name."""
        self.config.name = name
        return self
    
    def with_domain(self, domain: str) -> 'OrganizationBuilder':
        """Set organization domain."""
        self.config.domain = domain
        return self
    
    def with_size_range(self, min_size: int, max_size: int) -> 'OrganizationBuilder':
        """Set organization size range."""
        self.config.size_min = min_size
        self.config.size_max = max_size
        return self
    
    def with_industry(self, industry: str) -> 'OrganizationBuilder':
        """Set organization industry."""
        self.config.industry = industry.lower().replace(' ', '_')
        return self
    
    def with_team_range(self, min_teams: int, max_teams: int) -> 'OrganizationBuilder':
        """Set number of teams range."""
        self.config.num_teams_range = (min_teams, max_teams)
        return self
    
    def with_users_per_team_range(self, min_users: int, max_users: int) -> 'OrganizationBuilder':
        """Set users per team range."""
        self.config.num_users_per_team_range = (min_users, max_users)
        return self
    
    def with_time_range(self, start_date: datetime, end_date: datetime) -> 'OrganizationBuilder':
        """Set time range for data generation."""
        self.config.time_range = TimeRange(start_date=start_date, end_date=end_date)
        return self
    
    def with_b2b_saas_defaults(self) -> 'OrganizationBuilder':
        """Set defaults for B2B SaaS company."""
        return (self
                .with_industry("b2b_saas")
                .with_size_range(5000, 10000)
                .with_team_range(10, 20)
                .with_users_per_team_range(5, 15)
                .with_project_range(3, 10)
                .with_task_range(30, 150))
    
    def with_startup_defaults(self) -> 'OrganizationBuilder':
        """Set defaults for startup company."""
        return (self
                .with_industry("b2b_saas")
                .with_size_range(50, 200)
                .with_team_range(3, 8)
                .with_users_per_team_range(3, 8)
                .with_project_range(2, 5)
                .with_task_range(10, 50))
    
    def with_enterprise_defaults(self) -> 'OrganizationBuilder':
        """Set defaults for large enterprise."""
        return (self
                .with_industry("enterprise_software")
                .with_size_range(10000, 50000)
                .with_team_range(20, 50)
                .with_users_per_team_range(8, 25)
                .with_project_range(5, 15)
                .with_task_range(50, 200))
    
    def with_project_range(self, min_projects: int, max_projects: int) -> 'OrganizationBuilder':
        """Set projects per team range."""
        self.config.num_projects_per_team_range = (min_projects, max_projects)
        return self
    
    def with_task_range(self, min_tasks: int, max_tasks: int) -> 'OrganizationBuilder':
        """Set tasks per project range."""
        self.config.num_tasks_per_project_range = (min_tasks, max_tasks)
        return self
    
    def build(self) -> OrganizationConfig:
        """Build and validate the organization configuration."""
        if not self.config.is_valid():
            validation_results = self.config.validate()
            error_messages = [result['message'] for result in validation_results if result['level'] == 'CRITICAL']
            raise ValueError(f"Invalid organization configuration: {', '.join(error_messages)}")
        
        return self.config

# Example usage and testing
if __name__ == "__main__":
    # Setup logging for testing
    logging.basicConfig(level=logging.INFO)
    
    print("=== Organization Models Testing ===\n")
    
    try:
        # Test OrganizationConfig
        print("Testing OrganizationConfig:")
        
        # Valid configuration
        time_range = TimeRange(
            start_date=datetime(2025, 7, 1),
            end_date=datetime(2026, 1, 7)
        )
        
        org_config = OrganizationConfig(
            name="Test Organization",
            domain="test.org",
            size_min=1000,
            size_max=5000,
            industry="b2b_saas",
            num_teams_range=(5, 15),
            num_users_per_team_range=(3, 15),
            num_projects_per_team_range=(2, 8),
            num_tasks_per_project_range=(20, 100),
            time_range=time_range
        )
        
        validation_results = org_config.validate()
        print(f"  OrganizationConfig: {org_config}")
        print(f"  Organization size category: {org_config.get_organization_size().name}")
        print(f"  Estimated teams: {org_config.get_estimated_teams()}")
        print(f"  Estimated users per team: {org_config.get_estimated_users_per_team()}")
        
        if validation_results:
            print("  Validation errors:")
            for result in validation_results:
                print(f"    - {result['field']}: {result['message']} ({result['level']})")
        else:
            print("  ✓ Validation passed")
        
        # Test invalid configuration
        try:
            invalid_config = OrganizationConfig(
                name="X",  # Too short
                domain="invalid@domain.com",  # Invalid format
                size_min=5000,
                size_max=1000,
                industry="b2b_saas",
                num_teams_range=(15, 5),  # Min > Max
                num_users_per_team_range=(3, 15),
                num_projects_per_team_range=(2, 8),
                num_tasks_per_project_range=(20, 100),
                time_range=time_range
            )
            invalid_config.validate()
        except ValidationError as e:
            print(f"  ✓ Caught expected validation error: {e.message} (field: {e.field})")
        
        # Test DepartmentConfig
        print("\nTesting DepartmentConfig:")
        
        eng_team_structure = {
            'backend': {
                'size_range': (5, 12),
                'tech_stack': ['python', 'java', 'go'],
                'focus_areas': ['api', 'database', 'microservices']
            },
            'frontend': {
                'size_range': (4, 10),
                'tech_stack': ['react', 'typescript', 'css'],
                'focus_areas': ['ui', 'ux', 'performance']
            },
            'devops': {
                'size_range': (3, 8),
                'tech_stack': ['aws', 'kubernetes', 'terraform'],
                'focus_areas': ['infrastructure', 'ci/cd', 'monitoring']
            }
        }
        
        eng_department = DepartmentConfig(
            name="Engineering",
            description="Software development and technical infrastructure",
            head_count_range=(100, 300),
            team_structure=eng_team_structure,
            priority_level=1
        )
        
        print(f"  DepartmentConfig: {eng_department}")
        print(f"  Team types: {eng_department.get_team_types()}")
        print(f"  Backend team structure: {eng_department.get_team_structure_for_type('backend')}")
        
        # Test TeamStructureConfig
        print("\nTesting TeamStructureConfig:")
        
        backend_team = TeamStructureConfig(
            team_type="backend_engineering",
            size_range=(5, 12),
            role_distribution={
                'senior_engineer': 0.3,
                'engineer': 0.4,
                'junior_engineer': 0.2,
                'tech_lead': 0.1
            },
            reporting_structure=['tech_lead', 'engineering_manager', 'vp_engineering'],
            specializations=['api', 'database', 'microservices']
        )
        
        print(f"  TeamStructureConfig: {backend_team}")
        print(f"  Role for senior experience: {backend_team.get_role_for_experience('senior')}")
        print(f"  Role for junior experience: {backend_team.get_role_for_experience('junior')}")
        
        # Test OrganizationBuilder
        print("\nTesting OrganizationBuilder:")
        
        # Build B2B SaaS configuration
        saas_builder = OrganizationBuilder().with_b2b_saas_defaults()
        saas_config = saas_builder.build()
        print(f"  B2B SaaS Config: {saas_config}")
        
        # Build startup configuration
        startup_builder = OrganizationBuilder().with_startup_defaults()
        startup_config = startup_builder.build()
        print(f"  Startup Config: {startup_config}")
        
        # Build enterprise configuration
        enterprise_builder = OrganizationBuilder().with_enterprise_defaults()
        enterprise_config = enterprise_builder.build()
        print(f"  Enterprise Config: {enterprise_config}")
        
        print("\n✅ All organization model tests completed successfully!")
        
    except Exception as e:
        print(f"Test error: {str(e)}")
        import traceback
        traceback.print_exc()