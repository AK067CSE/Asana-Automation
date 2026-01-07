

"""
User model module for representing user structures and configurations.
This module defines data models for users, teams, memberships, and their configurations
with validation logic and business rules for enterprise workflow generation.

The models are designed to be:
- Type-safe: Strict type checking with validation
- Configurable: Adjustable parameters for different user types and roles
- Extensible: Base classes that can be inherited and extended
- Validatable: Built-in validation logic for business rules
- Serializable: Easy JSON/database serialization
"""

import logging
import random
from datetime import datetime, timedelta
from typing import Optional, Dict, List, Any, Tuple, Union
from enum import Enum, auto
from dataclasses import dataclass, field, asdict
import json

from src.models.base import BaseModel, ValidationError, ValidationLevel, TimeRange
from src.models.organization import OrganizationConfig, OrganizationSize
from src.utils.logging import get_logger

logger = get_logger(__name__)

class UserRole(Enum):
    """User role categories within organizations."""
    ADMIN = auto()           # Full administrative access
    MEMBER = auto()          # Regular team member with standard access
    GUEST = auto()           # Limited access, typically external users
    VIEWER = auto()          # Read-only access
    BILLING_MANAGER = auto() # Billing and payment management

class ExperienceLevel(Enum):
    """User experience level categories."""
    JUNIOR = auto()          # 0-2 years experience
    MID = auto()             # 2-5 years experience
    SENIOR = auto()          # 5-10 years experience
    EXECUTIVE = auto()       # 10+ years experience, leadership role
    INTERN = auto()          # Intern or entry-level

class Department(Enum):
    """Standard department categories."""
    ENGINEERING = auto()
    PRODUCT = auto()
    MARKETING = auto()
    SALES = auto()
    OPERATIONS = auto()
    FINANCE = auto()
    HR = auto()
    LEGAL = auto()
    CUSTOMER_SUCCESS = auto()
    DESIGN = auto()

@dataclass
class UserConfig(BaseModel):
    """
    User configuration model for seed data generation.
    
    This model defines the parameters and structure for generating realistic
    user data including roles, experience levels, departments, and demographics.
    """
    first_name: str
    last_name: str
    email: str
    role: UserRole
    department: Department
    experience_level: ExperienceLevel
    hire_date: datetime
    location: str = "Remote"
    manager_id: Optional[int] = None
    skills: List[str] = field(default_factory=list)
    availability_hours: float = 40.0  # Full-time equivalent
    
    def _validate_fields(self):
        """Validate individual fields."""
        if not self.first_name or len(self.first_name) < 2:
            raise ValidationError("First name must be at least 2 characters", "first_name")
        
        if not self.last_name or len(self.last_name) < 2:
            raise ValidationError("Last name must be at least 2 characters", "last_name")
        
        if not self.email or not re.match(r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$', self.email):
            raise ValidationError("Invalid email format", "email")
        
        if not isinstance(self.role, UserRole):
            raise ValidationError("Invalid user role", "role")
        
        if not isinstance(self.department, Department):
            raise ValidationError("Invalid department", "department")
        
        if not isinstance(self.experience_level, ExperienceLevel):
            raise ValidationError("Invalid experience level", "experience_level")
        
        if self.availability_hours <= 0:
            raise ValidationError("Availability hours must be positive", "availability_hours")
    
    def _validate_business_rules(self):
        """Validate business rules."""
        # Validate hire date is not in the future
        if self.hire_date > datetime.now():
            raise ValidationError(f"Hire date ({self.hire_date}) cannot be in the future", "hire_date")
        
        # Validate experience level vs hire date
        years_experience = (datetime.now() - self.hire_date).days / 365.25
        experience_validations = {
            ExperienceLevel.JUNIOR: (0, 2),
            ExperienceLevel.MID: (2, 5),
            ExperienceLevel.SENIOR: (5, 10),
            ExperienceLevel.EXECUTIVE: (10, 50),
            ExperienceLevel.INTERN: (0, 1)
        }
        
        min_years, max_years = experience_validations.get(self.experience_level, (0, 50))
        if years_experience < min_years or years_experience > max_years:
            logger.warning(f"User experience level ({self.experience_level.name}, {years_experience:.1f} years) may not match hire date expectations ({min_years}-{max_years} years)")
        
        # Validate role vs experience level
        role_experience_rules = {
            UserRole.ADMIN: [ExperienceLevel.SENIOR, ExperienceLevel.EXECUTIVE],
            UserRole.MEMBER: [ExperienceLevel.JUNIOR, ExperienceLevel.MID, ExperienceLevel.SENIOR],
            UserRole.GUEST: [ExperienceLevel.JUNIOR, ExperienceLevel.MID, ExperienceLevel.SENIOR, ExperienceLevel.EXECUTIVE, ExperienceLevel.INTERN],
            UserRole.VIEWER: [ExperienceLevel.JUNIOR, ExperienceLevel.MID, ExperienceLevel.SENIOR, ExperienceLevel.EXECUTIVE, ExperienceLevel.INTERN],
            UserRole.BILLING_MANAGER: [ExperienceLevel.MID, ExperienceLevel.SENIOR, ExperienceLevel.EXECUTIVE]
        }
        
        valid_levels = role_experience_rules.get(self.role, [])
        if valid_levels and self.experience_level not in valid_levels:
            logger.warning(f"Role {self.role.name} may not be appropriate for experience level {self.experience_level.name}")
    
    def get_full_name(self) -> str:
        """Get user's full name."""
        return f"{self.first_name} {self.last_name}"
    
    def get_experience_years(self) -> float:
        """Get years of experience based on hire date."""
        return (datetime.now() - self.hire_date).days / 365.25
    
    def is_manager(self) -> bool:
        """Check if user is a manager."""
        return self.role in [UserRole.ADMIN, UserRole.BILLING_MANAGER] or 'manager' in self.skills
    
    def get_work_capacity(self, project_complexity: int = 3) -> float:
        """
        Get realistic work capacity based on experience, role, and project complexity.
        
        Args:
            project_complexity: Complexity level of current project (1-5)
            
        Returns:
            Estimated capacity in story points or similar units
        """
        base_capacity = {
            ExperienceLevel.JUNIOR: 3.0,
            ExperienceLevel.MID: 5.0,
            ExperienceLevel.SENIOR: 8.0,
            ExperienceLevel.EXECUTIVE: 4.0,  # Executives have less hands-on capacity
            ExperienceLevel.INTERN: 2.0
        }
        
        role_multipliers = {
            UserRole.ADMIN: 0.7,      # Admins have management overhead
            UserRole.MEMBER: 1.0,
            UserRole.GUEST: 0.8,      # Guests may have context switching
            UserRole.VIEWER: 0.5,     # Viewers have minimal capacity
            UserRole.BILLING_MANAGER: 0.6
        }
        
        complexity_factor = 1.0 - (project_complexity - 3) * 0.1  # -0.2 to +0.2
        
        capacity = (
            base_capacity.get(self.experience_level, 5.0) *
            role_multipliers.get(self.role, 1.0) *
            complexity_factor *
            (self.availability_hours / 40.0)  # Scale by availability
        )
        
        return max(1.0, capacity)  # Minimum capacity of 1
    
    def __str__(self):
        return f"UserConfig(name='{self.get_full_name()}', role='{self.role.name}', department='{self.department.name}')"

@dataclass
class TeamConfig(BaseModel):
    """
    Team configuration model for seed data generation.
    
    This model defines the parameters and structure for generating realistic
    team data including structures, sizes, and leadership hierarchies.
    """
    name: str
    description: str
    department: Department
    size_range: Tuple[int, int] = field(default_factory=lambda: (3, 10))
    leadership_structure: Dict[str, str] = field(default_factory=dict)
    specialties: List[str] = field(default_factory=list)
    creation_date: datetime = field(default_factory=datetime.now)
    
    def _validate_fields(self):
        """Validate individual fields."""
        if not self.name or len(self.name) < 3:
            raise ValidationError("Team name must be at least 3 characters", "name")
        
        if not self.description or len(self.description) < 10:
            raise ValidationError("Team description must be at least 10 characters", "description")
        
        if not isinstance(self.department, Department):
            raise ValidationError("Invalid department", "department")
        
        if self.size_range[0] <= 0 or self.size_range[1] <= 0:
            raise ValidationError("Team size must be positive", "size_range")
        
        if self.size_range[0] > self.size_range[1]:
            raise ValidationError(f"size_range min ({self.size_range[0]}) cannot be greater than max ({self.size_range[1]})", "size_range")
    
    def _validate_business_rules(self):
        """Validate business rules."""
        # Validate leadership structure
        valid_roles = [r.name for r in UserRole]
        for role, title in self.leadership_structure.items():
            if role not in valid_roles:
                logger.warning(f"Leadership role '{role}' may not be valid. Valid roles: {valid_roles}")
    
    def get_team_size(self) -> int:
        """Get realistic team size based on department and company size."""
        avg_size = (self.size_range[0] + self.size_range[1]) // 2
        
        # Department-specific adjustments
        dept_adjustments = {
            Department.ENGINEERING: 1.2,    # Engineering teams tend to be larger
            Department.PRODUCT: 0.8,        # Product teams are typically smaller
            Department.MARKETING: 1.1,
            Department.SALES: 1.3,          # Sales teams can be larger
            Department.OPERATIONS: 1.0,
            Department.FINANCE: 0.7,        # Finance teams are typically smaller
            Department.HR: 0.6,
            Department.LEGAL: 0.5,          # Legal teams are typically small
            Department.CUSTOMER_SUCCESS: 1.4,  # Customer success teams can be large
            Department.DESIGN: 0.9
        }
        
        multiplier = dept_adjustments.get(self.department, 1.0)
        team_size = int(avg_size * multiplier)
        
        return max(2, min(25, team_size))  # Clamp between 2 and 25
    
    def get_leadership_roles(self) -> List[str]:
        """Get leadership roles for this team."""
        return list(self.leadership_structure.keys())
    
    def get_manager_role(self) -> str:
        """Get the primary manager role for this team."""
        leadership_roles = self.get_leadership_roles()
        if not leadership_roles:
            return UserRole.ADMIN.name
        
        # Priority order for manager roles
        priority_roles = ['ADMIN', 'BILLING_MANAGER', 'MEMBER']
        for role in priority_roles:
            if role in leadership_roles:
                return role
        
        return leadership_roles[0]
    
    def __str__(self):
        return f"TeamConfig(name='{self.name}', department='{self.department.name}', size={self.size_range[0]}-{self.size_range[1]})"

@dataclass
class TeamMembershipConfig(BaseModel):
    """
    Team membership configuration model for seed data generation.
    
    This model defines the parameters and structure for generating realistic
    team membership data including roles, start dates, and membership types.
    """
    user_id: int
    team_id: int
    role: str  # UserRole name or custom role
    start_date: datetime
    end_date: Optional[datetime] = None
    is_primary: bool = True
    responsibilities: List[str] = field(default_factory=list)
    
    def _validate_fields(self):
        """Validate individual fields."""
        if self.user_id <= 0:
            raise ValidationError("User ID must be positive", "user_id")
        
        if self.team_id <= 0:
            raise ValidationError("Team ID must be positive", "team_id")
        
        if not self.role or len(self.role) < 2:
            raise ValidationError("Role must be at least 2 characters", "role")
        
        if self.end_date and self.end_date < self.start_date:
            raise ValidationError(f"End date ({self.end_date}) cannot be before start date ({self.start_date})", "end_date")
    
    def _validate_business_rules(self):
        """Validate business rules."""
        # Validate role consistency
        if self.role.upper() in [r.name for r in UserRole]:
            role_enum = UserRole[self.role.upper()]
            if role_enum == UserRole.ADMIN and not self.is_primary:
                logger.warning("Admin role should typically be primary membership")
        
        # Validate membership duration
        if self.end_date:
            membership_duration = (self.end_date - self.start_date).days
            if membership_duration < 30:
                logger.warning(f"Very short membership duration: {membership_duration} days")
    
    def is_active(self, current_date: Optional[datetime] = None) -> bool:
        """Check if membership is active."""
        current_date = current_date or datetime.now()
        
        if self.end_date and current_date > self.end_date:
            return False
        
        return current_date >= self.start_date
    
    def get_membership_duration_days(self, current_date: Optional[datetime] = None) -> int:
        """Get membership duration in days."""
        current_date = current_date or datetime.now()
        
        if self.end_date and current_date > self.end_date:
            return (self.end_date - self.start_date).days
        
        return (current_date - self.start_date).days
    
    def __str__(self):
        return f"TeamMembershipConfig(user={self.user_id}, team={self.team_id}, role='{self.role}', active={self.is_active()})"

class UserTemplate:
    """
    User template class for generating realistic user structures.
    
    This class provides templates for different user types with pre-configured
    roles, skills, and career progression patterns based on industry patterns.
    """
    
    @staticmethod
    def get_template(experience_level: ExperienceLevel, department: Department) -> Dict[str, Any]:
        """
        Get user template based on experience level and department.
        
        Args:
            experience_level: Experience level
            department: Department name
            
        Returns:
            Dictionary with template configuration
        """
        templates = {
            (ExperienceLevel.JUNIOR, Department.ENGINEERING): {
                'skills': ['python', 'git', 'unit testing', 'basic algorithms'],
                'role': UserRole.MEMBER,
                'availability_hours': 40.0,
                'typical_roles': ['Junior Developer', 'Software Engineer I', 'Associate Engineer']
            },
            (ExperienceLevel.MID, Department.ENGINEERING): {
                'skills': ['python', 'java', 'system design', 'code reviews', 'debugging'],
                'role': UserRole.MEMBER,
                'availability_hours': 40.0,
                'typical_roles': ['Software Engineer', 'Developer', 'Engineer II']
            },
            (ExperienceLevel.SENIOR, Department.ENGINEERING): {
                'skills': ['system architecture', 'performance optimization', 'mentoring', 'technical leadership', 'cloud infrastructure'],
                'role': UserRole.ADMIN,
                'availability_hours': 35.0,  # Senior engineers often have management duties
                'typical_roles': ['Senior Engineer', 'Tech Lead', 'Engineering Manager']
            },
            (ExperienceLevel.JUNIOR, Department.PRODUCT): {
                'skills': ['user research', 'wireframing', 'basic analytics', 'communication'],
                'role': UserRole.MEMBER,
                'availability_hours': 40.0,
                'typical_roles': ['Associate Product Manager', 'Product Assistant']
            },
            (ExperienceLevel.SENIOR, Department.PRODUCT): {
                'skills': ['product strategy', 'market analysis', 'stakeholder management', 'roadmap planning', 'team leadership'],
                'role': UserRole.ADMIN,
                'availability_hours': 35.0,
                'typical_roles': ['Senior Product Manager', 'Product Director', 'Head of Product']
            },
            (ExperienceLevel.MID, Department.MARKETING): {
                'skills': ['content creation', 'social media', 'campaign management', 'analytics', 'seo'],
                'role': UserRole.MEMBER,
                'availability_hours': 40.0,
                'typical_roles': ['Marketing Manager', 'Content Specialist', 'Campaign Manager']
            },
            (ExperienceLevel.SENIOR, Department.SALES): {
                'skills': ['negotiation', 'enterprise sales', 'pipeline management', 'team leadership', 'strategic planning'],
                'role': UserRole.ADMIN,
                'availability_hours': 30.0,  # Sales leaders spend time on strategy
                'typical_roles': ['Sales Director', 'VP of Sales', 'Head of Revenue']
            }
        }
        
        # Get base template
        template = templates.get((experience_level, department))
        
        if not template:
            # Fallback to generic template based on experience level
            generic_templates = {
                ExperienceLevel.JUNIOR: {
                    'skills': ['communication', 'basic tools', 'teamwork', 'learning'],
                    'role': UserRole.MEMBER,
                    'availability_hours': 40.0,
                    'typical_roles': ['Junior Specialist', 'Associate', 'Coordinator']
                },
                ExperienceLevel.MID: {
                    'skills': ['project management', 'communication', 'technical skills', 'problem solving'],
                    'role': UserRole.MEMBER,
                    'availability_hours': 40.0,
                    'typical_roles': ['Specialist', 'Manager', 'Engineer']
                },
                ExperienceLevel.SENIOR: {
                    'skills': ['leadership', 'strategic thinking', 'mentoring', 'decision making', 'stakeholder management'],
                    'role': UserRole.ADMIN,
                    'availability_hours': 35.0,
                    'typical_roles': ['Senior Manager', 'Director', 'Lead']
                },
                ExperienceLevel.EXECUTIVE: {
                    'skills': ['executive leadership', 'strategic planning', 'board communication', 'company vision', 'financial management'],
                    'role': UserRole.ADMIN,
                    'availability_hours': 30.0,
                    'typical_roles': ['VP', 'SVP', 'C-level', 'Executive Director']
                },
                ExperienceLevel.INTERN: {
                    'skills': ['learning', 'assisting', 'basic tasks', 'curiosity'],
                    'role': UserRole.GUEST,
                    'availability_hours': 20.0,  # Interns typically work part-time
                    'typical_roles': ['Intern', 'Student', 'Trainee']
                }
            }
            
            template = generic_templates.get(experience_level, generic_templates[ExperienceLevel.MID])
        
        return template
    
    @staticmethod
    def get_department_roles(department: Department) -> List[str]:
        """Get typical roles for a department."""
        department_roles = {
            Department.ENGINEERING: ['Software Engineer', 'DevOps Engineer', 'QA Engineer', 'Engineering Manager', 'Tech Lead', 'Architect'],
            Department.PRODUCT: ['Product Manager', 'Product Designer', 'Product Analyst', 'Product Owner', 'UX Researcher'],
            Department.MARKETING: ['Marketing Manager', 'Content Marketer', 'SEO Specialist', 'Social Media Manager', 'Growth Marketer'],
            Department.SALES: ['Account Executive', 'Sales Development Representative', 'Sales Manager', 'Customer Success Manager', 'Sales Engineer'],
            Department.OPERATIONS: ['Operations Manager', 'Project Manager', 'Business Analyst', 'Process Analyst', 'Program Manager'],
            Department.FINANCE: ['Financial Analyst', 'Accountant', 'Finance Manager', 'Controller', 'FP&A Analyst'],
            Department.HR: ['HR Manager', 'Recruiter', 'HR Business Partner', 'Talent Acquisition', 'People Operations'],
            Department.LEGAL: ['Legal Counsel', 'Compliance Officer', 'Contracts Manager', 'Privacy Officer'],
            Department.CUSTOMER_SUCCESS: ['Customer Success Manager', 'Support Specialist', 'Account Manager', 'Customer Experience Lead'],
            Department.DESIGN: ['Product Designer', 'UX Designer', 'UI Designer', 'Visual Designer', 'Design Manager']
        }
        
        return department_roles.get(department, ['Specialist', 'Manager', 'Coordinator'])

@dataclass
class UserMetrics(BaseModel):
    """
    User metrics model for tracking user performance and analytics.
    
    This model defines metrics for measuring user productivity, engagement, and outcomes
    with validation logic and business rules.
    """
    user_id: int
    tasks_completed: int = 0
    tasks_assigned: int = 0
    avg_task_completion_time_days: float = 0.0
    on_time_completion_rate: float = 0.0
    quality_score: float = 0.0  # 0.0-1.0 scale
    collaboration_score: float = 0.0  # 0.0-1.0 scale
    last_active_date: datetime = field(default_factory=datetime.now)
    skill_growth_rate: float = 0.0
    
    def _validate_fields(self):
        """Validate individual fields."""
        if self.user_id <= 0:
            raise ValidationError("User ID must be positive", "user_id")
        
        if self.tasks_completed < 0:
            raise ValidationError("Tasks completed cannot be negative", "tasks_completed")
        
        if self.tasks_assigned < 0:
            raise ValidationError("Tasks assigned cannot be negative", "tasks_assigned")
        
        if self.avg_task_completion_time_days < 0:
            raise ValidationError("Average completion time cannot be negative", "avg_task_completion_time_days")
        
        if self.on_time_completion_rate < 0.0 or self.on_time_completion_rate > 1.0:
            raise ValidationError("On-time completion rate must be between 0.0 and 1.0", "on_time_completion_rate")
        
        if self.quality_score < 0.0 or self.quality_score > 1.0:
            raise ValidationError("Quality score must be between 0.0 and 1.0", "quality_score")
        
        if self.collaboration_score < 0.0 or self.collaboration_score > 1.0:
            raise ValidationError("Collaboration score must be between 0.0 and 1.0", "collaboration_score")
    
    def _validate_business_rules(self):
        """Validate business rules."""
        if self.tasks_completed > self.tasks_assigned:
            raise ValidationError(f"Tasks completed ({self.tasks_completed}) cannot exceed tasks assigned ({self.tasks_assigned})", "tasks_completed")
    
    def get_productivity_score(self) -> float:
        """Get overall productivity score (0.0-1.0)."""
        completion_rate = self.tasks_completed / self.tasks_assigned if self.tasks_assigned > 0 else 0.0
        
        productivity_score = (
            completion_rate * 0.4 +           # 40% weight on completion rate
            self.on_time_completion_rate * 0.3 +  # 30% weight on timeliness
            self.quality_score * 0.2 +         # 20% weight on quality
            self.collaboration_score * 0.1    # 10% weight on collaboration
        )
        
        return max(0.0, min(1.0, productivity_score))
    
    def get_performance_rating(self) -> str:
        """Get performance rating based on productivity score."""
        score = self.get_productivity_score()
        
        if score >= 0.8:
            return "Exceeds Expectations"
        elif score >= 0.6:
            return "Meets Expectations"
        elif score >= 0.4:
            return "Needs Improvement"
        else:
            return "Poor Performance"
    
    def update_metrics(self, new_data: Dict[str, Any]):
        """Update metrics with new data."""
        for key, value in new_data.items():
            if hasattr(self, key):
                current_value = getattr(self, key)
                if isinstance(current_value, (int, float)):
                    # For numeric fields, average the new value with current
                    if key == 'avg_task_completion_time_days' and self.tasks_completed > 0:
                        # Weighted average for completion time
                        total_time = self.avg_task_completion_time_days * self.tasks_completed
                        total_time += new_data.get('completion_time', 0.0)
                        if self.tasks_completed + new_data.get('new_completed', 0) > 0:
                            self.avg_task_completion_time_days = total_time / (self.tasks_completed + new_data.get('new_completed', 0))
                    else:
                        setattr(self, key, (current_value + value) / 2)
                else:
                    setattr(self, key, value)
        self.last_active_date = datetime.now()
    
    def __str__(self):
        return f"UserMetrics(user={self.user_id}, productivity={self.get_productivity_score():.2f}, rating='{self.get_performance_rating()}')"

# Example usage and testing
if __name__ == "__main__":
    # Setup logging for testing
    logging.basicConfig(level=logging.INFO)
    
    print("=== User Models Testing ===\n")
    
    try:
        # Test UserConfig
        print("Testing UserConfig:")
        
        # Valid configuration
        hire_date = datetime(2023, 6, 15)
        
        user_config = UserConfig(
            first_name="John",
            last_name="Doe",
            email="john.doe@example.com",
            role=UserRole.MEMBER,
            department=Department.ENGINEERING,
            experience_level=ExperienceLevel.MID,
            hire_date=hire_date,
            location="San Francisco",
            skills=['python', 'system design', 'code reviews'],
            availability_hours=40.0
        )
        
        validation_results = user_config.validate()
        print(f"  UserConfig: {user_config}")
        print(f"  Full name: {user_config.get_full_name()}")
        print(f"  Experience years: {user_config.get_experience_years():.1f}")
        print(f"  Work capacity: {user_config.get_work_capacity(project_complexity=3):.1f}")
        print(f"  Is manager: {user_config.is_manager()}")
        
        if validation_results:
            print("  Validation errors:")
            for result in validation_results:
                print(f"    - {result['field']}: {result['message']} ({result['level']})")
        else:
            print("  ✓ Validation passed")
        
        # Test invalid configuration
        try:
            invalid_config = UserConfig(
                first_name="J",  # Too short
                last_name="D",   # Too short
                email="invalid-email",  # Invalid format
                role=UserRole.MEMBER,
                department=Department.ENGINEERING,
                experience_level=ExperienceLevel.MID,
                hire_date=datetime.now() + timedelta(days=1),  # Future date
                availability_hours=-40.0  # Negative hours
            )
            invalid_config.validate()
        except ValidationError as e:
            print(f"  ✓ Caught expected validation error: {e.message} (field: {e.field})")
        
        # Test TeamConfig
        print("\nTesting TeamConfig:")
        
        engineering_leadership = {
            'ADMIN': 'Engineering Manager',
            'MEMBER': 'Senior Developer'
        }
        
        team_config = TeamConfig(
            name="Backend Engineering",
            description="Backend services and infrastructure team for core platform",
            department=Department.ENGINEERING,
            size_range=(5, 12),
            leadership_structure=engineering_leadership,
            specialties=['api', 'database', 'microservices', 'cloud'],
            creation_date=datetime(2023, 1, 1)
        )
        
        print(f"  TeamConfig: {team_config}")
        print(f"  Team size: {team_config.get_team_size()}")
        print(f"  Leadership roles: {team_config.get_leadership_roles()}")
        print(f"  Manager role: {team_config.get_manager_role()}")
        
        # Test TeamMembershipConfig
        print("\nTesting TeamMembershipConfig:")
        
        membership_config = TeamMembershipConfig(
            user_id=1,
            team_id=1,
            role=UserRole.ADMIN.name,
            start_date=datetime(2023, 1, 1),
            end_date=datetime(2026, 12, 31),
            is_primary=True,
            responsibilities=['team leadership', 'code reviews', 'architecture decisions']
        )
        
        print(f"  TeamMembershipConfig: {membership_config}")
        print(f"  Is active: {membership_config.is_active()}")
        print(f"  Membership duration: {membership_config.get_membership_duration_days()} days")
        
        # Test UserTemplate
        print("\nTesting UserTemplate:")
        
        junior_eng_template = UserTemplate.get_template(ExperienceLevel.JUNIOR, Department.ENGINEERING)
        print(f"  Junior Engineering Template:")
        print(f"    Skills: {junior_eng_template['skills']}")
        print(f"    Role: {junior_eng_template['role'].name}")
        print(f"    Availability: {junior_eng_template['availability_hours']} hours")
        print(f"    Typical roles: {junior_eng_template['typical_roles']}")
        
        senior_prod_template = UserTemplate.get_template(ExperienceLevel.SENIOR, Department.PRODUCT)
        print(f"  Senior Product Template:")
        print(f"    Skills: {senior_prod_template['skills']}")
        print(f"    Role: {senior_prod_template['role'].name}")
        print(f"    Availability: {senior_prod_template['availability_hours']} hours")
        
        # Test engineering department roles
        eng_roles = UserTemplate.get_department_roles(Department.ENGINEERING)
        print(f"  Engineering department roles: {eng_roles}")
        
        # Test UserMetrics
        print("\nTesting UserMetrics:")
        
        user_metrics = UserMetrics(
            user_id=1,
            tasks_completed=25,
            tasks_assigned=30,
            avg_task_completion_time_days=2.5,
            on_time_completion_rate=0.85,
            quality_score=0.9,
            collaboration_score=0.8,
            skill_growth_rate=0.1
        )
        
        print(f"  UserMetrics: {user_metrics}")
        print(f"  Productivity score: {user_metrics.get_productivity_score():.2f}")
        print(f"  Performance rating: {user_metrics.get_performance_rating()}")
        
        # Update metrics
        user_metrics.update_metrics({
            'tasks_completed': 30,
            'tasks_assigned': 35,
            'completion_time': 2.0,
            'new_completed': 5,
            'on_time_completion_rate': 0.9,
            'quality_score': 0.95
        })
        print(f"  Updated metrics: {user_metrics}")
        print(f"  Updated productivity score: {user_metrics.get_productivity_score():.2f}")
        
        print("\n✅ All user model tests completed successfully!")
        
    except Exception as e:
        print(f"Test error: {str(e)}")
        import traceback
        traceback.print_exc()