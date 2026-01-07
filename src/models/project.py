

"""
Project model module for representing project structures and configurations.
This module defines data models for projects, sections, tasks, and their configurations
with validation logic and business rules for enterprise workflow generation.

The models are designed to be:
- Type-safe: Strict type checking with validation
- Configurable: Adjustable parameters for different project types and complexities
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

class ProjectType(Enum):
    """Project type categories."""
    SPRINT = auto()              # Agile development sprint
    BUG_TRACKING = auto()        # Bug tracking and resolution
    FEATURE_DEVELOPMENT = auto() # Long-term feature development
    TECH_DEBT = auto()           # Technical debt reduction
    RESEARCH = auto()            # Research and exploration
    CAMPAIGN = auto()            # Marketing campaign
    CONTENT_CALENDAR = auto()    # Content planning and scheduling
    ROADMAP_PLANNING = auto()    # Strategic roadmap planning
    USER_RESEARCH = auto()       # User research and analysis
    PROCESS_IMPROVEMENT = auto() # Business process improvement
    BUDGET_PLANNING = auto()     # Financial planning and budgeting
    LEAD_GENERATION = auto()     # Sales lead generation
    SALES_PIPELINE = auto()      # Sales pipeline management

class ProjectStatus(Enum):
    """Project status categories."""
    ACTIVE = auto()      # Currently active and being worked on
    COMPLETED = auto()   # Successfully completed
    ARCHIVED = auto()    # Archived/archived (no longer active)
    BLOCKED = auto()     # Blocked and cannot progress
    PLANNING = auto()    # In planning phase
    ON_HOLD = auto()     # On hold temporarily

class PriorityLevel(Enum):
    """Priority levels for tasks and projects."""
    CRITICAL = auto()    # Must be done immediately
    HIGH = auto()        # Important, should be done soon
    MEDIUM = auto()      # Should be done eventually
    LOW = auto()         # Nice to have, can be deferred
    NONE = auto()        # No priority assigned

@dataclass
class ProjectConfig(BaseModel):
    """
    Project configuration model for seed data generation.
    
    This model defines the parameters and structure for generating realistic
    project data including types, timelines, and complexity levels.
    """
    name: str
    description: str
    project_type: ProjectType
    status: ProjectStatus
    department: str
    start_date: datetime
    end_date: Optional[datetime] = None
    priority: PriorityLevel = PriorityLevel.MEDIUM
    complexity_level: int = 3  # 1-5 scale
    team_size_range: Tuple[int, int] = field(default_factory=lambda: (3, 8))
    expected_completion_rate: float = 0.75  # 75% completion rate
    
    def _validate_fields(self):
        """Validate individual fields."""
        if not self.name or len(self.name) < 3:
            raise ValidationError("Project name must be at least 3 characters", "name")
        
        if not self.description or len(self.description) < 10:
            raise ValidationError("Project description must be at least 10 characters", "description")
        
        if not isinstance(self.project_type, ProjectType):
            raise ValidationError("Invalid project type", "project_type")
        
        if not isinstance(self.status, ProjectStatus):
            raise ValidationError("Invalid project status", "status")
        
        if not self.department or len(self.department) < 2:
            raise ValidationError("Department name must be at least 2 characters", "department")
        
        if self.complexity_level < 1 or self.complexity_level > 5:
            raise ValidationError("Complexity level must be between 1 and 5", "complexity_level")
        
        if self.team_size_range[0] <= 0 or self.team_size_range[1] <= 0:
            raise ValidationError("Team size must be positive", "team_size_range")
        
        if self.team_size_range[0] > self.team_size_range[1]:
            raise ValidationError(f"team_size_range min ({self.team_size_range[0]}) cannot be greater than max ({self.team_size_range[1]})", "team_size_range")
        
        if self.expected_completion_rate < 0.0 or self.expected_completion_rate > 1.0:
            raise ValidationError("Expected completion rate must be between 0.0 and 1.0", "expected_completion_rate")
    
    def _validate_business_rules(self):
        """Validate business rules."""
        # Validate dates
        if self.end_date and self.end_date < self.start_date:
            raise ValidationError(f"End date ({self.end_date}) cannot be before start date ({self.start_date})", "end_date")
        
        # Validate completion rate based on project type
        completion_rate_rules = {
            ProjectType.SPRINT: (0.7, 0.9),        # 70-90% completion
            ProjectType.BUG_TRACKING: (0.6, 0.8),  # 60-80% completion  
            ProjectType.FEATURE_DEVELOPMENT: (0.5, 0.7),  # 50-70% completion
            ProjectType.TECH_DEBT: (0.4, 0.6),     # 40-60% completion
            ProjectType.RESEARCH: (0.3, 0.5),      # 30-50% completion
            ProjectType.CAMPAIGN: (0.65, 0.85),    # 65-85% completion
            ProjectType.CONTENT_CALENDAR: (0.75, 0.95),  # 75-95% completion
            ProjectType.ROADMAP_PLANNING: (0.4, 0.6),    # 40-60% completion
            ProjectType.USER_RESEARCH: (0.5, 0.7),   # 50-70% completion
            ProjectType.PROCESS_IMPROVEMENT: (0.55, 0.75),  # 55-75% completion
            ProjectType.BUDGET_PLANNING: (0.6, 0.8),     # 60-80% completion
            ProjectType.LEAD_GENERATION: (0.65, 0.85),   # 65-85% completion
            ProjectType.SALES_PIPELINE: (0.5, 0.7)       # 50-70% completion
        }
        
        min_rate, max_rate = completion_rate_rules.get(self.project_type, (0.4, 0.8))
        if self.expected_completion_rate < min_rate or self.expected_completion_rate > max_rate:
            logger.warning(f"Expected completion rate {self.expected_completion_rate:.2f} may not be realistic for {self.project_type.name} projects. Recommended range: {min_rate:.2f}-{max_rate:.2f}")
        
        # Validate complexity level based on project type
        complexity_rules = {
            ProjectType.SPRINT: (2, 4),           # Moderate complexity
            ProjectType.BUG_TRACKING: (1, 3),     # Low to moderate complexity
            ProjectType.FEATURE_DEVELOPMENT: (3, 5),  # High complexity
            ProjectType.TECH_DEBT: (2, 4),        # Moderate complexity
            ProjectType.RESEARCH: (2, 5),         # Moderate to high complexity
            ProjectType.CAMPAIGN: (3, 4),         # Moderate to high complexity
            ProjectType.CONTENT_CALENDAR: (2, 3),  # Low to moderate complexity
            ProjectType.ROADMAP_PLANNING: (4, 5),   # High complexity
            ProjectType.USER_RESEARCH: (3, 4),    # Moderate to high complexity
            ProjectType.PROCESS_IMPROVEMENT: (3, 4),  # Moderate to high complexity
            ProjectType.BUDGET_PLANNING: (4, 5),     # High complexity
            ProjectType.LEAD_GENERATION: (2, 4),   # Moderate complexity
            ProjectType.SALES_PIPELINE: (3, 4)     # Moderate to high complexity
        }
        
        min_complexity, max_complexity = complexity_rules.get(self.project_type, (1, 5))
        if self.complexity_level < min_complexity or self.complexity_level > max_complexity:
            logger.warning(f"Complexity level {self.complexity_level} may not be realistic for {self.project_type.name} projects. Recommended range: {min_complexity}-{max_complexity}")
    
    def get_realistic_duration_days(self) -> int:
        """Get realistic project duration in days based on project type and complexity."""
        duration_rules = {
            ProjectType.SPRINT: (10, 16),           # 2 weeks
            ProjectType.BUG_TRACKING: (30, 90),     # 1-3 months
            ProjectType.FEATURE_DEVELOPMENT: (60, 180),  # 2-6 months
            ProjectType.TECH_DEBT: (30, 90),        # 1-3 months
            ProjectType.RESEARCH: (14, 45),         # 2 weeks - 1.5 months
            ProjectType.CAMPAIGN: (30, 60),         # 1-2 months
            ProjectType.CONTENT_CALENDAR: (30, 90),  # 1-3 months
            ProjectType.ROADMAP_PLANNING: (14, 30),   # 2-4 weeks
            ProjectType.USER_RESEARCH: (14, 45),    # 2 weeks - 1.5 months
            ProjectType.PROCESS_IMPROVEMENT: (30, 120),  # 1-4 months
            ProjectType.BUDGET_PLANNING: (14, 30),     # 2-4 weeks
            ProjectType.LEAD_GENERATION: (30, 90),   # 1-3 months
            ProjectType.SALES_PIPELINE: (30, 90)     # 1-3 months
        }
        
        min_days, max_days = duration_rules.get(self.project_type, (30, 90))
        
        # Adjust based on complexity level
        if self.complexity_level == 5:
            min_days = int(min_days * 1.5)
            max_days = int(max_days * 1.5)
        elif self.complexity_level == 1:
            min_days = int(min_days * 0.7)
            max_days = int(max_days * 0.7)
        
        return random.randint(min_days, max_days)
    
    def get_realistic_team_size(self) -> int:
        """Get realistic team size based on project type and complexity."""
        avg_team_size = (self.team_size_range[0] + self.team_size_range[1]) // 2
        
        # Adjust based on project type and complexity
        project_type_multipliers = {
            ProjectType.SPRINT: 1.0,
            ProjectType.BUG_TRACKING: 0.8,
            ProjectType.FEATURE_DEVELOPMENT: 1.2,
            ProjectType.TECH_DEBT: 0.9,
            ProjectType.RESEARCH: 0.7,
            ProjectType.CAMPAIGN: 1.3,
            ProjectType.CONTENT_CALENDAR: 1.1,
            ProjectType.ROADMAP_PLANNING: 0.8,
            ProjectType.USER_RESEARCH: 0.7,
            ProjectType.PROCESS_IMPROVEMENT: 1.0,
            ProjectType.BUDGET_PLANNING: 0.6,
            ProjectType.LEAD_GENERATION: 1.2,
            ProjectType.SALES_PIPELINE: 1.1
        }
        
        multiplier = project_type_multipliers.get(self.project_type, 1.0)
        team_size = int(avg_team_size * multiplier)
        
        # Adjust based on complexity
        if self.complexity_level == 5:
            team_size = int(team_size * 1.3)
        elif self.complexity_level == 1:
            team_size = max(2, int(team_size * 0.7))
        
        return max(2, min(20, team_size))  # Clamp between 2 and 20
    
    def __str__(self):
        return f"ProjectConfig(name='{self.name}', type='{self.project_type.name}', status='{self.status.name}', department='{self.department}')"

@dataclass
class SectionConfig(BaseModel):
    """
    Section configuration model for project sections.
    
    This model defines section-specific parameters and structures for
    generating realistic section data within projects.
    """
    name: str
    position: int = 0
    description: str = ""
    workflow_rules: Dict[str, Any] = field(default_factory=dict)
    auto_progress_rules: Dict[str, Any] = field(default_factory=dict)
    
    def _validate_fields(self):
        """Validate individual fields."""
        if not self.name or len(self.name) < 2:
            raise ValidationError("Section name must be at least 2 characters", "name")
        
        if self.position < 0:
            raise ValidationError("Section position cannot be negative", "position")
    
    def _validate_business_rules(self):
        """Validate business rules."""
        # Validate workflow rules
        if self.workflow_rules:
            if 'max_tasks' in self.workflow_rules and self.workflow_rules['max_tasks'] <= 0:
                raise ValidationError("Max tasks must be positive", "workflow_rules.max_tasks")
            
            if 'min_tasks' in self.workflow_rules and self.workflow_rules['min_tasks'] < 0:
                raise ValidationError("Min tasks cannot be negative", "workflow_rules.min_tasks")
    
    def get_workflow_limit(self) -> Optional[int]:
        """Get workflow limit for this section."""
        return self.workflow_rules.get('max_tasks')
    
    def has_auto_progress(self) -> bool:
        """Check if this section has auto-progress rules."""
        return bool(self.auto_progress_rules)
    
    def __str__(self):
        return f"SectionConfig(name='{self.name}', position={self.position})"

@dataclass
class TaskConfig(BaseModel):
    """
    Task configuration model for seed data generation.
    
    This model defines the parameters and structure for generating realistic
    task data including names, descriptions, due dates, and assignments.
    """
    name: str
    section_name: str
    description: Optional[str] = None
    assignee_role: Optional[str] = None
    due_date_offset: Optional[int] = None  # Days from project start
    priority: PriorityLevel = PriorityLevel.MEDIUM
    estimated_hours: float = 8.0  # Default 1 day
    requires_review: bool = False
    has_subtasks: bool = False
    completion_probability: float = 0.75
    
    def _validate_fields(self):
        """Validate individual fields."""
        if not self.name or len(self.name) < 3:
            raise ValidationError("Task name must be at least 3 characters", "name")
        
        if self.due_date_offset is not None and self.due_date_offset < 0:
            raise ValidationError("Due date offset cannot be negative", "due_date_offset")
        
        if self.estimated_hours <= 0:
            raise ValidationError("Estimated hours must be positive", "estimated_hours")
        
        if self.completion_probability < 0.0 or self.completion_probability > 1.0:
            raise ValidationError("Completion probability must be between 0.0 and 1.0", "completion_probability")
    
    def _validate_business_rules(self):
        """Validate business rules."""
        # Validate priority based on section name
        high_priority_sections = ['urgent', 'critical', 'blocked', 'today']
        if any(section in self.section_name.lower() for section in high_priority_sections):
            if self.priority not in [PriorityLevel.CRITICAL, PriorityLevel.HIGH]:
                logger.warning(f"Task in '{self.section_name}' section should have high priority, but has {self.priority.name}")
        
        # Validate estimated hours based on priority
        if self.priority == PriorityLevel.CRITICAL and self.estimated_hours > 4.0:
            logger.warning(f"Critical priority task should have low estimated hours (<=4), but has {self.estimated_hours}")
    
    def get_realistic_due_date(self, project_start_date: datetime) -> datetime:
        """Get realistic due date based on project start date and offset."""
        if self.due_date_offset is not None:
            return project_start_date + timedelta(days=self.due_date_offset)
        
        # Generate realistic offset based on priority and estimated hours
        priority_offsets = {
            PriorityLevel.CRITICAL: (1, 3),    # 1-3 days
            PriorityLevel.HIGH: (3, 7),        # 3-7 days  
            PriorityLevel.MEDIUM: (7, 14),     # 1-2 weeks
            PriorityLevel.LOW: (14, 30)        # 2-4 weeks
        }
        
        min_offset, max_offset = priority_offsets.get(self.priority, (7, 14))
        offset_days = random.randint(min_offset, max_offset)
        
        return project_start_date + timedelta(days=offset_days)
    
    def get_realistic_completion_probability(self, project_progress: float) -> float:
        """Get realistic completion probability based on project progress."""
        base_prob = self.completion_probability
        
        # Adjust based on priority
        priority_adjustments = {
            PriorityLevel.CRITICAL: 0.2,
            PriorityLevel.HIGH: 0.1,
            PriorityLevel.MEDIUM: 0.0,
            PriorityLevel.LOW: -0.1,
            PriorityLevel.NONE: -0.2
        }
        
        adjustment = priority_adjustments.get(self.priority, 0.0)
        adjusted_prob = base_prob + adjustment
        
        # Adjust based on project progress (tasks more likely completed in later stages)
        progress_adjustment = (project_progress - 0.5) * 0.4  # -0.2 to +0.2
        adjusted_prob += progress_adjustment
        
        return max(0.1, min(0.95, adjusted_prob))  # Clamp between 10% and 95%
    
    def __str__(self):
        return f"TaskConfig(name='{self.name}', section='{self.section_name}', priority='{self.priority.name}')"

class ProjectTemplate:
    """
    Project template class for generating realistic project structures.
    
    This class provides templates for different project types with pre-configured
    sections, tasks, and workflow rules based on industry patterns.
    """
    
    @staticmethod
    def get_template(project_type: ProjectType, department: str) -> Dict[str, Any]:
        """
        Get project template based on project type and department.
        
        Args:
            project_type: Project type
            department: Department name
            
        Returns:
            Dictionary with template configuration
        """
        templates = {
            ProjectType.SPRINT: {
                'sections': [
                    {'name': 'Backlog', 'position': 0, 'workflow_rules': {'max_tasks': 50}},
                    {'name': 'Ready', 'position': 1, 'workflow_rules': {'max_tasks': 10}},
                    {'name': 'In Progress', 'position': 2, 'workflow_rules': {'max_tasks': 5}},
                    {'name': 'In Review', 'position': 3, 'workflow_rules': {'max_tasks': 3}},
                    {'name': 'Done', 'position': 4}
                ],
                'task_patterns': [
                    {'name': 'Implement {feature} feature', 'priority': PriorityLevel.HIGH, 'estimated_hours': 8},
                    {'name': 'Fix bug in {module}', 'priority': PriorityLevel.CRITICAL, 'estimated_hours': 4},
                    {'name': 'Write unit tests for {component}', 'priority': PriorityLevel.MEDIUM, 'estimated_hours': 4},
                    {'name': 'Refactor {service} code', 'priority': PriorityLevel.LOW, 'estimated_hours': 6},
                    {'name': 'Update documentation for {feature}', 'priority': PriorityLevel.LOW, 'estimated_hours': 2}
                ],
                'completion_rate': 0.75,
                'complexity_level': 3
            },
            ProjectType.BUG_TRACKING: {
                'sections': [
                    {'name': 'New', 'position': 0},
                    {'name': 'Triaged', 'position': 1},
                    {'name': 'In Progress', 'position': 2},
                    {'name': 'Resolved', 'position': 3},
                    {'name': 'Verified', 'position': 4}
                ],
                'task_patterns': [
                    {'name': '[BUG] {severity}: {component} - {description}', 'priority': PriorityLevel.CRITICAL, 'estimated_hours': 2},
                    {'name': 'Fix {bug_type} in {module} when {condition}', 'priority': PriorityLevel.HIGH, 'estimated_hours': 4},
                    {'name': '{component} crashes when {scenario}', 'priority': PriorityLevel.HIGH, 'estimated_hours': 6},
                    {'name': 'Performance issue: {metric} degrades in {environment}', 'priority': PriorityLevel.MEDIUM, 'estimated_hours': 8}
                ],
                'completion_rate': 0.65,
                'complexity_level': 2
            },
            ProjectType.CAMPAIGN: {
                'sections': [
                    {'name': 'Planning', 'position': 0},
                    {'name': 'Content Creation', 'position': 1},
                    {'name': 'Review & Approval', 'position': 2},
                    {'name': 'Launch', 'position': 3},
                    {'name': 'Analysis', 'position': 4}
                ],
                'task_patterns': [
                    {'name': 'Create content calendar for {campaign}', 'priority': PriorityLevel.HIGH, 'estimated_hours': 6},
                    {'name': 'Design graphics for {platform}', 'priority': PriorityLevel.HIGH, 'estimated_hours': 8},
                    {'name': 'Write copy for {asset}', 'priority': PriorityLevel.MEDIUM, 'estimated_hours': 4},
                    {'name': 'Schedule posts for {channel}', 'priority': PriorityLevel.LOW, 'estimated_hours': 2},
                    {'name': 'Analyze campaign performance for {metric}', 'priority': PriorityLevel.MEDIUM, 'estimated_hours': 4}
                ],
                'completion_rate': 0.8,
                'complexity_level': 4
            },
            ProjectType.ROADMAP_PLANNING: {
                'sections': [
                    {'name': 'Q1 Planning', 'position': 0},
                    {'name': 'Q2 Planning', 'position': 1},
                    {'name': 'Q3 Planning', 'position': 2},
                    {'name': 'Q4 Planning', 'position': 3},
                    {'name': 'Long-term Vision', 'position': 4}
                ],
                'task_patterns': [
                    {'name': 'Define OKRs for {quarter}', 'priority': PriorityLevel.HIGH, 'estimated_hours': 8},
                    {'name': 'Prioritize features for {release}', 'priority': PriorityLevel.HIGH, 'estimated_hours': 6},
                    {'name': 'Estimate resource needs for {initiative}', 'priority': PriorityLevel.MEDIUM, 'estimated_hours': 4},
                    {'name': 'Coordinate with {team} on {dependency}', 'priority': PriorityLevel.MEDIUM, 'estimated_hours': 2},
                    {'name': 'Present roadmap to {stakeholders}', 'priority': PriorityLevel.HIGH, 'estimated_hours': 4}
                ],
                'completion_rate': 0.55,
                'complexity_level': 5
            }
        }
        
        # Get base template
        template = templates.get(project_type)
        
        if not template:
            # Fallback to generic template
            return {
                'sections': [
                    {'name': 'To Do', 'position': 0},
                    {'name': 'In Progress', 'position': 1},
                    {'name': 'Done', 'position': 2}
                ],
                'task_patterns': [
                    {'name': 'Complete {task} for {project}', 'priority': PriorityLevel.MEDIUM, 'estimated_hours': 4},
                    {'name': 'Review {deliverable} for {stakeholder}', 'priority': PriorityLevel.MEDIUM, 'estimated_hours': 2},
                    {'name': 'Prepare {document} for {meeting}', 'priority': PriorityLevel.LOW, 'estimated_hours': 3}
                ],
                'completion_rate': 0.65,
                'complexity_level': 3
            }
        
        # Adjust template based on department
        department_adjustments = {
            'engineering': {
                'completion_rate': 1.0,  # Use template rate
                'complexity_level': 1.1  # Slightly more complex
            },
            'marketing': {
                'completion_rate': 1.05,  # Slightly higher completion
                'complexity_level': 0.9  # Slightly less complex
            },
            'product': {
                'completion_rate': 0.95,  # Slightly lower completion
                'complexity_level': 1.2  # More complex
            },
            'sales': {
                'completion_rate': 1.1,   # Higher completion
                'complexity_level': 0.8   # Less complex
            },
            'operations': {
                'completion_rate': 0.9,   # Lower completion
                'complexity_level': 1.0   # Same complexity
            }
        }
        
        adjustments = department_adjustments.get(department.lower(), {})
        
        adjusted_template = template.copy()
        if 'completion_rate' in template:
            adjusted_template['completion_rate'] *= adjustments.get('completion_rate', 1.0)
            adjusted_template['completion_rate'] = max(0.3, min(0.95, adjusted_template['completion_rate']))
        
        if 'complexity_level' in template:
            adjusted_template['complexity_level'] = int(
                adjusted_template['complexity_level'] * adjustments.get('complexity_level', 1.0)
            )
            adjusted_template['complexity_level'] = max(1, min(5, adjusted_template['complexity_level']))
        
        return adjusted_template

@dataclass
class ProjectMetrics(BaseModel):
    """
    Project metrics model for tracking project performance and analytics.
    
    This model defines metrics for measuring project health, progress, and outcomes
    with validation logic and business rules.
    """
    total_tasks: int = 0
    completed_tasks: int = 0
    overdue_tasks: int = 0
    avg_completion_time_days: float = 0.0
    team_velocity: float = 0.0
    blocker_count: int = 0
    risk_score: float = 0.0  # 0.0-1.0 scale
    last_updated: datetime = field(default_factory=datetime.now)
    
    def _validate_fields(self):
        """Validate individual fields."""
        if self.total_tasks < 0:
            raise ValidationError("Total tasks cannot be negative", "total_tasks")
        
        if self.completed_tasks < 0:
            raise ValidationError("Completed tasks cannot be negative", "completed_tasks")
        
        if self.overdue_tasks < 0:
            raise ValidationError("Overdue tasks cannot be negative", "overdue_tasks")
        
        if self.completed_tasks > self.total_tasks:
            raise ValidationError(f"Completed tasks ({self.completed_tasks}) cannot exceed total tasks ({self.total_tasks})", "completed_tasks")
        
        if self.avg_completion_time_days < 0:
            raise ValidationError("Average completion time cannot be negative", "avg_completion_time_days")
        
        if self.team_velocity < 0:
            raise ValidationError("Team velocity cannot be negative", "team_velocity")
        
        if self.blocker_count < 0:
            raise ValidationError("Blocker count cannot be negative", "blocker_count")
        
        if self.risk_score < 0.0 or self.risk_score > 1.0:
            raise ValidationError("Risk score must be between 0.0 and 1.0", "risk_score")
    
    def _validate_business_rules(self):
        """Validate business rules."""
        # Calculate completion rate
        completion_rate = self.completed_tasks / self.total_tasks if self.total_tasks > 0 else 0.0
        
        # Validate risk score based on project health
        expected_risk = 0.0
        if completion_rate < 0.5:
            expected_risk += 0.3
        if self.overdue_tasks > 0:
            expected_risk += 0.2
        if self.blocker_count > 0:
            expected_risk += 0.2
        
        if abs(self.risk_score - expected_risk) > 0.3:
            logger.warning(f"Risk score ({self.risk_score:.2f}) may not match project health indicators. Expected around {expected_risk:.2f}")
    
    def get_completion_rate(self) -> float:
        """Get task completion rate."""
        return self.completed_tasks / self.total_tasks if self.total_tasks > 0 else 0.0
    
    def get_health_score(self) -> float:
        """Get overall project health score (0.0-1.0)."""
        completion_rate = self.get_completion_rate()
        overdue_rate = self.overdue_tasks / self.total_tasks if self.total_tasks > 0 else 0.0
        blocker_impact = min(0.5, self.blocker_count * 0.1)
        
        health_score = (
            completion_rate * 0.6 -    # 60% weight on completion
            overdue_rate * 0.25 -      # 25% penalty for overdue tasks
            blocker_impact * 0.15      # 15% penalty for blockers
        )
        
        return max(0.0, min(1.0, health_score))
    
    def update_metrics(self, new_data: Dict[str, Any]):
        """Update metrics with new data."""
        for key, value in new_data.items():
            if hasattr(self, key):
                setattr(self, key, value)
        self.last_updated = datetime.now()
    
    def __str__(self):
        return f"ProjectMetrics(tasks={self.completed_tasks}/{self.total_tasks}, completion={self.get_completion_rate():.1%}, health={self.get_health_score():.2f})"

# Example usage and testing
if __name__ == "__main__":
    # Setup logging for testing
    logging.basicConfig(level=logging.INFO)
    
    print("=== Project Models Testing ===\n")
    
    try:
        # Test ProjectConfig
        print("Testing ProjectConfig:")
        
        # Valid configuration
        start_date = datetime(2025, 12, 1)
        end_date = datetime(2026, 1, 15)
        
        project_config = ProjectConfig(
            name="Engineering Sprint Q1",
            description="Q1 development sprint for core platform features",
            project_type=ProjectType.SPRINT,
            status=ProjectStatus.ACTIVE,
            department="engineering",
            start_date=start_date,
            end_date=end_date,
            priority=PriorityLevel.HIGH,
            complexity_level=4,
            team_size_range=(5, 10),
            expected_completion_rate=0.8
        )
        
        validation_results = project_config.validate()
        print(f"  ProjectConfig: {project_config}")
        print(f"  Realistic duration: {project_config.get_realistic_duration_days()} days")
        print(f"  Realistic team size: {project_config.get_realistic_team_size()} members")
        
        if validation_results:
            print("  Validation errors:")
            for result in validation_results:
                print(f"    - {result['field']}: {result['message']} ({result['level']})")
        else:
            print("  ✓ Validation passed")
        
        # Test invalid configuration
        try:
            invalid_config = ProjectConfig(
                name="X",  # Too short
                description="Short desc",  # Too short
                project_type=ProjectType.SPRINT,
                status=ProjectStatus.ACTIVE,
                department="eng",  # Too short
                start_date=start_date,
                end_date=start_date - timedelta(days=1),  # End before start
                complexity_level=6,  # Too high
                team_size_range=(10, 5),  # Min > Max
                expected_completion_rate=1.5  # Too high
            )
            invalid_config.validate()
        except ValidationError as e:
            print(f"  ✓ Caught expected validation error: {e.message} (field: {e.field})")
        
        # Test SectionConfig
        print("\nTesting SectionConfig:")
        
        section_config = SectionConfig(
            name="In Progress",
            position=2,
            description="Tasks currently being worked on",
            workflow_rules={'max_tasks': 5, 'min_tasks': 1},
            auto_progress_rules={'max_age_days': 7, 'auto_move_to': 'Review'}
        )
        
        print(f"  SectionConfig: {section_config}")
        print(f"  Workflow limit: {section_config.get_workflow_limit()}")
        print(f"  Has auto-progress: {section_config.has_auto_progress()}")
        
        # Test TaskConfig
        print("\nTesting TaskConfig:")
        
        task_config = TaskConfig(
            name="Implement user authentication API",
            section_name="In Progress",
            assignee_role="Senior Engineer",
            due_date_offset=5,
            priority=PriorityLevel.HIGH,
            estimated_hours=12.0,
            requires_review=True,
            has_subtasks=True,
            completion_probability=0.85
        )
        
        print(f"  TaskConfig: {task_config}")
        print(f"  Realistic due date: {task_config.get_realistic_due_date(start_date)}")
        print(f"  Completion probability at 50% project progress: {task_config.get_realistic_completion_probability(0.5):.2f}")
        
        # Test ProjectTemplate
        print("\nTesting ProjectTemplate:")
        
        sprint_template = ProjectTemplate.get_template(ProjectType.SPRINT, "engineering")
        print(f"  Sprint template sections: {[section['name'] for section in sprint_template['sections']]}")
        print(f"  Sprint template task patterns: {[pattern['name'] for pattern in sprint_template['task_patterns']]}")
        print(f"  Sprint template completion rate: {sprint_template['completion_rate']:.2f}")
        
        campaign_template = ProjectTemplate.get_template(ProjectType.CAMPAIGN, "marketing")
        print(f"  Campaign template sections: {[section['name'] for section in campaign_template['sections']]}")
        print(f"  Campaign template completion rate: {campaign_template['completion_rate']:.2f}")
        
        # Test ProjectMetrics
        print("\nTesting ProjectMetrics:")
        
        metrics = ProjectMetrics(
            total_tasks=50,
            completed_tasks=35,
            overdue_tasks=5,
            avg_completion_time_days=3.2,
            team_velocity=15.5,
            blocker_count=2,
            risk_score=0.35
        )
        
        print(f"  ProjectMetrics: {metrics}")
        print(f"  Completion rate: {metrics.get_completion_rate():.1%}")
        print(f"  Health score: {metrics.get_health_score():.2f}")
        
        # Update metrics
        metrics.update_metrics({
            'completed_tasks': 40,
            'overdue_tasks': 3,
            'blocker_count': 1
        })
        print(f"  Updated metrics: {metrics}")
        print(f"  Updated health score: {metrics.get_health_score():.2f}")
        
        print("\n✅ All project model tests completed successfully!")
        
    except Exception as e:
        print(f"Test error: {str(e)}")
        import traceback
        traceback.print_exc()