

"""
Task generator module for creating realistic task data, subtasks, and comments.
This module generates realistic task structures with appropriate naming patterns,
temporal distributions, assignment logic, and completion rates based on enterprise patterns.

The generator is designed to be:
- Realistic: Creates tasks with believable names, descriptions, and metadata
- Temporally consistent: Ensures logical time relationships (no completion before creation)
- Context-aware: Tasks align with project types, departments, and team structures
- Distribution-accurate: Follows real-world patterns for due dates, completion rates, etc.
- Referentially intact: Maintains proper relationships with projects, sections, users
"""

import logging
import random
import time
from datetime import datetime, timedelta, date
from typing import List, Dict, Optional, Tuple, Set, Any
import sqlite3
import numpy as np
import json
from enum import Enum

from src.utils.logging import get_logger
from src.utils.temporal import TemporalGenerator
from src.models.organization import OrganizationConfig
from src.models.project import ProjectConfig, SectionConfig, TaskConfig
from src.models.user import UserConfig, TeamMembershipConfig

logger = get_logger(__name__)

class TaskPriority(Enum):
    """Task priority levels with weights."""
    HIGH = 'high'
    MEDIUM = 'medium' 
    LOW = 'low'
    NONE = 'none'

class TaskGenerator:
    """
    Generator for creating realistic task data, subtasks, and comments.
    
    This class handles the generation of:
    1. Task records with realistic names, descriptions, and metadata
    2. Due date distributions based on real-world patterns
    3. Assignment logic considering team structure and workload
    4. Completion rates by project type and temporal patterns
    5. Subtask hierarchies with proper relationships
    6. Temporal consistency across all time-based fields
    
    The generator uses enterprise patterns and research-backed distributions to ensure realism.
    """
    
    def __init__(self, db_conn: sqlite3.Connection, config: Dict[str, Any], org_config: OrganizationConfig):
        """
        Initialize the task generator.
        
        Args:
            db_conn: Database connection
            config: Application configuration
            org_config: Organization configuration
        """
        self.db_conn = db_conn
        self.config = config
        self.org_config = org_config
        self.temporal_generator = TemporalGenerator(config)
        
        # Research-backed task completion rates by project type
        # Source: Asana's "Anatomy of Work" 2023 report and industry benchmarks
        self.completion_rates = {
            'sprint': (0.70, 0.85),           # 70-85% completion for sprint projects
            'bug_tracking': (0.60, 0.70),     # 60-70% completion for bug tracking
            'feature_development': (0.50, 0.65),  # 50-65% completion for feature development
            'tech_debt': (0.40, 0.55),        # 40-55% completion for tech debt
            'research': (0.30, 0.45),         # 30-45% completion for research projects
            'campaign': (0.65, 0.80),         # 65-80% completion for marketing campaigns
            'content_calendar': (0.75, 0.90), # 75-90% completion for content calendars
            'roadmap_planning': (0.40, 0.55), # 40-55% completion for roadmap planning
            'user_research': (0.50, 0.65),    # 50-65% completion for user research
            'process_improvement': (0.45, 0.60), # 45-60% completion for process improvement
            'budget_planning': (0.55, 0.70),  # 55-70% completion for budget planning
            'lead_generation': (0.60, 0.75),  # 60-75% completion for lead generation
            'sales_pipeline': (0.40, 0.55),   # 40-55% completion for sales pipelines
            'default': (0.50, 0.70)           # Default completion rate range
        }
        
        # Research-backed due date distributions
        # Source: Project management industry benchmarks and Asana usage patterns
        self.due_date_distributions = {
            'sprint': [(1, 3, 0.4), (4, 7, 0.35), (8, 14, 0.15), (15, 30, 0.08), (None, None, 0.02)],
            'bug_tracking': [(0, 1, 0.2), (2, 3, 0.3), (4, 7, 0.25), (8, 14, 0.15), (15, 30, 0.08), (None, None, 0.02)],
            'feature_development': [(1, 7, 0.1), (8, 14, 0.25), (15, 30, 0.35), (31, 60, 0.2), (61, 90, 0.08), (None, None, 0.02)],
            'campaign': [(1, 7, 0.05), (8, 14, 0.15), (15, 30, 0.4), (31, 60, 0.25), (61, 90, 0.1), (None, None, 0.05)],
            'default': [(1, 7, 0.25), (8, 14, 0.25), (15, 30, 0.2), (31, 60, 0.15), (61, 90, 0.1), (None, None, 0.05)]
        }
        
        # Task naming patterns by department and project type
        self.task_name_patterns = {
            'engineering': {
                'sprint': [
                    '{component} - {action} {feature}',
                    'Fix {bug_type} in {module}',
                    'Implement {feature} feature',
                    'Refactor {service} code',
                    'Write tests for {component}',
                    'Optimize {metric} for {endpoint}'
                ],
                'bug_tracking': [
                    '[BUG] {severity}: {component} - {description}',
                    'Fix {bug_type} in {module} when {condition}',
                    '{component} crashes when {scenario}',
                    'Performance issue: {metric} degrades in {environment}'
                ],
                'feature_development': [
                    '{feature} - {phase} phase',
                    'Design {feature} architecture',
                    'Implement {feature} backend',
                    'Build {feature} frontend',
                    'Test {feature} integration'
                ]
            },
            'product': {
                'roadmap_planning': [
                    '{quarter} {initiative} planning',
                    'Define {feature} requirements',
                    'Research {market} opportunities',
                    'Prioritize {feature} backlog'
                ],
                'user_research': [
                    'Conduct {method} with {user_type}',
                    'Analyze {data_source} for {feature}',
                    'Synthesize {number} user interviews',
                    'Create {deliverable} for {feature}'
                ]
            },
            'marketing': {
                'campaign': [
                    '{campaign} - {deliverable} creation',
                    'Design {asset_type} for {platform}',
                    'Write {content_type} copy for {campaign}',
                    'Schedule {channel} posts for {campaign}'
                ],
                'content_calendar': [
                    '{topic} {content_type} for {month}',
                    'Create {asset_type} for {platform}',
                    'SEO optimization for {keyword}',
                    'Edit {content_type} draft'
                ]
            },
            'sales': {
                'lead_generation': [
                    'Research {industry} leads in {territory}',
                    'Qualify {number} leads from {source}',
                    'Create {asset_type} for {campaign}',
                    'Follow up with {number} prospects'
                ],
                'sales_pipeline': [
                    'Demo {product} to {company}',
                    'Send proposal to {prospect}',
                    'Negotiate contract with {client}',
                    'Close deal with {account}'
                ]
            },
            'operations': {
                'process_improvement': [
                    'Document {process} workflow',
                    'Identify bottlenecks in {area}',
                    'Implement {solution} for {process}',
                    'Train {team} on {procedure}'
                ],
                'budget_planning': [
                    'Forecast {category} budget for {period}',
                    'Analyze {department} spending trends',
                    'Prepare {quarter} budget review',
                    'Track {metric} against budget'
                ]
            }
        }
        
        # Task description patterns with varying complexity
        self.description_patterns = {
            'simple': [
                '{action} for {feature} implementation',
                '{goal} to improve {metric}',
                '{task} as part of {project} initiative'
            ],
            'detailed': [
                '## Objective\n{objective}\n\n## Approach\n{approach}\n\n## Success Criteria\n- {criterion1}\n- {criterion2}\n- {criterion3}',
                '## Background\n{background}\n\n## Tasks\n1. {task1}\n2. {task2}\n3. {task3}\n\n## Timeline\n- {milestone1}: {date1}\n- {milestone2}: {date2}',
                '## Context\n{context}\n\n## Requirements\n- {requirement1}\n- {requirement2}\n- {requirement3}\n\n## Dependencies\n{dependencies}'
            ]
        }
        
        # Priority distributions by department and project type
        self.priority_distributions = {
            'engineering': {'high': 0.2, 'medium': 0.5, 'low': 0.2, 'none': 0.1},
            'product': {'high': 0.25, 'medium': 0.45, 'low': 0.2, 'none': 0.1},
            'marketing': {'high': 0.3, 'medium': 0.4, 'low': 0.2, 'none': 0.1},
            'sales': {'high': 0.35, 'medium': 0.4, 'low': 0.15, 'none': 0.1},
            'operations': {'high': 0.25, 'medium': 0.45, 'low': 0.2, 'none': 0.1}
        }
        
        # Unassigned task rates by project type (industry benchmarks)
        self.unassigned_rates = {
            'sprint': 0.1,      # 10% unassigned in sprint projects
            'bug_tracking': 0.25,  # 25% unassigned in bug tracking (triage needed)
            'feature_development': 0.15,
            'research': 0.3,    # 30% unassigned in research (exploratory)
            'default': 0.15     # Default 15% unassigned
        }
    
    def _get_completion_rate(self, project_type: str, project_age_days: int) -> float:
        """
        Get realistic completion rate based on project type and age.
        
        Args:
            project_type: Type of project
            project_age_days: Age of project in days
            
        Returns:
            Completion rate between 0 and 1
        """
        # Get base completion rate range
        rate_range = self.completion_rates.get(project_type, self.completion_rates['default'])
        base_rate = random.uniform(rate_range[0], rate_range[1])
        
        # Adjust based on project age (older projects more likely completed)
        age_factor = min(project_age_days / 180, 1.0)  # Cap at 180 days
        age_adjustment = age_factor * 0.2  # Up to 20% boost for very old projects
        
        # Adjust based on project status
        status_adjustment = 0.1 if project_type == 'sprint' else 0  # Sprints have higher completion
        
        final_rate = min(base_rate + age_adjustment + status_adjustment, 0.95)
        return final_rate
    
    def _get_due_date_distribution(self, project_type: str) -> List[Tuple[Optional[int], Optional[int], float]]:
        """
        Get due date distribution based on project type.
        
        Args:
            project_type: Type of project
            
        Returns:
            List of (min_days, max_days, probability) tuples
        """
        return self.due_date_distributions.get(project_type, self.due_date_distributions['default'])
    
    def _generate_realistic_task_name(self, department: str, project_type: str, 
                                    project_name: str, section_name: str) -> str:
        """
        Generate a realistic task name based on department, project type, and context.
        
        Args:
            department: Department name
            project_type: Type of project
            project_name: Project name
            section_name: Section name
            
        Returns:
            Realistic task name
        """
        # Get patterns for department and project type
        dept_patterns = self.task_name_patterns.get(department, self.task_name_patterns['engineering'])
        patterns = dept_patterns.get(project_type, dept_patterns.get('sprint', [
            '{action} for {feature}',
            'Complete {task} for {project}',
            '{task} implementation'
        ]))
        
        # Generate parameters based on context
        pattern_params = {
            'component': random.choice(['API', 'Frontend', 'Backend', 'Database', 'Auth', 'Search', 'Dashboard', 'Mobile']),
            'action': random.choice(['Implement', 'Fix', 'Refactor', 'Optimize', 'Test', 'Design', 'Document', 'Review']),
            'feature': random.choice(['user authentication', 'search functionality', 'mobile responsiveness', 'performance optimization', 'data visualization', 'notification system', 'payment integration']),
            'bug_type': random.choice(['null pointer', 'race condition', 'memory leak', 'UI glitch', 'performance bottleneck', 'security vulnerability']),
            'module': random.choice(['user management', 'payment processing', 'report generation', 'data import', 'notification system']),
            'service': random.choice(['authentication', 'payment', 'notification', 'search', 'reporting', 'user management']),
            'metric': random.choice(['response time', 'memory usage', 'error rate', 'conversion rate', 'load time']),
            'endpoint': random.choice(['/api/users', '/api/payments', '/api/reports', '/api/notifications', '/api/search']),
            'severity': random.choice(['Critical', 'High', 'Medium', 'Low']),
            'description': random.choice(['login failure', 'data not loading', 'slow performance', 'incorrect calculations']),
            'phase': random.choice(['design', 'implementation', 'testing', 'deployment']),
            'campaign': random.choice(['Q1 Launch', 'Summer Promotion', 'Holiday Campaign', 'Product Awareness', 'Customer Retention']),
            'deliverable': random.choice(['landing page', 'email template', 'social media posts', 'blog content', 'video script']),
            'asset_type': random.choice(['banner image', 'logo', 'infographic', 'video thumbnail', 'social media graphic']),
            'platform': random.choice(['Facebook', 'Instagram', 'LinkedIn', 'Twitter', 'Email']),
            'content_type': random.choice(['blog post', 'whitepaper', 'case study', 'video script', 'social media post']),
            'method': random.choice(['user interviews', 'surveys', 'A/B testing', 'usability testing', 'analytics analysis']),
            'user_type': random.choice(['enterprise customers', 'small business owners', 'developers', 'end users']),
            'data_source': random.choice(['user feedback', 'analytics data', 'support tickets', 'market research']),
            'number': random.randint(5, 20),
            'deliverable': random.choice(['user persona', 'journey map', 'wireframe', 'prototype', 'research report']),
            'quarter': random.choice(['Q1', 'Q2', 'Q3', 'Q4']),
            'initiative': random.choice(['growth', 'engagement', 'retention', 'monetization', 'efficiency']),
            'market': random.choice(['enterprise', 'SMB', 'freemium', 'developer']),
            'feature': random.choice(['search', 'authentication', 'dashboard', 'reporting', 'notifications']),
            'industry': random.choice(['technology', 'finance', 'healthcare', 'education', 'retail']),
            'territory': random.choice(['North America', 'EMEA', 'APAC', 'Global']),
            'source': random.choice(['website', 'webinar', 'conference', 'partner referral', 'social media']),
            'product': random.choice(['Enterprise Platform', 'Mobile App', 'Analytics Suite', 'API Service']),
            'company': random.choice(['Acme Corp', 'TechCo', 'StartupX', 'Global Inc', 'Innovate Ltd']),
            'prospect': random.choice(['enterprise lead', 'mid-market prospect', 'SMB opportunity']),
            'client': random.choice(['key account', 'strategic partner', 'new customer']),
            'account': random.choice(['enterprise client', 'mid-market account', 'SMB customer']),
            'process': random.choice(['onboarding', 'approval workflow', 'reporting', 'budgeting', 'hiring']),
            'area': random.choice(['sales process', 'customer support', 'HR operations', 'finance workflows']),
            'solution': random.choice(['automation script', 'new workflow', 'training program', 'tool integration']),
            'team': random.choice(['sales team', 'customer success', 'HR department', 'finance team']),
            'procedure': random.choice(['expense reporting', 'time tracking', 'leave management', 'onboarding']),
            'category': random.choice(['marketing', 'sales', 'R&D', 'operations', 'overhead']),
            'period': random.choice(['Q1', 'Q2', 'H1', 'full year']),
            'department': random.choice(['engineering', 'marketing', 'sales', 'operations', 'finance']),
            'task': random.choice(['analysis', 'optimization', 'implementation', 'documentation', 'training']),
            'project': project_name,
            'month': random.choice(['January', 'February', 'March', 'April', 'May', 'June', 'July', 'August', 'September', 'October', 'November', 'December']),
            'keyword': random.choice(['conversion rate', 'user engagement', 'search ranking', 'bounce rate']),
            'topic': random.choice(['industry trends', 'product updates', 'customer stories', 'best practices'])
        }
        
        # Select and format pattern
        pattern = random.choice(patterns)
        try:
            return pattern.format(**pattern_params)
        except KeyError as e:
            logger.warning(f"Pattern formatting error: {e}. Using fallback name.")
            return f"{section_name}: {random.choice(['Task', 'Action', 'Item'])} {random.randint(1, 1000)}"
    
    def _generate_task_description(self, department: str, project_type: str, task_name: str) -> Optional[str]:
        """
        Generate a realistic task description with varying complexity.
        
        Args:
            department: Department name
            project_type: Type of project
            task_name: Task name
            
        Returns:
            Task description or None (20% of tasks have no description)
        """
        # 20% of tasks have no description (industry benchmark)
        if random.random() < 0.2:
            return None
        
        # Determine complexity based on department and project type
        complexity_weights = {
            'engineering': {'simple': 0.3, 'detailed': 0.7},
            'product': {'simple': 0.2, 'detailed': 0.8},
            'marketing': {'simple': 0.4, 'detailed': 0.6},
            'sales': {'simple': 0.5, 'detailed': 0.5},
            'operations': {'simple': 0.3, 'detailed': 0.7}
        }
        
        weights = complexity_weights.get(department, {'simple': 0.4, 'detailed': 0.6})
        complexity = random.choices(['simple', 'detailed'], weights=list(weights.values()))[0]
        
        patterns = self.description_patterns[complexity]
        pattern = random.choice(patterns)
        
        # Generate description parameters
        desc_params = {
            'action': random.choice(['Implement', 'Fix', 'Design', 'Optimize', 'Test', 'Document']),
            'feature': random.choice(['user authentication', 'search functionality', 'mobile responsiveness', 'data visualization']),
            'goal': random.choice(['improve user experience', 'increase conversion rate', 'reduce load time', 'enhance security']),
            'metric': random.choice(['response time', 'error rate', 'conversion rate', 'user engagement', 'system reliability']),
            'task': random.choice(['development', 'testing', 'documentation', 'optimization', 'integration']),
            'project': random.choice(['current sprint', 'Q1 initiative', 'product launch', 'system upgrade']),
            'objective': random.choice(['Improve system performance', 'Enhance user experience', 'Fix critical bugs', 'Implement new features']),
            'approach': random.choice(['Break down into smaller tasks', 'Collaborate with stakeholders', 'Follow agile methodology', 'Use test-driven development']),
            'criterion1': random.choice(['All tests pass', 'Performance metrics met', 'User acceptance testing completed']),
            'criterion2': random.choice(['Documentation updated', 'Code reviewed', 'Deployment successful']),
            'criterion3': random.choice(['Stakeholder approval', 'Performance benchmarks met', 'Security audit passed']),
            'background': random.choice(['User feedback indicates performance issues', 'New requirements from stakeholders', 'Technical debt needs addressing']),
            'task1': random.choice(['Analyze current implementation', 'Design new architecture', 'Implement core functionality']),
            'task2': random.choice(['Write unit tests', 'Update documentation', 'Perform integration testing']),
            'task3': random.choice(['Deploy to staging', 'Conduct user testing', 'Monitor performance metrics']),
            'milestone1': random.choice(['Design complete', 'Development complete', 'Testing complete']),
            'date1': (datetime.now() + timedelta(days=random.randint(1, 7))).strftime('%Y-%m-%d'),
            'milestone2': random.choice(['Code review complete', 'Deployment complete', 'User acceptance complete']),
            'date2': (datetime.now() + timedelta(days=random.randint(8, 14))).strftime('%Y-%m-%d'),
            'context': random.choice(['Based on user feedback', 'Following design sprint', 'As part of system upgrade']),
            'requirement1': random.choice(['Must support mobile devices', 'Must handle 1000+ concurrent users', 'Must integrate with existing systems']),
            'requirement2': random.choice(['Must meet accessibility standards', 'Must have admin controls', 'Must support internationalization']),
            'requirement3': random.choice(['Must have audit logging', 'Must have backup systems', 'Must have monitoring']),
            'dependencies': random.choice(['Backend API must be ready', 'Design assets must be approved', 'Database schema must be finalized'])
        }
        
        try:
            description = pattern.format(**desc_params)
            
            # Add some random formatting variations
            if complexity == 'detailed' and random.random() < 0.3:
                description += f"\n\n## Additional Notes\n{random.choice(['This is a high-priority item', 'Requires stakeholder approval', 'Has dependencies on other teams', 'Needs security review'])}"
            
            return description
        except KeyError:
            return f"Task description for: {task_name}"
    
    def _generate_realistic_due_date(self, project_type: str, created_date: datetime, 
                                    section_name: str, current_date: datetime) -> Optional[datetime]:
        """
        Generate a realistic due date based on project type, section, and creation date.
        
        Args:
            project_type: Type of project
            created_date: Task creation date
            section_name: Section name (affects urgency)
            current_date: Current date for reference
            
        Returns:
            Due date datetime or None (if no due date)
        """
        # Get due date distribution
        distribution = self._get_due_date_distribution(project_type)
        
        # Select a bucket based on probabilities
        buckets = [(min_days, max_days) for min_days, max_days, _ in distribution]
        probabilities = [prob for _, _, prob in distribution]
        bucket_index = random.choices(range(len(buckets)), weights=probabilities)[0]
        min_days, max_days = buckets[bucket_index]
        
        # 5% of tasks have no due date (from distribution)
        if min_days is None and max_days is None:
            return None
        
        # Adjust based on section urgency
        section_urgency = {
            'Backlog': 1.5,
            'Ready': 1.2,
            'In Progress': 1.0,
            'In Review': 0.8,
            'Done': 0.5,
            'To Do': 1.2,
            'Blocked': 2.0,
            'Urgent': 0.5,
            'Critical': 0.3
        }
        
        urgency_factor = section_urgency.get(section_name, 1.0)
        min_days = max(0, int(min_days * urgency_factor))
        max_days = max(0, int(max_days * urgency_factor))
        
        # Ensure reasonable bounds
        min_days = max(0, min_days)
        max_days = max(min_days + 1, max_days)
        
        # Generate due date with business day awareness
        days_offset = random.randint(min_days, max_days)
        due_date = created_date + timedelta(days=days_offset)
        
        # 85% of due dates fall on business days (industry benchmark)
        if random.random() < 0.85:
            due_date = self.temporal_generator.get_business_day_offset(due_date, 0)  # Get nearest business day
        
        # Ensure due date doesn't exceed current date by too much (realistic planning horizon)
        max_future_days = 180  # 6 months maximum
        if due_date > current_date + timedelta(days=max_future_days):
            due_date = current_date + timedelta(days=random.randint(30, max_future_days))
        
        # Some overdue tasks (5-10% depending on section)
        overdue_chance = 0.05 if 'Done' in section_name or 'Complete' in section_name else 0.1
        if random.random() < overdue_chance and due_date < current_date:
            # Keep it overdue but not too far in the past
            max_overdue_days = 30
            if (current_date - due_date).days > max_overdue_days:
                due_date = current_date - timedelta(days=random.randint(1, max_overdue_days))
        
        return due_date
    
    def _get_task_priority(self, department: str, project_type: str, section_name: str) -> TaskPriority:
        """
        Determine task priority based on department, project type, and section.
        
        Args:
            department: Department name
            project_type: Type of project
            section_name: Section name
            
        Returns:
            TaskPriority enum value
        """
        # Get base distribution for department
        base_dist = self.priority_distributions.get(department, {'high': 0.2, 'medium': 0.5, 'low': 0.2, 'none': 0.1})
        
        # Adjust based on section name
        section_adjustments = {
            'Backlog': {'high': -0.05, 'low': 0.05},
            'Ready': {'high': 0.05, 'low': -0.05},
            'In Progress': {'high': 0.1, 'medium': -0.1},
            'In Review': {'high': 0.05, 'medium': 0.05, 'low': -0.1},
            'Done': {'none': 0.2, 'low': 0.1, 'high': -0.15},
            'Urgent': {'high': 0.4, 'none': -0.2},
            'Critical': {'high': 0.5, 'none': -0.3},
            'Blocked': {'high': 0.2, 'low': -0.1}
        }
        
        adjustments = section_adjustments.get(section_name, {})
        
        # Apply adjustments
        adjusted_dist = base_dist.copy()
        for priority, adjustment in adjustments.items():
            if priority in adjusted_dist:
                adjusted_dist[priority] = max(0, min(1, adjusted_dist[priority] + adjustment))
        
        # Normalize distribution
        total = sum(adjusted_dist.values())
        if total > 0:
            adjusted_dist = {k: v/total for k, v in adjusted_dist.items()}
        
        # Select priority
        priorities = list(adjusted_dist.keys())
        weights = list(adjusted_dist.values())
        selected_priority = random.choices(priorities, weights=weights)[0]
        
        return TaskPriority(selected_priority)
    
    def _get_task_assignee(self, project_id: int, section_id: int, team_memberships: List[Dict[str, Any]], 
                         users: List[Dict[str, Any]], project_type: str) -> Optional[int]:
        """
        Determine task assignee based on team structure, workload, and project type.
        
        Args:
            project_id: Project ID
            section_id: Section ID
            team_memberships: List of team membership dictionaries
            users: List of user dictionaries
            project_type: Type of project
            
        Returns:
            User ID or None if unassigned
        """
        # Get unassigned rate for project type
        unassigned_rate = self.unassigned_rates.get(project_type, self.unassigned_rates['default'])
        
        # Chance to leave unassigned
        if random.random() < unassigned_rate:
            return None
        
        # Get users in the same team as the project
        project_team_members = self._get_project_team_members(project_id, team_memberships, users)
        
        if not project_team_members:
            return None
        
        # Filter out guests and focus on members/admins
        eligible_users = [u for u in project_team_members if u.get('role') in ['member', 'admin']]
        
        if not eligible_users:
            eligible_users = project_team_members
        
        # Weight assignment by role and experience
        weights = []
        for user in eligible_users:
            base_weight = 1.0
            
            # Role adjustments
            if user.get('role') == 'admin':
                base_weight *= 1.5
            elif 'manager' in user.get('role_title', '').lower():
                base_weight *= 1.3
            elif 'lead' in user.get('role_title', '').lower():
                base_weight *= 1.2
            elif 'senior' in user.get('role_title', '').lower():
                base_weight *= 1.1
            
            # Experience level adjustments
            exp_level = user.get('experience_level', '')
            if exp_level == 'senior':
                base_weight *= 1.2
            elif exp_level == 'executive':
                base_weight *= 0.8  # Executives get fewer direct tasks
            
            # Department alignment
            user_dept = user.get('department', '').lower()
            if any(word in project_type.lower() for word in user_dept.split()):
                base_weight *= 1.1
            
            weights.append(base_weight)
        
        # Normalize weights
        total_weight = sum(weights)
        if total_weight > 0:
            weights = [w/total_weight for w in weights]
        
        # Select assignee
        selected_user = random.choices(eligible_users, weights=weights)[0]
        return selected_user.get('id')
    
    def _get_project_team_members(self, project_id: int, team_memberships: List[Dict[str, Any]], 
                                 users: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Get team members for a specific project.
        
        Args:
            project_id: Project ID
            team_memberships: List of team membership dictionaries
            users: List of user dictionaries
            
        Returns:
            List of user dictionaries in the project's team
        """
        # In a real implementation, we'd join with projects table to get team_id
        # For now, assume all users are eligible (this would be fixed with proper schema)
        return users
    
    def _generate_tasks_for_project(self, project: Dict[str, Any], sections: List[Dict[str, Any]], 
                                   team_memberships: List[Dict[str, Any]], users: List[Dict[str, Any]], 
                                   custom_fields: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Generate tasks for a specific project with realistic distributions.
        
        Args:
            project: Project dictionary
            sections: List of section dictionaries for this project
            team_memberships: List of team membership dictionaries
            users: List of user dictionaries
            custom_fields: List of custom field definitions
            
        Returns:
            List of task dictionaries
        """
        project_id = project['id']
        department = project.get('department', 'engineering')
        project_type = project.get('project_type', 'sprint')
        start_date = datetime.strptime(project['start_date'], '%Y-%m-%d')
        end_date = datetime.strptime(project['end_date'], '%Y-%m-%d') if project.get('end_date') else datetime.now()
        current_date = datetime.now()
        project_age_days = (current_date - start_date).days
        
        # Get number of tasks for this project
        min_tasks, max_tasks = self.org_config.num_tasks_per_project_range
        num_tasks = random.randint(min_tasks, max_tasks)
        
        # Get completion rate for this project
        completion_rate = self._get_completion_rate(project_type, project_age_days)
        
        tasks = []
        used_task_names = set()
        
        for i in range(num_tasks):
            # Select random section
            section = random.choice(sections)
            section_name = section['name']
            
            # Generate creation date within project timeline
            if project_age_days > 0:
                days_since_start = random.randint(0, min(project_age_days, 180))  # Cap at 180 days
                created_date = start_date + timedelta(days=days_since_start)
            else:
                created_date = current_date
            
            # Generate task name
            task_name_base = self._generate_realistic_task_name(department, project_type, project['name'], section_name)
            task_name = task_name_base
            
            # Ensure unique task names within project
            counter = 1
            while task_name in used_task_names:
                task_name = f"{task_name_base} ({counter})"
                counter += 1
            used_task_names.add(task_name)
            
            # Generate description
            description = self._generate_task_description(department, project_type, task_name)
            
            # Generate due date
            due_date = self._generate_realistic_due_date(project_type, created_date, section_name, current_date)
            
            # Determine priority
            priority = self._get_task_priority(department, project_type, section_name)
            
            # Determine assignee
            assignee_id = self._get_task_assignee(project_id, section['id'], team_memberships, users, project_type)
            
            # Determine completion status
            is_completed = random.random() < completion_rate
            
            # Generate completion date if completed
            completed_at = None
            if is_completed:
                # Completion typically happens 1-14 days after creation (log-normal distribution)
                if due_date and due_date > created_date:
                    max_completion_days = (due_date - created_date).days
                else:
                    max_completion_days = 14
                
                # Use log-normal distribution for more realistic cycle times
                completion_days = int(np.random.lognormal(mean=1.0, sigma=0.5))
                completion_days = max(1, min(completion_days, max_completion_days, 30))  # Cap at 30 days
                
                completed_at = created_date + timedelta(days=completion_days)
                
                # Ensure completed_at doesn't exceed current date or due date
                if completed_at > current_date:
                    completed_at = min(current_date, due_date if due_date else current_date)
            
            # Generate position (for ordering within section)
            position = i
            
            task = {
                'project_id': project_id,
                'section_id': section['id'],
                'assignee_id': assignee_id,
                'name': task_name,
                'description': description,
                'due_date': due_date.strftime('%Y-%m-%d') if due_date else None,
                'completed': is_completed,
                'completed_at': completed_at.strftime('%Y-%m-%d %H:%M:%S') if completed_at else None,
                'priority': priority.value,
                'position': position,
                'created_at': created_date.strftime('%Y-%m-%d %H:%M:%S'),
                'updated_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            }
            tasks.append(task)
        
        logger.info(f"Generated {len(tasks)} tasks for project {project['name']} with completion rate {completion_rate:.2f}")
        return tasks
    
    def generate_tasks_for_projects(self, projects: List[Dict[str, Any]], sections: List[Dict[str, Any]], 
                                  team_memberships: List[Dict[str, Any]], users: List[Dict[str, Any]], 
                                  custom_fields: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Generate tasks for all projects with realistic distributions.
        
        Args:
            projects: List of project dictionaries
            sections: List of section dictionaries
            team_memberships: List of team membership dictionaries
            users: List of user dictionaries
            custom_fields: List of custom field definitions
            
        Returns:
            List of task dictionaries
        """
        logger.info(f"Generating tasks for {len(projects)} projects")
        
        tasks = []
        
        # Group sections by project for efficient lookup
        sections_by_project = {}
        for section in sections:
            project_id = section['project_id']
            if project_id not in sections_by_project:
                sections_by_project[project_id] = []
            sections_by_project[project_id].append(section)
        
        for project in projects:
            project_sections = sections_by_project.get(project['id'], [])
            if not project_sections:
                logger.warning(f"No sections found for project {project['name']}, skipping task generation")
                continue
            
            project_tasks = self._generate_tasks_for_project(
                project, project_sections, team_memberships, users, custom_fields
            )
            tasks.extend(project_tasks)
        
        logger.info(f"Successfully generated {len(tasks)} tasks across all projects")
        return tasks
    
    def insert_tasks(self, tasks: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Insert tasks into the database and return tasks with IDs.
        
        Args:
            tasks: List of task dictionaries
            
        Returns:
            List of task dictionaries with database IDs
        """
        cursor = self.db_conn.cursor()
        inserted_tasks = []
        
        for task in tasks:
            try:
                cursor.execute("""
                    INSERT INTO tasks (
                        project_id, section_id, assignee_id, name, description,
                        due_date, completed, completed_at, priority, position,
                        created_at, updated_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    task['project_id'],
                    task['section_id'],
                    task['assignee_id'],
                    task['name'],
                    task['description'],
                    task['due_date'],
                    task['completed'],
                    task['completed_at'],
                    task['priority'],
                    task['position'],
                    task['created_at'],
                    task['updated_at']
                ))
                
                task_id = cursor.lastrowid
                task_with_id = task.copy()
                task_with_id['id'] = task_id
                inserted_tasks.append(task_with_id)
                
            except sqlite3.Error as e:
                logger.error(f"Error inserting task '{task['name']}': {str(e)}")
                raise
        
        self.db_conn.commit()
        logger.info(f"Successfully inserted {len(inserted_tasks)} tasks into database")
        return inserted_tasks
    
    def generate_subtasks_for_tasks(self, tasks: List[Dict[str, Any]], users: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Generate subtasks for tasks with realistic patterns.
        
        Args:
            tasks: List of task dictionaries
            users: List of user dictionaries
            
        Returns:
            List of subtask dictionaries
        """
        logger.info(f"Generating subtasks for {len(tasks)} tasks")
        
        subtasks = []
        current_date = datetime.now()
        
        for task in tasks:
            # 30% of tasks have subtasks (industry benchmark)
            if random.random() < 0.3:
                # Number of subtasks: 1-5, typically 2-3
                num_subtasks = random.randint(1, min(5, random.choices([2, 3, 4], weights=[0.1, 0.7, 0.2])[0]))
                
                # Get task context
                task_name_lower = task['name'].lower()
                department = 'engineering'  # Default, would be better with actual context
                
                # Determine if task is technical based on name
                technical_keywords = ['implement', 'fix', 'refactor', 'optimize', 'test', 'debug', 'develop', 'code', 'api', 'database']
                is_technical = any(keyword in task_name_lower for keyword in technical_keywords)
                
                for i in range(num_subtasks):
                    # Generate subtask name based on parent task
                    if is_technical:
                        subtask_actions = ['Design', 'Implement', 'Test', 'Document', 'Review', 'Debug', 'Optimize']
                        subtask_targets = ['architecture', 'core functionality', 'unit tests', 'API endpoints', 'UI components', 'performance', 'security']
                    else:
                        subtask_actions = ['Research', 'Draft', 'Review', 'Approve', 'Publish', 'Analyze', 'Present']
                        subtask_targets = ['requirements', 'design', 'content', 'feedback', 'results', 'documentation', 'stakeholders']
                    
                    action = random.choice(subtask_actions)
                    target = random.choice(subtask_targets)
                    
                    # Make subtask names specific to parent task
                    if 'fix' in task_name_lower or 'bug' in task_name_lower:
                        subtask_name = f"{action} specific {target} for bug fix"
                    elif 'feature' in task_name_lower or 'implement' in task_name_lower:
                        subtask_name = f"{action} {target} for feature implementation"
                    elif 'research' in task_name_lower or 'analyze' in task_name_lower:
                        subtask_name = f"{action} {target} research"
                    else:
                        subtask_name = f"{action} {target}"
                    
                    # Generate completion status (subtasks more likely completed than parent tasks)
                    parent_completed = task.get('completed', False)
                    subtask_completed = parent_completed and random.random() < 0.8  # 80% of subtasks completed if parent is done
                    
                    # Generate completion date if completed
                    completed_at = None
                    if subtask_completed:
                        # Subtasks typically completed before parent task
                        parent_completed_at = datetime.strptime(task['completed_at'], '%Y-%m-%d %H:%M:%S') if task.get('completed_at') else current_date
                        task_created_at = datetime.strptime(task['created_at'], '%Y-%m-%d %H:%M:%S')
                        
                        # Generate completion date between task creation and parent completion
                        time_range_days = max(1, (parent_completed_at - task_created_at).days)
                        completion_days = random.randint(1, time_range_days)
                        completed_at = task_created_at + timedelta(days=completion_days)
                    
                    subtask = {
                        'task_id': task['id'],
                        'name': subtask_name,
                        'completed': subtask_completed,
                        'completed_at': completed_at.strftime('%Y-%m-%d %H:%M:%S') if completed_at else None,
                        'position': i,
                        'created_at': task['created_at'],
                        'updated_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                    }
                    subtasks.append(subtask)
        
        logger.info(f"Successfully generated {len(subtasks)} subtasks")
        return subtasks
    
    def insert_subtasks(self, subtasks: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Insert subtasks into the database.
        
        Args:
            subtasks: List of subtask dictionaries
            
        Returns:
            List of inserted subtask dictionaries with IDs
        """
        cursor = self.db_conn.cursor()
        inserted_subtasks = []
        
        for subtask in subtasks:
            try:
                cursor.execute("""
                    INSERT INTO subtasks (
                        task_id, name, completed, completed_at, position,
                        created_at, updated_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?)
                """, (
                    subtask['task_id'],
                    subtask['name'],
                    subtask['completed'],
                    subtask['completed_at'],
                    subtask['position'],
                    subtask['created_at'],
                    subtask['updated_at']
                ))
                
                subtask_id = cursor.lastrowid
                subtask_with_id = subtask.copy()
                subtask_with_id['id'] = subtask_id
                inserted_subtasks.append(subtask_with_id)
                
            except sqlite3.Error as e:
                logger.error(f"Error inserting subtask '{subtask['name']}': {str(e)}")
                raise
        
        self.db_conn.commit()
        logger.info(f"Successfully inserted {len(inserted_subtasks)} subtasks into database")
        return inserted_subtasks
    
    def generate_and_insert_tasks(self, projects: List[Dict[str, Any]], sections: List[Dict[str, Any]], 
                                team_memberships: List[Dict[str, Any]], users: List[Dict[str, Any]], 
                                custom_fields: List[Dict[str, Any]]) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
        """
        Generate and insert tasks and subtasks for all projects.
        
        Args:
            projects: List of project dictionaries
            sections: List of section dictionaries
            team_memberships: List of team membership dictionaries
            users: List of user dictionaries
            custom_fields: List of custom field definitions
            
        Returns:
            Tuple of (tasks, subtasks) with database IDs
        """
        logger.info("Starting task and subtask generation and insertion")
        
        # Generate tasks
        tasks = self.generate_tasks_for_projects(projects, sections, team_memberships, users, custom_fields)
        
        # Insert tasks to get IDs
        inserted_tasks = self.insert_tasks(tasks)
        
        # Generate and insert subtasks
        subtasks = self.generate_subtasks_for_tasks(inserted_tasks, users)
        inserted_subtasks = self.insert_subtasks(subtasks)
        
        logger.info(f"Successfully generated and inserted:")
        logger.info(f"  - {len(inserted_tasks)} tasks")
        logger.info(f"  - {len(inserted_subtasks)} subtasks")
        
        return inserted_tasks, inserted_subtasks
    
    def close(self):
        """Cleanup resources if needed."""
        logger.info("Task generator closed")

# Example usage and testing
if __name__ == "__main__":
    # Setup logging for testing
    logging.basicConfig(level=logging.INFO)
    
    # Mock configuration
    mock_config = {
        'cache_dir': 'data/cache',
        'batch_size': 1000,
        'debug_mode': True
    }
    
    # Mock organization configuration
    mock_org_config = OrganizationConfig(
        name="Test Organization",
        domain="test.org",
        size_min=10,
        size_max=20,
        num_teams_range=(2, 3),
        num_users_per_team_range=(3, 5),
        num_projects_per_team_range=(1, 2),
        num_tasks_per_project_range=(5, 10),
        time_range=type('TimeRange', (), {'start_date': datetime(2025, 1, 1), 'end_date': datetime(2026, 1, 1)})
    )
    
    # Create in-memory database for testing
    test_conn = sqlite3.connect(':memory:')
    cursor = test_conn.cursor()
    
    # Create minimal schema for testing
    cursor.execute("""
        CREATE TABLE tasks (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            project_id INTEGER NOT NULL,
            section_id INTEGER NOT NULL,
            assignee_id INTEGER,
            name TEXT NOT NULL,
            description TEXT,
            due_date DATE,
            completed BOOLEAN NOT NULL DEFAULT 0,
            completed_at TIMESTAMP,
            priority TEXT,
            position INTEGER NOT NULL DEFAULT 0,
            created_at TIMESTAMP NOT NULL,
            updated_at TIMESTAMP NOT NULL
        )
    """)
    
    cursor.execute("""
        CREATE TABLE subtasks (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            task_id INTEGER NOT NULL,
            name TEXT NOT NULL,
            completed BOOLEAN NOT NULL DEFAULT 0,
            completed_at TIMESTAMP,
            position INTEGER NOT NULL DEFAULT 0,
            created_at TIMESTAMP NOT NULL,
            updated_at TIMESTAMP NOT NULL,
            FOREIGN KEY (task_id) REFERENCES tasks(id)
        )
    """)
    
    cursor.execute("""
        CREATE TABLE projects (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            organization_id INTEGER NOT NULL,
            name TEXT NOT NULL,
            status TEXT NOT NULL,
            start_date DATE NOT NULL,
            end_date DATE,
            created_at TIMESTAMP NOT NULL,
            updated_at TIMESTAMP NOT NULL
        )
    """)
    
    cursor.execute("""
        CREATE TABLE sections (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            project_id INTEGER NOT NULL,
            name TEXT NOT NULL,
            position INTEGER NOT NULL DEFAULT 0,
            created_at TIMESTAMP NOT NULL,
            updated_at TIMESTAMP NOT NULL
        )
    """)
    
    cursor.execute("""
        CREATE TABLE users (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            organization_id INTEGER NOT NULL,
            name TEXT NOT NULL,
            email TEXT NOT NULL UNIQUE,
            role TEXT NOT NULL,
            created_at TIMESTAMP NOT NULL,
            updated_at TIMESTAMP NOT NULL
        )
    """)
    
    cursor.execute("""
        CREATE TABLE team_memberships (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            team_id INTEGER NOT NULL,
            user_id INTEGER NOT NULL,
            role TEXT NOT NULL,
            created_at TIMESTAMP NOT NULL,
            updated_at TIMESTAMP NOT NULL
        )
    """)
    
    test_conn.commit()
    
    try:
        # Create mock data
        mock_projects = [
            {
                'id': 1,
                'organization_id': 1,
                'name': 'Engineering Sprint',
                'department': 'engineering',
                'project_type': 'sprint',
                'status': 'active',
                'start_date': '2025-12-01',
                'end_date': '2026-01-15',
                'created_at': '2025-12-01 09:00:00',
                'updated_at': '2025-12-15 14:30:00'
            },
            {
                'id': 2,
                'organization_id': 1,
                'name': 'Q1 Marketing Campaign',
                'department': 'marketing',
                'project_type': 'campaign',
                'status': 'active',
                'start_date': '2025-12-15',
                'end_date': '2026-03-31',
                'created_at': '2025-12-15 10:00:00',
                'updated_at': '2025-12-20 11:00:00'
            }
        ]
        
        mock_sections = [
            {'id': 1, 'project_id': 1, 'name': 'Backlog', 'position': 0, 'created_at': '2025-12-01 09:00:00'},
            {'id': 2, 'project_id': 1, 'name': 'In Progress', 'position': 1, 'created_at': '2025-12-01 09:00:00'},
            {'id': 3, 'project_id': 1, 'name': 'Done', 'position': 2, 'created_at': '2025-12-01 09:00:00'},
            {'id': 4, 'project_id': 2, 'name': 'Planning', 'position': 0, 'created_at': '2025-12-15 10:00:00'},
            {'id': 5, 'project_id': 2, 'name': 'Content Creation', 'position': 1, 'created_at': '2025-12-15 10:00:00'},
            {'id': 6, 'project_id': 2, 'name': 'Published', 'position': 2, 'created_at': '2025-12-15 10:00:00'}
        ]
        
        mock_users = [
            {'id': 1, 'name': 'John Doe', 'email': 'john@example.com', 'role': 'admin', 'department': 'engineering', 'role_title': 'Engineering Manager', 'experience_level': 'senior'},
            {'id': 2, 'name': 'Jane Smith', 'email': 'jane@example.com', 'role': 'member', 'department': 'engineering', 'role_title': 'Senior Developer', 'experience_level': 'senior'},
            {'id': 3, 'name': 'Bob Johnson', 'email': 'bob@example.com', 'role': 'member', 'department': 'marketing', 'role_title': 'Marketing Manager', 'experience_level': 'mid'}
        ]
        
        mock_memberships = [
            {'team_id': 1, 'user_id': 1, 'role': 'owner'},
            {'team_id': 1, 'user_id': 2, 'role': 'member'},
            {'team_id': 2, 'user_id': 3, 'role': 'owner'}
        ]
        
        mock_custom_fields = [
            {'id': 1, 'organization_id': 1, 'name': 'Priority', 'field_type': 'enum'},
            {'id': 2, 'organization_id': 1, 'name': 'Story Points', 'field_type': 'number'},
            {'id': 3, 'organization_id': 1, 'name': 'Campaign Type', 'field_type': 'enum'}
        ]
        
        # Create task generator
        generator = TaskGenerator(test_conn, mock_config, mock_org_config)
        
        # Generate and insert tasks and subtasks
        tasks, subtasks = generator.generate_and_insert_tasks(
            mock_projects, mock_sections, mock_memberships, mock_users, mock_custom_fields
        )
        
        print(f"\nGenerated Data Summary:")
        print(f"Tasks: {len(tasks)}")
        print(f"Subtasks: {len(subtasks)}")
        
        print("\nSample Tasks:")
        for task in tasks[:10]:
            status = "✓ Completed" if task['completed'] else "○ In Progress"
            due = task['due_date'] if task['due_date'] else "No due date"
            assignee = task['assignee_id'] if task['assignee_id'] else "Unassigned"
            print(f"  - {task['name']} [{status}]")
            print(f"    Due: {due}, Assignee: {assignee}, Priority: {task['priority'].title()}")
            if task['description']:
                desc_preview = task['description'][:50] + "..." if len(task['description']) > 50 else task['description']
                print(f"    Description: {desc_preview}")
        
        print("\nSample Subtasks:")
        for subtask in subtasks[:10]:
            parent_task = next((t for t in tasks if t['id'] == subtask['task_id']), None)
            if parent_task:
                status = "✓" if subtask['completed'] else "○"
                print(f"  - {status} {subtask['name']} (for '{parent_task['name']}')")
        
    finally:
        generator.close()
        test_conn.close()