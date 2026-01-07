# src/generators/projects.py

"""
Project generator module for creating realistic project data, sections, and custom fields.
This module generates realistic project structures with appropriate sections, custom fields,
and metadata based on scraped template data and enterprise patterns.

The generator is designed to be:
- Realistic: Creates projects with believable names, structures, and metadata
- Context-aware: Projects align with team departments and organizational goals
- Temporally consistent: Project timelines follow realistic patterns and constraints
- Configurable: Adapts to different project types and complexity levels
- Referentially intact: Maintains proper relationships with teams, users, and other entities
"""

import logging
import random
import time
from datetime import datetime, timedelta
from typing import List, Dict, Optional, Tuple, Set, Any
import sqlite3
import numpy as np
import json

from src.utils.logging import get_logger
from src.scrapers.template_scraper import TemplateScraper
from src.models.organization import OrganizationConfig
from src.models.project import ProjectConfig, SectionConfig, TaskConfig

logger = get_logger(__name__)

class ProjectGenerator:
    """
    Generator for creating realistic project data, sections, and custom fields.
    
    This class handles the generation of:
    1. Project records with realistic names, descriptions, and timelines
    2. Project sections with appropriate ordering and naming
    3. Custom field definitions and values for project metadata
    4. Temporal patterns for project creation, start, and end dates
    
    The generator uses scraped template data and enterprise patterns to ensure realism.
    """
    
    def __init__(self, db_conn: sqlite3.Connection, config: Dict[str, Any], org_config: OrganizationConfig):
        """
        Initialize the project generator.
        
        Args:
            db_conn: Database connection
            config: Application configuration
            org_config: Organization configuration
        """
        self.db_conn = db_conn
        self.config = config
        self.org_config = org_config
        self.template_scraper = TemplateScraper(cache_dir=config.get('cache_dir', 'data/cache'))
        
        # Project type distributions by department
        self.project_type_distributions = {
            'engineering': {
                'sprint': 0.4,           # Sprint-based development
                'bug_tracking': 0.25,    # Bug tracking and resolution
                'feature_development': 0.2,  # Long-term feature development
                'tech_debt': 0.1,        # Technical debt reduction
                'research': 0.05         # Research and exploration
            },
            'product': {
                'roadmap_planning': 0.35,
                'user_research': 0.25,
                'feature_specification': 0.2,
                'competitive_analysis': 0.1,
                'metrics_tracking': 0.1
            },
            'marketing': {
                'campaign': 0.4,         # Marketing campaigns
                'content_calendar': 0.25,  # Content planning and scheduling
                'brand_strategy': 0.15,   # Brand development and strategy
                'product_launch': 0.1,    # Product launch coordination
                'seo_optimization': 0.1   # SEO and content optimization
            },
            'sales': {
                'lead_generation': 0.3,
                'sales_pipeline': 0.25,
                'customer_success': 0.2,
                'renewal_tracking': 0.15,
                'territory_planning': 0.1
            },
            'operations': {
                'process_improvement': 0.3,
                'budget_planning': 0.25,
                'resource_allocation': 0.2,
                'compliance_tracking': 0.15,
                'vendor_management': 0.1
            }
        }
        
        # Project status distributions
        self.project_status_distributions = {
            'active': 0.6,      # Currently active projects
            'completed': 0.3,   # Completed projects
            'archived': 0.1     # Archived/archived projects
        }
        
        # Section patterns by project type
        self.section_patterns = {
            'sprint': ['Backlog', 'Ready', 'In Progress', 'In Review', 'Done'],
            'bug_tracking': ['New', 'Triaged', 'In Progress', 'Resolved', 'Verified'],
            'feature_development': ['Planning', 'Design', 'Development', 'Testing', 'Launch'],
            'campaign': ['Planning', 'Content Creation', 'Review & Approval', 'Launch', 'Analysis'],
            'roadmap_planning': ['Q1 Planning', 'Q2 Planning', 'Q3 Planning', 'Q4 Planning', 'Long-term'],
            'process_improvement': ['Discovery', 'Analysis', 'Implementation', 'Review', 'Optimization']
        }
        
        # Custom field patterns by department
        self.custom_field_patterns = {
            'engineering': [
                {'name': 'Priority', 'type': 'enum', 'options': ['Critical', 'High', 'Medium', 'Low']},
                {'name': 'Story Points', 'type': 'number', 'unit': 'points'},
                {'name': 'Sprint', 'type': 'text'},
                {'name': 'Component', 'type': 'text'},
                {'name': 'Bug Severity', 'type': 'enum', 'options': ['Blocker', 'Critical', 'Major', 'Minor', 'Trivial']},
                {'name': 'Tech Debt', 'type': 'boolean'}
            ],
            'product': [
                {'name': 'Impact', 'type': 'enum', 'options': ['High', 'Medium', 'Low']},
                {'name': 'Effort', 'type': 'enum', 'options': ['High', 'Medium', 'Low']},
                {'name': 'ICE Score', 'type': 'number'},
                {'name': 'Customer Request', 'type': 'boolean'},
                {'name': 'Strategic Theme', 'type': 'text'},
                {'name': 'OKR Alignment', 'type': 'text'}
            ],
            'marketing': [
                {'name': 'Campaign Type', 'type': 'enum', 'options': ['Product Launch', 'Brand Awareness', 'Lead Generation', 'Customer Retention']},
                {'name': 'Budget', 'type': 'number', 'unit': 'USD'},
                {'name': 'Target Audience', 'type': 'text'},
                {'name': 'Platform', 'type': 'enum', 'options': ['Social Media', 'Email', 'Web', 'Print', 'Video']},
                {'name': 'KPI Target', 'type': 'number'},
                {'name': 'Creative Assets', 'type': 'number'}
            ],
            'sales': [
                {'name': 'Deal Size', 'type': 'number', 'unit': 'USD'},
                {'name': 'Probability', 'type': 'number', 'unit': '%'},
                {'name': 'Sales Stage', 'type': 'enum', 'options': ['Prospecting', 'Qualification', 'Proposal', 'Negotiation', 'Closed Won', 'Closed Lost']},
                {'name': 'Account Tier', 'type': 'enum', 'options': ['Enterprise', 'Mid-Market', 'SMB']},
                {'name': 'Close Date', 'type': 'date'},
                {'name': 'Competitor', 'type': 'text'}
            ],
            'operations': [
                {'name': 'Priority', 'type': 'enum', 'options': ['Critical', 'High', 'Medium', 'Low']},
                {'name': 'Budget Impact', 'type': 'enum', 'options': ['High', 'Medium', 'Low']},
                {'name': 'Stakeholder', 'type': 'text'},
                {'name': 'Risk Level', 'type': 'enum', 'options': ['High', 'Medium', 'Low']},
                {'name': 'Resource Required', 'type': 'text'},
                {'name': 'Deadline Type', 'type': 'enum', 'options': ['Hard Deadline', 'Soft Deadline', 'Milestone']}
            ]
        }
    
    def _get_project_type_distribution(self, department: str) -> Dict[str, float]:
        """Get project type distribution based on department."""
        return self.project_type_distributions.get(department, self.project_type_distributions['engineering'])
    
    def _generate_realistic_project_name(self, department: str, project_type: str, team_name: str) -> str:
        """
        Generate a realistic project name based on department, project type, and team.
        
        Args:
            department: Department name (engineering, product, marketing, etc.)
            project_type: Type of project (sprint, campaign, roadmap_planning, etc.)
            team_name: Team name
            
        Returns:
            Realistic project name
        """
        name_patterns = {
            'engineering': {
                'sprint': [
                    '{team} Sprint {number}',
                    'Sprint {number}: {feature}',
                    '{feature} Development Sprint',
                    '{team} Engineering Sprint {number}'
                ],
                'bug_tracking': [
                    '{team} Bug Triage',
                    '{product} Bug Tracking',
                    'Quality Improvement: {area}',
                    '{team} Stability Sprint'
                ],
                'feature_development': [
                    '{feature} Feature Development',
                    '{product} {version} Roadmap',
                    '{team}: {feature} Implementation',
                    '{feature} MVP Development'
                ]
            },
            'product': {
                'roadmap_planning': [
                    '{year} Product Roadmap',
                    '{quarter} Planning: {theme}',
                    '{product} Strategic Planning',
                    '{team} Roadmap {year}'
                ],
                'user_research': [
                    '{feature} User Research',
                    'Customer Insights: {area}',
                    '{product} User Feedback Analysis',
                    '{team} Research Initiative'
                ]
            },
            'marketing': {
                'campaign': [
                    '{campaign} Campaign',
                    '{product} Launch Campaign',
                    '{quarter} Marketing Campaign',
                    '{brand} Awareness Campaign'
                ],
                'content_calendar': [
                    '{month} Content Calendar',
                    '{topic} Content Strategy',
                    '{brand} Editorial Calendar',
                    '{quarter} Content Planning'
                ]
            },
            'sales': {
                'lead_generation': [
                    '{quarter} Lead Generation',
                    '{territory} Lead Development',
                    '{product} Sales Pipeline',
                    '{team} Lead Generation Q{quarter}'
                ],
                'sales_pipeline': [
                    '{quarter} Sales Pipeline',
                    '{team} Pipeline Management',
                    '{product} Sales Forecast',
                    'Q{quarter} Revenue Planning'
                ]
            },
            'operations': {
                'process_improvement': [
                    '{process} Process Optimization',
                    '{department} Process Improvement',
                    '{team} Efficiency Initiative',
                    '{area} Workflow Optimization'
                ],
                'budget_planning': [
                    '{year} Budget Planning',
                    '{department} Budget Forecast',
                    'Q{quarter} Financial Planning',
                    '{team} Resource Planning'
                ]
            }
        }
        
        # Get patterns for department and project type
        dept_patterns = name_patterns.get(department, name_patterns['engineering'])
        patterns = dept_patterns.get(project_type, dept_patterns.get('sprint', ['{team} Project']))
        
        # Generate parameters for patterns
        pattern_params = {
            'team': team_name.split(' ')[0] if team_name else 'Team',
            'number': random.randint(1, 20),
            'feature': random.choice(['User Authentication', 'Search Optimization', 'Mobile Experience', 'Dashboard Analytics', 'API Integration', 'Performance Optimization', 'Security Enhancement']),
            'product': random.choice(['Platform', 'Enterprise', 'Cloud', 'Mobile', 'Analytics', 'AI']),
            'version': f"{random.randint(1, 3)}.{random.randint(0, 9)}",
            'area': random.choice(['Backend', 'Frontend', 'Data', 'Infrastructure', 'Security', 'Performance']),
            'year': datetime.now().year,
            'quarter': random.randint(1, 4),
            'month': random.choice(['January', 'February', 'March', 'April', 'May', 'June', 'July', 'August', 'September', 'October', 'November', 'December']),
            'campaign': random.choice(['Q1 Launch', 'Summer Promotion', 'Holiday Season', 'Brand Refresh', 'Product Awareness', 'Customer Retention']),
            'brand': random.choice(['Enterprise', 'Professional', 'Growth', 'Startup', 'Business']),
            'theme': random.choice(['Growth', 'Efficiency', 'Innovation', 'Customer Experience', 'Revenue', 'Platform']),
            'territory': random.choice(['North America', 'EMEA', 'APAC', 'Global', 'Enterprise', 'SMB']),
            'process': random.choice(['Onboarding', 'Approval', 'Reporting', 'Budgeting', 'Hiring', 'Procurement']),
            'department': department.title(),
            'topic': random.choice(['Industry Trends', 'Product Updates', 'Customer Stories', 'Technical Deep Dives', 'Company Culture'])
        }
        
        # Select and format pattern
        pattern = random.choice(patterns)
        try:
            return pattern.format(**pattern_params)
        except KeyError:
            # Fallback if pattern has unknown keys
            return f"{team_name} {project_type.replace('_', ' ').title()} Project"
    
    def _generate_project_description(self, project_name: str, department: str, project_type: str) -> str:
        """
        Generate a realistic project description.
        
        Args:
            project_name: Project name
            department: Department name
            project_type: Type of project
            
        Returns:
            Project description
        """
        descriptions = {
            'engineering': {
                'sprint': [
                    "Two-week sprint focused on delivering user stories and bug fixes for {product}. Includes planning, development, testing, and retrospectives.",
                    "Development sprint for {feature} feature. Team will work on implementation, code reviews, and quality assurance.",
                    "Sprint dedicated to improving system performance and addressing technical debt. Includes performance optimization and refactoring tasks."
                ],
                'bug_tracking': [
                    "Centralized bug tracking system for {product}. Team triages, prioritizes, and resolves bugs reported by users and QA.",
                    "Quality assurance project focused on identifying and resolving critical bugs in {area}. Includes regression testing and verification.",
                    "Stability improvement initiative tracking and resolving bugs across {team}'s codebase. Focus on high-impact issues affecting user experience."
                ]
            },
            'product': {
                'roadmap_planning': [
                    "Strategic planning for {product}'s {year} roadmap. Includes market research, customer feedback analysis, and feature prioritization.",
                    "{quarter} planning for {team} focusing on key initiatives and OKR alignment. Includes resource allocation and timeline planning.",
                    "Long-term roadmap planning for {product} with focus on {theme} initiatives. Includes competitive analysis and customer research."
                ],
                'user_research': [
                    "Research project to understand user needs and pain points for {feature}. Includes user interviews, surveys, and usability testing.",
                    "Customer feedback analysis to identify opportunities for product improvement. Includes sentiment analysis and feature request prioritization.",
                    "User behavior research to optimize {area} experience. Includes analytics analysis, A/B testing, and user journey mapping."
                ]
            },
            'marketing': {
                'campaign': [
                    "Comprehensive marketing campaign for {product} launch. Includes content creation, channel strategy, and performance tracking.",
                    "{campaign} campaign targeting {audience} with focus on {goal}. Includes creative assets, media planning, and conversion optimization.",
                    "Integrated marketing campaign across {channels} to drive {metric}. Includes content calendar, budget allocation, and performance analysis."
                ],
                'content_calendar': [
                    "Editorial calendar for {brand}'s content marketing strategy. Includes blog posts, social media content, and email newsletters.",
                    "{month} content planning for {team} focusing on {theme} content. Includes topic ideation, creation schedule, and distribution planning.",
                    "Quarterly content calendar for {product} with focus on {audience} engagement. Includes content types, publishing schedule, and performance tracking."
                ]
            }
        }
        
        dept_desc = descriptions.get(department, descriptions['engineering'])
        type_desc = dept_desc.get(project_type, dept_desc.get('sprint', [
            "Project focused on {team}'s key initiatives for the current quarter. Includes task tracking, milestone management, and progress reporting."
        ]))
        
        description = random.choice(type_desc)
        params = {
            'product': random.choice(['Platform', 'Enterprise', 'Cloud', 'AI Assistant']),
            'feature': random.choice(['search', 'authentication', 'dashboard', 'mobile app', 'API']),
            'area': random.choice(['user experience', 'system performance', 'data quality', 'security']),
            'team': project_name.split(' ')[0] if project_name else 'Team',
            'year': datetime.now().year,
            'quarter': f"Q{random.randint(1, 4)}",
            'campaign': random.choice(['summer', 'holiday', 'product launch', 'brand awareness']),
            'audience': random.choice(['enterprise customers', 'small businesses', 'developers', 'end users']),
            'goal': random.choice(['brand awareness', 'lead generation', 'customer retention', 'revenue growth']),
            'channels': random.choice(['email and social media', 'paid and organic', 'digital and print']),
            'metric': random.choice(['conversion rate', 'engagement', 'revenue', 'customer acquisition']),
            'brand': random.choice(['Enterprise', 'Growth', 'Professional']),
            'month': random.choice(['January', 'February', 'March', 'April', 'May', 'June']),
            'theme': random.choice(['industry trends', 'product updates', 'customer stories', 'thought leadership'])
        }
        
        try:
            return description.format(**params)
        except KeyError:
            return f"{project_name} project managed by the team for tracking tasks and milestones."
    
    def _get_realistic_project_timeline(self, project_type: str, department: str, 
                                      company_start_date: datetime, current_date: datetime) -> Tuple[datetime, Optional[datetime]]:
        """
        Generate realistic project start and end dates based on project type and department.
        
        Args:
            project_type: Type of project
            department: Department name
            company_start_date: Company founding/start date
            current_date: Current date for reference
            
        Returns:
            Tuple of (start_date, end_date) where end_date can be None for ongoing projects
        """
        # Base duration ranges in days by project type
        duration_ranges = {
            'sprint': (10, 16),           # 2 weeks
            'bug_tracking': (30, 90),     # 1-3 months, ongoing
            'feature_development': (60, 180),  # 2-6 months
            'tech_debt': (30, 90),        # 1-3 months
            'research': (14, 45),         # 2 weeks - 1.5 months
            'campaign': (30, 60),         # 1-2 months
            'content_calendar': (30, 90), # 1-3 months (quarterly)
            'roadmap_planning': (14, 30), # 2-4 weeks
            'user_research': (14, 45),    # 2 weeks - 1.5 months
            'process_improvement': (30, 120), # 1-4 months
            'budget_planning': (14, 30),  # 2-4 weeks (quarterly)
            'lead_generation': (30, 90),  # 1-3 months
            'sales_pipeline': (30, 90),   # 1-3 months, ongoing
        }
        
        # Get duration range, default to 1-3 months
        min_days, max_days = duration_ranges.get(project_type, (30, 90))
        
        # Adjust duration based on department
        if department == 'executive':
            min_days = int(min_days * 1.5)
            max_days = int(max_days * 1.5)
        
        # Generate project start date
        project_age = (current_date - company_start_date).days
        if project_age < 30:  # Company is very new
            start_days_back = random.randint(0, min(7, project_age))
        else:
            # Most projects start within last 6 months, with some older
            start_distribution = [0.5, 0.3, 0.15, 0.05]  # weights for time buckets
            time_buckets = [
                (0, 30),      # Last month: 50%
                (30, 90),     # 1-3 months ago: 30%
                (90, 180),    # 3-6 months ago: 15%
                (180, project_age)  # 6+ months ago: 5%
            ]
            
            bucket = random.choices(time_buckets, weights=start_distribution)[0]
            start_days_back = random.randint(bucket[0], min(bucket[1], project_age))
        
        start_date = current_date - timedelta(days=start_days_back)
        start_date = max(start_date, company_start_date)
        
        # Determine if project should have an end date
        status_distribution = {'active': 0.6, 'completed': 0.35, 'archived': 0.05}
        project_status = random.choices(list(status_distribution.keys()), 
                                       weights=list(status_distribution.values()))[0]
        
        end_date = None
        if project_status in ['completed', 'archived']:
            # Generate duration within range
            duration_days = random.randint(min_days, max_days)
            end_date = start_date + timedelta(days=duration_days)
            
            # Ensure end date doesn't exceed current date for completed projects
            if end_date > current_date:
                end_date = current_date
        
        return start_date, end_date
    
    def _get_section_names(self, project_type: str, department: str) -> List[str]:
        """
        Get section names based on project type and department.
        
        Args:
            project_type: Type of project
            department: Department name
            
        Returns:
            List of section names
        """
        # Try to get sections from pattern mapping first
        if project_type in self.section_patterns:
            return self.section_patterns[project_type]
        
        # Get sections based on department
        dept_sections = {
            'engineering': ['Backlog', 'Ready', 'In Progress', 'In Review', 'Done'],
            'product': ['Backlog', 'Research', 'Design', 'Development', 'Testing', 'Launch'],
            'marketing': ['Planning', 'Content Creation', 'Review', 'Published', 'Analysis'],
            'sales': ['Prospecting', 'Qualification', 'Proposal', 'Negotiation', 'Closed'],
            'operations': ['To Do', 'In Progress', 'Blocked', 'Review', 'Done']
        }
        
        return dept_sections.get(department, ['To Do', 'In Progress', 'Done'])
    
    def _get_custom_fields(self, department: str, project_type: str) -> List[Dict[str, Any]]:
        """
        Get custom field definitions based on department and project type.
        
        Args:
            department: Department name
            project_type: Type of project
            
        Returns:
            List of custom field definitions
        """
        # Get base fields for department
        base_fields = self.custom_field_patterns.get(department, [])
        
        # Add project-type specific fields
        project_type_fields = {
            'sprint': [
                {'name': 'Sprint Number', 'type': 'number'},
                {'name': 'Sprint Goal', 'type': 'text'},
                {'name': 'Velocity Target', 'type': 'number'}
            ],
            'bug_tracking': [
                {'name': 'Reporter', 'type': 'text'},
                {'name': 'Repro Steps', 'type': 'text'},
                {'name': 'Environment', 'type': 'enum', 'options': ['Production', 'Staging', 'Development']}
            ],
            'campaign': [
                {'name': 'Campaign Budget', 'type': 'number', 'unit': 'USD'},
                {'name': 'Target CPA', 'type': 'number', 'unit': 'USD'},
                {'name': 'Campaign Start', 'type': 'date'},
                {'name': 'Campaign End', 'type': 'date'}
            ],
            'roadmap_planning': [
                {'name': 'Quarter', 'type': 'enum', 'options': ['Q1', 'Q2', 'Q3', 'Q4']},
                {'name': 'Strategic Pillar', 'type': 'text'},
                {'name': 'Resource Allocation', 'type': 'number', 'unit': '%'}
            ]
        }
        
        additional_fields = project_type_fields.get(project_type, [])
        
        # Combine and shuffle fields
        all_fields = base_fields + additional_fields
        random.shuffle(all_fields)
        
        # Limit to 3-6 fields for realism
        num_fields = random.randint(3, min(6, len(all_fields)))
        return all_fields[:num_fields]
    
    def generate_projects_for_teams(self, teams: List[Dict[str, Any]], 
                                 users: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Generate projects for teams based on team departments and organizational structure.
        
        Args:
            teams: List of team dictionaries
            users: List of user dictionaries
            
        Returns:
            List of project dictionaries
        """
        logger.info(f"Generating projects for {len(teams)} teams")
        
        projects = []
        current_date = datetime.now()
        company_start_date = self.org_config.time_range.start_date
        
        # Create user email to ID map for assignee resolution
        user_email_to_id = {user['email']: user['id'] for user in users if 'id' in user}
        
        for team in teams:
            team_id = team['id']
            team_name = team['name']
            department = team.get('department', 'engineering')
            
            # Get number of projects for this team
            min_projects, max_projects = self.org_config.num_projects_per_team_range
            num_projects = random.randint(min_projects, max_projects)
            
            # Get project type distribution for department
            project_type_dist = self._get_project_type_distribution(department)
            project_types = list(project_type_dist.keys())
            project_weights = list(project_type_dist.values())
            
            for i in range(num_projects):
                # Select project type
                project_type = random.choices(project_types, weights=project_weights)[0]
                
                # Generate project name and description
                project_name = self._generate_realistic_project_name(department, project_type, team_name)
                description = self._generate_project_description(project_name, department, project_type)
                
                # Generate timeline
                start_date, end_date = self._get_realistic_project_timeline(
                    project_type, department, company_start_date, current_date
                )
                
                # Determine project status
                status = 'completed' if end_date and end_date <= current_date else 'active'
                if status == 'active' and random.random() < 0.1:  # 10% chance of being archived
                    status = 'archived'
                
                # Select project lead (team lead or senior member)
                team_members = [u for u in users if any(m['team_id'] == team_id for m in u.get('memberships', []))]
                project_lead = None
                if team_members:
                    # Prefer team lead or managers
                    leads = [u for u in team_members if u.get('role') == 'admin' or 'manager' in u.get('role_title', '').lower()]
                    if leads:
                        project_lead = random.choice(leads)
                    else:
                        project_lead = random.choice(team_members)
                
                project = {
                    'organization_id': team['organization_id'],
                    'team_id': team_id,
                    'name': project_name,
                    'description': description,
                    'status': status,
                    'start_date': start_date.strftime('%Y-%m-%d'),
                    'end_date': end_date.strftime('%Y-%m-%d') if end_date else None,
                    'created_at': start_date.strftime('%Y-%m-%d %H:%M:%S'),
                    'updated_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                    'project_lead_id': project_lead['id'] if project_lead and 'id' in project_lead else None,
                    'department': department,
                    'project_type': project_type
                }
                projects.append(project)
        
        logger.info(f"Successfully generated {len(projects)} projects for teams")
        return projects
    
    def generate_sections_for_projects(self, projects: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Generate sections for projects based on project types and departments.
        
        Args:
            projects: List of project dictionaries
            
        Returns:
            List of section dictionaries
        """
        logger.info(f"Generating sections for {len(projects)} projects")
        
        sections = []
        
        for project in projects:
            project_id = project['id'] if 'id' in project else len(sections) + 1
            department = project.get('department', 'engineering')
            project_type = project.get('project_type', 'sprint')
            
            # Get section names based on project type and department
            section_names = self._get_section_names(project_type, department)
            
            # Add randomization to section order for realism
            if random.random() < 0.3:  # 30% chance of custom section order
                section_names = section_names.copy()
                random.shuffle(section_names)
            
            # Create sections with positions
            for position, section_name in enumerate(section_names):
                section = {
                    'project_id': project_id,
                    'name': section_name,
                    'position': position,
                    'created_at': project['created_at'],
                    'updated_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                }
                sections.append(section)
        
        logger.info(f"Successfully generated {len(sections)} sections for projects")
        return sections
    
    def generate_custom_fields_for_organization(self, organization_id: int, 
                                             departments: List[str]) -> List[Dict[str, Any]]:
        """
        Generate custom field definitions for an organization based on departments.
        
        Args:
            organization_id: Organization ID
            departments: List of departments in the organization
            
        Returns:
            List of custom field definition dictionaries
        """
        logger.info(f"Generating custom fields for organization {organization_id}")
        
        custom_fields = []
        used_field_names = set()
        
        # Get all unique departments
        unique_departments = list(set(departments))
        
        for department in unique_departments:
            # Get base fields for department
            dept_fields = self.custom_field_patterns.get(department, [])
            
            # Add some project-type specific fields
            project_type_fields = []
            if department == 'engineering':
                project_type_fields = [
                    {'name': 'Sprint', 'type': 'text'},
                    {'name': 'Component', 'type': 'text'},
                    {'name': 'Epic', 'type': 'text'}
                ]
            elif department == 'marketing':
                project_type_fields = [
                    {'name': 'Campaign', 'type': 'text'},
                    {'name': 'Channel', 'type': 'text'},
                    {'name': 'Target Audience', 'type': 'text'}
                ]
            
            all_fields = dept_fields + project_type_fields
            
            # Shuffle and select fields
            random.shuffle(all_fields)
            num_fields = random.randint(3, min(8, len(all_fields)))
            selected_fields = all_fields[:num_fields]
            
            for field_def in selected_fields:
                field_name = field_def['name']
                field_type = field_def['type']
                
                # Ensure unique field names within organization
                base_name = field_name
                counter = 1
                while field_name in used_field_names:
                    field_name = f"{base_name} {counter}"
                    counter += 1
                used_field_names.add(field_name)
                
                enum_options = field_def.get('options', []) if field_type == 'enum' else None
                
                custom_field = {
                    'organization_id': organization_id,
                    'name': field_name,
                    'field_type': field_type,
                    'enum_options': json.dumps(enum_options) if enum_options else None,
                    'created_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                    'updated_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                }
                custom_fields.append(custom_field)
        
        logger.info(f"Successfully generated {len(custom_fields)} custom field definitions for organization")
        return custom_fields
    
    def insert_projects(self, projects: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Insert projects into the database and return projects with IDs.
        
        Args:
            projects: List of project dictionaries
            
        Returns:
            List of project dictionaries with database IDs
        """
        cursor = self.db_conn.cursor()
        inserted_projects = []
        
        for project in projects:
            try:
                cursor.execute("""
                    INSERT INTO projects (
                        organization_id, name, description, status,
                        start_date, end_date, created_at, updated_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    project['organization_id'],
                    project['name'],
                    project['description'],
                    project['status'],
                    project['start_date'],
                    project['end_date'],
                    project['created_at'],
                    project['updated_at']
                ))
                
                project_id = cursor.lastrowid
                project_with_id = project.copy()
                project_with_id['id'] = project_id
                inserted_projects.append(project_with_id)
                
            except sqlite3.Error as e:
                logger.error(f"Error inserting project {project['name']}: {str(e)}")
                raise
        
        self.db_conn.commit()
        logger.info(f"Successfully inserted {len(inserted_projects)} projects into database")
        return inserted_projects
    
    def insert_sections(self, sections: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Insert sections into the database.
        
        Args:
            sections: List of section dictionaries
            
        Returns:
            List of inserted section dictionaries
        """
        cursor = self.db_conn.cursor()
        inserted_sections = []
        
        for section in sections:
            try:
                cursor.execute("""
                    INSERT INTO sections (
                        project_id, name, position, created_at, updated_at
                    ) VALUES (?, ?, ?, ?, ?)
                """, (
                    section['project_id'],
                    section['name'],
                    section['position'],
                    section['created_at'],
                    section['updated_at']
                ))
                
                section_id = cursor.lastrowid
                section_with_id = section.copy()
                section_with_id['id'] = section_id
                inserted_sections.append(section_with_id)
                
            except sqlite3.Error as e:
                logger.error(f"Error inserting section {section['name']}: {str(e)}")
                raise
        
        self.db_conn.commit()
        logger.info(f"Successfully inserted {len(inserted_sections)} sections into database")
        return inserted_sections
    
    def insert_custom_field_definitions(self, custom_fields: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Insert custom field definitions into the database.
        
        Args:
            custom_fields: List of custom field definition dictionaries
            
        Returns:
            List of inserted custom field definition dictionaries
        """
        cursor = self.db_conn.cursor()
        inserted_fields = []
        
        for field in custom_fields:
            try:
                cursor.execute("""
                    INSERT INTO custom_field_definitions (
                        organization_id, name, field_type, enum_options,
                        created_at, updated_at
                    ) VALUES (?, ?, ?, ?, ?, ?)
                """, (
                    field['organization_id'],
                    field['name'],
                    field['field_type'],
                    field['enum_options'],
                    field['created_at'],
                    field['updated_at']
                ))
                
                field_id = cursor.lastrowid
                field_with_id = field.copy()
                field_with_id['id'] = field_id
                inserted_fields.append(field_with_id)
                
            except sqlite3.Error as e:
                logger.error(f"Error inserting custom field {field['name']}: {str(e)}")
                raise
        
        self.db_conn.commit()
        logger.info(f"Successfully inserted {len(inserted_fields)} custom field definitions into database")
        return inserted_fields
    
    def generate_and_insert(self, teams: List[Dict[str, Any]], users: List[Dict[str, Any]], 
                          organization_id: int) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]], List[Dict[str, Any]]]:
        """
        Generate and insert projects, sections, and custom fields for an organization.
        
        Args:
            teams: List of team dictionaries
            users: List of user dictionaries
            organization_id: Organization ID
            
        Returns:
            Tuple of (projects, sections, custom_fields) with database IDs
        """
        logger.info(f"Generating and inserting projects, sections, and custom fields for organization {organization_id}")
        
        # Generate projects
        projects = self.generate_projects_for_teams(teams, users)
        
        # Insert projects to get IDs
        inserted_projects = self.insert_projects(projects)
        
        # Generate and insert sections
        sections = self.generate_sections_for_projects(inserted_projects)
        inserted_sections = self.insert_sections(sections)
        
        # Generate and insert custom fields
        departments = [team.get('department', 'engineering') for team in teams]
        custom_fields = self.generate_custom_fields_for_organization(organization_id, departments)
        inserted_custom_fields = self.insert_custom_field_definitions(custom_fields)
        
        logger.info(f"Successfully generated and inserted:")
        logger.info(f"  - {len(inserted_projects)} projects")
        logger.info(f"  - {len(inserted_sections)} sections")
        logger.info(f"  - {len(inserted_custom_fields)} custom field definitions")
        
        return inserted_projects, inserted_sections, inserted_custom_fields
    
    def close(self):
        """Close the template scraper session."""
        self.template_scraper.close()
        logger.info("Project generator closed")

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
        CREATE TABLE projects (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            organization_id INTEGER NOT NULL,
            team_id INTEGER NOT NULL,
            name TEXT NOT NULL,
            description TEXT,
            status TEXT NOT NULL,
            start_date DATE,
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
            updated_at TIMESTAMP NOT NULL,
            FOREIGN KEY (project_id) REFERENCES projects(id)
        )
    """)
    
    cursor.execute("""
        CREATE TABLE custom_field_definitions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            organization_id INTEGER NOT NULL,
            name TEXT NOT NULL,
            field_type TEXT NOT NULL,
            enum_options TEXT,
            created_at TIMESTAMP NOT NULL,
            updated_at TIMESTAMP NOT NULL
        )
    """)
    
    cursor.execute("""
        CREATE TABLE teams (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            organization_id INTEGER NOT NULL,
            name TEXT NOT NULL,
            description TEXT,
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
            updated_at TIMESTAMP NOT NULL,
            UNIQUE(team_id, user_id)
        )
    """)
    
    cursor.execute("""
        CREATE TABLE organizations (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            domain TEXT NOT NULL UNIQUE,
            created_at TIMESTAMP NOT NULL,
            updated_at TIMESTAMP NOT NULL
        )
    """)
    
    test_conn.commit()
    
    try:
        # Create mock data
        mock_organization = {'id': 1, 'name': 'Test Org', 'domain': 'test.org'}
        
        mock_teams = [
            {'id': 1, 'organization_id': 1, 'name': 'Engineering Team', 'department': 'engineering'},
            {'id': 2, 'organization_id': 1, 'name': 'Marketing Team', 'department': 'marketing'}
        ]
        
        mock_users = [
            {'id': 1, 'email': 'john@example.com', 'name': 'John Doe', 'role': 'admin', 'role_title': 'Engineering Manager'},
            {'id': 2, 'email': 'jane@example.com', 'name': 'Jane Smith', 'role': 'member', 'role_title': 'Product Manager'},
            {'id': 3, 'email': 'bob@example.com', 'name': 'Bob Johnson', 'role': 'member', 'role_title': 'Marketing Manager'}
        ]
        
        # Create project generator
        generator = ProjectGenerator(test_conn, mock_config, mock_org_config)
        
        # Generate and insert projects, sections, and custom fields
        projects, sections, custom_fields = generator.generate_and_insert(
            mock_teams, mock_users, mock_organization['id']
        )
        
        print(f"\nGenerated Data Summary:")
        print(f"Projects: {len(projects)}")
        print(f"Sections: {len(sections)}")
        print(f"Custom Fields: {len(custom_fields)}")
        
        print("\nSample Projects:")
        for project in projects[:3]:
            print(f"  - {project['name']} ({project['status']})")
            print(f"    Department: {project.get('department', 'N/A')}, Type: {project.get('project_type', 'N/A')}")
            print(f"    Timeline: {project['start_date']} to {project['end_date'] or 'ongoing'}")
        
        print("\nSample Sections:")
        for section in sections[:5]:
            project = next((p for p in projects if p.get('id') == section['project_id']), None)
            if project:
                print(f"  - {section['name']} in {project['name']} (Position: {section['position']})")
        
        print("\nSample Custom Fields:")
        for field in custom_fields[:5]:
            print(f"  - {field['name']} ({field['field_type']})")
            if field['field_type'] == 'enum' and field['enum_options']:
                options = json.loads(field['enum_options'])
                print(f"    Options: {', '.join(options[:3])}{'...' if len(options) > 3 else ''}")
        
    finally:
        generator.close()
        test_conn.close()