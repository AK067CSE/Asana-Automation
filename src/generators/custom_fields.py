# src/generators/custom_fields.py

"""
Custom field generator module for creating realistic custom field values and metadata.
This module generates realistic custom field values based on field definitions,
department patterns, and enterprise metadata requirements.

The generator is designed to be:
- Type-aware: Generates appropriate values for different field types (text, number, date, enum, boolean)
- Context-sensitive: Values match department context and business requirements
- Distribution-accurate: Follows real-world patterns for custom field usage
- Referentially intact: Maintains proper relationships with tasks, projects, and organizations
- Configurable: Adaptable to different business domains and metadata requirements
"""

import logging
import random
import time
from datetime import datetime, timedelta, date
from typing import List, Dict, Optional, Tuple, Any, Union
import sqlite3
import numpy as np
import json

from src.utils.logging import get_logger
from src.utils.temporal import get_business_day_offset, is_business_day, get_random_business_date
from src.models.organization import OrganizationConfig

logger = get_logger(__name__)

class CustomFieldGenerator:
    """
    Generator for creating realistic custom field values and metadata.
    
    This class handles the generation of:
    1. Custom field values for tasks and projects
    2. Realistic value distributions based on field types and business context
    3. Temporal patterns for date fields
    4. Enum value selections based on business rules
    5. Numeric value generation with realistic ranges and distributions
    
    The generator uses enterprise patterns and statistical distributions to ensure
    custom field values feel authentic and support realistic RL environment training.
    """
    
    def __init__(self, db_conn: sqlite3.Connection, config: Dict[str, Any], org_config: OrganizationConfig):
        """
        Initialize the custom field generator.
        
        Args:
            db_conn: Database connection
            config: Application configuration
            org_config: Organization configuration
        """
        self.db_conn = db_conn
        self.config = config
        self.org_config = org_config
        
        # Field type patterns and value distributions
        self.field_value_patterns = {
            'text': {
                'engineering': [
                    'API endpoint', 'database schema', 'microservice', 'container image',
                    'CI/CD pipeline', 'test coverage', 'performance metric', 'security scan',
                    'code review', 'documentation update', 'refactoring target', 'tech debt item'
                ],
                'product': [
                    'user story', 'acceptance criteria', 'feature flag', 'A/B test',
                    'user persona', 'journey map', 'wireframe', 'prototype', 'user feedback',
                    'market research', 'competitive analysis', 'success metric'
                ],
                'marketing': [
                    'campaign name', 'target audience', 'channel strategy', 'creative asset',
                    'landing page', 'email template', 'social media post', 'blog article',
                    'SEO keyword', 'conversion rate', 'engagement metric', 'brand guideline'
                ],
                'sales': [
                    'deal size', 'sales stage', 'probability', 'close date', 'account tier',
                    'territory', 'product line', 'competitive landscape', 'customer pain point',
                    'ROI analysis', 'implementation timeline', 'stakeholder map'
                ],
                'operations': [
                    'process step', 'approval workflow', 'budget category', 'resource allocation',
                    'compliance requirement', 'risk level', 'vendor contract', 'SLA metric',
                    'cost center', 'headcount plan', 'training requirement', 'policy document'
                ]
            },
            'number': {
                'engineering': {
                    'story_points': {'min': 1, 'max': 13, 'distribution': [0.1, 0.2, 0.3, 0.2, 0.1, 0.05, 0.05]},
                    'priority': {'min': 1, 'max': 5, 'distribution': [0.05, 0.15, 0.4, 0.3, 0.1]},
                    'risk_level': {'min': 1, 'max': 10, 'distribution': [0.1, 0.1, 0.2, 0.2, 0.15, 0.1, 0.05, 0.05, 0.03, 0.02]}
                },
                'product': {
                    'ice_score': {'min': 1, 'max': 100, 'distribution': 'normal', 'mean': 50, 'std': 20},
                    'effort_level': {'min': 1, 'max': 10, 'distribution': [0.05, 0.1, 0.15, 0.2, 0.2, 0.15, 0.1, 0.03, 0.01, 0.01]},
                    'impact_score': {'min': 1, 'max': 10, 'distribution': [0.02, 0.03, 0.05, 0.1, 0.2, 0.3, 0.2, 0.07, 0.02, 0.01]}
                },
                'marketing': {
                    'budget_usd': {'min': 100, 'max': 50000, 'distribution': 'lognormal', 'mean': 2500, 'std': 1.5},
                    'target_ctr': {'min': 0.1, 'max': 10.0, 'distribution': 'normal', 'mean': 2.5, 'std': 1.0},
                    'creative_assets': {'min': 1, 'max': 20, 'distribution': [0.1, 0.2, 0.3, 0.2, 0.1, 0.05, 0.03, 0.01, 0.005, 0.005]}
                },
                'sales': {
                    'deal_size': {'min': 1000, 'max': 1000000, 'distribution': 'lognormal', 'mean': 50000, 'std': 2.0},
                    'probability': {'min': 1, 'max': 100, 'distribution': [0.05, 0.05, 0.1, 0.15, 0.2, 0.2, 0.15, 0.05, 0.03, 0.02]},
                    'timeline_weeks': {'min': 1, 'max': 26, 'distribution': 'normal', 'mean': 8, 'std': 4}
                },
                'operations': {
                    'budget_impact': {'min': -10000, 'max': 10000, 'distribution': 'normal', 'mean': 0, 'std': 3000},
                    'risk_score': {'min': 1, 'max': 10, 'distribution': [0.05, 0.05, 0.1, 0.15, 0.2, 0.2, 0.15, 0.07, 0.02, 0.01]},
                    'resource_hours': {'min': 1, 'max': 160, 'distribution': 'normal', 'mean': 40, 'std': 30}
                }
            },
            'date': {
                'engineering': {
                    'sprint_start': {'offset_days': (-30, 0)},    # 30 days before to today
                    'sprint_end': {'offset_days': (1, 14)},     # 1-14 days from now
                    'release_date': {'offset_days': (7, 90)}    # 1 week to 3 months from now
                },
                'marketing': {
                    'campaign_start': {'offset_days': (-7, 7)},   # 1 week before to 1 week after
                    'campaign_end': {'offset_days': (7, 60)},    # 1 week to 2 months after start
                    'content_publish': {'offset_days': (1, 30)}  # 1 day to 1 month from now
                },
                'product': {
                    'research_deadline': {'offset_days': (3, 21)},  # 3 days to 3 weeks
                    'design_review': {'offset_days': (2, 14)},    # 2 days to 2 weeks
                    'launch_date': {'offset_days': (30, 180)}     # 1-6 months from now
                },
                'sales': {
                    'proposal_due': {'offset_days': (1, 7)},     # 1-7 days from now
                    'demo_date': {'offset_days': (2, 14)},      # 2 days to 2 weeks
                    'close_date': {'offset_days': (14, 90)}     # 2 weeks to 3 months
                },
                'operations': {
                    'approval_deadline': {'offset_days': (1, 14)},  # 1 day to 2 weeks
                    'implementation_date': {'offset_days': (7, 60)},  # 1 week to 2 months
                    'review_date': {'offset_days': (30, 120)}    # 1-4 months from now
                }
            },
            'enum': {
                'engineering': {
                    'priority': ['Critical', 'High', 'Medium', 'Low'],
                    'component': ['Frontend', 'Backend', 'Database', 'API', 'Infrastructure', 'Security'],
                    'bug_severity': ['Blocker', 'Critical', 'Major', 'Minor', 'Trivial'],
                    'environment': ['Production', 'Staging', 'Development', 'QA']
                },
                'product': {
                    'impact': ['High', 'Medium', 'Low'],
                    'effort': ['High', 'Medium', 'Low'],
                    'strategic_theme': ['Growth', 'Engagement', 'Retention', 'Monetization', 'Efficiency'],
                    'customer_type': ['Enterprise', 'Mid-Market', 'SMB', 'Freemium']
                },
                'marketing': {
                    'campaign_type': ['Product Launch', 'Brand Awareness', 'Lead Generation', 'Customer Retention', 'Event Promotion'],
                    'target_audience': ['Enterprise', 'Mid-Market', 'SMB', 'Developers', 'End Users'],
                    'platform': ['Social Media', 'Email', 'Web', 'Print', 'Video', 'Paid Search'],
                    'content_type': ['Blog Post', 'Whitepaper', 'Case Study', 'Video', 'Infographic', 'Webinar']
                },
                'sales': {
                    'sales_stage': ['Prospecting', 'Qualification', 'Proposal', 'Negotiation', 'Closed Won', 'Closed Lost'],
                    'account_tier': ['Enterprise', 'Mid-Market', 'SMB', 'Strategic Partner'],
                    'deal_type': ['New Business', 'Expansion', 'Renewal', 'Cross-sell', 'Up-sell'],
                    'probability': ['10%', '25%', '50%', '75%', '90%']
                },
                'operations': {
                    'priority': ['Critical', 'High', 'Medium', 'Low'],
                    'risk_level': ['High', 'Medium', 'Low'],
                    'deadline_type': ['Hard Deadline', 'Soft Deadline', 'Milestone', 'Nice to Have'],
                    'resource_type': ['People', 'Budget', 'Time', 'Tools', 'External Services']
                }
            },
            'boolean': {
                'engineering': {
                    'tech_debt': [0.3, 0.7],    # 30% true, 70% false
                    'security_review': [0.2, 0.8],  # 20% true, 80% false
                    'performance_critical': [0.4, 0.6],  # 40% true, 60% false
                    'customer_facing': [0.5, 0.5]   # 50% true, 50% false
                },
                'product': {
                    'customer_request': [0.6, 0.4],  # 60% true, 40% false
                    'regulatory_requirement': [0.15, 0.85],  # 15% true, 85% false
                    'competitive_gap': [0.25, 0.75],  # 25% true, 75% false
                    'revenue_impact': [0.45, 0.55]   # 45% true, 55% false
                },
                'marketing': {
                    'approved': [0.8, 0.2],     # 80% true, 20% false
                    'published': [0.7, 0.3],    # 70% true, 30% false
                    'requires_legal_review': [0.2, 0.8],  # 20% true, 80% false
                    'a_b_test': [0.3, 0.7]      # 30% true, 70% false
                },
                'sales': {
                    'executive_sponsor': [0.4, 0.6],  # 40% true, 60% false
                    'competitive_deal': [0.35, 0.65],  # 35% true, 65% false
                    'renewal': [0.25, 0.75],    # 25% true, 75% false
                    'requires_discount': [0.3, 0.7]   # 30% true, 70% false
                },
                'operations': {
                    'compliance_required': [0.25, 0.75],  # 25% true, 75% false
                    'budget_approved': [0.6, 0.4],   # 60% true, 40% false
                    'stakeholder_alignment': [0.55, 0.45],  # 55% true, 45% false
                    'vendor_involved': [0.35, 0.65]   # 35% true, 65% false
                }
            }
        }
        
        # Field usage patterns by department and project type
        self.field_usage_patterns = {
            'engineering': {
                'sprint': ['priority', 'story_points', 'component', 'sprint', 'bug_severity', 'tech_debt'],
                'bug_tracking': ['bug_severity', 'environment', 'priority', 'reporter', 'repro_steps'],
                'feature_development': ['component', 'priority', 'story_points', 'customer_request', 'technical_complexity'],
                'tech_debt': ['priority', 'impact', 'effort', 'component', 'tech_debt'],
                'research': ['priority', 'impact', 'resource_hours', 'deliverable', 'research_type']
            },
            'product': {
                'roadmap_planning': ['strategic_theme', 'impact', 'effort', 'ice_score', 'quarter', 'resource_allocation'],
                'user_research': ['research_type', 'participants', 'duration_days', 'deliverable', 'customer_type'],
                'feature_specification': ['customer_request', 'impact', 'effort', 'success_metrics', 'dependencies'],
                'competitive_analysis': ['competitor', 'feature_gap', 'priority', 'market_impact', 'timeline'],
                'metrics_tracking': ['metric_name', 'current_value', 'target_value', 'time_period', 'owner']
            },
            'marketing': {
                'campaign': ['campaign_type', 'target_audience', 'budget_usd', 'platform', 'start_date', 'end_date', 'kpi_target'],
                'content_calendar': ['content_type', 'target_audience', 'publish_date', 'author', 'approval_status', 'platform'],
                'brand_strategy': ['brand_guideline', 'target_audience', 'messaging', 'visual_identity', 'tone_of_voice'],
                'product_launch': ['launch_date', 'target_audience', 'channels', 'budget_usd', 'success_metrics', 'stakeholders'],
                'seo_optimization': ['keyword', 'current_ranking', 'target_ranking', 'content_type', 'implementation_effort']
            },
            'sales': {
                'lead_generation': ['source', 'industry', 'company_size', 'lead_score', 'assigned_rep', 'follow_up_date'],
                'sales_pipeline': ['deal_size', 'probability', 'close_date', 'sales_stage', 'account_tier', 'competitive_landscape'],
                'customer_success': ['health_score', 'renewal_date', 'expansion_opportunity', 'csat_score', 'account_manager', 'critical_issues'],
                'renewal_tracking': ['renewal_date', 'current_contract_value', 'expansion_potential', 'risk_factors', 'stakeholder_map', 'success_plan'],
                'territory_planning': ['territory', 'quota', 'current_pipeline', 'target_accounts', 'growth_strategy', 'resource_allocation']
            },
            'operations': {
                'process_improvement': ['process_name', 'current_efficiency', 'target_efficiency', 'implementation_effort', 'risk_level', 'stakeholders'],
                'budget_planning': ['budget_category', 'current_spend', 'planned_spend', 'variance', 'approval_status', 'owner'],
                'resource_allocation': ['resource_type', 'current_allocation', 'planned_allocation', 'utilization_rate', 'priority', 'timeline'],
                'compliance_tracking': ['compliance_requirement', 'due_date', 'status', 'risk_level', 'owner', 'audit_frequency'],
                'vendor_management': ['vendor_name', 'contract_value', 'renewal_date', 'service_level', 'performance_rating', 'relationship_owner']
            }
        }
        
        # Completion rates for custom fields (industry benchmarks)
        self.field_completion_rates = {
            'required_fields': 0.95,    # 95% completion for required fields
            'important_fields': 0.80,   # 80% completion for important fields  
            'optional_fields': 0.45     # 45% completion for optional fields
        }
    
    def _get_field_value_distribution(self, field_definition: Dict[str, Any], 
                                     department: str, project_type: str) -> Union[List, Dict, str]:
        """
        Get value distribution for a custom field based on its type and context.
        
        Args:
            field_definition: Field definition dictionary
            department: Department name
            project_type: Project type
            
        Returns:
            Value distribution (list for enum, dict for number/date, etc.)
        """
        field_type = field_definition['field_type']
        field_name = field_definition['name'].lower()
        
        # Get department patterns
        dept_patterns = self.field_value_patterns.get(field_type, {}).get(department, {})
        
        if field_type == 'enum':
            # Look for field-specific enum options
            if field_name in dept_patterns:
                return dept_patterns[field_name]
            
            # Look for generic enum patterns by category
            for category, pattern in dept_patterns.items():
                if category in field_name:
                    return pattern
            
            # Fallback to generic enums
            return ['High', 'Medium', 'Low']
        
        elif field_type == 'number':
            # Look for field-specific number patterns
            if field_name in dept_patterns:
                return dept_patterns[field_name]
            
            # Look for generic number patterns
            for category, pattern in dept_patterns.items():
                if category in field_name:
                    return pattern
            
            # Fallback to generic number pattern
            return {'min': 1, 'max': 100, 'distribution': 'uniform'}
        
        elif field_type == 'date':
            # Look for field-specific date patterns
            if field_name in dept_patterns:
                return dept_patterns[field_name]
            
            # Look for generic date patterns
            for category, pattern in dept_patterns.items():
                if category in field_name:
                    return pattern
            
            # Fallback to generic date pattern
            return {'offset_days': (0, 30)}
        
        elif field_type == 'boolean':
            # Look for field-specific boolean patterns
            if field_name in dept_patterns:
                return dept_patterns[field_name]
            
            # Look for generic boolean patterns
            for category, pattern in dept_patterns.items():
                if category in field_name:
                    return pattern
            
            # Fallback to generic boolean pattern
            return [0.5, 0.5]  # 50% true, 50% false
        
        elif field_type == 'text':
            # Return text patterns for the department
            return dept_patterns or ['value1', 'value2', 'value3']
        
        return None
    
    def _generate_field_value(self, field_definition: Dict[str, Any], 
                           department: str, project_type: str, 
                           task_created_at: datetime) -> Any:
        """
        Generate a realistic value for a custom field.
        
        Args:
            field_definition: Field definition dictionary
            department: Department name
            project_type: Project type
            task_created_at: Task creation timestamp
            
        Returns:
            Generated field value
        """
        field_type = field_definition['field_type']
        field_name = field_definition['name'].lower()
        
        # Get value distribution
        distribution = self._get_field_value_distribution(field_definition, department, project_type)
        
        if not distribution:
            return None
        
        if field_type == 'enum':
            # Handle enum options stored as JSON string
            if 'enum_options' in field_definition and field_definition['enum_options']:
                try:
                    enum_options = json.loads(field_definition['enum_options'])
                    if isinstance(enum_options, list):
                        return random.choice(enum_options)
                except (json.JSONDecodeError, TypeError):
                    pass
            
            # Use distribution patterns
            if isinstance(distribution, list):
                return random.choice(distribution)
            return random.choice(list(distribution.keys()))
        
        elif field_type == 'number':
            if isinstance(distribution, dict):
                dist_type = distribution.get('distribution', 'uniform')
                
                if dist_type == 'uniform':
                    min_val = distribution.get('min', 1)
                    max_val = distribution.get('max', 100)
                    return random.uniform(min_val, max_val)
                
                elif dist_type == 'normal':
                    mean = distribution.get('mean', 50)
                    std = distribution.get('std', 15)
                    min_val = distribution.get('min', 0)
                    max_val = distribution.get('max', 100)
                    
                    # Generate normal distribution value
                    value = np.random.normal(mean, std)
                    # Clamp to bounds
                    value = max(min_val, min(max_val, value))
                    # Round to reasonable precision
                    if max_val > 1000:
                        return int(round(value, -2))  # Round to hundreds
                    elif max_val > 100:
                        return int(round(value, -1))  # Round to tens
                    return round(value, 1)
                
                elif dist_type == 'lognormal':
                    mean = distribution.get('mean', 3.0)
                    std = distribution.get('std', 1.0)
                    min_val = distribution.get('min', 1)
                    max_val = distribution.get('max', 1000)
                    
                    # Generate log-normal distribution
                    value = np.random.lognormal(mean, std)
                    # Clamp to bounds
                    value = max(min_val, min(max_val, value))
                    return int(round(value))
                
                elif isinstance(dist_type, list):  # Discrete distribution
                    values = list(range(distribution.get('min', 1), distribution.get('max', 10) + 1))
                    return random.choices(values, weights=dist_type)[0]
            
            # Fallback
            return random.randint(1, 100)
        
        elif field_type == 'date':
            if isinstance(distribution, dict):
                offset_min = distribution.get('offset_days', [0, 30])[0]
                offset_max = distribution.get('offset_days', [0, 30])[1]
                
                # Generate random offset within range
                offset_days = random.randint(offset_min, offset_max)
                value_date = task_created_at + timedelta(days=offset_days)
                
                # 85% chance of business day
                if random.random() < 0.85:
                    value_date = get_business_day_offset(value_date, 0)
                
                return value_date.strftime('%Y-%m-%d')
            
            # Fallback
            return (task_created_at + timedelta(days=random.randint(0, 30))).strftime('%Y-%m-%d')
        
        elif field_type == 'boolean':
            if isinstance(distribution, list) and len(distribution) == 2:
                return random.choices([True, False], weights=distribution)[0]
            return random.random() < 0.5
        
        elif field_type == 'text':
            if isinstance(distribution, list):
                return random.choice(distribution)
            elif isinstance(distribution, str):
                return distribution
            return f"{field_name.replace('_', ' ').title()} value"
        
        return None
    
    def _determine_field_completion(self, field_definition: Dict[str, Any], 
                                  department: str, project_type: str) -> bool:
        """
        Determine if a custom field should be completed based on its importance and context.
        
        Args:
            field_definition: Field definition dictionary
            department: Department name
            project_type: Project type
            
        Returns:
            True if field should be completed, False otherwise
        """
        field_name = field_definition['name'].lower()
        field_type = field_definition['field_type']
        
        # Check if field is in usage patterns
        dept_patterns = self.field_usage_patterns.get(department, {})
        project_patterns = dept_patterns.get(project_type, [])
        
        # Determine field importance
        if any(important in field_name for important in ['priority', 'due', 'deadline', 'required', 'critical', 'mandatory']):
            completion_rate = self.field_completion_rates['required_fields']
        elif any(important in field_name for important in ['impact', 'effort', 'score', 'value', 'target', 'budget', 'cost']):
            completion_rate = self.field_completion_rates['important_fields']
        else:
            completion_rate = self.field_completion_rates['optional_fields']
        
        # Adjust based on if field is in the project's usage patterns
        if field_name in project_patterns or any(field_name in pattern for pattern in project_patterns):
            completion_rate *= 1.2  # 20% boost if field is relevant to this project type
        
        # Random decision based on completion rate
        return random.random() < min(1.0, completion_rate)
    
    def generate_custom_field_values_for_tasks(self, tasks: List[Dict[str, Any]], 
                                             custom_field_definitions: List[Dict[str, Any]], 
                                             projects: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Generate custom field values for tasks based on field definitions and context.
        
        Args:
            tasks: List of task dictionaries
            custom_field_definitions: List of custom field definition dictionaries
            projects: List of project dictionaries
            
        Returns:
            List of custom field value dictionaries
        """
        logger.info(f"Generating custom field values for {len(tasks)} tasks")
        
        field_values = []
        
        # Create project mapping for quick lookup
        project_map = {project['id']: project for project in projects}
        
        for task in tasks:
            task_id = task.get('id')
            project_id = task.get('project_id')
            
            if not task_id or not project_id:
                continue
            
            # Get project context
            project = project_map.get(project_id, {})
            department = project.get('department', 'engineering')
            project_type = project.get('project_type', 'sprint')
            task_created_at = datetime.strptime(task.get('created_at', datetime.now().strftime('%Y-%m-%d %H:%M:%S')), '%Y-%m-%d %H:%M:%S')
            
            # Get relevant custom field definitions for this organization
            org_id = project.get('organization_id', 1)
            relevant_fields = [field for field in custom_field_definitions if field.get('organization_id') == org_id]
            
            for field_definition in relevant_fields:
                # Determine if this field should be completed for this task
                if not self._determine_field_completion(field_definition, department, project_type):
                    continue
                
                # Generate field value
                value = self._generate_field_value(field_definition, department, project_type, task_created_at)
                
                if value is None:
                    continue
                
                # Create field value record
                field_value = {
                    'custom_field_definition_id': field_definition['id'],
                    'task_id': task_id,
                    'created_at': task_created_at.strftime('%Y-%m-%d %H:%M:%S'),
                    'updated_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                }
                
                # Set value based on field type
                field_type = field_definition['field_type']
                if field_type == 'text':
                    field_value['value_text'] = str(value)
                elif field_type == 'number':
                    field_value['value_number'] = float(value) if isinstance(value, (int, float)) else float(str(value).replace(',', ''))
                elif field_type == 'date':
                    field_value['value_date'] = value
                elif field_type == 'boolean':
                    field_value['value_boolean'] = bool(value)
                elif field_type == 'enum':
                    field_value['value_enum'] = str(value)
                
                field_values.append(field_value)
        
        logger.info(f"Successfully generated {len(field_values)} custom field values for tasks")
        return field_values
    
    def insert_custom_field_values(self, field_values: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Insert custom field values into the database and return values with IDs.
        
        Args:
            field_values: List of custom field value dictionaries
            
        Returns:
            List of custom field value dictionaries with database IDs
        """
        cursor = self.db_conn.cursor()
        inserted_values = []
        
        for value in field_values:
            try:
                cursor.execute("""
                    INSERT INTO custom_field_values (
                        custom_field_definition_id, task_id, value_text, value_number, 
                        value_date, value_boolean, value_enum, created_at, updated_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    value['custom_field_definition_id'],
                    value['task_id'],
                    value.get('value_text'),
                    value.get('value_number'),
                    value.get('value_date'),
                    value.get('value_boolean'),
                    value.get('value_enum'),
                    value['created_at'],
                    value['updated_at']
                ))
                
                value_id = cursor.lastrowid
                value_with_id = value.copy()
                value_with_id['id'] = value_id
                inserted_values.append(value_with_id)
                
            except sqlite3.Error as e:
                logger.error(f"Error inserting custom field value: {str(e)}")
                # Continue with other values
                continue
        
        self.db_conn.commit()
        logger.info(f"Successfully inserted {len(inserted_values)} custom field values into database")
        return inserted_values
    
    def generate_and_insert_custom_field_values(self, tasks: List[Dict[str, Any]], 
                                              custom_field_definitions: List[Dict[str, Any]], 
                                              projects: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Generate and insert custom field values for all tasks.
        
        Args:
            tasks: List of task dictionaries
            custom_field_definitions: List of custom field definition dictionaries
            projects: List of project dictionaries
            
        Returns:
            List of inserted custom field value dictionaries with IDs
        """
        logger.info("Starting custom field value generation and insertion")
        
        # Generate custom field values
        field_values = self.generate_custom_field_values_for_tasks(tasks, custom_field_definitions, projects)
        
        # Insert custom field values
        inserted_values = self.insert_custom_field_values(field_values)
        
        logger.info(f"Successfully generated and inserted {len(inserted_values)} custom field values")
        return inserted_values
    
    def close(self):
        """Cleanup resources."""
        logger.info("Custom field generator closed")

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
        CREATE TABLE custom_field_values (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            custom_field_definition_id INTEGER NOT NULL,
            task_id INTEGER NOT NULL,
            value_text TEXT,
            value_number REAL,
            value_date DATE,
            value_boolean BOOLEAN,
            value_enum TEXT,
            created_at TIMESTAMP NOT NULL,
            updated_at TIMESTAMP NOT NULL,
            FOREIGN KEY (custom_field_definition_id) REFERENCES custom_field_definitions(id),
            FOREIGN KEY (task_id) REFERENCES tasks(id)
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
        CREATE TABLE tasks (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            project_id INTEGER NOT NULL,
            created_at TIMESTAMP NOT NULL
        )
    """)
    
    cursor.execute("""
        CREATE TABLE projects (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            organization_id INTEGER NOT NULL,
            team_id INTEGER NOT NULL,
            name TEXT NOT NULL,
            department TEXT,
            project_type TEXT
        )
    """)
    
    test_conn.commit()
    
    try:
        # Create mock data
        mock_tasks = [
            {'id': 1, 'project_id': 1, 'created_at': '2025-12-15 09:30:00'},
            {'id': 2, 'project_id': 1, 'created_at': '2025-12-16 10:15:00'},
            {'id': 3, 'project_id': 2, 'created_at': '2025-12-17 11:20:00'}
        ]
        
        mock_custom_fields = [
            {'id': 1, 'organization_id': 1, 'name': 'Priority', 'field_type': 'enum', 'enum_options': json.dumps(['Critical', 'High', 'Medium', 'Low'])},
            {'id': 2, 'organization_id': 1, 'name': 'Story Points', 'field_type': 'number'},
            {'id': 3, 'organization_id': 1, 'name': 'Component', 'field_type': 'enum', 'enum_options': json.dumps(['Frontend', 'Backend', 'Database', 'API'])},
            {'id': 4, 'organization_id': 1, 'name': 'Bug Severity', 'field_type': 'enum', 'enum_options': json.dumps(['Blocker', 'Critical', 'Major', 'Minor'])},
            {'id': 5, 'organization_id': 1, 'name': 'Sprint', 'field_type': 'text'},
            {'id': 6, 'organization_id': 1, 'name': 'Tech Debt', 'field_type': 'boolean'},
            {'id': 7, 'organization_id': 2, 'name': 'Campaign Type', 'field_type': 'enum', 'enum_options': json.dumps(['Product Launch', 'Brand Awareness', 'Lead Generation'])},
            {'id': 8, 'organization_id': 2, 'name': 'Budget USD', 'field_type': 'number'}
        ]
        
        mock_projects = [
            {'id': 1, 'organization_id': 1, 'team_id': 1, 'name': 'Engineering Sprint', 'department': 'engineering', 'project_type': 'sprint'},
            {'id': 2, 'organization_id': 2, 'team_id': 2, 'name': 'Q1 Marketing Campaign', 'department': 'marketing', 'project_type': 'campaign'}
        ]
        
        # Create custom field generator
        generator = CustomFieldGenerator(test_conn, mock_config, mock_org_config)
        
        # Generate and insert custom field values
        field_values = generator.generate_and_insert_custom_field_values(
            tasks=mock_tasks,
            custom_field_definitions=mock_custom_fields,
            projects=mock_projects
        )
        
        print(f"\nGenerated Data Summary:")
        print(f"Custom Field Values: {len(field_values)}")
        
        print("\nSample Custom Field Values:")
        for i, value in enumerate(field_values[:10], 1):
            field_def = next((f for f in mock_custom_fields if f['id'] == value['custom_field_definition_id']), None)
            task = next((t for t in mock_tasks if t['id'] == value['task_id']), None)
            if field_def and task:
                # Get the actual value based on field type
                field_type = field_def['field_type']
                actual_value = None
                
                if field_type == 'text':
                    actual_value = value.get('value_text')
                elif field_type == 'number':
                    actual_value = value.get('value_number')
                elif field_type == 'date':
                    actual_value = value.get('value_date')
                elif field_type == 'boolean':
                    actual_value = value.get('value_boolean')
                elif field_type == 'enum':
                    actual_value = value.get('value_enum')
                
                print(f"  {i}. Task {task['id']} - {field_def['name']} ({field_type}): {actual_value}")
        
        # Test statistics
        print(f"\nField values per task:")
        from collections import Counter
        task_value_counts = Counter(value['task_id'] for value in field_values)
        for task_id, count in task_value_counts.items():
            print(f"  Task {task_id}: {count} custom field values")
        
        print(f"\nUnique fields used:")
        field_counts = Counter(value['custom_field_definition_id'] for value in field_values)
        for field_id, count in field_counts.items():
            field_name = next((f['name'] for f in mock_custom_fields if f['id'] == field_id), f"Field {field_id}")
            print(f"  {field_name}: {count} values")
        
        print("\n✅ All custom field generator tests completed successfully!")
    
    finally:
        generator.close()
        test_conn.close()