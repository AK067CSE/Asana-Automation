

"""
Tag generator module for creating realistic tags and task-tag associations.
This module generates realistic tags that simulate enterprise workflow categorization,
priority labeling, and metadata organization patterns.

The generator is designed to be:
- Context-aware: Tags match department context, project types, and task purposes
- Distribution-realistic: Follows real-world tag usage patterns and frequencies
- Referentially intact: Maintains proper relationships with tasks and organizations
- Semantically meaningful: Tags have clear business meaning and categorization logic
- Configurable: Adaptable to different business domains and tagging strategies
"""

import logging
import random
import time
from datetime import datetime, timedelta
from typing import List, Dict, Optional, Tuple, Any
import sqlite3
import numpy as np

from src.utils.logging import get_logger
from src.models.organization import OrganizationConfig

logger = get_logger(__name__)

class TagGenerator:
    """
    Generator for creating realistic tags and task-tag associations.
    
    This class handles the generation of:
    1. Realistic tag names and metadata based on business contexts
    2. Tag assignment patterns that match enterprise workflow categorization
    3. Tag usage frequencies and distribution across tasks and projects
    4. Semantic relationships between tags (priority, status, category, etc.)
    5. Temporal patterns for tag creation and usage
    
    The generator uses research-backed patterns and statistical distributions
    to ensure tags feel authentic and support realistic RL environment training.
    """
    
    def __init__(self, db_conn: sqlite3.Connection, config: Dict[str, Any], org_config: OrganizationConfig):
        """
        Initialize the tag generator.
        
        Args:
            db_conn: Database connection
            config: Application configuration
            org_config: Organization configuration
        """
        self.db_conn = db_conn
        self.config = config
        self.org_config = org_config
        
        # Tag patterns by department and purpose
        self.tag_patterns = {
            'engineering': {
                'priority': ['urgent', 'high-priority', 'asap', 'critical-path', 'blocker'],
                'status': ['in-progress', 'needs-review', 'blocked', 'ready-for-qa', 'ready-for-release'],
                'category': ['bug-fix', 'feature', 'tech-debt', 'refactoring', 'performance', 'security', 'documentation'],
                'component': ['frontend', 'backend', 'database', 'api', 'infrastructure', 'devops', 'testing'],
                'workflow': ['sprint-1', 'sprint-2', 'sprint-3', 'q1-2026', 'q2-2026', 'tech-debt-sprint']
            },
            'product': {
                'priority': ['p0-critical', 'p1-high', 'p2-medium', 'p3-low', 'backlog'],
                'status': ['researching', 'designing', 'specifying', 'in-development', 'in-review', 'waiting-feedback'],
                'category': ['user-research', 'feature-request', 'bug-report', 'improvement', 'technical-debt', 'analytics'],
                'impact': ['high-impact', 'medium-impact', 'low-impact', 'customer-request', 'strategic'],
                'workflow': ['discovery', 'definition', 'design', 'development', 'validation', 'launch']
            },
            'marketing': {
                'priority': ['urgent-campaign', 'high-priority', 'time-sensitive', 'asap-content', 'critical-messaging'],
                'status': ['in-creation', 'in-review', 'waiting-approval', 'approved', 'scheduled', 'published', 'analyzing'],
                'category': ['content', 'campaign', 'social-media', 'email', 'seo', 'brand', 'event', 'video'],
                'audience': ['enterprise', 'mid-market', 'smb', 'developers', 'end-users', 'partners'],
                'workflow': ['q1-campaign', 'q2-campaign', 'q3-campaign', 'q4-campaign', 'evergreen-content']
            },
            'sales': {
                'priority': ['hot-lead', 'high-value', 'expansion-opportunity', 'renewal-risk', 'strategic-account'],
                'status': ['prospecting', 'qualified', 'demo-scheduled', 'proposal-sent', 'negotiating', 'closed-won', 'closed-lost'],
                'category': ['new-business', 'expansion', 'renewal', 'cross-sell', 'up-sell', 'partner-deal'],
                'deal_size': ['enterprise', 'mid-market', 'smb', 'strategic', 'tactical'],
                'workflow': ['lead-gen', 'qualification', 'proposal', 'negotiation', 'closing', 'post-sale']
            },
            'operations': {
                'priority': ['critical-process', 'high-impact', 'cost-saving', 'compliance-required', 'efficiency-improvement'],
                'status': ['planning', 'implementing', 'testing', 'reviewing', 'approved', 'monitoring'],
                'category': ['process-improvement', 'cost-reduction', 'compliance', 'resource-planning', 'vendor-management', 'risk-mitigation'],
                'impact_area': ['finance', 'hr', 'legal', 'it', 'facilities', 'executive'],
                'workflow': ['q1-initiatives', 'q2-initiatives', 'q3-initiatives', 'q4-initiatives', 'annual-planning']
            }
        }
        
        # Tag usage patterns and frequencies
        self.tag_usage_patterns = {
            'engineering': {
                'sprint': {
                    'priority_tags': 0.3,      # 30% of tasks have priority tags
                    'status_tags': 0.8,       # 80% have status tags
                    'category_tags': 0.6,     # 60% have category tags
                    'component_tags': 0.4,    # 40% have component tags
                    'workflow_tags': 0.7      # 70% have workflow/sprint tags
                },
                'bug_tracking': {
                    'priority_tags': 0.5,     # High priority focus for bugs
                    'status_tags': 0.9,       # Very high status tracking
                    'category_tags': 0.7,     # High category usage
                    'component_tags': 0.6,    # Important for bug location
                    'workflow_tags': 0.3      # Less workflow focus
                },
                'feature_development': {
                    'priority_tags': 0.4,
                    'status_tags': 0.7,
                    'category_tags': 0.8,     # High category usage for features
                    'component_tags': 0.5,
                    'workflow_tags': 0.6
                },
                'tech_debt': {
                    'priority_tags': 0.2,     # Lower priority focus
                    'status_tags': 0.6,
                    'category_tags': 0.9,     # Very high category usage
                    'component_tags': 0.7,    # Important for tech debt location
                    'workflow_tags': 0.4
                }
            },
            'marketing': {
                'campaign': {
                    'priority_tags': 0.4,
                    'status_tags': 0.9,       # Very high status tracking for campaigns
                    'category_tags': 0.8,     # High category usage
                    'audience_tags': 0.7,     # Important for targeting
                    'workflow_tags': 0.8      # High workflow/campaign tracking
                },
                'content_calendar': {
                    'priority_tags': 0.3,
                    'status_tags': 0.85,      # High status tracking for content
                    'category_tags': 0.75,    # High category usage
                    'audience_tags': 0.5,     # Moderate audience focus
                    'workflow_tags': 0.9      # Very high workflow/campaign tracking
                }
            },
            'product': {
                'roadmap_planning': {
                    'priority_tags': 0.5,     # High priority focus for roadmap
                    'status_tags': 0.7,
                    'category_tags': 0.6,
                    'impact_tags': 0.8,       # High impact tracking for roadmap
                    'workflow_tags': 0.6
                },
                'user_research': {
                    'priority_tags': 0.2,
                    'status_tags': 0.6,
                    'category_tags': 0.7,
                    'impact_tags': 0.3,
                    'workflow_tags': 0.5
                }
            }
        }
        
        # Tag color schemes (hex colors for visual differentiation)
        self.tag_colors = {
            'priority': ['#FF4444', '#FF6B6B', '#FF9E80', '#FFD166'],
            'status': ['#4CAF50', '#2196F3', '#FF9800', '#9C27B0', '#F44336'],
            'category': ['#3F51B5', '#2196F3', '#03A9F4', '#00BCD4', '#009688'],
            'component': ['#795548', '#607D8B', '#455A64', '#37474F', '#263238'],
            'workflow': ['#E91E63', '#9C27B0', '#673AB7', '#3F51B5', '#2196F3'],
            'audience': ['#FF5722', '#FF9800', '#FFC107', '#FFEB3B', '#CDDC39'],
            'impact': ['#D32F2F', '#C62828', '#B71C1C', '#F44336', '#E53935'],
            'deal_size': ['#1A237E', '#283593', '#303F9F', '#3949AB', '#3F51B5']
        }
        
        # Tag name conflict resolution strategies
        self.tag_name_variations = {
            'prefix': ['team-', 'dept-', 'org-', 'project-', 'q1-', 'q2-', 'q3-', 'q4-'],
            'suffix': ['-team', '-dept', '-org', '-project', '-q1', '-q2', '-q3', '-q4'],
            'separator': ['-', '_', '.', ':']
        }
    
    def _get_tag_patterns_for_context(self, department: str, project_type: str, tag_category: str) -> List[str]:
        """
        Get tag patterns for a specific context and category.
        
        Args:
            department: Department name
            project_type: Project type
            tag_category: Tag category (priority, status, category, etc.)
            
        Returns:
            List of tag patterns for the context
        """
        # Get base patterns for department
        dept_patterns = self.tag_patterns.get(department, {})
        
        # Get patterns for category
        category_patterns = dept_patterns.get(tag_category, [])
        
        if not category_patterns:
            # Try generic patterns if department-specific not found
            generic_patterns = {
                'priority': ['high', 'medium', 'low', 'urgent', 'critical'],
                'status': ['todo', 'in-progress', 'review', 'done', 'blocked'],
                'category': ['work', 'personal', 'important', 'urgent', 'planning'],
                'workflow': ['sprint', 'quarter', 'year', 'phase', 'milestone']
            }
            return generic_patterns.get(tag_category, [f"{tag_category}-tag"])
        
        return category_patterns
    
    def _get_tag_usage_probability(self, department: str, project_type: str, tag_category: str) -> float:
        """
        Get the probability that a task should have a tag of a specific category.
        
        Args:
            department: Department name
            project_type: Project type
            tag_category: Tag category
            
        Returns:
            Probability (0-1) that a task should have this tag category
        """
        # Get usage patterns for department and project type
        dept_patterns = self.tag_usage_patterns.get(department, {})
        project_patterns = dept_patterns.get(project_type, {})
        
        # Get base probability for category
        base_prob = project_patterns.get(f"{tag_category}_tags", 0.4)  # Default 40%
        
        # Adjust based on category importance
        category_importance = {
            'priority': 1.2,
            'status': 1.3,
            'category': 1.1,
            'component': 0.9,
            'workflow': 1.0,
            'audience': 0.8,
            'impact': 1.1,
            'deal_size': 0.7
        }
        
        importance_factor = category_importance.get(tag_category, 1.0)
        adjusted_prob = min(1.0, base_prob * importance_factor)
        
        return adjusted_prob
    
    def _generate_realistic_tag_name(self, department: str, project_type: str, tag_category: str) -> str:
        """
        Generate a realistic tag name based on context.
        
        Args:
            department: Department name
            project_type: Project type
            tag_category: Tag category
            
        Returns:
            Realistic tag name
        """
        # Get patterns for context
        patterns = self._get_tag_patterns_for_context(department, project_type, tag_category)
        
        if not patterns:
            return f"{department}-{tag_category}-{random.randint(1, 10)}"
        
        # Select base pattern
        base_name = random.choice(patterns)
        
        # Apply variations based on context
        variation_chance = 0.3  # 30% chance of variation
        
        if random.random() < variation_chance:
            variation_type = random.choice(['prefix', 'suffix'])
            separator = random.choice(self.tag_name_variations['separator'])
            
            if variation_type == 'prefix':
                prefix = random.choice(self.tag_name_variations['prefix'])
                return f"{prefix}{separator}{base_name}"
            else:
                suffix = random.choice(self.tag_name_variations['suffix'])
                return f"{base_name}{separator}{suffix}"
        
        return base_name
    
    def _select_tag_color(self, tag_category: str, tag_name: str) -> str:
        """
        Select a realistic tag color based on category and name.
        
        Args:
            tag_category: Tag category
            tag_name: Tag name
            
        Returns:
            Hex color string
        """
        # Get colors for category
        category_colors = self.tag_colors.get(tag_category, [])
        
        # Try to match color based on name content
        if 'urgent' in tag_name.lower() or 'critical' in tag_name.lower() or 'high' in tag_name.lower():
            return '#FF4444'  # Red for urgent/critical/high
        elif 'done' in tag_name.lower() or 'complete' in tag_name.lower() or 'approved' in tag_name.lower():
            return '#4CAF50'  # Green for done/complete
        elif 'in-progress' in tag_name.lower() or 'working' in tag_name.lower() or 'developing' in tag_name.lower():
            return '#2196F3'  # Blue for in-progress
        elif 'blocked' in tag_name.lower() or 'waiting' in tag_name.lower():
            return '#FF9800'  # Orange for blocked/waiting
        elif 'low' in tag_name.lower() or 'minor' in tag_name.lower():
            return '#9E9E9E'  # Gray for low/minor
        
        # Random color from category if no semantic match
        if category_colors:
            return random.choice(category_colors)
        
        # Default color
        return '#3F51B5'
    
    def generate_tags_for_organization(self, organization_id: int, departments: List[str]) -> List[Dict[str, Any]]:
        """
        Generate tags for an organization based on departments and usage patterns.
        
        Args:
            organization_id: Organization ID
            departments: List of departments in the organization
            
        Returns:
            List of tag dictionaries
        """
        logger.info(f"Generating tags for organization {organization_id} with departments: {departments}")
        
        tags = []
        used_tag_names = set()
        
        # Define tag categories to generate for each department
        tag_categories = ['priority', 'status', 'category', 'workflow']
        
        # Engineering gets component tags
        engineering_categories = tag_categories + ['component']
        
        # Marketing gets audience tags  
        marketing_categories = tag_categories + ['audience']
        
        # Product gets impact tags
        product_categories = tag_categories + ['impact']
        
        # Sales gets deal_size tags
        sales_categories = tag_categories + ['deal_size']
        
        for department in departments:
            # Select appropriate categories for department
            if department == 'engineering':
                categories = engineering_categories
            elif department == 'marketing':
                categories = marketing_categories
            elif department == 'product':
                categories = product_categories
            elif department == 'sales':
                categories = sales_categories
            else:
                categories = tag_categories
            
            # Generate tags for each category
            for category in categories:
                # Number of tags per category (3-8)
                num_tags = random.randint(3, 8)
                
                for _ in range(num_tags):
                    tag_name = self._generate_realistic_tag_name(department, 'default', category)
                    
                    # Ensure unique tag names within organization
                    base_name = tag_name
                    counter = 1
                    while tag_name in used_tag_names:
                        tag_name = f"{base_name}-{counter}"
                        counter += 1
                    used_tag_names.add(tag_name)
                    
                    # Select color
                    tag_color = self._select_tag_color(category, tag_name)
                    
                    tag = {
                        'organization_id': organization_id,
                        'name': tag_name,
                        'color': tag_color,
                        'category': category,
                        'department': department,
                        'created_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                        'updated_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                    }
                    tags.append(tag)
        
        logger.info(f"Successfully generated {len(tags)} tags for organization {organization_id}")
        return tags
    
    def assign_tags_to_tasks(self, tasks: List[Dict[str, Any]], tags: List[Dict[str, Any]], 
                          projects: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Assign tags to tasks based on realistic patterns and distributions.
        
        Args:
            tasks: List of task dictionaries
            tags: List of tag dictionaries
            projects: List of project dictionaries
            
        Returns:
            List of task-tag association dictionaries
        """
        logger.info(f"Assigning tags to {len(tasks)} tasks")
        
        task_tag_associations = []
        tag_assignments_count = {}
        
        # Create mappings for quick lookup
        project_map = {project['id']: project for project in projects}
        tag_map = {}
        
        # Organize tags by department and category
        for tag in tags:
            dept = tag.get('department', 'engineering')
            category = tag.get('category', 'category')
            
            if dept not in tag_map:
                tag_map[dept] = {}
            if category not in tag_map[dept]:
                tag_map[dept][category] = []
            
            tag_map[dept][category].append(tag)
        
        for task in tasks:
            task_id = task.get('id')
            project_id = task.get('project_id')
            
            if not task_id or not project_id:
                continue
            
            # Get project context
            project = project_map.get(project_id, {})
            department = project.get('department', 'engineering')
            project_type = project.get('project_type', 'sprint')
            
            # Get available tags for this department
            dept_tags = tag_map.get(department, {})
            
            # Number of tags per task (0-5, typically 1-3)
            max_tags_per_task = 5
            base_num_tags = int(np.random.lognormal(mean=0.5, sigma=0.7))
            num_tags = min(max_tags_per_task, max(0, base_num_tags))
            
            # Sometimes no tags (10% chance)
            if random.random() < 0.1:
                num_tags = 0
            
            assigned_categories = set()
            assigned_tags = []
            
            # Assign tags by category based on usage probabilities
            for category, tags_in_category in dept_tags.items():
                if not tags_in_category:
                    continue
                
                # Get usage probability for this category
                usage_prob = self._get_tag_usage_probability(department, project_type, category)
                
                # Determine if this task should get a tag from this category
                if random.random() < usage_prob and len(assigned_tags) < num_tags:
                    # Select a random tag from this category
                    tag = random.choice(tags_in_category)
                    
                    # Ensure we don't assign the same tag multiple times to the same task
                    if tag['id'] not in [t['tag_id'] for t in assigned_tags]:
                        association = {
                            'task_id': task_id,
                            'tag_id': tag['id'],
                            'created_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                        }
                        assigned_tags.append(association)
                        
                        # Track tag assignments
                        tag_assignments_count[tag['id']] = tag_assignments_count.get(tag['id'], 0) + 1
            
            task_tag_associations.extend(assigned_tags)
        
        logger.info(f"Successfully created {len(task_tag_associations)} task-tag associations")
        return task_tag_associations
    
    def insert_tags(self, tags: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Insert tags into the database and return tags with IDs.
        
        Args:
            tags: List of tag dictionaries
            
        Returns:
            List of tag dictionaries with database IDs
        """
        cursor = self.db_conn.cursor()
        inserted_tags = []
        tag_name_to_id = {}
        
        for tag in tags:
            try:
                cursor.execute("""
                    INSERT INTO tags (
                        organization_id, name, color, 
                        created_at, updated_at
                    ) VALUES (?, ?, ?, ?, ?)
                """, (
                    tag['organization_id'],
                    tag['name'],
                    tag['color'],
                    tag['created_at'],
                    tag['updated_at']
                ))
                
                tag_id = cursor.lastrowid
                tag_with_id = tag.copy()
                tag_with_id['id'] = tag_id
                inserted_tags.append(tag_with_id)
                tag_name_to_id[tag['name']] = tag_id
                
            except sqlite3.IntegrityError as e:
                if 'UNIQUE constraint failed: tags.organization_id, tags.name' in str(e):
                    logger.warning(f"Duplicate tag name '{tag['name']}' for organization {tag['organization_id']}. Skipping.")
                else:
                    logger.error(f"Error inserting tag '{tag['name']}': {str(e)}")
                    raise
            except sqlite3.Error as e:
                logger.error(f"Error inserting tag '{tag['name']}': {str(e)}")
                raise
        
        self.db_conn.commit()
        logger.info(f"Successfully inserted {len(inserted_tags)} tags into database")
        return inserted_tags
    
    def insert_task_tag_associations(self, associations: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Insert task-tag associations into the database.
        
        Args:
            associations: List of task-tag association dictionaries
            
        Returns:
            List of inserted association dictionaries with IDs
        """
        cursor = self.db_conn.cursor()
        inserted_associations = []
        
        for association in associations:
            try:
                cursor.execute("""
                    INSERT INTO task_tags (
                        task_id, tag_id, created_at
                    ) VALUES (?, ?, ?)
                """, (
                    association['task_id'],
                    association['tag_id'],
                    association['created_at']
                ))
                
                association_id = cursor.lastrowid
                association_with_id = association.copy()
                association_with_id['id'] = association_id
                inserted_associations.append(association_with_id)
                
            except sqlite3.IntegrityError as e:
                if 'UNIQUE constraint failed: task_tags.task_id, task_tags.tag_id' in str(e):
                    logger.warning(f"Duplicate task-tag association for task {association['task_id']} and tag {association['tag_id']}. Skipping.")
                else:
                    logger.error(f"Error inserting task-tag association: {str(e)}")
                    raise
            except sqlite3.Error as e:
                logger.error(f"Error inserting task-tag association: {str(e)}")
                raise
        
        self.db_conn.commit()
        logger.info(f"Successfully inserted {len(inserted_associations)} task-tag associations into database")
        return inserted_associations
    
    def generate_and_insert_tags(self, organization_id: int, departments: List[str], 
                               tasks: List[Dict[str, Any]], projects: List[Dict[str, Any]]) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
        """
        Generate and insert tags and task-tag associations for an organization.
        
        Args:
            organization_id: Organization ID
            departments: List of departments
            tasks: List of task dictionaries
            projects: List of project dictionaries
            
        Returns:
            Tuple of (tags, task_tag_associations) with database IDs
        """
        logger.info(f"Starting tag generation and insertion for organization {organization_id}")
        
        # Generate tags
        tags = self.generate_tags_for_organization(organization_id, departments)
        
        # Insert tags
        inserted_tags = self.insert_tags(tags)
        
        # Assign and insert task-tag associations
        associations = self.assign_tags_to_tasks(tasks, inserted_tags, projects)
        inserted_associations = self.insert_task_tag_associations(associations)
        
        logger.info(f"Successfully generated and inserted:")
        logger.info(f"  - {len(inserted_tags)} tags")
        logger.info(f"  - {len(inserted_associations)} task-tag associations")
        
        return inserted_tags, inserted_associations
    
    def close(self):
        """Cleanup resources."""
        logger.info("Tag generator closed")

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
        CREATE TABLE tags (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            organization_id INTEGER NOT NULL,
            name TEXT NOT NULL,
            color TEXT,
            created_at TIMESTAMP NOT NULL,
            updated_at TIMESTAMP NOT NULL,
            UNIQUE(organization_id, name)
        )
    """)
    
    cursor.execute("""
        CREATE TABLE task_tags (
            task_id INTEGER NOT NULL,
            tag_id INTEGER NOT NULL,
            created_at TIMESTAMP NOT NULL,
            PRIMARY KEY (task_id, tag_id),
            FOREIGN KEY (task_id) REFERENCES tasks(id),
            FOREIGN KEY (tag_id) REFERENCES tags(id)
        )
    """)
    
    cursor.execute("""
        CREATE TABLE tasks (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            project_id INTEGER NOT NULL,
            name TEXT NOT NULL
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
        mock_organization_id = 1
        mock_departments = ['engineering', 'marketing', 'product']
        
        mock_projects = [
            {'id': 1, 'organization_id': 1, 'team_id': 1, 'name': 'Engineering Sprint', 'department': 'engineering', 'project_type': 'sprint'},
            {'id': 2, 'organization_id': 1, 'team_id': 2, 'name': 'Q1 Marketing Campaign', 'department': 'marketing', 'project_type': 'campaign'},
            {'id': 3, 'organization_id': 1, 'team_id': 3, 'name': 'Product Roadmap Q1', 'department': 'product', 'project_type': 'roadmap_planning'}
        ]
        
        mock_tasks = [
            {'id': 1, 'project_id': 1, 'name': 'Implement user authentication'},
            {'id': 2, 'project_id': 1, 'name': 'Fix login bug'},
            {'id': 3, 'project_id': 1, 'name': 'Optimize database queries'},
            {'id': 4, 'project_id': 2, 'name': 'Create campaign landing page'},
            {'id': 5, 'project_id': 2, 'name': 'Design social media graphics'},
            {'id': 6, 'project_id': 2, 'name': 'Write email marketing copy'},
            {'id': 7, 'project_id': 3, 'name': 'Define Q1 product features'},
            {'id': 8, 'project_id': 3, 'name': 'Research user feedback'},
            {'id': 9, 'project_id': 3, 'name': 'Prioritize backlog items'}
        ]
        
        # Create tag generator
        generator = TagGenerator(test_conn, mock_config, mock_org_config)
        
        # Generate and insert tags and associations
        tags, associations = generator.generate_and_insert_tags(
            organization_id=mock_organization_id,
            departments=mock_departments,
            tasks=mock_tasks,
            projects=mock_projects
        )
        
        print(f"\nGenerated Data Summary:")
        print(f"Tags: {len(tags)}")
        print(f"Task-Tag Associations: {len(associations)}")
        
        print("\nSample Tags:")
        for i, tag in enumerate(tags[:15], 1):
            print(f"  {i}. {tag['name']} ({tag['category']} - {tag['department']}) - Color: {tag['color']}")
        
        print("\nSample Task-Tag Associations:")
        # Create task name mapping
        task_names = {task['id']: task['name'] for task in mock_tasks}
        tag_names = {tag['id']: tag['name'] for tag in tags}
        
        for i, assoc in enumerate(associations[:15], 1):
            task_name = task_names.get(assoc['task_id'], f"Task {assoc['task_id']}")
            tag_name = tag_names.get(assoc['tag_id'], f"Tag {assoc['tag_id']}")
            print(f"  {i}. Task: '{task_name}' → Tag: '{tag_name}'")
        
        # Test statistics
        print(f"\nTags per department:")
        from collections import Counter
        dept_counts = Counter(tag['department'] for tag in tags)
        for dept, count in dept_counts.items():
            print(f"  {dept.title()}: {count} tags")
        
        print(f"\nTags per category:")
        category_counts = Counter(tag['category'] for tag in tags)
        for category, count in category_counts.items():
            print(f"  {category.title()}: {count} tags")
        
        print(f"\nTasks with tags:")
        task_with_tag_counts = Counter(assoc['task_id'] for assoc in associations)
        for task_id, count in task_with_tag_counts.items():
            task_name = task_names.get(task_id, f"Task {task_id}")
            print(f"  '{task_name}': {count} tags")
        
        print(f"\nMost used tags:")
        tag_usage_counts = Counter(assoc['tag_id'] for assoc in associations)
        for tag_id, count in tag_usage_counts.most_common(5):
            tag_name = tag_names.get(tag_id, f"Tag {tag_id}")
            print(f"  '{tag_name}': {count} tasks")
        
        print("\n✅ All tag generator tests completed successfully!")
    
    finally:
        generator.close()
        test_conn.close()