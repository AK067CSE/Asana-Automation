

"""
User generator module for creating realistic user data and team memberships.
This module generates realistic user records with proper relationships to teams,
organizations, and other entities based on scraped name data and enterprise patterns.

The generator is designed to be:
- Realistic: Creates users with believable demographics, roles, and relationships
- Scalable: Handles generation of thousands of users efficiently
- Configurable: Adapts to different company sizes and structures
- Consistent: Maintains referential integrity and realistic distributions
- Temporal-aware: Creates users with realistic hire dates and career progression
"""

import logging
import random
import time
from datetime import datetime, timedelta
from typing import List, Dict, Optional, Tuple, Set, Any
import sqlite3
import numpy as np

from src.utils.logging import get_logger
from src.scrapers.name_scraper import NameScraper
from src.models.organization import OrganizationConfig
from src.models.user import UserConfig, TeamConfig, TeamMembershipConfig

logger = get_logger(__name__)

class UserGenerator:
    """
    Generator for creating realistic user data and team memberships.
    
    This class handles the generation of:
    1. User records with realistic demographics and roles
    2. Team structures and hierarchies
    3. Team memberships with appropriate roles
    4. Temporal patterns for user creation and team assignments
    
    The generator uses scraped name data and enterprise patterns to ensure realism.
    """
    
    def __init__(self, db_conn: sqlite3.Connection, config: Dict[str, Any], org_config: OrganizationConfig):
        """
        Initialize the user generator.
        
        Args:
            db_conn: Database connection
            config: Application configuration
            org_config: Organization configuration
        """
        self.db_conn = db_conn
        self.config = config
        self.org_config = org_config
        self.name_scraper = NameScraper(cache_dir=config.get('cache_dir', 'data/cache'))
        
        # Role distributions by department (based on real enterprise data)
        self.role_distributions = {
            'engineering': {
                'Software Engineer': 0.35,
                'Senior Software Engineer': 0.25,
                'Engineering Manager': 0.15,
                'DevOps Engineer': 0.10,
                'QA Engineer': 0.08,
                'Data Engineer': 0.05,
                'Principal Engineer': 0.02
            },
            'product': {
                'Product Manager': 0.40,
                'Senior Product Manager': 0.25,
                'Product Designer': 0.20,
                'UX Designer': 0.10,
                'Product Analyst': 0.05
            },
            'marketing': {
                'Marketing Manager': 0.30,
                'Content Marketing Manager': 0.25,
                'Digital Marketing Specialist': 0.20,
                'Brand Manager': 0.15,
                'Growth Marketing Manager': 0.10
            },
            'sales': {
                'Account Executive': 0.35,
                'Sales Representative': 0.30,
                'Sales Manager': 0.20,
                'Sales Engineer': 0.10,
                'Customer Success Manager': 0.05
            },
            'operations': {
                'Operations Manager': 0.30,
                'Project Manager': 0.25,
                'Business Operations Manager': 0.20,
                'Administrative Assistant': 0.15,
                'Executive Assistant': 0.10
            },
            'executive': {
                'CEO': 0.05,
                'CTO': 0.05,
                'VP of Engineering': 0.10,
                'VP of Product': 0.10,
                'VP of Sales': 0.10,
                'VP of Marketing': 0.10,
                'Director of Engineering': 0.20,
                'Director of Product': 0.20,
                'Director of Operations': 0.10
            }
        }
        
        # Department distributions by company size
        self.department_distributions = {
            'small': {'engineering': 0.4, 'product': 0.2, 'marketing': 0.15, 'sales': 0.15, 'operations': 0.1},
            'medium': {'engineering': 0.35, 'product': 0.2, 'marketing': 0.15, 'sales': 0.2, 'operations': 0.1},
            'large': {'engineering': 0.3, 'product': 0.15, 'marketing': 0.15, 'sales': 0.25, 'operations': 0.1, 'executive': 0.05},
            'enterprise': {'engineering': 0.25, 'product': 0.15, 'marketing': 0.15, 'sales': 0.3, 'operations': 0.1, 'executive': 0.05}
        }
    
    def _get_department_distribution(self) -> Dict[str, float]:
        """Get department distribution based on company size."""
        company_size = 'enterprise' if self.org_config.size_max >= 5000 else 'large'
        return self.department_distributions.get(company_size, self.department_distributions['large'])
    
    def _generate_realistic_email(self, first_name: str, last_name: str, domain: str) -> str:
        """
        Generate a realistic email address based on name patterns.
        
        Args:
            first_name: User's first name
            last_name: User's last name
            domain: Company domain
            
        Returns:
            Realistic email address
        """
        patterns = [
            f"{first_name.lower()}.{last_name.lower()}@{domain}",
            f"{first_name[0].lower()}{last_name.lower()}@{domain}",
            f"{first_name.lower()}_{last_name.lower()}@{domain}",
            f"{last_name.lower()}{first_name[0].lower()}@{domain}"
        ]
        
        # Add some randomness but prefer the most common pattern
        weights = [0.6, 0.2, 0.1, 0.1]
        return random.choices(patterns, weights=weights)[0]
    
    def _get_realistic_hire_date(self, experience_level: str, company_start_date: datetime) -> datetime:
        """
        Generate a realistic hire date based on experience level.
        
        Args:
            experience_level: User's experience level (junior, mid, senior, executive)
            company_start_date: Company founding/start date
            
        Returns:
            Realistic hire date
        """
        now = datetime.now()
        company_age = (now - company_start_date).days
        
        if experience_level == 'executive':
            # Executives typically hired within first 2 years or recently for growth
            if random.random() < 0.7:  # 70% chance hired early
                max_days = min(730, company_age)
                days_back = random.randint(365, max(365, max_days)) if max_days >= 365 else random.randint(0, max_days)
            else:
                days_back = random.randint(0, 180)
        elif experience_level == 'senior':
            # Senior people hired 1-3 years ago
            max_days = min(1095, company_age)
            days_back = random.randint(365, max(365, max_days)) if max_days >= 365 else random.randint(0, max_days)
        elif experience_level == 'mid':
            # Mid-level hired 6 months to 2 years ago
            days_back = random.randint(180, min(730, company_age))
        else:  # junior
            # Junior people hired recently (0-6 months)
            days_back = random.randint(0, min(180, company_age))
        
        hire_date = now - timedelta(days=days_back)
        return max(hire_date, company_start_date)  # Ensure hire date is after company start
    
    def _determine_experience_level(self, role_title: str) -> str:
        """
        Determine experience level based on role title.
        
        Args:
            role_title: User's role title
            
        Returns:
            Experience level (junior, mid, senior, executive)
        """
        role_lower = role_title.lower()
        
        if any(title in role_lower for title in ['ceo', 'cto', 'cfo', 'coo', 'vp', 'director', 'head of']):
            return 'executive'
        elif any(title in role_lower for title in ['senior', 'principal', 'staff', 'lead', 'manager', 'architect']):
            return 'senior'
        elif any(title in role_lower for title in ['engineer', 'developer', 'specialist', 'analyst', 'designer']):
            return 'mid'
        else:
            return 'junior'
    
    def _get_department_from_role(self, role_title: str) -> str:
        """
        Determine department from role title.
        
        Args:
            role_title: User's role title
            
        Returns:
            Department name
        """
        role_lower = role_title.lower()
        
        if any(word in role_lower for word in ['engineer', 'developer', 'devops', 'data', 'qa', 'tech', 'architecture']):
            return 'engineering'
        elif any(word in role_lower for word in ['product', 'designer', 'ux', 'ui', 'researcher']):
            return 'product'
        elif any(word in role_lower for word in ['marketing', 'content', 'growth', 'brand', 'seo', 'social']):
            return 'marketing'
        elif any(word in role_lower for word in ['sales', 'account', 'business development', 'revenue', 'customer success']):
            return 'sales'
        elif any(word in role_lower for word in ['operations', 'project', 'program', 'admin', 'executive assistant', 'hr', 'finance']):
            return 'operations'
        else:
            return 'general'
    
    def _generate_user_demographics(self, department: str) -> Dict[str, Any]:
        """
        Generate realistic user demographics based on department.
        
        Args:
            department: User's department
            
        Returns:
            Dictionary with demographic information
        """
        # Location distribution by department (based on real tech company data)
        location_weights = {
            'engineering': {'San Francisco': 0.3, 'Seattle': 0.25, 'Austin': 0.2, 'New York': 0.15, 'Remote': 0.1},
            'product': {'San Francisco': 0.4, 'New York': 0.3, 'Seattle': 0.15, 'Austin': 0.1, 'Remote': 0.05},
            'marketing': {'New York': 0.35, 'San Francisco': 0.3, 'Chicago': 0.2, 'Los Angeles': 0.1, 'Remote': 0.05},
            'sales': {'New York': 0.4, 'San Francisco': 0.25, 'Chicago': 0.2, 'Boston': 0.1, 'Remote': 0.05},
            'operations': {'San Francisco': 0.3, 'New York': 0.3, 'Chicago': 0.2, 'Boston': 0.15, 'Remote': 0.05}
        }
        
        weights = location_weights.get(department, location_weights['engineering'])
        locations = list(weights.keys())
        location_weights_list = list(weights.values())
        
        location = random.choices(locations, weights=location_weights_list)[0]
        
        # Education distribution
        education_levels = {
            'executive': {'PhD': 0.1, 'MBA': 0.4, 'MS': 0.3, 'BS': 0.2},
            'senior': {'MS': 0.3, 'BS': 0.6, 'PhD': 0.1},
            'mid': {'BS': 0.7, 'MS': 0.25, 'Associate': 0.05},
            'junior': {'BS': 0.6, 'Associate': 0.3, 'MS': 0.1}
        }
        
        # Get experience level from department distribution
        exp_level = random.choices(
            ['executive', 'senior', 'mid', 'junior'],
            weights=[0.05, 0.25, 0.5, 0.2]
        )[0]
        
        edu_weights = education_levels[exp_level]
        education = random.choices(list(edu_weights.keys()), weights=list(edu_weights.values()))[0]
        
        return {
            'location': location,
            'education': education,
            'experience_level': exp_level
        }
    
    def generate_users_for_organization(self, organization_id: int, organization_name: str, 
                                      domain: str, target_user_count: int) -> List[Dict[str, Any]]:
        """
        Generate users for a specific organization.
        
        Args:
            organization_id: Organization ID
            organization_name: Organization name
            domain: Organization domain
            target_user_count: Target number of users to generate
            
        Returns:
            List of user dictionaries
        """
        logger.info(f"Generating {target_user_count} users for organization {organization_name}")
        
        # Get department distribution
        dept_distribution = self._get_department_distribution()
        departments = list(dept_distribution.keys())
        dept_weights = list(dept_distribution.values())
        
        # Get realistic names from scraper
        users_data = self.name_scraper.get_names(
            source="hybrid",
            count=target_user_count,
            company_size="enterprise" if target_user_count >= 5000 else "large",
            industry="b2b_saas"
        )
        
        users = []
        company_start_date = self.org_config.time_range.start_date
        
        for i, user_data in enumerate(users_data):
            if i >= target_user_count:
                break
            
            # Determine department and role
            department = random.choices(departments, weights=dept_weights)[0]
            role_distribution = self.role_distributions.get(department, self.role_distributions['engineering'])
            
            role_titles = list(role_distribution.keys())
            role_weights = list(role_distribution.values())
            role_title = random.choices(role_titles, weights=role_weights)[0]
            
            # Determine experience level
            experience_level = self._determine_experience_level(role_title)
            
            # Generate demographics
            demographics = self._generate_user_demographics(department)
            
            # Generate realistic email
            email = self._generate_realistic_email(user_data['first_name'], user_data['last_name'], domain)
            
            # Generate hire date
            hire_date = self._get_realistic_hire_date(experience_level, company_start_date)
            
            # Determine role level (admin, member, guest)
            if any(title in role_title.lower() for title in ['ceo', 'cto', 'director', 'vp', 'head', 'manager']):
                role = 'admin'
            elif any(title in role_title.lower() for title in ['intern', 'contractor', 'consultant']):
                role = 'guest'
            else:
                role = 'member'
            
            user = {
                'organization_id': organization_id,
                'name': user_data['full_name'],
                'email': email,
                'role': role,
                'location': demographics['location'],
                'department': department,
                'role_title': role_title,
                'experience_level': demographics['experience_level'],
                'education': demographics['education'],
                'hire_date': hire_date.strftime('%Y-%m-%d'),
                'created_at': hire_date.strftime('%Y-%m-%d %H:%M:%S'),
                'updated_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            }
            users.append(user)
        
        logger.info(f"Successfully generated {len(users)} users for organization {organization_name}")
        return users
    
    def generate_teams_for_organization(self, organization_id: int, organization_name: str, 
                                      users: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Generate teams for an organization based on users and company structure.
        
        Args:
            organization_id: Organization ID
            organization_name: Organization name
            users: List of user dictionaries for the organization
            
        Returns:
            List of team dictionaries
        """
        logger.info(f"Generating teams for organization {organization_name}")
        
        # Get team count based on organization size
        min_teams, max_teams = self.org_config.num_teams_range
        num_teams = random.randint(min_teams, max_teams)
        
        teams = []
        used_team_names = set()
        
        # Create team name patterns based on departments
        team_name_patterns = {
            'engineering': [
                '{prefix} Engineering', 'Engineering {suffix}', '{product} Team',
                '{domain} Platform', 'Core {domain}', '{feature} Squad'
            ],
            'product': [
                '{prefix} Product', 'Product {suffix}', '{product} Team',
                'UX/UI Team', 'Product Design', 'Research Team'
            ],
            'marketing': [
                '{prefix} Marketing', 'Marketing {suffix}', 'Growth Team',
                'Content Team', 'Brand Team', 'Digital Marketing'
            ],
            'sales': [
                '{prefix} Sales', 'Sales {suffix}', 'Revenue Team',
                'Enterprise Sales', 'SMB Sales', 'Customer Success'
            ],
            'operations': [
                '{prefix} Operations', 'Operations {suffix}', 'BizOps Team',
                'Finance Team', 'HR Team', 'Admin Team'
            ]
        }
        
        prefixes = ['Alpha', 'Beta', 'Gamma', 'Delta', 'Sigma', 'Omega', 'Apex', 'Vertex', 'Nexus', 'Horizon']
        suffixes = ['Team', 'Group', 'Squad', 'Tribe', 'Collective', 'Crew', 'Unit']
        products = ['Platform', 'Enterprise', 'Cloud', 'Mobile', 'Data', 'AI', 'Security', 'Core']
        domains = ['Backend', 'Frontend', 'Mobile', 'Data', 'Infrastructure', 'Security']
        features = ['Search', 'Recommendation', 'Analytics', 'Billing', 'Auth', 'Notification']
        
        for i in range(num_teams):
            # Determine team department based on user distribution
            dept_users = [u for u in users if u['department'] == list(self._get_department_distribution().keys())[i % len(self._get_department_distribution().keys())]]
            if not dept_users:
                department = random.choice(list(self._get_department_distribution().keys()))
            else:
                department = dept_users[0]['department']
            
            # Generate team name
            patterns = team_name_patterns.get(department, team_name_patterns['engineering'])
            pattern = random.choice(patterns)
            
            team_name = pattern.format(
                prefix=random.choice(prefixes),
                suffix=random.choice(suffixes),
                product=random.choice(products),
                domain=random.choice(domains),
                feature=random.choice(features)
            )
            
            # Ensure unique team names
            counter = 1
            base_name = team_name
            while team_name in used_team_names:
                team_name = f"{base_name} {counter}"
                counter += 1
            used_team_names.add(team_name)
            
            # Get department head or senior person as team lead
            dept_users = [u for u in users if u['department'] == department]
            if dept_users:
                team_lead = random.choice([u for u in dept_users if u['role'] == 'admin'] or dept_users[:5] or dept_users)
            else:
                team_lead = users[0]
            
            team = {
                'organization_id': organization_id,
                'name': team_name,
                'description': f"{team_name} responsible for {department} functions at {organization_name}",
                'department': department,
                'team_lead_id': None,  # Will be set after user insertion
                'created_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                'updated_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            }
            teams.append(team)
        
        logger.info(f"Successfully generated {len(teams)} teams for organization {organization_name}")
        return teams
    
    def generate_team_memberships(self, teams: List[Dict[str, Any]], 
                                users: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Generate team memberships for users and teams.
        
        Args:
            teams: List of team dictionaries
            users: List of user dictionaries
            
        Returns:
            List of team membership dictionaries
        """
        logger.info(f"Generating team memberships for {len(teams)} teams and {len(users)} users")
        
        memberships = []
        user_team_assignments = {user['email']: [] for user in users}
        
        # First, assign team leads
        for team in teams:
            # Find users in the same department as the team
            dept_users = [u for u in users if u['department'] == team['department']]
            if dept_users:
                # Prefer admins or senior roles for team leads
                potential_leads = [u for u in dept_users if u['role'] == 'admin'] or \
                                [u for u in dept_users if 'manager' in u['role_title'].lower()] or \
                                dept_users[:10]
                team_lead = random.choice(potential_leads)
                team['team_lead_id'] = team_lead['email']  # Store email temporarily
        
        # Assign users to teams based on department and realistic team sizes
        for team in teams:
            dept_users = [u for u in users if u['department'] == team['department']]
            if not dept_users:
                dept_users = users
            
            # Determine team size based on company size and department
            min_users, max_users = self.org_config.num_users_per_team_range
            team_size = random.randint(min_users, max_users)
            
            # Ensure team lead is included
            team_lead = next((u for u in dept_users if u['email'] == team['team_lead_id']), None)
            if team_lead:
                team_members = [team_lead]
                remaining_users = [u for u in dept_users if u != team_lead]
            else:
                team_members = []
                remaining_users = dept_users
            
            # Add remaining team members
            remaining_spots = max(0, team_size - len(team_members))
            if remaining_users and remaining_spots > 0:
                additional_members = random.sample(remaining_users, min(remaining_spots, len(remaining_users)))
                team_members.extend(additional_members)
            
            # Create memberships
            for i, user in enumerate(team_members):
                is_team_lead = user['email'] == team['team_lead_id']
                role = 'owner' if is_team_lead else 'member'
                
                membership = {
                    'team_id': team['name'],  # Use name temporarily, will be replaced with ID
                    'user_id': user['email'],  # Use email temporarily, will be replaced with ID
                    'role': role,
                    'created_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                    'updated_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                }
                memberships.append(membership)
                
                # Track assignments
                user_team_assignments[user['email']].append(team['name'])
        
        # Ensure all users are assigned to at least one team
        unassigned_users = [u for u, teams in user_team_assignments.items() if not teams]
        if unassigned_users:
            logger.warning(f"{len(unassigned_users)} users not assigned to any team. Assigning to random teams.")
            for user_email in unassigned_users:
                random_team = random.choice(teams)
                membership = {
                    'team_id': random_team['name'],
                    'user_id': user_email,
                    'role': 'member',
                    'created_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                    'updated_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                }
                memberships.append(membership)
        
        logger.info(f"Successfully generated {len(memberships)} team memberships")
        return memberships
    
    def insert_users(self, users: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Insert users into the database and return users with IDs.
        
        Args:
            users: List of user dictionaries
            
        Returns:
            List of user dictionaries with database IDs
        """
        cursor = self.db_conn.cursor()
        inserted_users = []
        
        for user in users:
            try:
                cursor.execute("""
                    INSERT INTO users (
                        organization_id, name, email, role, 
                        created_at, updated_at
                    ) VALUES (?, ?, ?, ?, ?, ?)
                """, (
                    user['organization_id'],
                    user['name'],
                    user['email'],
                    user['role'],
                    user['created_at'],
                    user['updated_at']
                ))
                
                user_id = cursor.lastrowid
                user_with_id = user.copy()
                user_with_id['id'] = user_id
                inserted_users.append(user_with_id)
                
            except sqlite3.IntegrityError as e:
                if 'UNIQUE constraint failed: users.email' in str(e):
                    logger.warning(f"Duplicate email found: {user['email']}. Skipping.")
                else:
                    logger.error(f"Error inserting user {user['email']}: {str(e)}")
                    raise
        
        self.db_conn.commit()
        logger.info(f"Successfully inserted {len(inserted_users)} users into database")
        return inserted_users
    
    def insert_teams(self, teams: List[Dict[str, Any]], users: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Insert teams into the database and return teams with IDs.
        
        Args:
            teams: List of team dictionaries
            users: List of user dictionaries (to resolve team lead IDs)
            
        Returns:
            List of team dictionaries with database IDs
        """
        cursor = self.db_conn.cursor()
        inserted_teams = []
        
        # Create a map of user emails to IDs
        user_email_to_id = {user['email']: user['id'] for user in users}
        
        for team in teams:
            try:
                # Resolve team lead ID
                team_lead_id = None
                if team['team_lead_id']:
                    team_lead_id = user_email_to_id.get(team['team_lead_id'])
                
                cursor.execute("""
                    INSERT INTO teams (
                        organization_id, name, description, 
                        created_at, updated_at
                    ) VALUES (?, ?, ?, ?, ?)
                """, (
                    team['organization_id'],
                    team['name'],
                    team['description'],
                    team['created_at'],
                    team['updated_at']
                ))
                
                team_id = cursor.lastrowid
                
                # Update team lead in teams table if exists
                if team_lead_id:
                    cursor.execute("""
                        UPDATE teams 
                        SET team_lead_id = ? 
                        WHERE id = ?
                    """, (team_lead_id, team_id))
                
                team_with_id = team.copy()
                team_with_id['id'] = team_id
                inserted_teams.append(team_with_id)
                
            except sqlite3.Error as e:
                logger.error(f"Error inserting team {team['name']}: {str(e)}")
                raise
        
        self.db_conn.commit()
        logger.info(f"Successfully inserted {len(inserted_teams)} teams into database")
        return inserted_teams
    
    def insert_team_memberships(self, memberships: List[Dict[str, Any]], 
                              teams: List[Dict[str, Any]], users: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Insert team memberships into the database.
        
        Args:
            memberships: List of membership dictionaries
            teams: List of team dictionaries with IDs
            users: List of user dictionaries with IDs
            
        Returns:
            List of inserted membership dictionaries
        """
        cursor = self.db_conn.cursor()
        inserted_memberships = []
        
        # Create maps for team names to IDs and user emails to IDs
        team_name_to_id = {team['name']: team['id'] for team in teams}
        user_email_to_id = {user['email']: user['id'] for user in users}
        
        for membership in memberships:
            try:
                team_id = team_name_to_id.get(membership['team_id'])
                user_id = user_email_to_id.get(membership['user_id'])
                
                if not team_id or not user_id:
                    logger.warning(f"Could not find team or user for membership: {membership}")
                    continue
                
                cursor.execute("""
                    INSERT INTO team_memberships (
                        team_id, user_id, role, 
                        created_at, updated_at
                    ) VALUES (?, ?, ?, ?, ?)
                """, (
                    team_id,
                    user_id,
                    membership['role'],
                    membership['created_at'],
                    membership['updated_at']
                ))
                
                membership_id = cursor.lastrowid
                membership_with_id = membership.copy()
                membership_with_id['id'] = membership_id
                membership_with_id['team_id'] = team_id
                membership_with_id['user_id'] = user_id
                inserted_memberships.append(membership_with_id)
                
            except sqlite3.IntegrityError as e:
                if 'UNIQUE constraint failed: team_memberships.team_id, team_memberships.user_id' in str(e):
                    logger.warning(f"Duplicate membership for team {membership['team_id']} and user {membership['user_id']}. Skipping.")
                else:
                    logger.error(f"Error inserting membership: {str(e)}")
                    raise
            except sqlite3.Error as e:
                logger.error(f"Error inserting membership: {str(e)}")
                raise
        
        self.db_conn.commit()
        logger.info(f"Successfully inserted {len(inserted_memberships)} team memberships into database")
        return inserted_memberships
    
    def generate_organizations(self, num_organizations: int) -> List[Dict[str, Any]]:
        """
        Generate organizations and their users, teams, and memberships.
        
        Args:
            num_organizations: Number of organizations to generate
            
        Returns:
            List of organization dictionaries with all related data
        """
        logger.info(f"Generating {num_organizations} organizations")
        
        cursor = self.db_conn.cursor()
        organizations = []
        
        for i in range(num_organizations):
            # Generate organization name and domain
            org_name = f"Acme Corporation {i + 1}" if num_organizations > 1 else "Acme Corporation"
            domain = f"acme{i + 1}.com" if num_organizations > 1 else "acme.corp"
            
            try:
                cursor.execute("""
                    INSERT INTO organizations (name, domain, created_at, updated_at)
                    VALUES (?, ?, ?, ?)
                """, (
                    org_name,
                    domain,
                    datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                    datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                ))
                
                org_id = cursor.lastrowid
                
                # Generate users for this organization
                target_user_count = random.randint(self.org_config.size_min, self.org_config.size_max)
                users = self.generate_users_for_organization(org_id, org_name, domain, target_user_count)
                
                # Insert users and get IDs
                inserted_users = self.insert_users(users)
                
                # Generate teams
                teams = self.generate_teams_for_organization(org_id, org_name, inserted_users)
                
                # Insert teams and get IDs
                inserted_teams = self.insert_teams(teams, inserted_users)
                
                # Generate and insert team memberships
                memberships = self.generate_team_memberships(inserted_teams, inserted_users)
                inserted_memberships = self.insert_team_memberships(memberships, inserted_teams, inserted_users)
                
                organization = {
                    'id': org_id,
                    'name': org_name,
                    'domain': domain,
                    'users': inserted_users,
                    'teams': inserted_teams,
                    'memberships': inserted_memberships
                }
                organizations.append(organization)
                
                logger.info(f"Successfully generated organization {org_name} with {len(inserted_users)} users, "
                          f"{len(inserted_teams)} teams, and {len(inserted_memberships)} memberships")
                
            except sqlite3.Error as e:
                logger.error(f"Error generating organization {org_name}: {str(e)}")
                raise
        
        self.db_conn.commit()
        logger.info(f"Successfully generated {len(organizations)} organizations")
        return organizations
    
    def close(self):
        """Close the name scraper session."""
        self.name_scraper.close()
        logger.info("User generator closed")

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
        CREATE TABLE organizations (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            domain TEXT NOT NULL UNIQUE,
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
            updated_at TIMESTAMP NOT NULL,
            FOREIGN KEY (organization_id) REFERENCES organizations(id)
        )
    """)
    
    cursor.execute("""
        CREATE TABLE teams (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            organization_id INTEGER NOT NULL,
            name TEXT NOT NULL,
            description TEXT,
            created_at TIMESTAMP NOT NULL,
            updated_at TIMESTAMP NOT NULL,
            FOREIGN KEY (organization_id) REFERENCES organizations(id)
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
            UNIQUE(team_id, user_id),
            FOREIGN KEY (team_id) REFERENCES teams(id),
            FOREIGN KEY (user_id) REFERENCES users(id)
        )
    """)
    
    test_conn.commit()
    
    try:
        # Create user generator
        generator = UserGenerator(test_conn, mock_config, mock_org_config)
        
        # Generate one organization
        organizations = generator.generate_organizations(1)
        
        org = organizations[0]
        print(f"\nGenerated Organization: {org['name']}")
        print(f"Domain: {org['domain']}")
        print(f"Total Users: {len(org['users'])}")
        print(f"Total Teams: {len(org['teams'])}")
        print(f"Total Memberships: {len(org['memberships'])}")
        
        print("\nUsers:")
        for user in org['users'][:5]:  # Show first 5 users
            print(f"  - {user['name']} ({user['email']}) - {user['role_title']} in {user['department']}")
        
        print("\nTeams:")
        for team in org['teams']:
            print(f"  - {team['name']} ({team['department']} department)")
        
        print("\nTeam Memberships:")
        for i, membership in enumerate(org['memberships'][:10]):  # Show first 10 memberships
            user = next((u for u in org['users'] if u['id'] == membership['user_id']), None)
            team = next((t for t in org['teams'] if t['id'] == membership['team_id']), None)
            if user and team:
                print(f"  - {user['name']} is {membership['role']} of {team['name']}")
        
    finally:
        generator.close()
        test_conn.close()