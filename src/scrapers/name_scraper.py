

"""
Name scraper module for fetching realistic user names and demographics.
This module provides functionality to scrape or generate realistic user names
from various sources including census data, public datasets, and demographic patterns.

The scraper is designed to be:
- Ethical: Uses public datasets and respects privacy
- Realistic: Generates names that match enterprise workforce demographics
- Diverse: Includes various ethnicities, genders, and age groups
- Configurable: Can be adjusted for different company cultures and regions
- Cachable: Stores results to avoid repeated data fetching
"""

import logging
import random
import time
import json
import csv
from typing import List, Dict, Optional, Tuple, Set
from pathlib import Path
import requests
from fake_useragent import UserAgent

from src.utils.logging import get_logger

logger = get_logger(__name__)

class NameScraper:
    """
    Scraper for realistic user names and demographic data.
    
    This class handles fetching name data from multiple sources:
    1. US Census data (public datasets)
    2. International name databases
    3. Predefined enterprise name patterns
    4. Synthetic generation for diversity
    
    The scraper respects ethical data collection practices and privacy regulations.
    """
    
    def __init__(self, cache_dir: str = "data/cache", max_retries: int = 3):
        """
        Initialize the name scraper.
        
        Args:
            cache_dir: Directory to cache scraped results
            max_retries: Maximum number of retries for failed requests
        """
        self.cache_dir = Path(cache_dir)
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        self.max_retries = max_retries
        self.user_agent = UserAgent()
        self.session = requests.Session()
        
        # Predefined name patterns for enterprise environments
        self.first_names = {
            'male': ['James', 'John', 'Robert', 'Michael', 'William', 'David', 'Richard', 'Joseph', 'Thomas', 'Charles',
                    'Christopher', 'Daniel', 'Matthew', 'Anthony', 'Donald', 'Mark', 'Paul', 'Steven', 'Andrew', 'Kenneth',
                    'Joshua', 'Kevin', 'Brian', 'George', 'Timothy', 'Ronald', 'Edward', 'Jason', 'Jeffrey', 'Ryan',
                    'Jacob', 'Gary', 'Nicholas', 'Eric', 'Jonathan', 'Stephen', 'Larry', 'Justin', 'Scott', 'Brandon',
                    'Benjamin', 'Samuel', 'Frank', 'Raymond', 'Patrick', 'Alexander', 'Jack', 'Dennis', 'Jerry', 'Tyler'],
            
            'female': ['Mary', 'Patricia', 'Jennifer', 'Linda', 'Elizabeth', 'Barbara', 'Susan', 'Jessica', 'Sarah', 'Karen',
                      'Lisa', 'Nancy', 'Betty', 'Margaret', 'Sandra', 'Ashley', 'Kimberly', 'Emily', 'Donna', 'Michelle',
                      'Carol', 'Amanda', 'Dorothy', 'Melissa', 'Deborah', 'Stephanie', 'Rebecca', 'Sharon', 'Laura', 'Cynthia',
                      'Kathleen', 'Amy', 'Shirley', 'Angela', 'Helen', 'Anna', 'Brenda', 'Pamela', 'Nicole', 'Ruth',
                      'Katherine', 'Samantha', 'Christine', 'Emma', 'Catherine', 'Virginia', 'Debra', 'Rachel', 'Carolyn', 'Janet'],
            
            'neutral': ['Alex', 'Jordan', 'Taylor', 'Casey', 'Riley', 'Jamie', 'Morgan', 'Avery', 'Quinn', 'Skyler',
                       'Dakota', 'Finley', 'Rowan', 'Ellis', 'Sage', 'Reese', 'Parker', 'Hayden', 'Phoenix', 'River']
        }
        
        self.last_names = ['Smith', 'Johnson', 'Williams', 'Brown', 'Jones', 'Garcia', 'Miller', 'Davis', 'Rodriguez', 'Martinez',
                          'Hernandez', 'Lopez', 'Gonzalez', 'Wilson', 'Anderson', 'Thomas', 'Taylor', 'Moore', 'Jackson', 'Martin',
                          'Lee', 'Perez', 'Thompson', 'White', 'Harris', 'Sanchez', 'Clark', 'Ramirez', 'Lewis', 'Robinson',
                          'Walker', 'Young', 'Allen', 'King', 'Wright', 'Scott', 'Torres', 'Nguyen', 'Hill', 'Flores',
                          'Green', 'Adams', 'Nelson', 'Baker', 'Hall', 'Rivera', 'Campbell', 'Mitchell', 'Carter', 'Roberts']
        
        # Enterprise role patterns
        self.role_titles = {
            'engineering': ['Software Engineer', 'Senior Engineer', 'Engineering Manager', 'DevOps Engineer', 
                          'QA Engineer', 'Data Engineer', 'Frontend Developer', 'Backend Developer', 'Full Stack Engineer',
                          'Principal Engineer', 'Staff Engineer', 'Engineering Director', 'VP of Engineering', 'CTO'],
            
            'product': ['Product Manager', 'Senior Product Manager', 'Product Owner', 'Product Designer', 
                       'UX Designer', 'UI Designer', 'Product Analyst', 'Director of Product', 'VP of Product', 'Chief Product Officer'],
            
            'sales': ['Sales Representative', 'Account Executive', 'Sales Manager', 'Sales Director', 
                     'VP of Sales', 'Chief Revenue Officer', 'Business Development Manager', 'Sales Engineer',
                     'Customer Success Manager', 'Account Manager'],
            
            'marketing': ['Marketing Manager', 'Content Marketing Manager', 'Digital Marketing Specialist',
                         'Growth Marketing Manager', 'Brand Manager', 'Marketing Director', 'VP of Marketing', 'CMO'],
            
            'operations': ['Operations Manager', 'Project Manager', 'Program Manager', 'Business Operations Manager',
                          'Chief Operating Officer', 'Administrative Assistant', 'Executive Assistant', 'Office Manager'],
            
            'executive': ['CEO', 'CFO', 'COO', 'CTO', 'VP', 'Director', 'Head of Department', 'Chief Strategy Officer']
        }
    
    def _get_headers(self) -> Dict[str, str]:
        """Get random headers to avoid being blocked."""
        return {
            'User-Agent': self.user_agent.random,
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.5',
            'Accept-Encoding': 'gzip, deflate',
            'Connection': 'keep-alive',
            'DNT': '1',
            'Upgrade-Insecure-Requests': '1'
        }
    
    def _make_request(self, url: str, max_retries: Optional[int] = None) -> Optional[requests.Response]:
        """
        Make a request with retries and error handling.
        
        Args:
            url: URL to request
            max_retries: Override default max retries
        
        Returns:
            Response object or None if failed
        """
        retries = max_retries or self.max_retries
        delay = 1
        
        for attempt in range(retries):
            try:
                headers = self._get_headers()
                response = self.session.get(url, headers=headers, timeout=10)
                
                if response.status_code == 200:
                    return response
                
                if response.status_code == 429:
                    logger.warning(f"Rate limited on attempt {attempt + 1}. Waiting {delay} seconds...")
                    time.sleep(delay)
                    delay *= 2
                    continue
                
                if response.status_code >= 400:
                    logger.warning(f"Request failed with status {response.status_code}: {url}")
                    continue
                
            except requests.exceptions.RequestException as e:
                logger.warning(f"Request failed on attempt {attempt + 1}: {str(e)}")
                time.sleep(delay)
                delay *= 2
        
        logger.error(f"Failed to get response after {retries} attempts: {url}")
        return None
    
    def _cache_key(self, source: str, params: Dict) -> str:
        """Generate a cache key for a request."""
        import hashlib
        key_str = f"{source}_{json.dumps(params, sort_keys=True)}"
        return hashlib.md5(key_str.encode()).hexdigest()
    
    def _get_cached_data(self, cache_key: str) -> Optional[List[Dict]]:
        """Get cached data if available."""
        cache_file = self.cache_dir / f"{cache_key}.json"
        if cache_file.exists():
            try:
                with open(cache_file, 'r') as f:
                    return json.load(f)
            except Exception as e:
                logger.warning(f"Failed to load cached data for {cache_key}: {str(e)}")
        return None
    
    def _cache_data(self, cache_key: str, data: List[Dict]):
        """Cache data to file."""
        cache_file = self.cache_dir / f"{cache_key}.json"
        try:
            with open(cache_file, 'w') as f:
                json.dump(data, f, indent=2)
        except Exception as e:
            logger.warning(f"Failed to cache data for {cache_key}: {str(e)}")
    
    def scrape_us_census_names(self, limit: int = 1000) -> List[Dict]:
        """
        Scrape US Census name data (mock implementation).
        
        Note: This uses mock data since actual census data requires proper licensing.
        In production, this would use official census datasets with proper attribution.
        
        Args:
            limit: Maximum number of names to return
        
        Returns:
            List of name dictionaries with demographics
        """
        logger.info("Scraping US Census names (mock implementation)")
        
        # Mock census data - in reality, this would use official datasets
        census_data = []
        
        for i in range(limit):
            gender = random.choice(['male', 'female', 'neutral'])
            first_name = random.choice(self.first_names[gender])
            last_name = random.choice(self.last_names)
            
            # Add demographic diversity
            ethnicity = random.choice(['caucasian', 'african_american', 'hispanic', 'asian', 'other'])
            age_range = random.choice(['20-30', '30-40', '40-50', '50-60', '60+'])
            
            census_data.append({
                "first_name": first_name,
                "last_name": last_name,
                "gender": gender,
                "ethnicity": ethnicity,
                "age_range": age_range,
                "frequency": random.randint(1000, 100000)  # Mock frequency count
            })
        
        return census_data[:limit]
    
    def scrape_international_names(self, countries: List[str] = ['uk', 'canada', 'australia', 'germany', 'france'], 
                                 limit_per_country: int = 100) -> List[Dict]:
        """
        Scrape international name data (mock implementation).
        
        Args:
            countries: List of countries to include
            limit_per_country: Number of names per country
        
        Returns:
            List of international name dictionaries
        """
        logger.info(f"Scraping international names for countries: {countries}")
        
        international_data = []
        
        # Mock international names
        international_names = {
            'uk': {'first': ['Oliver', 'George', 'Harry', 'Noah', 'Jack', 'Amelia', 'Olivia', 'Isla', 'Ava', 'Emily'],
                  'last': ['Smith', 'Jones', 'Williams', 'Taylor', 'Brown', 'Davies', 'Evans', 'Wilson', 'Thomas', 'Johnson']},
            
            'canada': {'first': ['Liam', 'Noah', 'Oliver', 'William', 'Elijah', 'Emma', 'Olivia', 'Ava', 'Charlotte', 'Sophia'],
                     'last': ['Smith', 'Johnson', 'Williams', 'Brown', 'Jones', 'Miller', 'Davis', 'Garcia', 'Wilson', 'Moore']},
            
            'australia': {'first': ['Oliver', 'Noah', 'William', 'Jack', 'Leo', 'Charlotte', 'Olivia', 'Amelia', 'Isla', 'Ava'],
                        'last': ['Smith', 'Jones', 'Williams', 'Brown', 'Wilson', 'Taylor', 'Johnson', 'White', 'Martin', 'Anderson']},
            
            'germany': {'first': ['Ben', 'Paul', 'Leon', 'Finn', 'Luis', 'Emma', 'Mia', 'Hanna', 'Lea', 'Sophie'],
                      'last': ['Müller', 'Schmidt', 'Schneider', 'Fischer', 'Weber', 'Meyer', 'Wagner', 'Becker', 'Schulz', 'Hoffmann']},
            
            'france': {'first': ['Gabriel', 'Léo', 'Raphaël', 'Louis', 'Maël', 'Jade', 'Louise', 'Emma', 'Alice', 'Chloé'],
                     'last': ['Martin', 'Bernard', 'Dubois', 'Thomas', 'Robert', 'Richard', 'Petit', 'Durand', 'Leroy', 'Moreau']}
        }
        
        for country in countries:
            if country not in international_names:
                logger.warning(f"Country {country} not in mock data. Using default patterns.")
                continue
            
            names = international_names[country]
            for _ in range(limit_per_country):
                first_name = random.choice(names['first'])
                last_name = random.choice(names['last'])
                
                international_data.append({
                    "first_name": first_name,
                    "last_name": last_name,
                    "country": country,
                    "region": random.choice(['urban', 'suburban', 'rural']),
                    "language": country if country in ['germany', 'france'] else 'english'
                })
        
        return international_data
    
    def generate_enterprise_names(self, count: int, company_size: str = "large", 
                                industry: str = "b2b_saas") -> List[Dict]:
        """
        Generate realistic enterprise user names with professional context.
        
        Args:
            count: Number of names to generate
            company_size: Company size category ('small', 'medium', 'large', 'enterprise')
            industry: Industry type for role patterns
        
        Returns:
            List of user dictionaries with professional details
        """
        logger.info(f"Generating {count} enterprise names for {company_size} {industry} company")
        
        users = []
        used_emails = set()
        
        # Determine role distribution based on company size
        role_distribution = self._get_role_distribution(company_size, industry)
        
        for _ in range(count):
            # Select role based on distribution
            role_category = random.choices(
                list(role_distribution.keys()),
                weights=list(role_distribution.values())
            )[0]
            
            role_title = random.choice(self.role_titles.get(role_category, self.role_titles['engineering']))
            
            # Generate name with demographic diversity
            gender = random.choices(['male', 'female', 'neutral'], weights=[0.48, 0.48, 0.04])[0]
            first_name = random.choice(self.first_names[gender])
            last_name = random.choice(self.last_names)
            
            # Generate professional email
            email_pattern = random.choice([
                f"{first_name.lower()}.{last_name.lower()}",
                f"{first_name[0].lower()}{last_name.lower()}",
                f"{first_name.lower()}_{last_name.lower()}",
                f"{last_name.lower()}{first_name[0].lower()}"
            ])
            
            domain = random.choice(['company.com', 'enterprise.com', 'techcorp.com', 'globex.com', 'acme.corp'])
            email = f"{email_pattern}@{domain}"
            
            # Ensure unique emails
            counter = 1
            base_email = email.split('@')[0]
            while email in used_emails:
                email = f"{base_email}{counter}@{domain}"
                counter += 1
            
            used_emails.add(email)
            
            # Generate realistic demographics
            experience_level = self._get_experience_level(role_title)
            department = self._get_department_from_role(role_title)
            
            users.append({
                "first_name": first_name,
                "last_name": last_name,
                "full_name": f"{first_name} {last_name}",
                "email": email,
                "role_title": role_title,
                "gender": gender,
                "department": department,
                "experience_level": experience_level,
                "location": random.choice(['San Francisco', 'New York', 'Austin', 'Seattle', 'Boston', 'Remote']),
                "hire_date": self._generate_realistic_hire_date(experience_level),
                "education": random.choice(['BS', 'MS', 'PhD', 'MBA']) + ' in ' + random.choice(['Computer Science', 'Business', 'Engineering', 'Mathematics', 'Design'])
            })
        
        return users
    
    def _get_role_distribution(self, company_size: str, industry: str) -> Dict[str, float]:
        """Get realistic role distribution based on company size and industry."""
        distributions = {
            'small': {'engineering': 0.4, 'product': 0.2, 'sales': 0.2, 'marketing': 0.1, 'operations': 0.1},
            'medium': {'engineering': 0.35, 'product': 0.2, 'sales': 0.2, 'marketing': 0.1, 'operations': 0.1, 'executive': 0.05},
            'large': {'engineering': 0.3, 'product': 0.15, 'sales': 0.25, 'marketing': 0.15, 'operations': 0.1, 'executive': 0.05},
            'enterprise': {'engineering': 0.25, 'product': 0.15, 'sales': 0.3, 'marketing': 0.15, 'operations': 0.1, 'executive': 0.05}
        }
        
        return distributions.get(company_size, distributions['large'])
    
    def _get_experience_level(self, role_title: str) -> str:
        """Determine experience level based on role title."""
        if any(title in role_title.lower() for title in ['senior', 'principal', 'staff', 'director', 'vp', 'chief', 'head']):
            levels = ['senior', 'expert']
            weights = [0.7, 0.3]
        elif any(title in role_title.lower() for title in ['manager', 'lead']):
            levels = ['mid', 'senior']
            weights = [0.3, 0.7]
        else:
            levels = ['junior', 'mid']
            weights = [0.4, 0.6]
        
        return random.choices(levels, weights=weights)[0]
    
    def _get_department_from_role(self, role_title: str) -> str:
        """Determine department from role title."""
        role_lower = role_title.lower()
        
        if any(word in role_lower for word in ['engineer', 'developer', 'devops', 'data', 'qa', 'tech']):
            return 'Engineering'
        elif any(word in role_lower for word in ['product', 'designer', 'ux', 'ui']):
            return 'Product'
        elif any(word in role_lower for word in ['sales', 'account', 'business development', 'revenue']):
            return 'Sales'
        elif any(word in role_lower for word in ['marketing', 'content', 'growth', 'brand']):
            return 'Marketing'
        elif any(word in role_lower for word in ['operations', 'project', 'program', 'admin', 'executive assistant']):
            return 'Operations'
        elif any(word in role_lower for word in ['executive', 'director', 'vp', 'chief', 'president']):
            return 'Executive'
        else:
            return 'General'
    
    def _generate_realistic_hire_date(self, experience_level: str) -> str:
        """Generate realistic hire date based on experience level."""
        from datetime import datetime, timedelta
        
        now = datetime.now()
        
        if experience_level == 'junior':
            # 0-2 years experience
            days_back = random.randint(0, 730)
        elif experience_level == 'mid':
            # 2-5 years experience
            days_back = random.randint(730, 1825)
        elif experience_level == 'senior':
            # 5-10 years experience
            days_back = random.randint(1825, 3650)
        else:  # expert
            # 10+ years experience
            days_back = random.randint(3650, 7300)
        
        hire_date = now - timedelta(days=days_back)
        return hire_date.strftime('%Y-%m-%d')
    
    def get_names(self, source: str = "hybrid", count: int = 100, company_size: str = "large", 
                 industry: str = "b2b_saas") -> List[Dict]:
        """
        Get names from specified source.
        
        Args:
            source: Source to use ('census', 'international', 'synthetic', 'hybrid')
            count: Number of names to retrieve
            company_size: Company size for role distribution
            industry: Industry type for role patterns
        
        Returns:
            List of user dictionaries
        """
        logger.info(f"Getting {count} names from {source} source for {company_size} {industry} company")
        
        cache_key = self._cache_key("names", {
            "source": source,
            "count": count,
            "company_size": company_size,
            "industry": industry
        })
        
        cached_data = self._get_cached_data(cache_key)
        if cached_data:
            logger.info("Using cached name data")
            return cached_data[:count]
        
        users = []
        
        try:
            if source == "census":
                census_data = self.scrape_us_census_names(count)
                users = self._convert_census_to_users(census_data, company_size, industry)
            elif source == "international":
                international_data = self.scrape_international_names(limit_per_country=count//5)
                users = self._convert_international_to_users(international_data, company_size, industry)
            elif source == "synthetic":
                users = self.generate_enterprise_names(count, company_size, industry)
            elif source == "hybrid":
                # Mix of different sources
                synthetic_count = int(count * 0.7)  # 70% synthetic
                census_count = int(count * 0.2)      # 20% census
                intl_count = count - synthetic_count - census_count  # 10% international
                
                synthetic_users = self.generate_enterprise_names(synthetic_count, company_size, industry)
                census_users = self._convert_census_to_users(
                    self.scrape_us_census_names(census_count), 
                    company_size, industry
                )
                intl_users = self._convert_international_to_users(
                    self.scrape_international_names(limit_per_country=intl_count//5),
                    company_size, industry
                )
                
                users = synthetic_users + census_users + intl_users
                random.shuffle(users)
            else:
                logger.warning(f"Unknown source: {source}. Using synthetic generation.")
                users = self.generate_enterprise_names(count, company_size, industry)
        
        except Exception as e:
            logger.error(f"Error getting names: {str(e)}")
            logger.info("Falling back to synthetic generation")
            users = self.generate_enterprise_names(count, company_size, industry)
        
        # Ensure we have exactly count users
        users = users[:count]
        
        # Cache the results
        self._cache_data(cache_key, users)
        
        logger.info(f"Successfully retrieved {len(users)} names")
        return users
    
    def _convert_census_to_users(self, census_data: List[Dict], company_size: str, industry: str) -> List[Dict]:
        """Convert census data to enterprise user format."""
        users = []
        
        for data in census_data:
            # Get role distribution
            role_distribution = self._get_role_distribution(company_size, industry)
            role_category = random.choices(
                list(role_distribution.keys()),
                weights=list(role_distribution.values())
            )[0]
            
            role_title = random.choice(self.role_titles.get(role_category, self.role_titles['engineering']))
            
            # Generate professional details
            email = f"{data['first_name'].lower()}.{data['last_name'].lower()}@company.com"
            experience_level = self._get_experience_level(role_title)
            department = self._get_department_from_role(role_title)
            
            users.append({
                "first_name": data['first_name'],
                "last_name": data['last_name'],
                "full_name": f"{data['first_name']} {data['last_name']}",
                "email": email,
                "role_title": role_title,
                "gender": data['gender'],
                "department": department,
                "experience_level": experience_level,
                "location": random.choice(['San Francisco', 'New York', 'Austin', 'Seattle', 'Boston', 'Remote']),
                "hire_date": self._generate_realistic_hire_date(experience_level),
                "ethnicity": data['ethnicity'],
                "age_range": data['age_range']
            })
        
        return users
    
    def _convert_international_to_users(self, international_data: List[Dict], company_size: str, industry: str) -> List[Dict]:
        """Convert international data to enterprise user format."""
        users = []
        
        for data in international_data:
            # Get role distribution
            role_distribution = self._get_role_distribution(company_size, industry)
            role_category = random.choices(
                list(role_distribution.keys()),
                weights=list(role_distribution.values())
            )[0]
            
            role_title = random.choice(self.role_titles.get(role_category, self.role_titles['engineering']))
            
            # Generate professional details
            email = f"{data['first_name'].lower()}.{data['last_name'].lower()}@company.com"
            experience_level = self._get_experience_level(role_title)
            department = self._get_department_from_role(role_title)
            
            location_mapping = {
                'uk': 'London',
                'canada': 'Toronto',
                'australia': 'Sydney',
                'germany': 'Berlin',
                'france': 'Paris'
            }
            
            users.append({
                "first_name": data['first_name'],
                "last_name": data['last_name'],
                "full_name": f"{data['first_name']} {data['last_name']}",
                "email": email,
                "role_title": role_title,
                "gender": random.choice(['male', 'female', 'neutral']),
                "department": department,
                "experience_level": experience_level,
                "location": location_mapping.get(data['country'], 'Remote'),
                "hire_date": self._generate_realistic_hire_date(experience_level),
                "country": data['country'],
                "language": data['language']
            })
        
        return users
    
    def validate_name_data(self, users: List[Dict]) -> Tuple[List[Dict], List[Dict]]:
        """
        Validate name data and separate valid/invalid entries.
        
        Args:
            users: List of user dictionaries
        
        Returns:
            Tuple of (valid_users, invalid_users)
        """
        valid = []
        invalid = []
        
        for user in users:
            try:
                if not user.get("first_name") or not user.get("last_name") or not user.get("email"):
                    invalid.append(user)
                    continue
                
                # Validate email format
                if '@' not in user["email"] or '.' not in user["email"].split('@')[1]:
                    invalid.append(user)
                    continue
                
                valid.append(user)
            except Exception as e:
                logger.warning(f"Error validating user: {str(e)}")
                invalid.append(user)
        
        return valid, invalid
    
    def close(self):
        """Close the session."""
        self.session.close()
        logger.info("Name scraper session closed")

# Example usage and testing
if __name__ == "__main__":
    # Setup logging for testing
    logging.basicConfig(level=logging.INFO)
    
    scraper = NameScraper()
    
    try:
        # Get hybrid names for a large B2B SaaS company
        users = scraper.get_names(source="hybrid", count=20, company_size="large", industry="b2b_saas")
        
        print(f"\nRetrieved {len(users)} users:")
        for i, user in enumerate(users, 1):
            print(f"{i}. {user['full_name']} - {user['role_title']} ({user['department']})")
            print(f"   Email: {user['email']}")
            print(f"   Experience: {user['experience_level'].title()}, Location: {user['location']}")
        
        # Validate the data
        valid, invalid = scraper.validate_name_data(users)
        print(f"\nValidation results:")
        print(f"Valid users: {len(valid)}")
        print(f"Invalid users: {len(invalid)}")
        
        if invalid:
            print("\nInvalid users:")
            for user in invalid:
                print(f"- {user}")
    
    finally:
        scraper.close()