# src/scrapers/company_scraper.py

"""
Company scraper module for fetching realistic company names and domains.
This module provides functionality to scrape or generate realistic company data
from various sources including YCombinator, Crunchbase, and other public datasets.

The scraper is designed to be:
- Ethical: Uses public APIs and respects rate limits
- Realistic: Generates company names that match B2B SaaS patterns
- Configurable: Can be adjusted for different industries and sizes
- Cachable: Stores results to avoid repeated API calls
"""

import logging
import random
import re
import time
from typing import List, Dict, Optional, Tuple
from pathlib import Path
import json
import requests
from bs4 import BeautifulSoup
from fake_useragent import UserAgent

from src.utils.logging import get_logger

logger = get_logger(__name__)

class CompanyScraper:
    """
    Scraper for realistic company names and domains.
    
    This class handles fetching company data from multiple sources:
    1. YCombinator public company list
    2. Crunchbase API (if available)
    3. Predefined B2B SaaS company datasets
    4. Synthetic generation for missing data
    
    The scraper respects ethical scraping practices and rate limits.
    """
    
    def __init__(self, cache_dir: str = "data/cache", max_retries: int = 3):
        """
        Initialize the company scraper.
        
        Args:
            cache_dir: Directory to cache scraped results
            max_retries: Maximum number of retries for failed requests
        """
        self.cache_dir = Path(cache_dir)
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        self.max_retries = max_retries
        self.user_agent = UserAgent()
        self.session = requests.Session()
        
        # Predefined B2B SaaS company patterns
        self.company_prefixes = [
            "Acme", "Apex", "Vertex", "Nexus", "Orion", "Zenith", "Pinnacle",
            "Summit", "Catalyst", "Horizon", "Vantage", "Elevate", "Momentum",
            "Synergy", "Fusion", "Nova", "Quantum", "Pulse", "Stride", "Vista"
        ]
        
        self.company_suffixes = [
            "Tech", "Systems", "Solutions", "Labs", "Innovations", "Dynamics",
            "Digital", "Cloud", "Data", "Analytics", "Enterprise", "Global",
            "Network", "Platform", "Studio", "Works", "Group", "Partners",
            "Holdings", "Corporation"
        ]
        
        self.saas_suffixes = [
            "Inc", "Corp", "LLC", "Co", "Technologies", "Software", "AI",
            "Automation", "Intelligence", "Platform", "Hub", "Suite"
        ]
    
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
                logger.warning(f"Failed to load cached  {str(e)}")
        return None
    
    def _cache_data(self, cache_key: str, data: List[Dict]):
        """Cache data to file."""
        cache_file = self.cache_dir / f"{cache_key}.json"
        try:
            with open(cache_file, 'w') as f:
                json.dump(data, f, indent=2)
        except Exception as e:
            logger.warning(f"Failed to cache  {str(e)}")
    
    def scrape_yc_companies(self, limit: int = 100) -> List[Dict]:
        """
        Scrape YCombinator companies.
        
        This is a mock implementation since YCombinator doesn't have a public API.
        In a real implementation, this would use their public directory or API.
        
        Args:
            limit: Maximum number of companies to return
        
        Returns:
            List of company dictionaries with name and domain
        """
        logger.info("Scraping YCombinator companies (mock implementation)")
        
        # Mock YC companies - in reality, this would scrape https://www.ycombinator.com/companies
        yc_companies = [
            {"name": "Airbnb", "domain": "airbnb.com"},
            {"name": "Dropbox", "domain": "dropbox.com"},
            {"name": "Stripe", "domain": "stripe.com"},
            {"name": "DoorDash", "domain": "doordash.com"},
            {"name": "Instacart", "domain": "instacart.com"},
            {"name": "Coinbase", "domain": "coinbase.com"},
            {"name": "Reddit", "domain": "reddit.com"},
            {"name": "Twitch", "domain": "twitch.tv"},
            {"name": "Gusto", "domain": "gusto.com"},
            {"name": "Rappi", "domain": "rappi.com"},
            {"name": "Nubank", "domain": "nubank.com.br"},
            {"name": "Flexport", "domain": "flexport.com"},
            {"name": "Brex", "domain": "brex.com"},
            {"name": "Scale AI", "domain": "scale.com"},
            {"name": "Plaid", "domain": "plaid.com"},
            {"name": "Notion", "domain": "notion.so"},
            {"name": "Figma", "domain": "figma.com"},
            {"name": "Canva", "domain": "canva.com"},
            {"name": "OpenAI", "domain": "openai.com"},
            {"name": "Anthropic", "domain": "anthropic.com"}
        ]
        
        return random.sample(yc_companies, min(limit, len(yc_companies)))
    
    def scrape_crunchbase_companies(self, industry: str = "saas", limit: int = 50) -> List[Dict]:
        """
        Scrape Crunchbase companies (mock implementation).
        
        Note: This is a mock since Crunchbase requires API access.
        In production, this would use the Crunchbase API with proper authentication.
        
        Args:
            industry: Industry filter (saas, fintech, etc.)
            limit: Maximum number of companies to return
        
        Returns:
            List of company dictionaries
        """
        logger.info(f"Scraping Crunchbase companies for industry: {industry} (mock implementation)")
        
        # Mock Crunchbase SaaS companies
        saas_companies = [
            {"name": "Salesforce", "domain": "salesforce.com"},
            {"name": "HubSpot", "domain": "hubspot.com"},
            {"name": "Zoom", "domain": "zoom.us"},
            {"name": "Slack", "domain": "slack.com"},
            {"name": "Atlassian", "domain": "atlassian.com"},
            {"name": "Shopify", "domain": "shopify.com"},
            {"name": "Snowflake", "domain": "snowflake.com"},
            {"name": "Datadog", "domain": "datadoghq.com"},
            {"name": "Twilio", "domain": "twilio.com"},
            {"name": "MongoDB", "domain": "mongodb.com"},
            {"name": "Elastic", "domain": "elastic.co"},
            {"name": "PagerDuty", "domain": "pagerduty.com"},
            {"name": "Okta", "domain": "okta.com"},
            {"name": "Zscaler", "domain": "zscaler.com"},
            {"name": "CrowdStrike", "domain": "crowdstrike.com"},
            {"name": "ServiceNow", "domain": "servicenow.com"},
            {"name": "Workday", "domain": "workday.com"},
            {"name": "Adobe", "domain": "adobe.com"},
            {"name": "Intuit", "domain": "intuit.com"},
            {"name": "Square", "domain": "squareup.com"}
        ]
        
        return random.sample(saas_companies, min(limit, len(saas_companies)))
    
    def generate_synthetic_companies(self, count: int, industry: str = "b2b_saas") -> List[Dict]:
        """
        Generate synthetic but realistic company names and domains.
        
        Args:
            count: Number of companies to generate
            industry: Industry type for naming patterns
        
        Returns:
            List of company dictionaries with realistic names and domains
        """
        logger.info(f"Generating {count} synthetic {industry} companies")
        
        companies = []
        used_names = set()
        
        for _ in range(count):
            # Generate company name
            prefix = random.choice(self.company_prefixes)
            suffix = random.choice(self.company_suffixes)
            saas_suffix = random.choice(self.saas_suffixes)
            
            # Create variations
            name_patterns = [
                f"{prefix} {suffix}",
                f"{prefix}{suffix}",
                f"{prefix} {suffix} {saas_suffix}",
                f"{prefix}{suffix}{saas_suffix}",
                f"{prefix} {saas_suffix}",
                f"{prefix}{saas_suffix}"
            ]
            
            name = random.choice(name_patterns)
            
            # Ensure unique names
            counter = 1
            base_name = name
            while name in used_names:
                name = f"{base_name} {counter}"
                counter += 1
            
            used_names.add(name)
            
            # Generate domain
            domain_base = re.sub(r'[^a-zA-Z0-9]', '', base_name.lower())
            domain_tlds = ['.com', '.io', '.ai', '.co', '.tech', '.app']
            domain = f"{domain_base}{random.choice(domain_tlds)}"
            
            # Add company to list
            companies.append({
                "name": name,
                "domain": domain,
                "industry": industry,
                "size_range": self._get_realistic_company_size(industry),
                "founded_year": random.randint(2010, 2025)
            })
        
        return companies
    
    def _get_realistic_company_size(self, industry: str) -> str:
        """Get realistic company size range based on industry."""
        size_mapping = {
            "b2b_saas": random.choice(["50-200", "200-500", "500-1000", "1000-5000", "5000-10000"]),
            "fintech": random.choice(["100-500", "500-2000", "2000-10000"]),
            "ecommerce": random.choice(["50-200", "200-1000", "1000-5000"]),
            "healthtech": random.choice(["50-200", "200-500", "500-2000"]),
            "edtech": random.choice(["50-200", "200-500", "500-1000"])
        }
        return size_mapping.get(industry, "100-500")
    
    def get_companies(self, source: str = "hybrid", count: int = 100, industry: str = "b2b_saas") -> List[Dict]:
        """
        Get companies from specified source.
        
        Args:
            source: Source to use ('yc', 'crunchbase', 'synthetic', 'hybrid')
            count: Number of companies to retrieve
            industry: Industry type for synthetic generation
        
        Returns:
            List of company dictionaries
        """
        logger.info(f"Getting {count} companies from {source} source for {industry} industry")
        
        cache_key = self._get_cached_data(f"companies_{source}_{industry}_{count}")
        if cache_key:
            logger.info("Using cached company data")
            return cache_key
        
        companies = []
        
        try:
            if source == "yc":
                companies = self.scrape_yc_companies(count)
            elif source == "crunchbase":
                companies = self.scrape_crunchbase_companies(industry, count)
            elif source == "synthetic":
                companies = self.generate_synthetic_companies(count, industry)
            elif source == "hybrid":
                # Mix of real and synthetic companies
                real_count = min(20, count // 2)
                synthetic_count = count - real_count
                
                yc_companies = self.scrape_yc_companies(real_count // 2)
                cb_companies = self.scrape_crunchbase_companies(industry, real_count // 2)
                synthetic_companies = self.generate_synthetic_companies(synthetic_count, industry)
                
                companies = yc_companies + cb_companies + synthetic_companies
                random.shuffle(companies)
            else:
                logger.warning(f"Unknown source: {source}. Using synthetic generation.")
                companies = self.generate_synthetic_companies(count, industry)
        
        except Exception as e:
            logger.error(f"Error getting companies: {str(e)}")
            logger.info("Falling back to synthetic generation")
            companies = self.generate_synthetic_companies(count, industry)
        
        # Ensure we have exactly count companies
        companies = companies[:count]
        
        # Cache the results
        self._cache_data(f"companies_{source}_{industry}_{count}", companies)
        
        logger.info(f"Successfully retrieved {len(companies)} companies")
        return companies
    
    def get_company_domains(self, companies: List[Dict]) -> List[str]:
        """
        Extract domains from company list.
        
        Args:
            companies: List of company dictionaries
        
        Returns:
            List of domain strings
        """
        return [company.get("domain", "").lower().strip() for company in companies if company.get("domain")]
    
    def validate_company_data(self, companies: List[Dict]) -> Tuple[List[Dict], List[Dict]]:
        """
        Validate company data and separate valid/invalid entries.
        
        Args:
            companies: List of company dictionaries
        
        Returns:
            Tuple of (valid_companies, invalid_companies)
        """
        valid = []
        invalid = []
        
        for company in companies:
            try:
                if not company.get("name") or not company.get("domain"):
                    invalid.append(company)
                    continue
                
                # Validate domain format
                domain = company["domain"].lower().strip()
                if not re.match(r'^[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$', domain):
                    invalid.append(company)
                    continue
                
                valid.append(company)
            except Exception as e:
                logger.warning(f"Error validating company: {str(e)}")
                invalid.append(company)
        
        return valid, invalid
    
    def close(self):
        """Close the session."""
        self.session.close()
        logger.info("Company scraper session closed")

# Example usage and testing
if __name__ == "__main__":
    # Setup logging for testing
    logging.basicConfig(level=logging.INFO)
    
    scraper = CompanyScraper()
    
    try:
        # Get hybrid companies
        companies = scraper.get_companies(source="hybrid", count=10, industry="b2b_saas")
        
        print(f"\nRetrieved {len(companies)} companies:")
        for i, company in enumerate(companies, 1):
            print(f"{i}. {company['name']} - {company['domain']}")
        
        # Validate the data
        valid, invalid = scraper.validate_company_data(companies)
        print(f"\nValidation results:")
        print(f"Valid companies: {len(valid)}")
        print(f"Invalid companies: {len(invalid)}")
        
        if invalid:
            print("\nInvalid companies:")
            for company in invalid:
                print(f"- {company}")
    
    finally:
        scraper.close()