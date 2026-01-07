

"""
Template scraper module for fetching realistic project templates and workflows.
This module provides functionality to scrape or generate realistic project templates
from various sources including public project management templates, GitHub repositories,
and industry-specific workflow patterns.

The scraper is designed to be:
- Ethical: Uses public templates and respects licensing
- Realistic: Generates templates that match enterprise workflow patterns
- Industry-specific: Adapts to different business domains
- Configurable: Can be adjusted for company size and maturity
- Cachable: Stores results to avoid repeated requests
"""

import logging
import random
import time
import json
import re
from typing import List, Dict, Optional, Tuple, Set
from pathlib import Path
import requests
from bs4 import BeautifulSoup
from fake_useragent import UserAgent

from src.utils.logging import get_logger

logger = get_logger(__name__)

class TemplateScraper:
    """
    Scraper for realistic project templates and workflows.
    
    This class handles fetching template data from multiple sources:
    1. Public project management template repositories
    2. GitHub project templates
    3. Industry-specific workflow patterns
    4. Synthetic generation for custom templates
    
    The scraper respects ethical data collection practices and licensing requirements.
    """
    
    def __init__(self, cache_dir: str = "data/cache", max_retries: int = 3):
        """
        Initialize the template scraper.
        
        Args:
            cache_dir: Directory to cache scraped results
            max_retries: Maximum number of retries for failed requests
        """
        self.cache_dir = Path(cache_dir)
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        self.max_retries = max_retries
        self.user_agent = UserAgent()
        self.session = requests.Session()
        
        # Predefined template patterns for different industries
        self.template_categories = {
            'engineering': {
                'software_development': {
                    'sections': ['Backlog', 'Ready for Development', 'In Development', 'Code Review', 'QA Testing', 'Ready for Release', 'Released'],
                    'task_patterns': [
                        'Implement {feature} feature',
                        'Fix bug in {module}',
                        'Write unit tests for {component}',
                        'Refactor {service} code',
                        'Update documentation for {feature}',
                        'Performance optimization for {endpoint}',
                        'Security review for {module}',
                        'Database migration for {table}'
                    ],
                    'custom_fields': [
                        {'name': 'Priority', 'type': 'enum', 'options': ['Critical', 'High', 'Medium', 'Low']},
                        {'name': 'Estimate', 'type': 'number', 'unit': 'points'},
                        {'name': 'Sprint', 'type': 'text'},
                        {'name': 'Component', 'type': 'text'},
                        {'name': 'Bug Severity', 'type': 'enum', 'options': ['Blocker', 'Critical', 'Major', 'Minor', 'Trivial']}
                    ]
                },
                'devops': {
                    'sections': ['Planning', 'Implementation', 'Testing', 'Deployment', 'Monitoring'],
                    'task_patterns': [
                        'Set up CI/CD pipeline for {service}',
                        'Configure monitoring for {environment}',
                        'Implement infrastructure as code for {component}',
                        'Security hardening for {system}',
                        'Performance tuning for {database}',
                        'Disaster recovery testing for {service}',
                        'Cost optimization for {cloud_resource}'
                    ],
                    'custom_fields': [
                        {'name': 'Environment', 'type': 'enum', 'options': ['Production', 'Staging', 'Development', 'QA']},
                        {'name': 'Cost Impact', 'type': 'enum', 'options': ['High', 'Medium', 'Low']},
                        {'name': 'Risk Level', 'type': 'enum', 'options': ['High', 'Medium', 'Low']},
                        {'name': 'Downtime Required', 'type': 'boolean'}
                    ]
                }
            },
            'marketing': {
                'campaign_management': {
                    'sections': ['Planning', 'Content Creation', 'Review & Approval', 'Launch', 'Performance Analysis'],
                    'task_patterns': [
                        'Create content calendar for {campaign}',
                        'Design graphics for {platform}',
                        'Write copy for {asset}',
                        'Schedule posts for {channel}',
                        'Analyze campaign performance for {metric}',
                        'A/B test {element} variations',
                        'Coordinate with {team} for {deliverable}'
                    ],
                    'custom_fields': [
                        {'name': 'Campaign Type', 'type': 'enum', 'options': ['Product Launch', 'Brand Awareness', 'Lead Generation', 'Customer Retention']},
                        {'name': 'Target Audience', 'type': 'text'},
                        {'name': 'Budget', 'type': 'number', 'unit': 'USD'},
                        {'name': 'Platform', 'type': 'enum', 'options': ['Social Media', 'Email', 'Web', 'Print', 'Video']},
                        {'name': 'KPI Target', 'type': 'number'}
                    ]
                },
                'content_creation': {
                    'sections': ['Ideation', 'Research', 'Drafting', 'Editing', 'Publishing', 'Promotion'],
                    'task_patterns': [
                        'Research topics for {content_type}',
                        'Create outline for {topic}',
                        'Write first draft of {article}',
                        'Edit and proofread {content}',
                        'Create visuals for {piece}',
                        'SEO optimization for {content}',
                        'Schedule publication for {platform}'
                    ],
                    'custom_fields': [
                        {'name': 'Content Type', 'type': 'enum', 'options': ['Blog Post', 'Whitepaper', 'Case Study', 'Video Script', 'Social Media Post']},
                        {'name': 'Word Count Target', 'type': 'number'},
                        {'name': 'SEO Keywords', 'type': 'text'},
                        {'name': 'Tone of Voice', 'type': 'enum', 'options': ['Professional', 'Casual', 'Technical', 'Inspirational']},
                        {'name': 'Approval Required', 'type': 'boolean'}
                    ]
                }
            },
            'operations': {
                'project_management': {
                    'sections': ['To Do', 'In Progress', 'Blocked', 'Review', 'Done'],
                    'task_patterns': [
                        'Coordinate with {department} for {task}',
                        'Prepare documentation for {process}',
                        'Schedule meeting with {stakeholders}',
                        'Track progress on {milestone}',
                        'Resolve blocker for {task}',
                        'Update project timeline for {phase}',
                        'Communicate status to {audience}'
                    ],
                    'custom_fields': [
                        {'name': 'Priority', 'type': 'enum', 'options': ['Critical', 'High', 'Medium', 'Low']},
                        {'name': 'Stakeholder', 'type': 'text'},
                        {'name': 'Deadline Type', 'type': 'enum', 'options': ['Hard Deadline', 'Soft Deadline', 'Milestone']},
                        {'name': 'Resource Required', 'type': 'text'},
                        {'name': 'Risk Level', 'type': 'enum', 'options': ['High', 'Medium', 'Low']}
                    ]
                },
                'hr_processes': {
                    'sections': ['New Hire Onboarding', 'Training & Development', 'Performance Reviews', 'Offboarding'],
                    'task_patterns': [
                        'Prepare onboarding package for {role}',
                        'Schedule training session for {topic}',
                        'Conduct performance review for {employee}',
                        'Process termination paperwork for {employee}',
                        'Update employee handbook for {section}',
                        'Coordinate benefits enrollment for {new_hire}',
                        'Plan team building activity for {department}'
                    ],
                    'custom_fields': [
                        {'name': 'Employee Type', 'type': 'enum', 'options': ['Full-time', 'Part-time', 'Contractor', 'Intern']},
                        {'name': 'Department', 'type': 'text'},
                        {'name': 'Confidential', 'type': 'boolean'},
                        {'name': 'Compliance Required', 'type': 'boolean'},
                        {'name': 'Training Hours', 'type': 'number'}
                    ]
                }
            },
            'product': {
                'product_development': {
                    'sections': ['Research', 'Design', 'Development', 'Testing', 'Launch', 'Post-Launch'],
                    'task_patterns': [
                        'Conduct user research for {feature}',
                        'Create wireframes for {screen}',
                        'Develop user stories for {epic}',
                        'Test usability of {flow}',
                        'Coordinate launch plan for {release}',
                        'Gather user feedback on {feature}',
                        'Analyze feature adoption for {metric}'
                    ],
                    'custom_fields': [
                        {'name': 'Product Area', 'type': 'enum', 'options': ['Frontend', 'Backend', 'Mobile', 'Data', 'Infrastructure']},
                        {'name': 'User Impact', 'type': 'enum', 'options': ['High', 'Medium', 'Low']},
                        {'name': 'Technical Complexity', 'type': 'enum', 'options': ['High', 'Medium', 'Low']},
                        {'name': 'Customer Request', 'type': 'boolean'},
                        {'name': 'Revenue Impact', 'type': 'enum', 'options': ['High', 'Medium', 'Low', 'None']}
                    ]
                },
                'roadmap_planning': {
                    'sections': ['Q1 Planning', 'Q2 Planning', 'Q3 Planning', 'Q4 Planning', 'Long-term Vision'],
                    'task_patterns': [
                        'Define OKRs for {quarter}',
                        'Prioritize features for {release}',
                        'Estimate resource needs for {initiative}',
                        'Coordinate with {team} on {dependency}',
                        'Present roadmap to {stakeholders}',
                        'Track progress against {goals}',
                        'Adjust priorities based on {feedback}'
                    ],
                    'custom_fields': [
                        {'name': 'Quarter', 'type': 'enum', 'options': ['Q1', 'Q2', 'Q3', 'Q4']},
                        {'name': 'Strategic Theme', 'type': 'text'},
                        {'name': 'Resource Allocation', 'type': 'number', 'unit': '%'},
                        {'name': 'Dependencies', 'type': 'text'},
                        {'name': 'Executive Sponsor', 'type': 'text'}
                    ]
                }
            }
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
    
    def scrape_github_templates(self, repository_types: List[str] = ['software-development', 'project-management', 'marketing'], 
                              limit_per_type: int = 5) -> List[Dict]:
        """
        Scrape GitHub project templates (mock implementation).
        
        Note: This is a mock implementation since GitHub API requires authentication.
        In production, this would use the GitHub API with proper authentication.
        
        Args:
            repository_types: Types of repositories to search for
            limit_per_type: Number of templates per type
        
        Returns:
            List of template dictionaries
        """
        logger.info(f"Scraping GitHub templates for types: {repository_types}")
        
        github_templates = []
        
        # Mock GitHub templates
        mock_templates = {
            'software-development': [
                {
                    'name': 'Agile Software Development',
                    'description': 'Scrum-based template for software development teams',
                    'sections': ['Product Backlog', 'Sprint Backlog', 'In Progress', 'Code Review', 'Testing', 'Done'],
                    'task_templates': [
                        'Implement user authentication',
                        'Create API endpoints for {feature}',
                        'Write unit tests for {module}',
                        'Fix bug in {component}',
                        'Update documentation'
                    ],
                    'custom_fields': [
                        {'name': 'Story Points', 'type': 'number'},
                        {'name': 'Sprint', 'type': 'text'},
                        {'name': 'Component', 'type': 'text'},
                        {'name': 'Bug Priority', 'type': 'enum', 'options': ['Critical', 'High', 'Medium', 'Low']}
                    ],
                    'tags': ['development', 'agile', 'scrum', 'engineering']
                },
                {
                    'name': 'Kanban Development',
                    'description': 'Continuous flow template for development teams',
                    'sections': ['Backlog', 'Ready', 'In Progress', 'Review', 'Testing', 'Deployed'],
                    'task_templates': [
                        'Refactor {service} code',
                        'Optimize database queries for {table}',
                        'Implement caching for {endpoint}',
                        'Set up monitoring for {service}',
                        'Security audit for {module}'
                    ],
                    'custom_fields': [
                        {'name': 'Effort', 'type': 'number', 'unit': 'hours'},
                        {'name': 'Environment', 'type': 'enum', 'options': ['dev', 'staging', 'prod']},
                        {'name': 'Risk Level', 'type': 'enum', 'options': ['High', 'Medium', 'Low']},
                        {'name': 'Downtime Required', 'type': 'boolean'}
                    ],
                    'tags': ['kanban', 'continuous-delivery', 'devops']
                }
            ],
            'project-management': [
                {
                    'name': 'Executive Project Tracking',
                    'description': 'High-level project tracking for executives',
                    'sections': ['Planning', 'Execution', 'Monitoring', 'Closure'],
                    'task_templates': [
                        'Define project scope and objectives',
                        'Identify key stakeholders and sponsors',
                        'Develop risk management plan',
                        'Track budget vs actuals',
                        'Prepare executive status report'
                    ],
                    'custom_fields': [
                        {'name': 'Budget', 'type': 'number', 'unit': 'USD'},
                        {'name': 'Risk Level', 'type': 'enum', 'options': ['Critical', 'High', 'Medium', 'Low']},
                        {'name': 'Strategic Alignment', 'type': 'enum', 'options': ['High', 'Medium', 'Low']},
                        {'name': 'Executive Sponsor', 'type': 'text'}
                    ],
                    'tags': ['executive', 'strategic', 'portfolio']
                },
                {
                    'name': 'Cross-functional Team Project',
                    'description': 'Template for projects involving multiple departments',
                    'sections': ['Backlog', 'Design', 'Development', 'Testing', 'Launch', 'Post-mortem'],
                    'task_templates': [
                        'Coordinate requirements gathering with {team}',
                        'Design user experience flows',
                        'Develop integration with {system}',
                        'Conduct user acceptance testing',
                        'Plan go-to-market strategy'
                    ],
                    'custom_fields': [
                        {'name': 'Team', 'type': 'enum', 'options': ['Engineering', 'Product', 'Marketing', 'Sales', 'Operations']},
                        {'name': 'Integration Points', 'type': 'number'},
                        {'name': 'Customer Impact', 'type': 'enum', 'options': ['High', 'Medium', 'Low']},
                        {'name': 'Dependencies', 'type': 'text'}
                    ],
                    'tags': ['cross-functional', 'collaboration', 'enterprise']
                }
            ],
            'marketing': [
                {
                    'name': 'Digital Marketing Campaign',
                    'description': 'End-to-end template for digital marketing campaigns',
                    'sections': ['Strategy', 'Content Creation', 'Design', 'Publishing', 'Analysis'],
                    'task_templates': [
                        'Define target audience personas',
                        'Create content calendar for {platform}',
                        'Design social media graphics',
                        'Write email marketing copy',
                        'Analyze campaign performance metrics'
                    ],
                    'custom_fields': [
                        {'name': 'Campaign Type', 'type': 'enum', 'options': ['Awareness', 'Lead Gen', 'Sales', 'Retention']},
                        {'name': 'Budget', 'type': 'number', 'unit': 'USD'},
                        {'name': 'Target KPI', 'type': 'text'},
                        {'name': 'Platform', 'type': 'enum', 'options': ['Facebook', 'Instagram', 'LinkedIn', 'Email', 'Web']}
                    ],
                    'tags': ['marketing', 'digital', 'campaign', 'social-media']
                },
                {
                    'name': 'Content Marketing Pipeline',
                    'description': 'Template for managing content creation workflows',
                    'sections': ['Ideation', 'Research', 'Writing', 'Editing', 'SEO', 'Publishing', 'Promotion'],
                    'task_templates': [
                        'Brainstorm content ideas for {topic}',
                        'Research keywords for {article}',
                        'Write first draft of {content}',
                        'Edit and optimize for SEO',
                        'Create promotional social posts'
                    ],
                    'custom_fields': [
                        {'name': 'Content Type', 'type': 'enum', 'options': ['Blog Post', 'Whitepaper', 'Video', 'Infographic', 'Case Study']},
                        {'name': 'Word Count Target', 'type': 'number'},
                        {'name': 'SEO Difficulty', 'type': 'enum', 'options': ['High', 'Medium', 'Low']},
                        {'name': 'Promotion Channels', 'type': 'text'}
                    ],
                    'tags': ['content', 'seo', 'editorial', 'blog']
                }
            ]
        }
        
        for repo_type in repository_types:
            if repo_type in mock_templates:
                templates = mock_templates[repo_type][:limit_per_type]
                for template in templates:
                    template['source'] = 'github'
                    template['repository_type'] = repo_type
                    github_templates.append(template)
        
        return github_templates
    
    def scrape_public_templates(self, template_sources: List[str] = ['asana', 'trello', 'clickup'], 
                              limit_per_source: int = 3) -> List[Dict]:
        """
        Scrape public project management templates (mock implementation).
        
        Args:
            template_sources: Sources to scrape from
            limit_per_source: Number of templates per source
        
        Returns:
            List of template dictionaries
        """
        logger.info(f"Scraping public templates from sources: {template_sources}")
        
        public_templates = []
        
        # Mock public templates
        mock_public_templates = {
            'asana': [
                {
                    'name': 'Product Launch Checklist',
                    'description': 'Comprehensive checklist for launching new products',
                    'sections': ['Planning', 'Development', 'Marketing', 'Sales Enablement', 'Launch Day', 'Post-Launch'],
                    'task_templates': [
                        'Define product requirements document',
                        'Create go-to-market strategy',
                        'Develop sales training materials',
                        'Set up analytics tracking',
                        'Plan launch event logistics'
                    ],
                    'custom_fields': [
                        {'name': 'Launch Date', 'type': 'date'},
                        {'name': 'Product Line', 'type': 'text'},
                        {'name': 'Target Market', 'type': 'text'},
                        {'name': 'Budget Owner', 'type': 'text'},
                        {'name': 'Success Metrics', 'type': 'text'}
                    ],
                    'tags': ['product-launch', 'gtm', 'marketing', 'sales']
                },
                {
                    'name': 'Customer Onboarding',
                    'description': 'Template for onboarding new enterprise customers',
                    'sections': ['Pre-boarding', 'Setup', 'Training', 'Go-Live', 'Post-Go-Live'],
                    'task_templates': [
                        'Collect customer requirements',  
                        'Configure system settings',
                        'Create user accounts',
                        'Schedule training sessions',
                        'Conduct post-implementation review'
                    ],
                    'custom_fields': [
                        {'name': 'Customer Tier', 'type': 'enum', 'options': ['Enterprise', 'Business', 'Professional', 'Basic']},
                        {'name': 'Contract Value', 'type': 'number', 'unit': 'USD'},
                        {'name': 'Onboarding Specialist', 'type': 'text'},
                        {'name': 'Go-Live Date', 'type': 'date'},
                        {'name': 'Success Criteria', 'type': 'text'}
                    ],
                    'tags': ['customer-success', 'onboarding', 'implementation']
                }
            ],
            'trello': [
                {
                    'name': 'Startup MVP Development',
                    'description': 'Lean template for startup minimum viable product development',
                    'sections': ['Icebox', 'Backlog', 'This Week', 'Doing', 'Done'],
                    'task_templates': [
                        'Validate problem with customer interviews',
                        'Design core user flows',
                        'Build landing page for validation',
                        'Set up analytics infrastructure',
                        'Prepare pitch deck for investors'
                    ],
                    'custom_fields': [
                        {'name': 'Customer Validation', 'type': 'enum', 'options': ['Validated', 'Needs Validation', 'Invalid']},
                        {'name': 'Effort', 'type': 'enum', 'options': ['1 day', '2-3 days', '1 week', '2+ weeks']},
                        {'name': 'Learning Outcome', 'type': 'text'},
                        {'name': 'Next Steps', 'type': 'text'}
                    ],
                    'tags': ['startup', 'mvp', 'lean', 'validation']
                },
                {
                    'name': 'Event Planning',
                    'description': 'Template for planning corporate events and conferences',
                    'sections': ['Planning', 'Vendor Management', 'Logistics', 'Marketing', 'Execution', 'Follow-up'],
                    'task_templates': [
                        'Define event objectives and KPIs',
                        'Book venue and catering',
                        'Create event website and registration',
                        'Coordinate with speakers and sponsors',
                        'Plan post-event follow-up strategy'
                    ],
                    'custom_fields': [
                        {'name': 'Event Type', 'type': 'enum', 'options': ['Conference', 'Workshop', 'Webinar', 'Team Building', 'Client Event']},
                        {'name': 'Expected Attendees', 'type': 'number'},
                        {'name': 'Budget', 'type': 'number', 'unit': 'USD'},
                        {'name': 'Location', 'type': 'text'},
                        {'name': 'Sponsors', 'type': 'text'}
                    ],
                    'tags': ['events', 'conference', 'planning', 'logistics']
                }
            ],
            'clickup': [
                {
                    'name': 'SaaS Customer Success',
                    'description': 'Template for managing SaaS customer success operations',
                    'sections': ['New Customer', 'Onboarding', 'Adoption', 'Growth', 'Renewal'],
                    'task_templates': [
                        'Conduct kickoff call with customer',
                        'Create success plan and KPIs',
                        'Schedule quarterly business reviews',
                        'Identify expansion opportunities',
                        'Prepare renewal proposal'
                    ],
                    'custom_fields': [
                        {'name': 'Customer Health Score', 'type': 'number', 'unit': '%'},
                        {'name': 'ARR', 'type': 'number', 'unit': 'USD'},
                        {'name': 'CSM', 'type': 'text'},
                        {'name': 'Renewal Date', 'type': 'date'},
                        {'name': 'Expansion Potential', 'type': 'enum', 'options': ['High', 'Medium', 'Low']}
                    ],
                    'tags': ['customer-success', 'saas', 'retention', 'growth']
                },
                {
                    'name': 'Engineering Sprint Planning',
                    'description': 'Detailed template for engineering sprint planning and execution',
                    'sections': ['Sprint Planning', 'Development', 'Code Review', 'QA', 'Deployment', 'Retrospective'],
                    'task_templates': [
                        'Refine user stories and acceptance criteria',
                        'Estimate story points for sprint items',
                        'Identify technical dependencies',
                        'Plan deployment strategy',
                        'Document lessons learned'
                    ],
                    'custom_fields': [
                        {'name': 'Sprint Number', 'type': 'number'},
                        {'name': 'Story Points', 'type': 'number'},
                        {'name': 'Tech Debt', 'type': 'enum', 'options': ['High', 'Medium', 'Low', 'None']},
                        {'name': 'QA Owner', 'type': 'text'},
                        {'name': 'Deployment Window', 'type': 'text'}
                    ],
                    'tags': ['engineering', 'agile', 'sprint', 'development']
                }
            ]
        }
        
        for source in template_sources:
            if source in mock_public_templates:
                templates = mock_public_templates[source][:limit_per_source]
                for template in templates:
                    template['source'] = source
                    public_templates.append(template)
        
        return public_templates
    
    def generate_synthetic_templates(self, count: int, industry: str = 'b2b_saas', 
                                   company_size: str = 'large') -> List[Dict]:
        """
        Generate synthetic but realistic project templates.
        
        Args:
            count: Number of templates to generate
            industry: Industry type for template patterns
            company_size: Company size for complexity level
        
        Returns:
            List of template dictionaries
        """
        logger.info(f"Generating {count} synthetic templates for {industry} {company_size} company")
        
        templates = []
        
        # Map industry to template categories
        industry_mapping = {
            'b2b_saas': ['engineering', 'product', 'sales', 'marketing'],
            'fintech': ['engineering', 'compliance', 'product', 'operations'],
            'ecommerce': ['marketing', 'operations', 'engineering', 'customer-success'],
            'healthcare': ['compliance', 'operations', 'product', 'engineering'],
            'education': ['content', 'operations', 'product', 'marketing']
        }
        
        categories = industry_mapping.get(industry, ['engineering', 'product', 'marketing', 'operations'])
        
        for i in range(count):
            category = random.choice(categories)
            subcategories = list(self.template_categories.get(category, {}).keys())
            
            if not subcategories:
                continue
            
            subcategory = random.choice(subcategories)
            template_pattern = self.template_categories[category][subcategory]
            
            # Generate template name based on category and subcategory
            template_names = {
                'engineering': [f'{subcategory.replace("_", " ").title()} Sprint', 
                              f'{subcategory.replace("_", " ").title()} Workflow',
                              f'Engineering {subcategory.replace("_", " ").title()}'],
                'marketing': [f'{subcategory.replace("_", " ").title()} Campaign',
                             f'{subcategory.replace("_", " ").title()} Pipeline',
                             f'Marketing {subcategory.replace("_", " ").title()}'],
                'operations': [f'{subcategory.replace("_", " ").title()} Process',
                              f'Operational {subcategory.replace("_", " ").title()}',
                              f'{subcategory.replace("_", " ").title()} Workflow'],
                'product': [f'{subcategory.replace("_", " ").title()} Roadmap',
                           f'Product {subcategory.replace("_", " ").title()}',
                           f'{subcategory.replace("_", " ").title()} Planning']
            }
            
            name_templates = template_names.get(category, [f'{category.title()} {subcategory.title()}'])
            name = random.choice(name_templates)
            
            # Adjust complexity based on company size
            complexity_factor = {
                'small': 0.7,
                'medium': 1.0,
                'large': 1.3,
                'enterprise': 1.5
            }.get(company_size, 1.0)
            
            # Generate realistic task patterns
            task_patterns = template_pattern['task_patterns']
            num_tasks = int(len(task_patterns) * complexity_factor)
            selected_tasks = random.sample(task_patterns, min(num_tasks, len(task_patterns)))
            
            # Generate custom fields
            custom_fields = template_pattern['custom_fields']
            num_fields = int(len(custom_fields) * complexity_factor * 0.8)  # Slightly fewer fields
            selected_fields = random.sample(custom_fields, min(num_fields, len(custom_fields)))
            
            # Generate tags
            base_tags = [category, subcategory.replace('_', '-'), industry]
            additional_tags = ['workflow', 'process', 'team', 'project']
            tags = base_tags + random.sample(additional_tags, min(2, len(additional_tags)))
            
            template = {
                'name': name,
                'description': f'Realistic {name.lower()} template for {industry} companies',
                'sections': template_pattern['sections'],
                'task_templates': selected_tasks,
                'custom_fields': selected_fields,
                'tags': tags,
                'category': category,
                'subcategory': subcategory,
                'industry': industry,
                'complexity': company_size,
                'source': 'synthetic'
            }
            
            templates.append(template)
        
        return templates
    
    def get_templates(self, source: str = 'hybrid', count: int = 10, industry: str = 'b2b_saas', 
                     company_size: str = 'large') -> List[Dict]:
        """
        Get templates from specified source.
        
        Args:
            source: Source to use ('github', 'public', 'synthetic', 'hybrid')
            count: Number of templates to retrieve
            industry: Industry type for template patterns
            company_size: Company size for complexity level
        
        Returns:
            List of template dictionaries
        """
        logger.info(f"Getting {count} templates from {source} source for {industry} {company_size} company")
        
        cache_key = self._cache_key("templates", {
            "source": source,
            "count": count,
            "industry": industry,
            "company_size": company_size
        })
        
        cached_data = self._get_cached_data(cache_key)
        if cached_data:
            logger.info("Using cached template data")
            return cached_data[:count]
        
        templates = []
        
        try:
            if source == 'github':
                templates = self.scrape_github_templates(limit_per_type=count//3)
            elif source == 'public':
                templates = self.scrape_public_templates(limit_per_source=count//3)
            elif source == 'synthetic':
                templates = self.generate_synthetic_templates(count, industry, company_size)
            elif source == 'hybrid':
                # Mix of different sources
                github_count = int(count * 0.3)  # 30% GitHub
                public_count = int(count * 0.3)  # 30% Public
                synthetic_count = count - github_count - public_count  # 40% Synthetic
                
                github_templates = self.scrape_github_templates(limit_per_type=github_count//3 + 1)
                public_templates = self.scrape_public_templates(limit_per_source=public_count//3 + 1)
                synthetic_templates = self.generate_synthetic_templates(synthetic_count, industry, company_size)
                
                templates = github_templates + public_templates + synthetic_templates
                random.shuffle(templates)
            else:
                logger.warning(f"Unknown source: {source}. Using synthetic generation.")
                templates = self.generate_synthetic_templates(count, industry, company_size)
        
        except Exception as e:
            logger.error(f"Error getting templates: {str(e)}")
            logger.info("Falling back to synthetic generation")
            templates = self.generate_synthetic_templates(count, industry, company_size)
        
        # Ensure we have exactly count templates
        templates = templates[:count]
        
        # Cache the results
        self._cache_data(cache_key, templates)
        
        logger.info(f"Successfully retrieved {len(templates)} templates")
        return templates
    
    def validate_template_data(self, templates: List[Dict]) -> Tuple[List[Dict], List[Dict]]:
        """
        Validate template data and separate valid/invalid entries.
        
        Args:
            templates: List of template dictionaries
        
        Returns:
            Tuple of (valid_templates, invalid_templates)
        """
        valid = []
        invalid = []
        
        for template in templates:
            try:
                if not template.get('name') or not template.get('sections') or not template.get('task_templates'):
                    invalid.append(template)
                    continue
                
                # Validate sections
                if not isinstance(template['sections'], list) or len(template['sections']) < 2:
                    invalid.append(template)
                    continue
                
                # Validate task templates
                if not isinstance(template['task_templates'], list) or len(template['task_templates']) < 3:
                    invalid.append(template)
                    continue
                
                # Validate custom fields if present
                if 'custom_fields' in template:
                    if not isinstance(template['custom_fields'], list):
                        invalid.append(template)
                        continue
                    
                    for field in template['custom_fields']:
                        if not field.get('name') or not field.get('type'):
                            invalid.append(template)
                            break
                
                valid.append(template)
            except Exception as e:
                logger.warning(f"Error validating template: {str(e)}")
                invalid.append(template)
        
        return valid, invalid
    
    def close(self):
        """Close the session."""
        self.session.close()
        logger.info("Template scraper session closed")

# Example usage and testing
if __name__ == "__main__":
    # Setup logging for testing
    logging.basicConfig(level=logging.INFO)
    
    scraper = TemplateScraper()
    
    try:
        # Get hybrid templates for a large B2B SaaS company
        templates = scraper.get_templates(source="hybrid", count=8, industry="b2b_saas", company_size="large")
        
        print(f"\nRetrieved {len(templates)} templates:")
        for i, template in enumerate(templates, 1):
            print(f"\n{i}. {template['name']}")
            print(f"   Category: {template.get('category', 'N/A')} - {template.get('subcategory', 'N/A')}")
            print(f"   Sections: {', '.join(template['sections'])}")
            print(f"   Sample Tasks: {', '.join(template['task_templates'][:3])}")
            print(f"   Tags: {', '.join(template.get('tags', []))}")
            
            if 'custom_fields' in template:
                print(f"   Custom Fields: {len(template['custom_fields'])} fields")
        
        # Validate the data
        valid, invalid = scraper.validate_template_data(templates)
        print(f"\nValidation results:")
        print(f"Valid templates: {len(valid)}")
        print(f"Invalid templates: {len(invalid)}")
        
        if invalid:
            print("\nInvalid templates:")
            for template in invalid:
                print(f"- {template.get('name', 'Unnamed template')}")
    
    finally:
        scraper.close()