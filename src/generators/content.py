

"""
Content generator module for creating realistic text content using LLM integration.
This module coordinates with the LLM utility to generate high-quality, context-aware
content for task names, descriptions, comments, and other text fields in the seed data.

The generator is designed to be:
- Context-aware: Generates content that matches department, project type, and business context
- Quality-focused: Uses validation and fallback strategies to ensure content quality
- Efficient: Batches LLM calls and caches results for performance
- Configurable: Adjustable parameters for different content types and quality levels
- Observable: Detailed logging and metrics for content generation performance
"""

import logging
import random
import time
from typing import List, Dict, Optional, Tuple, Any
from pathlib import Path
import json

from src.utils.logging import get_logger
from src.utils.llm import LLMContentGenerator
from src.models.organization import OrganizationConfig

logger = get_logger(__name__)

class ContentGenerator:
    """
    Generator for creating realistic text content using LLM integration.
    
    This class handles:
    1. Coordinating LLM calls for different content types
    2. Context injection for relevant, realistic content
    3. Content validation and quality filtering
    4. Fallback strategies when LLM generation fails
    5. Caching and performance optimization
    
    The generator works with the LLM utility to create enterprise-grade content
    that follows real-world patterns and business contexts.
    """
    
    def __init__(self, config: Dict[str, Any], org_config: OrganizationConfig):
        """
        Initialize the content generator.
        
        Args:
            config: Application configuration
            org_config: Organization configuration
        """
        self.config = config
        self.org_config = org_config
        
        # Initialize LLM content generator
        self.llm_generator = LLMContentGenerator(config)
        
        # Content type mappings and fallback strategies
        self.content_type_mappings = {
            'task_name': {
                'required_context': ['department', 'project_type', 'section_name'],
                'fallback_strategy': 'pattern_based'
            },
            'task_description': {
                'required_context': ['task_name', 'department', 'project_type'],
                'fallback_strategy': 'template_based'
            },
            'comment': {
                'required_context': ['task_name', 'department', 'commenter_role'],
                'fallback_strategy': 'role_based'
            },
            'project_description': {
                'required_context': ['project_name', 'department', 'project_type'],
                'fallback_strategy': 'industry_based'
            },
            'team_description': {
                'required_context': ['team_name', 'department'],
                'fallback_strategy': 'department_based'
            }
        }
        
        # Content quality thresholds
        self.quality_thresholds = {
            'min_length': {
                'task_name': 5,
                'task_description': 20,
                'comment': 3,
                'project_description': 30
            },
            'max_length': {
                'task_name': 120,
                'task_description': 2000,
                'comment': 500,
                'project_description': 1000
            },
            'toxicity_threshold': 0.1  # Maximum allowed toxicity score
        }
        
        # Caching for content generation
        self.content_cache = {}
        self.cache_dir = Path(config.get('cache_dir', 'data/cache/content'))
        self.cache_dir.mkdir(parents=True, exist_ok=True)
    
    def _get_cache_key(self, content_type: str, context: Dict[str, Any]) -> str:
        """
        Generate cache key for content generation request.
        
        Args:
            content_type: Type of content to generate
            context: Context dictionary
            
        Returns:
            Cache key string
        """
        import hashlib
        context_str = json.dumps(context, sort_keys=True)
        key_str = f"{content_type}_{context_str}_{self.config.get('content_version', '1.0')}"
        return hashlib.md5(key_str.encode()).hexdigest()
    
    def _get_cached_content(self, cache_key: str) -> Optional[str]:
        """
        Get cached content if available and valid.
        
        Args:
            cache_key: Cache key to look up
            
        Returns:
            Cached content or None if not found/invalid
        """
        if cache_key in self.content_cache:
            return self.content_cache[cache_key]
        
        cache_file = self.cache_dir / f"{cache_key}.json"
        if cache_file.exists():
            try:
                with open(cache_file, 'r', encoding='utf-8') as f:
                    cache_data = json.load(f)
                
                # Check cache expiration (24 hours)
                cached_time = datetime.fromisoformat(cache_data['timestamp'])
                if (datetime.now() - cached_time).total_seconds() < 86400:
                    content = cache_data['content']
                    self.content_cache[cache_key] = content
                    return content
            except Exception as e:
                logger.warning(f"Error reading cache file {cache_file}: {str(e)}")
        
        return None
    
    def _cache_content(self, cache_key: str, content: str):
        """
        Cache generated content.
        
        Args:
            cache_key: Cache key
            content: Content to cache
        """
        self.content_cache[cache_key] = content
        
        try:
            cache_file = self.cache_dir / f"{cache_key}.json"
            cache_data = {
                'timestamp': datetime.now().isoformat(),
                'content': content,
                'content_type': cache_key.split('_')[0]
            }
            
            with open(cache_file, 'w', encoding='utf-8') as f:
                json.dump(cache_data, f, indent=2)
        except Exception as e:
            logger.warning(f"Error caching content: {str(e)}")
    
    def _validate_content_quality(self, content: str, content_type: str) -> Tuple[bool, str]:
        """
        Validate content quality against thresholds.
        
        Args:
            content: Generated content
            content_type: Type of content
            
        Returns:
            Tuple of (is_valid, reason)
        """
        if not content or len(content.strip()) == 0:
            return False, "Empty content"
        
        content = content.strip()
        
        # Check length requirements
        min_len = self.quality_thresholds['min_length'].get(content_type, 1)
        max_len = self.quality_thresholds['max_length'].get(content_type, 10000)
        
        if len(content) < min_len:
            return False, f"Content too short (min {min_len} characters)"
        if len(content) > max_len:
            return False, f"Content too long (max {max_len} characters)"
        
        # Check for toxic content
        toxic_keywords = ['hate', 'violence', 'discrimination', 'harassment', 'illegal', 
                         'pornography', 'weapon', 'drug', 'suicide', 'self-harm']
        
        content_lower = content.lower()
        for keyword in toxic_keywords:
            if keyword in content_lower:
                return False, f"Contains toxic content: {keyword}"
        
        # Check for placeholder text
        placeholder_patterns = ['lorem ipsum', 'dummy text', 'placeholder', 'insert text here']
        for pattern in placeholder_patterns:
            if pattern in content_lower:
                return False, f"Contains placeholder text: {pattern}"
        
        return True, "Valid content"
    
    def generate_task_name(self, department: str, project_type: str, section_name: str, 
                          context: Dict[str, Any] = None) -> str:
        """
        Generate a realistic task name using LLM.
        
        Args:
            department: Department name
            project_type: Project type
            section_name: Section name
            context: Additional context
            
        Returns:
            Generated task name
        """
        context = context or {}
        context.update({
            'department': department,
            'project_type': project_type,
            'section_name': section_name,
            'industry': self.org_config.industry
        })
        
        cache_key = self._get_cache_key('task_name', context)
        cached_content = self._get_cached_content(cache_key)
        if cached_content:
            return cached_content
        
        try:
            # Generate content using LLM
            content = self.llm_generator.generate_task_name(
                department=department,
                project_type=project_type,
                section_name=section_name,
                context=context
            )
            
            # Validate quality
            is_valid, reason = self._validate_content_quality(content, 'task_name')
            if not is_valid:
                logger.warning(f"Invalid task name generated: {reason}. Using fallback.")
                content = self._get_fallback_task_name(department, project_type, section_name, context)
            
            # Cache the result
            self._cache_content(cache_key, content)
            return content
            
        except Exception as e:
            logger.error(f"Error generating task name: {str(e)}. Using fallback.")
            return self._get_fallback_task_name(department, project_type, section_name, context)
    
    def _get_fallback_task_name(self, department: str, project_type: str, section_name: str, 
                              context: Dict[str, Any]) -> str:
        """
        Generate fallback task name when LLM generation fails.
        
        Args:
            department: Department name
            project_type: Project type
            section_name: Section name
            context: Context dictionary
            
        Returns:
            Fallback task name
        """
        fallback_patterns = {
            'engineering': {
                'sprint': [
                    'Implement {feature} module',
                    'Fix bug in {component}',
                    'Refactor {service} code',
                    'Write tests for {feature}',
                    'Optimize {metric} performance'
                ],
                'bug_tracking': [
                    'Fix critical bug in {component}',
                    'Resolve {issue_type} in {module}',
                    'Patch security vulnerability in {service}',
                    'Fix UI rendering issue',
                    'Resolve data inconsistency'
                ]
            },
            'marketing': {
                'campaign': [
                    'Create {asset_type} for {campaign}',
                    'Design social media graphics',
                    'Write email marketing copy',
                    'Schedule {channel} posts',
                    'Analyze campaign performance'
                ],
                'content_calendar': [
                    'Write blog post about {topic}',
                    'Create social media content',
                    'Edit video script',
                    'Schedule content publication',
                    'Optimize SEO keywords'
                ]
            },
            'product': {
                'roadmap_planning': [
                    'Define requirements for {feature}',
                    'Prioritize backlog items',
                    'Research market opportunities',
                    'Create product specification',
                    'Plan quarterly roadmap'
                ],
                'user_research': [
                    'Conduct user interviews for {feature}',
                    'Analyze user feedback',
                    'Create user personas',
                    'Test prototype usability',
                    'Synthesize research findings'
                ]
            },
            'sales': {
                'lead_generation': [
                    'Research leads in {territory}',
                    'Qualify leads from {source}',
                    'Create sales collateral',
                    'Follow up with prospects',
                    'Schedule demo calls'
                ],
                'sales_pipeline': [
                    'Demo product to {company}',
                    'Send proposal to {prospect}',
                    'Negotiate contract terms',
                    'Close deal with {account}',
                    'Handle customer objections'
                ]
            },
            'operations': {
                'process_improvement': [
                    'Document {process} workflow',
                    'Identify bottlenecks in {area}',
                    'Implement {solution} for {process}',
                    'Train team on {procedure}',
                    'Measure process efficiency'
                ],
                'budget_planning': [
                    'Forecast budget for {category}',
                    'Analyze spending trends',
                    'Prepare budget review',
                    'Track expenses against budget',
                    'Identify cost savings opportunities'
                ]
            }
        }
        
        # Get appropriate patterns
        dept_patterns = fallback_patterns.get(department, fallback_patterns['engineering'])
        type_patterns = dept_patterns.get(project_type, dept_patterns.get('sprint', [
            'Complete project task',
            'Review project deliverable',
            'Update project documentation',
            'Coordinate with team members',
            'Prepare project status report'
        ]))
        
        # Select random pattern and fill in context
        pattern = random.choice(type_patterns)
        context_params = {
            'feature': random.choice(['user authentication', 'search functionality', 'mobile responsiveness', 'data visualization']),
            'component': random.choice(['API', 'frontend', 'backend', 'database', 'authentication']),
            'service': random.choice(['payment processing', 'notification system', 'user management', 'reporting']),
            'metric': random.choice(['response time', 'error rate', 'conversion rate', 'load time']),
            'issue_type': random.choice(['null pointer', 'race condition', 'memory leak', 'UI glitch']),
            'module': random.choice(['user management', 'payment processing', 'report generation', 'notification system']),
            'asset_type': random.choice(['landing page', 'email template', 'social media posts', 'blog content']),
            'campaign': random.choice(['Q1 Launch', 'Summer Promotion', 'Holiday Campaign', 'Product Awareness']),
            'channel': random.choice(['Facebook', 'Instagram', 'LinkedIn', 'Twitter', 'Email']),
            'topic': random.choice(['industry trends', 'product updates', 'customer stories', 'best practices']),
            'territory': random.choice(['North America', 'EMEA', 'APAC', 'Global']),
            'source': random.choice(['website', 'webinar', 'conference', 'partner referral']),
            'company': random.choice(['Acme Corp', 'TechCo', 'StartupX', 'Global Inc']),
            'prospect': random.choice(['enterprise lead', 'mid-market prospect', 'SMB opportunity']),
            'account': random.choice(['enterprise client', 'mid-market account', 'SMB customer']),
            'process': random.choice(['onboarding', 'approval workflow', 'reporting', 'budgeting']),
            'area': random.choice(['sales process', 'customer support', 'HR operations', 'finance workflows']),
            'solution': random.choice(['automation script', 'new workflow', 'training program', 'tool integration']),
            'procedure': random.choice(['expense reporting', 'time tracking', 'leave management', 'onboarding']),
            'category': random.choice(['marketing', 'sales', 'R&D', 'operations', 'overhead'])
        }
        
        try:
            return pattern.format(**context_params)
        except KeyError:
            return random.choice(type_patterns).replace('{', '').replace('}', '')
    
    def generate_task_description(self, task_name: str, department: str, project_type: str, 
                                 context: Dict[str, Any] = None) -> Optional[str]:
        """
        Generate a realistic task description using LLM.
        
        Args:
            task_name: Task name
            department: Department name
            project_type: Project type
            context: Additional context
            
        Returns:
            Generated task description or None
        """
        # 20% chance of no description (industry benchmark)
        if random.random() < 0.2:
            return None
        
        context = context or {}
        context.update({
            'task_name': task_name,
            'department': department,
            'project_type': project_type,
            'industry': self.org_config.industry
        })
        
        cache_key = self._get_cache_key('task_description', context)
        cached_content = self._get_cached_content(cache_key)
        if cached_content:
            return cached_content
        
        try:
            # Generate content using LLM
            content = self.llm_generator.generate_task_description(
                task_name=task_name,
                department=department,
                project_type=project_type,
                context=context
            )
            
            if content:
                # Validate quality
                is_valid, reason = self._validate_content_quality(content, 'task_description')
                if not is_valid:
                    logger.warning(f"Invalid task description generated: {reason}. Using fallback or returning None.")
                    content = self._get_fallback_task_description(task_name, department, project_type, context)
            
            # Cache the result
            self._cache_content(cache_key, content or '')
            return content
            
        except Exception as e:
            logger.error(f"Error generating task description: {str(e)}. Using fallback.")
            return self._get_fallback_task_description(task_name, department, project_type, context)
    
    def _get_fallback_task_description(self, task_name: str, department: str, project_type: str, 
                                      context: Dict[str, Any]) -> Optional[str]:
        """
        Generate fallback task description when LLM generation fails.
        
        Args:
            task_name: Task name
            department: Department name
            project_type: Project type
            context: Context dictionary
            
        Returns:
            Fallback task description or None
        """
        # Simple fallback descriptions based on department
        fallback_descriptions = {
            'engineering': {
                'sprint': '{task_name}\n\n## Objective\nImplement the specified functionality according to requirements.\n\n## Approach\nFollow agile development practices with test-driven development.\n\n## Success Criteria\n- All tests pass\n- Code reviewed and approved\n- Documentation updated\n- Performance benchmarks met',
                'bug_tracking': '{task_name}\n\n## Context\nBug reported by users affecting core functionality.\n\n## Steps to Reproduce\n1. Navigate to the affected page\n2. Perform the action\n3. Observe the error\n\n## Expected Behavior\nSystem should handle the input gracefully without errors.\n\n## Impact\nHigh - affects 25% of users'
            },
            'marketing': {
                'campaign': '{task_name}\n\n## Objective\nCreate engaging content for the marketing campaign.\n\n## Target Audience\n{target_audience}\n\n## Key Messages\n- Primary message\n- Secondary message\n- Call to action\n\n## Deliverables\n- Content copy\n- Visual assets\n- Performance metrics'
            },
            'product': {
                'roadmap_planning': '{task_name}\n\n## Background\nBased on user feedback and market analysis.\n\n## Requirements\n- Functional requirements\n- Non-functional requirements\n- Success metrics\n\n## Timeline\nQ{quarter} 2026\n\n## Dependencies\nStakeholder approval required'
            }
        }
        
        dept_fallbacks = fallback_descriptions.get(department, {})
        desc_template = dept_fallbacks.get(project_type, dept_fallbacks.get('sprint', '{task_name}\n\n## Description\nTask description will be added during implementation.\n\n## Notes\nStandard task for project completion.'))
        
        # Fill in context
        context_params = {
            'task_name': task_name,
            'target_audience': random.choice(['enterprise customers', 'small business owners', 'developers', 'end users']),
            'quarter': random.randint(1, 4)
        }
        
        try:
            return desc_template.format(**context_params)
        except KeyError:
            return desc_template.replace('{', '').replace('}', '')
    
    def generate_comment(self, task_name: str, department: str, commenter_role: str, 
                       context: Dict[str, Any] = None) -> str:
        """
        Generate a realistic comment using LLM.
        
        Args:
            task_name: Task name being commented on
            department: Department name
            commenter_role: Role of commenter
            context: Additional context
            
        Returns:
            Generated comment
        """
        context = context or {}
        context.update({
            'task_name': task_name,
            'department': department,
            'commenter_role': commenter_role,
            'industry': self.org_config.industry
        })
        
        cache_key = self._get_cache_key('comment', context)
        cached_content = self._get_cached_content(cache_key)
        if cached_content:
            return cached_content
        
        try:
            # Generate content using LLM
            content = self.llm_generator.generate_comment(
                task_name=task_name,
                department=department,
                commenter_role=commenter_role,
                context=context
            )
            
            # Validate quality
            is_valid, reason = self._validate_content_quality(content, 'comment')
            if not is_valid:
                logger.warning(f"Invalid comment generated: {reason}. Using fallback.")
                content = self._get_fallback_comment(task_name, department, commenter_role, context)
            
            # Cache the result
            self._cache_content(cache_key, content)
            return content
            
        except Exception as e:
            logger.error(f"Error generating comment: {str(e)}. Using fallback.")
            return self._get_fallback_comment(task_name, department, commenter_role, context)
    
    def _get_fallback_comment(self, task_name: str, department: str, commenter_role: str, 
                            context: Dict[str, Any]) -> str:
        """
        Generate fallback comment when LLM generation fails.
        
        Args:
            task_name: Task name
            department: Department name
            commenter_role: Commenter role
            context: Context dictionary
            
        Returns:
            Fallback comment
        """
        comment_types = {
            'progress': [
                'Making good progress on this task. Should be completed by end of week.',
                'I\'ve completed the first phase and am now working on the implementation.',
                'This is on track. I\'ll update again tomorrow with more details.',
                'Almost done with the core functionality. Just need to add tests.',
                'I\'ve integrated the component and it\'s working as expected.'
            ],
            'question': [
                'Could you clarify the requirements for this task?',
                'I need more details about the expected behavior.',
                'What are the performance requirements for this feature?',
                'Can we discuss the approach before I proceed?',
                'I have a question about the edge cases we need to handle.'
            ],
            'feedback': [
                'The implementation looks solid. I suggest adding more unit tests.',
                'Good work on this. I have a few suggestions for improvement.',
                'The code is well-structured. Let\'s add some documentation.',
                'This meets the requirements. I found one minor issue that needs fixing.',
                'Great progress. The performance improvements are significant.'
            ],
            'blocker': [
                'This is blocked waiting for input from the backend team.',
                'I need access to the staging environment to test this properly.',
                'Waiting for design assets from the UX team before I can proceed.',
                'This requires approval from the security team before implementation.',
                'Blocked on external dependency that\'s currently down.'
            ]
        }
        
        # Choose comment type based on role and department
        if 'manager' in commenter_role.lower() or 'lead' in commenter_role.lower():
            comment_type = random.choice(['feedback', 'question'])
        elif 'engineer' in department.lower() or 'developer' in commenter_role.lower():
            comment_type = random.choice(['progress', 'blocker'])
        else:
            comment_type = random.choice(list(comment_types.keys()))
        
        return random.choice(comment_types[comment_type])
    
    def batch_generate_content(self, content_requests: List[Dict[str, Any]], 
                             max_concurrent: int = 5) -> List[Optional[str]]:
        """
        Batch generate content for multiple requests.
        
        Args:
            content_requests: List of content request dictionaries
            max_concurrent: Maximum concurrent requests
            
        Returns:
            List of generated content in same order as requests
        """
        results = [None] * len(content_requests)
        
        # Process in batches
        for i in range(0, len(content_requests), max_concurrent):
            batch = content_requests[i:i + max_concurrent]
            batch_results = []
            
            for req in batch:
                try:
                    content_type = req.get('content_type', 'task_name')
                    
                    if content_type == 'task_name':
                        content = self.generate_task_name(
                            department=req['department'],
                            project_type=req['project_type'],
                            section_name=req['section_name'],
                            context=req.get('context', {})
                        )
                    elif content_type == 'task_description':
                        content = self.generate_task_description(
                            task_name=req['task_name'],
                            department=req['department'],
                            project_type=req['project_type'],
                            context=req.get('context', {})
                        )
                    elif content_type == 'comment':
                        content = self.generate_comment(
                            task_name=req['task_name'],
                            department=req['department'],
                            commenter_role=req['commenter_role'],
                            context=req.get('context', {})
                        )
                    else:
                        logger.warning(f"Unknown content type: {content_type}. Using fallback.")
                        content = f"Generated content for {content_type}"
                    
                    batch_results.append(content)
                except Exception as e:
                    logger.error(f"Error generating content in batch: {str(e)}")
                    batch_results.append(None)
            
            # Add delay between batches to avoid rate limits
            if i + max_concurrent < len(content_requests):
                time.sleep(1)
            
            results[i:i + len(batch)] = batch_results
        
        return results
    
    def get_generation_stats(self) -> Dict[str, Any]:
        """
        Get content generation statistics.
        
        Returns:
            Dictionary with generation metrics
        """
        return {
            'total_requests': getattr(self, '_total_requests', 0),
            'successful_requests': getattr(self, '_successful_requests', 0),
            'fallback_count': getattr(self, '_fallback_count', 0),
            'cache_hits': len(self.content_cache),
            'llm_stats': self.llm_generator.get_token_usage_stats('')  # This would need to be implemented
        }
    
    def close(self):
        """Cleanup resources."""
        self.llm_generator.close()
        logger.info("Content generator closed")

# Example usage and testing
if __name__ == "__main__":
    # Setup logging for testing
    logging.basicConfig(level=logging.INFO)
    
    # Mock configuration
    mock_config = {
        'openai_api_key': 'your_openai_api_key_here',  # Replace with actual key for testing
        'openai_model': 'gpt-4-turbo',
        'openai_temperature': 0.7,
        'openai_max_tokens': 500,
        'cache_dir': 'data/cache',
        'debug_mode': True
    }
    
    # Mock organization configuration
    mock_org_config = type('OrganizationConfig', (), {
        'name': 'Test Organization',
        'domain': 'test.org',
        'size_min': 10,
        'size_max': 20,
        'industry': 'b2b_saas'
    })
    
    try:
        print("=== Content Generator Testing ===\n")
        
        # Create content generator
        generator = ContentGenerator(mock_config, mock_org_config)
        
        # Test task name generation
        print("Task Name Generation Tests:")
        test_cases = [
            {'department': 'engineering', 'project_type': 'sprint', 'section_name': 'Backlog'},
            {'department': 'marketing', 'project_type': 'campaign', 'section_name': 'Content Creation'},
            {'department': 'product', 'project_type': 'roadmap_planning', 'section_name': 'Q1 Planning'},
            {'department': 'sales', 'project_type': 'lead_generation', 'section_name': 'Prospecting'},
            {'department': 'operations', 'project_type': 'process_improvement', 'section_name': 'Analysis'}
        ]
        
        for i, test_case in enumerate(test_cases, 1):
            task_name = generator.generate_task_name(**test_case)
            print(f"  Test {i}: {task_name}")
        
        # Test task description generation
        print("\nTask Description Generation Tests:")
        description_tests = [
            {'task_name': 'Implement user authentication', 'department': 'engineering', 'project_type': 'sprint'},
            {'task_name': 'Create marketing campaign', 'department': 'marketing', 'project_type': 'campaign'},
            {'task_name': 'Define product requirements', 'department': 'product', 'project_type': 'roadmap_planning'}
        ]
        
        for i, test_case in enumerate(description_tests, 1):
            description = generator.generate_task_description(**test_case)
            print(f"  Test {i}: '{test_case['task_name']}'")
            if description:
                preview = description[:150] + '...' if len(description) > 150 else description
                print(f"    Description preview: {preview}")
            else:
                print("    No description generated")
        
        # Test comment generation
        print("\nComment Generation Tests:")
        comment_tests = [
            {'task_name': 'Fix login bug', 'department': 'engineering', 'commenter_role': 'Senior Engineer'},
            {'task_name': 'Design homepage', 'department': 'marketing', 'commenter_role': 'Marketing Manager'},
            {'task_name': 'Plan Q1 roadmap', 'department': 'product', 'commenter_role': 'Product Director'}
        ]
        
        for i, test_case in enumerate(comment_tests, 1):
            comment = generator.generate_comment(**test_case)
            print(f"  Test {i}: '{test_case['task_name']}'")
            print(f"    Comment: {comment}")
        
        # Test batch generation
        print("\nBatch Generation Test:")
        batch_requests = [
            {'content_type': 'task_name', 'department': 'engineering', 'project_type': 'sprint', 'section_name': 'In Progress'},
            {'content_type': 'task_name', 'department': 'engineering', 'project_type': 'sprint', 'section_name': 'In Review'},
            {'content_type': 'task_name', 'department': 'engineering', 'project_type': 'sprint', 'section_name': 'Done'},
            {'content_type': 'task_name', 'department': 'engineering', 'project_type': 'sprint', 'section_name': 'Backlog'},
            {'content_type': 'task_name', 'department': 'engineering', 'project_type': 'sprint', 'section_name': 'Ready'}
        ]
        
        batch_results = generator.batch_generate_content(batch_requests, max_concurrent=3)
        for i, result in enumerate(batch_results, 1):
            print(f"  Batch result {i}: {result}")
        
        print("\n✅ All content generator tests completed successfully!")
        
    except ValueError as e:
        print(f"Configuration error: {str(e)}")
        print("Please set a valid OPENAI_API_KEY in the configuration to run tests.")
    except Exception as e:
        print(f"Unexpected error: {str(e)}")
        import traceback
        traceback.print_exc()
    finally:
        try:
            generator.close()
        except:
            pass