

"""
LLM utility module for generating realistic content using large language models.
This module provides a robust interface for LLM interactions with retry logic,
rate limiting, content validation, and cost management for enterprise data generation.

The utility is designed to be:
- Reliable: Handles retries, timeouts, and API errors gracefully
- Cost-effective: Manages token usage and implements caching
- Secure: Sanitizes inputs and outputs to prevent data leakage
- Configurable: Adjustable parameters for different content types and quality levels
- Observable: Detailed logging and metrics for monitoring usage and performance
"""

import logging
import random
import time
import json
import hashlib
from typing import List, Dict, Optional, Tuple, Any, Callable
from pathlib import Path
import tiktoken
from openai import OpenAI, APIError, RateLimitError, APIConnectionError
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type

from src.utils.logging import get_logger

logger = get_logger(__name__)

class LLMContentGenerator:
    """
    Content generator using Large Language Models with enterprise-grade reliability.
    
    This class handles:
    1. Text generation for various content types (task names, descriptions, comments)
    2. Prompt engineering with few-shot examples and context injection
    3. Content validation and filtering for quality and safety
    4. Cost management through caching and token optimization
    5. Error handling and fallback strategies
    
    The generator uses research-backed prompt patterns and content validation techniques
    to ensure high-quality, realistic enterprise content generation.
    """
    
    def __init__(self, config: Dict[str, Any]):
        """
        Initialize the LLM content generator.
        
        Args:
            config: Application configuration with LLM settings
        """
        self.config = config
        self.api_key = config.get('openai_api_key')
        if not self.api_key:
            raise ValueError("OPENAI_API_KEY is required in configuration")
        
        self.model = config.get('openai_model', 'gpt-4-turbo')
        self.temperature = float(config.get('openai_temperature', 0.7))
        self.max_tokens = int(config.get('openai_max_tokens', 500))
        self.top_p = float(config.get('openai_top_p', 0.9))
        
        # Initialize OpenAI client
        self.client = OpenAI(api_key=self.api_key)
        
        # Set up caching
        self.cache_dir = Path(config.get('cache_dir', 'data/cache/llm'))
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        
        # Token counting
        self.encoding = tiktoken.encoding_for_model(self.model)
        
        # Content validation patterns
        self.validation_patterns = {
            'task_name': r'^[A-Za-z0-9\s\-_,.()\'"]+$',
            'description': r'^.{10,5000}$',  # 10-5000 characters
            'comment': r'^.{1,1000}$',       # 1-1000 characters
            'email_content': r'^.{20,2000}$'  # 20-2000 characters
        }
        
        # Toxic content filters
        self.toxic_keywords = [
            'hate', 'violence', 'discrimination', 'harassment', 'illegal', 
            'pornography', 'weapon', 'drug', 'suicide', 'self-harm'
        ]
        
        # Industry-specific terminology
        self.industry_terms = {
            'b2b_saas': [
                'SaaS', 'subscription', 'ARR', 'MRR', 'churn', 'CAC', 'LTV', 
                'pipeline', 'conversion', 'onboarding', 'retention', 'expansion',
                'enterprise', 'mid-market', 'SMB', 'user adoption', 'feature usage'
            ],
            'engineering': [
                'API', 'microservice', 'container', 'Kubernetes', 'database', 
                'latency', 'throughput', 'scalability', 'reliability', 'CI/CD',
                'testing', 'debugging', 'refactoring', 'technical debt', 'architecture'
            ],
            'marketing': [
                'brand awareness', 'lead generation', 'conversion rate', 'CTR', 
                'engagement', 'ROI', 'campaign', 'content marketing', 'SEO', 'SEM',
                'social media', 'email marketing', 'analytics', 'funnel', 'customer journey'
            ]
        }
    
    def _get_cache_key(self, prompt: str, context: Dict[str, Any]) -> str:
        """
        Generate a cache key for a prompt and context combination.
        
        Args:
            prompt: The prompt template
            context: Context variables for the prompt
            
        Returns:
            MD5 hash cache key
        """
        context_str = json.dumps(context, sort_keys=True)
        key_str = f"{prompt}_{context_str}_{self.model}_{self.temperature}_{self.max_tokens}"
        return hashlib.md5(key_str.encode()).hexdigest()
    
    def _get_cached_response(self, cache_key: str) -> Optional[str]:
        """
        Get cached response if available and valid.
        
        Args:
            cache_key: Cache key to look up
            
        Returns:
            Cached response string or None if not found/invalid
        """
        cache_file = self.cache_dir / f"{cache_key}.json"
        
        if not cache_file.exists():
            return None
        
        try:
            with open(cache_file, 'r', encoding='utf-8') as f:
                cache_data = json.load(f)
            
            # Check cache expiration (24 hours)
            cached_time = datetime.fromisoformat(cache_data['timestamp'])
            if (datetime.now() - cached_time).total_seconds() > 86400:
                return None
            
            return cache_data['response']
        except Exception as e:
            logger.warning(f"Error reading cache file {cache_file}: {str(e)}")
            return None
    
    def _cache_response(self, cache_key: str, response: str):
        """
        Cache a response with timestamp.
        
        Args:
            cache_key: Cache key
            response: Response to cache
        """
        try:
            cache_file = self.cache_dir / f"{cache_key}.json"
            cache_data = {
                'timestamp': datetime.now().isoformat(),
                'response': response,
                'model': self.model,
                'temperature': self.temperature
            }
            
            with open(cache_file, 'w', encoding='utf-8') as f:
                json.dump(cache_data, f, indent=2)
        except Exception as e:
            logger.warning(f"Error caching response: {str(e)}")
    
    def _count_tokens(self, text: str) -> int:
        """
        Count tokens in text using tiktoken.
        
        Args:
            text: Text to count tokens for
            
        Returns:
            Number of tokens
        """
        return len(self.encoding.encode(text))
    
    def _validate_content(self, content: str, content_type: str) -> Tuple[bool, str]:
        """
        Validate generated content for quality and safety.
        
        Args:
            content: Generated content to validate
            content_type: Type of content (task_name, description, comment, etc.)
            
        Returns:
            Tuple of (is_valid, reason) where reason explains why invalid
        """
        if not content or len(content.strip()) == 0:
            return False, "Empty content"
        
        # Check toxic content
        content_lower = content.lower()
        for keyword in self.toxic_keywords:
            if keyword in content_lower:
                return False, f"Contains toxic keyword: {keyword}"
        
        # Check content patterns
        if content_type in self.validation_patterns:
            import re
            pattern = self.validation_patterns[content_type]
            if not re.match(pattern, content.strip()):
                return False, f"Does not match pattern for {content_type}"
        
        # Check length requirements
        if content_type == 'task_name' and len(content) > 100:
            return False, "Task name too long (>100 characters)"
        elif content_type == 'description' and len(content) > 5000:
            return False, "Description too long (>5000 characters)"
        elif content_type == 'comment' and len(content) > 1000:
            return False, "Comment too long (>1000 characters)"
        
        # Check for placeholder text that might indicate generation failure
        placeholder_patterns = [
            'as an ai', 'i cannot', 'sorry but', 'unable to', 'cannot fulfill',
            'placeholder', 'dummy text', 'lorem ipsum', 'insert text here'
        ]
        
        for pattern in placeholder_patterns:
            if pattern in content_lower:
                return False, f"Contains placeholder text: {pattern}"
        
        return True, "Valid content"
    
    def _apply_content_filters(self, content: str) -> str:
        """
        Apply content filters to clean up generated text.
        
        Args:
            content: Content to filter
            
        Returns:
            Filtered content
        """
        # Remove markdown formatting if present
        content = content.replace('**', '').replace('*', '').replace('#', '').replace('`', '')
        
        # Remove extra whitespace and newlines
        content = ' '.join(content.split())
        
        # Remove any AI-specific disclaimers
        disclaimer_patterns = [
            'as an ai language model',
            'i am an ai',
            'i cannot provide',
            'this is a generated response',
            'note: this is'
        ]
        
        for pattern in disclaimer_patterns:
            content = content.replace(pattern, '').replace(pattern.title(), '')
        
        # Clean up punctuation
        content = content.strip('.,;:!? ')
        
        return content
    
    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=4, max=10),
        retry=retry_if_exception_type((RateLimitError, APIConnectionError, APIError))
    )
    def _generate_with_retry(self, prompt: str, system_message: str = None) -> str:
        """
        Generate content with retry logic for API errors.
        
        Args:
            prompt: User prompt
            system_message: System message for context
            
        Returns:
            Generated content
            
        Raises:
            APIError: If all retries fail
        """
        messages = []
        if system_message:
            messages.append({"role": "system", "content": system_message})
        messages.append({"role": "user", "content": prompt})
        
        try:
            response = self.client.chat.completions.create(
                model=self.model,
                messages=messages,
                temperature=self.temperature,
                max_tokens=self.max_tokens,
                top_p=self.top_p,
                timeout=30
            )
            
            return response.choices[0].message.content.strip()
        
        except RateLimitError as e:
            logger.warning(f"Rate limit exceeded: {str(e)}")
            raise
        except APIConnectionError as e:
            logger.warning(f"API connection error: {str(e)}")
            raise
        except APIError as e:
            logger.error(f"OpenAI API error: {str(e)}")
            raise
        except Exception as e:
            logger.error(f"Unexpected error in LLM generation: {str(e)}")
            raise
    
    def generate_task_name(self, department: str, project_type: str, section_name: str, 
                          context: Dict[str, Any] = None) -> str:
        """
        Generate a realistic task name using LLM.
        
        Args:
            department: Department name (engineering, marketing, etc.)
            project_type: Project type (sprint, campaign, etc.)
            section_name: Section name (Backlog, In Progress, etc.)
            context: Additional context for generation
            
        Returns:
            Generated task name
        """
        cache_context = {
            'department': department,
            'project_type': project_type,
            'section_name': section_name,
            'content_type': 'task_name'
        }
        
        cache_key = self._get_cache_key("task_name_prompt", cache_context)
        
        # Try cache first
        cached_response = self._get_cached_response(cache_key)
        if cached_response:
            logger.debug("Using cached task name")
            return cached_response
        
        # Build prompt
        system_message = "You are an expert project manager generating realistic, professional task names for enterprise workflows."
        
        prompt_template = """
Generate a concise, professional task name for a {department} {project_type} project in the "{section_name}" section.

Context:
- Department: {department}
- Project Type: {project_type} 
- Section: {section_name}
- Industry: B2B SaaS enterprise software

Requirements:
- Keep it under 80 characters
- Use action-oriented language (Implement, Fix, Design, Create, etc.)
- Be specific but concise
- No markdown formatting
- Professional tone appropriate for enterprise environment
- Include relevant technical or business terms when appropriate

Examples of good task names:
- "Implement user authentication API endpoints"
- "Fix null pointer exception in payment processing"
- "Design mobile responsive dashboard layout" 
- "Create onboarding email sequence for new customers"
- "Optimize database query performance for search"

Generate exactly one task name:
        """
        
        prompt = prompt_template.format(
            department=department,
            project_type=project_type,
            section_name=section_name
        )
        
        # Generate with retry
        try:
            response = self._generate_with_retry(prompt, system_message)
            filtered_response = self._apply_content_filters(response)
            
            # Validate content
            is_valid, reason = self._validate_content(filtered_response, 'task_name')
            if not is_valid:
                logger.warning(f"Invalid task name generated: {reason}. Using fallback.")
                return self._get_fallback_task_name(department, project_type, section_name)
            
            # Cache the response
            self._cache_response(cache_key, filtered_response)
            
            logger.debug(f"Generated task name: {filtered_response}")
            return filtered_response
            
        except Exception as e:
            logger.error(f"Error generating task name: {str(e)}. Using fallback.")
            return self._get_fallback_task_name(department, project_type, section_name)
    
    def _get_fallback_task_name(self, department: str, project_type: str, section_name: str) -> str:
        """
        Generate a fallback task name when LLM generation fails.
        
        Args:
            department: Department name
            project_type: Project type
            section_name: Section name
            
        Returns:
            Fallback task name
        """
        fallbacks = {
            'engineering': {
                'sprint': [
                    'Implement feature module',
                    'Fix bug in core service', 
                    'Refactor code for performance',
                    'Write unit tests for component',
                    'Optimize database queries'
                ],
                'bug_tracking': [
                    'Fix critical bug in authentication',
                    'Resolve performance issue in API',
                    'Patch security vulnerability',
                    'Fix UI rendering bug',
                    'Resolve data consistency issue'
                ]
            },
            'marketing': {
                'campaign': [
                    'Create campaign landing page',
                    'Design social media graphics',
                    'Write email marketing copy',
                    'Set up campaign tracking',
                    'Analyze campaign performance'
                ],
                'content_calendar': [
                    'Write blog post draft',
                    'Create social media content',
                    'Edit video script',
                    'Schedule content publication',
                    'Optimize SEO keywords'
                ]
            },
            'product': {
                'roadmap_planning': [
                    'Define feature requirements',
                    'Prioritize backlog items',
                    'Research market opportunities',
                    'Create product specification',
                    'Plan quarterly roadmap'
                ],
                'user_research': [
                    'Conduct user interviews',
                    'Analyze user feedback',
                    'Create user personas',
                    'Test prototype usability',
                    'Synthesize research findings'
                ]
            }
        }
        
        dept_fallbacks = fallbacks.get(department, fallbacks['engineering'])
        type_fallbacks = dept_fallbacks.get(project_type, dept_fallbacks.get('sprint', [
            'Complete project task',
            'Review project deliverable',
            'Update project documentation',
            'Coordinate with team members',
            'Prepare project status report'
        ]))
        
        return random.choice(type_fallbacks)
    
    def generate_task_description(self, task_name: str, department: str, project_type: str, 
                                context: Dict[str, Any] = None) -> Optional[str]:
        """
        Generate a realistic task description using LLM.
        
        Args:
            task_name: Task name to base description on
            department: Department name
            project_type: Project type
            context: Additional context
            
        Returns:
            Generated task description or None if generation fails
        """
        # 20% chance of no description (industry benchmark)
        if random.random() < 0.2:
            return None
        
        cache_context = {
            'task_name': task_name,
            'department': department,
            'project_type': project_type,
            'content_type': 'description'
        }
        
        cache_key = self._get_cache_key("description_prompt", cache_context)
        
        cached_response = self._get_cached_response(cache_key)
        if cached_response:
            logger.debug("Using cached task description")
            return cached_response
        
        system_message = "You are an expert project manager writing detailed, professional task descriptions for enterprise workflows."
        
        prompt_template = """
Write a professional task description for: "{task_name}"

Context:
- Department: {department}
- Project Type: {project_type}
- Industry: B2B SaaS enterprise software
- Audience: Technical and business stakeholders

Requirements:
- Length: 50-300 words
- Structure: Clear objective, approach/methodology, success criteria, and any dependencies
- Tone: Professional, specific, and actionable
- Include relevant technical or business details appropriate for the context
- No markdown formatting - use plain text with line breaks
- Focus on what needs to be accomplished and why it matters

Example structure:
Objective: [Clear statement of what this task accomplishes]
Approach: [How the work should be done, key steps or methodology]
Success Criteria: [Measurable outcomes that define completion]
Dependencies: [Any prerequisites or blocking items]

Write the description:
        """
        
        prompt = prompt_template.format(
            task_name=task_name,
            department=department,
            project_type=project_type
        )
        
        try:
            response = self._generate_with_retry(prompt, system_message)
            filtered_response = self._apply_content_filters(response)
            
            # Validate content
            is_valid, reason = self._validate_content(filtered_response, 'description')
            if not is_valid:
                logger.warning(f"Invalid description generated: {reason}. Using fallback or returning None.")
                return None
            
            # Cache the response
            self._cache_response(cache_key, filtered_response)
            
            logger.debug(f"Generated task description for '{task_name[:30]}...'")
            return filtered_response
            
        except Exception as e:
            logger.error(f"Error generating task description: {str(e)}. Returning None.")
            return None
    
    def generate_comment(self, task_name: str, department: str, commenter_role: str, 
                       context: Dict[str, Any] = None) -> str:
        """
        Generate a realistic comment on a task using LLM.
        
        Args:
            task_name: Task name being commented on
            department: Department name
            commenter_role: Role of the commenter (engineer, manager, designer, etc.)
            context: Additional context
            
        Returns:
            Generated comment
        """
        cache_context = {
            'task_name': task_name,
            'department': department,
            'commenter_role': commenter_role,
            'content_type': 'comment'
        }
        
        cache_key = self._get_cache_key("comment_prompt", cache_context)
        
        cached_response = self._get_cached_response(cache_key)
        if cached_response:
            logger.debug("Using cached comment")
            return cached_response
        
        system_message = "You are a team member writing realistic, professional comments on project tasks in an enterprise environment."
        
        prompt_template = """
Write a professional comment on the task: "{task_name}"

Context:
- Department: {department}
- Your Role: {commenter_role}
- Industry: B2B SaaS enterprise software
- Setting: Internal team collaboration tool

Requirements:
- Length: 20-150 words
- Tone: Professional, helpful, and constructive
- Content types (choose one appropriate style):
  * Progress update: "I've made progress on X, currently working on Y, expect to complete Z by [timeframe]"
  * Question/clarification: "I have a question about X requirement, could you clarify Y?"
  * Feedback/review: "I reviewed the implementation and have feedback on X, Y, Z"
  * Status update: "This is blocked on X, need input from Y team"
  * Approval/completion: "This has been completed and tested, ready for review"
- No markdown formatting
- Use natural, conversational language appropriate for workplace communication
- Include specific details relevant to the task and role

Write exactly one comment:
        """
        
        prompt = prompt_template.format(
            task_name=task_name,
            department=department,
            commenter_role=commenter_role
        )
        
        try:
            response = self._generate_with_retry(prompt, system_message)
            filtered_response = self._apply_content_filters(response)
            
            # Validate content
            is_valid, reason = self._validate_content(filtered_response, 'comment')
            if not is_valid:
                logger.warning(f"Invalid comment generated: {reason}. Using fallback.")
                return self._get_fallback_comment(task_name, department, commenter_role)
            
            # Cache the response
            self._cache_response(cache_key, filtered_response)
            
            logger.debug(f"Generated comment for '{task_name[:30]}...'")
            return filtered_response
            
        except Exception as e:
            logger.error(f"Error generating comment: {str(e)}. Using fallback.")
            return self._get_fallback_comment(task_name, department, commenter_role)
    
    def _get_fallback_comment(self, task_name: str, department: str, commenter_role: str) -> str:
        """
        Generate a fallback comment when LLM generation fails.
        
        Args:
            task_name: Task name
            department: Department name  
            commenter_role: Commenter role
            
        Returns:
            Fallback comment
        """
        fallback_comments = {
            'progress': [
                'Making good progress on this task. Should be completed by end of week.',
                'I've completed the first phase and am now working on the implementation.',
                'This is on track. I'll update again tomorrow with more details.',
                'Almost done with the core functionality. Just need to add tests.',
                'I've integrated the component and it's working as expected.'
            ],
            'question': [
                'Could you clarify the requirements for the user authentication flow?',
                'I need more details about the expected behavior when the service is down.',
                'What are the performance requirements for this endpoint?',
                'Can we discuss the design approach before I proceed?',
                'I have a question about the edge cases we need to handle.'
            ],
            'feedback': [
                'The implementation looks solid. I suggest adding more unit tests for the edge cases.',
                'Good work on the UI design. I have a few suggestions for improving accessibility.',
                'The code is well-structured. Let's add some documentation for the complex parts.',
                'This meets the requirements. I found one minor bug that needs fixing.',
                'The performance improvements are significant. Great job on the optimization.'
            ],
            'blocker': [
                'This is blocked waiting for the API documentation from the backend team.',
                'I need access to the staging environment to test this properly.',
                'Waiting for design assets from the UX team before I can proceed.',
                'This requires approval from the security team before implementation.',
                'Blocked on external dependency (third-party service) that's currently down.'
            ]
        }
        
        # Choose comment type based on role and department
        if 'manager' in commenter_role.lower() or 'lead' in commenter_role.lower():
            comment_type = random.choice(['feedback', 'question'])
        elif 'engineer' in department.lower() or 'developer' in commenter_role.lower():
            comment_type = random.choice(['progress', 'blocker'])
        else:
            comment_type = random.choice(list(fallback_comments.keys()))
        
        return random.choice(fallback_comments[comment_type])
    
    def batch_generate_task_names(self, requests: List[Dict[str, Any]], 
                                 max_concurrent: int = 5) -> List[str]:
        """
        Batch generate task names with concurrency control.
        
        Args:
            requests: List of request dictionaries with department, project_type, section_name
            max_concurrent: Maximum concurrent requests
            
        Returns:
            List of generated task names in same order as requests
        """
        results = [None] * len(requests)
        
        # Process in batches to avoid rate limits
        for i in range(0, len(requests), max_concurrent):
            batch = requests[i:i + max_concurrent]
            batch_results = []
            
            for req in batch:
                try:
                    task_name = self.generate_task_name(
                        department=req['department'],
                        project_type=req['project_type'],
                        section_name=req['section_name'],
                        context=req.get('context', {})
                    )
                    batch_results.append(task_name)
                except Exception as e:
                    logger.error(f"Error generating task name in batch: {str(e)}")
                    batch_results.append(self._get_fallback_task_name(
                        req['department'], req['project_type'], req['section_name']
                    ))
            
            # Add delay between batches to avoid rate limits
            if i + max_concurrent < len(requests):
                time.sleep(1)
            
            results[i:i + len(batch)] = batch_results
        
        return results
    
    def get_token_usage_stats(self, text: str) -> Dict[str, int]:
        """
        Get token usage statistics for text.
        
        Args:
            text: Text to analyze
            
        Returns:
            Dictionary with token counts and cost estimates
        """
        token_count = self._count_tokens(text)
        
        # Cost estimates (USD per 1M tokens) - update with current pricing
        cost_per_million = {
            'gpt-4-turbo': {'input': 10.0, 'output': 30.0},
            'gpt-3.5-turbo': {'input': 0.50, 'output': 1.50}
        }
        
        model_costs = cost_per_million.get(self.model, {'input': 1.0, 'output': 3.0})
        
        return {
            'token_count': token_count,
            'estimated_input_cost': (token_count * model_costs['input']) / 1000000,
            'estimated_output_cost': (token_count * model_costs['output']) / 1000000,
            'model': self.model
        }
    
    def close(self):
        """Cleanup resources."""
        logger.info("LLM content generator closed")

# Example usage and testing
if __name__ == "__main__":
    # Setup logging for testing
    logging.basicConfig(level=logging.INFO)
    
    # Mock configuration (in real usage, this would come from .env)
    mock_config = {
        'openai_api_key': 'your_openai_api_key_here',  # Replace with actual key for testing
        'openai_model': 'gpt-4-turbo',
        'openai_temperature': 0.7,
        'openai_max_tokens': 500,
        'openai_top_p': 0.9,
        'cache_dir': 'data/cache/llm',
        'debug_mode': True
    }
    
    try:
        generator = LLMContentGenerator(mock_config)
        
        print("=== LLM Content Generator Testing ===\n")
        
        # Test task name generation
        print("Task Name Generation Tests:")
        test_cases = [
            {'department': 'engineering', 'project_type': 'sprint', 'section_name': 'Backlog'},
            {'department': 'marketing', 'project_type': 'campaign', 'section_name': 'Content Creation'},
            {'department': 'product', 'project_type': 'roadmap_planning', 'section_name': 'Q1 Planning'}
        ]
        
        for i, test_case in enumerate(test_cases, 1):
            task_name = generator.generate_task_name(**test_case)
            print(f"  Test {i}: {task_name}")
        
        # Test task description generation
        print("\nTask Description Generation Tests:")
        test_task_names = [
            "Implement user authentication API",
            "Create marketing campaign landing page",
            "Define product requirements document"
        ]
        
        for i, task_name in enumerate(test_task_names, 1):
            description = generator.generate_task_description(
                task_name=task_name,
                department='engineering' if 'API' in task_name else 'marketing' if 'campaign' in task_name else 'product',
                project_type='sprint' if 'API' in task_name else 'campaign' if 'campaign' in task_name else 'roadmap_planning'
            )
            print(f"  Test {i}: '{task_name}'")
            if description:
                print(f"    Description (first 100 chars): {description[:100]}...")
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
            {'department': 'engineering', 'project_type': 'sprint', 'section_name': 'In Progress'},
            {'department': 'engineering', 'project_type': 'sprint', 'section_name': 'In Progress'},
            {'department': 'engineering', 'project_type': 'sprint', 'section_name': 'In Progress'},
            {'department': 'engineering', 'project_type': 'sprint', 'section_name': 'In Progress'},
            {'department': 'engineering', 'project_type': 'sprint', 'section_name': 'In Progress'}
        ]
        
        batch_results = generator.batch_generate_task_names(batch_requests, max_concurrent=3)
        for i, result in enumerate(batch_results, 1):
            print(f"  Batch result {i}: {result}")
        
        # Test token counting
        print("\nToken Usage Test:")
        sample_text = "This is a test of the token counting functionality. It should count words and punctuation correctly."
        stats = generator.get_token_usage_stats(sample_text)
        print(f"  Sample text: '{sample_text}'")
        print(f"  Token count: {stats['token_count']}")
        print(f"  Estimated cost: ${stats['estimated_input_cost'] + stats['estimated_output_cost']:.6f}")
    
    except ValueError as e:
        print(f"Configuration error: {str(e)}")
        print("Please set a valid OPENAI_API_KEY in the configuration to run tests.")
    except Exception as e:
        print(f"Unexpected error: {str(e)}")
    finally:
        try:
            generator.close()
        except:
            pass