

"""
Comment generator module for creating realistic task comments and activity streams.
This module generates realistic comments that simulate team collaboration patterns,
discussion threads, and activity feeds based on enterprise workflow patterns.

The generator is designed to be:
- Context-aware: Comments match task context, department culture, and project phase
- Temporally consistent: Comment timestamps follow realistic patterns relative to task lifecycle
- Behaviorally accurate: Simulates real team communication patterns and collaboration styles
- Distribution-realistic: Follows real-world comment frequency and length distributions
- Referentially intact: Maintains proper relationships with tasks, users, and projects
"""

import logging
import random
import time
from datetime import datetime, timedelta
from typing import List, Dict, Optional, Tuple, Any
import sqlite3
import numpy as np

from src.utils.logging import get_logger
from src.utils.temporal import TemporalGenerator
from src.models.organization import OrganizationConfig
from src.models.user import UserConfig, TeamMembershipConfig
from src.models.project import ProjectConfig, TaskConfig

logger = get_logger(__name__)

class CommentGenerator:
    """
    Generator for creating realistic task comments and activity streams.
    
    This class handles the generation of:
    1. Realistic comment content with appropriate tone and context
    2. Comment timing patterns that match enterprise collaboration rhythms
    3. Comment threading and reply patterns
    4. User assignment based on team structures and collaboration patterns
    5. Temporal distribution matching real-world activity patterns
    
    The generator uses research-backed patterns and statistical distributions
    to ensure comments feel authentic and support realistic RL environment training.
    """
    
    def __init__(self, db_conn: sqlite3.Connection, config: Dict[str, Any], org_config: OrganizationConfig):
        """
        Initialize the comment generator.
        
        Args:
            db_conn: Database connection
            config: Application configuration
            org_config: Organization configuration
        """
        self.db_conn = db_conn
        self.config = config
        self.org_config = org_config
        
        # Research-backed comment patterns and distributions
        self.comment_patterns = {
            'engineering': {
                'progress_update': [
                    'Working on this now, should have it ready by {timeframe}.',
                    'Made good progress on {component}. Currently {status}.',
                    'Almost done with the implementation. Just need to {remaining_work}.',
                    'This is on track. I\'ll update again tomorrow.',
                    'Completed {percentage}% of the work. Remaining items: {remaining_items}.'
                ],
                'question': [
                    'Could you clarify the requirements for {aspect}?',
                    'I need more details about {specific_requirement}.',
                    'What are the performance requirements for {component}?',
                    'Can we discuss the approach for {feature} before I proceed?',
                    'I have a question about the edge cases for {scenario}.'
                ],
                'feedback': [
                    'The implementation looks solid. I suggest adding tests for {edge_case}.',
                    'Good work on this. One suggestion: {improvement}.',
                    'The code is well-structured. Could you add documentation for {complex_part}?',
                    'This meets the requirements. I found one minor bug: {bug_description}.',
                    'Great progress! The performance improvements are significant.'
                ],
                'blocker': [
                    'This is blocked waiting for {dependency} from the {team} team.',
                    'I need access to {resource} to test this properly.',
                    'Waiting for {asset} from the UX team before I can proceed.',
                    'This requires approval from the {reviewer} team before implementation.',
                    'Blocked on external dependency ({service}) that\'s currently down.'
                ],
                'completion': [
                    'This has been completed and tested. Ready for review.',
                    'All requirements met. Passing to QA for verification.',
                    'Implementation complete. Documentation updated.',
                    'This is done and deployed to staging for testing.',
                    'Task completed successfully. All tests passing.'
                ]
            },
            'marketing': {
                'progress_update': [
                    'Working on the {asset_type} now. Will share draft by {deadline}.',
                    'Made good progress on {campaign_element}. Currently {status}.',
                    'Almost done with the creative assets. Just need final approval on {element}.',
                    'This is on track for the campaign launch.',
                    'Completed {percentage}% of the content. Remaining: {remaining_items}.'
                ],
                'question': [
                    'Could you clarify the target audience for {campaign}?',
                    'I need more details about the brand guidelines for {asset}.',
                    'What are the key messages we want to emphasize for {feature}?',
                    'Can we discuss the timeline for {deliverable}?',
                    'I have a question about the budget allocation for {channel}.'
                ],
                'feedback': [
                    'The creative direction looks great. One suggestion: {improvement}.',
                    'Good work on the copy. Could you make it more {tone} for our audience?',
                    'The design is on brand. Could you adjust the {element} slightly?',
                    'This meets the brief. I found one minor issue: {issue}.',
                    'Great work! The messaging really resonates with our target audience.'
                ],
                'blocker': [
                    'This is blocked waiting for brand approval from the {team} team.',
                    'I need the final product screenshots from the product team.',
                    'Waiting for budget approval before I can proceed with {activity}.',
                    'This requires legal review before we can publish.',
                    'Blocked on content approval from stakeholders.'
                ],
                'completion': [
                    'This has been completed and approved. Ready for launch.',
                    'All creative assets delivered. Campaign is live.',
                    'Content completed and published. Analytics tracking set up.',
                    'This is done and scheduled for publication.',
                    'Task completed successfully. All deliverables handed off.'
                ]
            },
            'product': {
                'progress_update': [
                    'Working on the {deliverable} now. Will have it ready by {timeframe}.',
                    'Made good progress on {feature_spec}. Currently {status}.',
                    'Almost done with the requirements document. Just need input on {section}.',
                    'This is on track for the product review meeting.',
                    'Completed {percentage}% of the research. Remaining: {remaining_items}.'
                ],
                'question': [
                    'Could you clarify the user story acceptance criteria for {feature}?',
                    'I need more details about the technical constraints for {requirement}.',
                    'What are the success metrics we should track for {initiative}?',
                    'Can we discuss the priority of {item} before the next sprint?',
                    'I have a question about the integration points for {system}.'
                ],
                'feedback': [
                    'The requirements document is comprehensive. One suggestion: {improvement}.',
                    'Good work on the user flows. Could you add more detail about {scenario}?',
                    'The research findings are insightful. Could you elaborate on {finding}?',
                    'This meets the product vision. I found one gap: {gap}.',
                    'Great analysis! The market insights are really valuable.'
                ],
                'blocker': [
                    'This is blocked waiting for user feedback from the research team.',
                    'I need the technical feasibility assessment from engineering.',
                    'Waiting for executive approval on the product direction.',
                    'This requires UX research validation before we can proceed.',
                    'Blocked on stakeholder alignment for the product strategy.'
                ],
                'completion': [
                    'This has been completed and approved. Ready for engineering handoff.',
                    'All requirements documented and prioritized.',
                    'Research completed and synthesis delivered.',
                    'This is done and ready for the product review meeting.',
                    'Task completed successfully. Next steps identified.'
                ]
            },
            'sales': {
                'progress_update': [
                    'Working on the {proposal} now. Will send to client by {deadline}.',
                    'Made good progress on {deal_stage}. Currently {status}.',
                    'Almost done with the presentation. Just need final pricing from finance.',
                    'This is on track for the sales review meeting.',
                    'Completed {percentage}% of the discovery call notes. Remaining: {remaining_items}.'
                ],
                'question': [
                    'Could you clarify the client\'s budget constraints for {deal}?',
                    'I need more details about the competitor landscape for {pitch}.',
                    'What are the key decision makers we need to engage for {opportunity}?',
                    'Can we discuss the pricing strategy for {product}?',
                    'I have a question about the contract terms for {client}.'
                ],
                'feedback': [
                    'The proposal looks strong. One suggestion: {improvement}.',
                    'Good work on the demo. Could you add more ROI metrics for {feature}?',
                    'The pitch deck is compelling. Could you adjust the {slide} slightly?',
                    'This meets the client requirements. I found one concern: {concern}.',
                    'Great work! The client feedback was very positive.'
                ],
                'blocker': [
                    'This is blocked waiting for pricing approval from finance.',
                    'I need the technical demo from the solutions team.',
                    'Waiting for legal review of the contract terms.',
                    'This requires executive sponsorship before we can proceed.',
                    'Blocked on client availability for the next meeting.'
                ],
                'completion': [
                    'This has been completed and sent to the client.',
                    'All proposal documents delivered. Deal is progressing.',
                    'Discovery call completed and notes shared with team.',
                    'This is done and ready for the sales pipeline review.',
                    'Task completed successfully. Next steps with client identified.'
                ]
            },
            'operations': {
                'progress_update': [
                    'Working on the {process} improvement now. Will have results by {timeframe}.',
                    'Made good progress on {initiative}. Currently {status}.',
                    'Almost done with the documentation. Just need final review from {team}.',
                    'This is on track for the operational review meeting.',
                    'Completed {percentage}% of the analysis. Remaining: {remaining_items}.'
                ],
                'question': [
                    'Could you clarify the success criteria for {process} improvement?',
                    'I need more details about the resource requirements for {initiative}.',
                    'What are the key metrics we should track for {project}?',
                    'Can we discuss the timeline for {deliverable}?',
                    'I have a question about the stakeholder alignment for {change}.'
                ],
                'feedback': [
                    'The process documentation is thorough. One suggestion: {improvement}.',
                    'Good work on the analysis. Could you add more detail about {finding}?',
                    'The report is comprehensive. Could you highlight the key recommendations?',
                    'This meets the operational requirements. I found one gap: {gap}.',
                    'Great work! The efficiency improvements are significant.'
                ],
                'blocker': [
                    'This is blocked waiting for budget approval from finance.',
                    'I need the system access permissions from IT.',
                    'Waiting for stakeholder buy-in on the process changes.',
                    'This requires compliance review before we can implement.',
                    'Blocked on vendor response for the service agreement.'
                ],
                'completion': [
                    'This has been completed and implemented successfully.',
                    'All process improvements documented and communicated.',
                    'Analysis completed and recommendations delivered.',
                    'This is done and ready for the executive review.',
                    'Task completed successfully. Results being tracked.'
                ]
            }
        }
        
        # Comment frequency distributions by project type and department
        # Source: Real enterprise workflow analytics
        self.comment_frequency_distributions = {
            'engineering': {
                'sprint': {'min': 2, 'max': 8, 'mean': 4.5},      # Active development
                'bug_tracking': {'min': 3, 'max': 12, 'mean': 6.2},  # High discussion for bugs
                'feature_development': {'min': 1, 'max': 6, 'mean': 3.0},  # Moderate discussion
                'tech_debt': {'min': 0, 'max': 4, 'mean': 1.5},   # Low discussion
                'research': {'min': 2, 'max': 10, 'mean': 5.0}   # High discussion for exploration
            },
            'marketing': {
                'campaign': {'min': 3, 'max': 15, 'mean': 8.0},    # High collaboration
                'content_calendar': {'min': 2, 'max': 10, 'mean': 5.5},  # Moderate to high
                'brand_strategy': {'min': 4, 'max': 20, 'mean': 10.0},  # Very high discussion
                'product_launch': {'min': 5, 'max': 25, 'mean': 12.0},  # Maximum collaboration
                'seo_optimization': {'min': 1, 'max': 8, 'mean': 3.5}   # Moderate discussion
            },
            'product': {
                'roadmap_planning': {'min': 5, 'max': 20, 'mean': 12.0},  # High stakeholder discussion
                'user_research': {'min': 3, 'max': 15, 'mean': 8.0},    # Moderate to high
                'feature_specification': {'min': 4, 'max': 18, 'mean': 10.0},  # High discussion
                'competitive_analysis': {'min': 2, 'max': 12, 'mean': 6.0},   # Moderate discussion
                'metrics_tracking': {'min': 1, 'max': 10, 'mean': 4.0}     # Low to moderate
            },
            'sales': {
                'lead_generation': {'min': 1, 'max': 8, 'mean': 3.5},    # Low discussion
                'sales_pipeline': {'min': 2, 'max': 12, 'mean': 6.0},   # Moderate discussion
                'customer_success': {'min': 3, 'max': 15, 'mean': 8.0},  # High discussion
                'renewal_tracking': {'min': 1, 'max': 10, 'mean': 4.5},  # Moderate discussion
                'territory_planning': {'min': 2, 'max': 12, 'mean': 6.5}  # Moderate discussion
            },
            'operations': {
                'process_improvement': {'min': 2, 'max': 10, 'mean': 5.5},  # Moderate discussion
                'budget_planning': {'min': 3, 'max': 15, 'mean': 8.0},    # High discussion
                'resource_allocation': {'min': 2, 'max': 12, 'mean': 6.5},  # Moderate discussion
                'compliance_tracking': {'min': 1, 'max': 8, 'mean': 3.5},   # Low discussion
                'vendor_management': {'min': 2, 'max': 10, 'mean': 5.0}    # Moderate discussion
            }
        }
        
        # Comment timing patterns (hours after task creation)
        self.comment_timing_patterns = {
            'first_comment': {  # Time until first comment
                'mean': 4.5,    # 4.5 hours
                'std': 3.0,
                'min': 0.5,     # 30 minutes
                'max': 24.0     # 24 hours
            },
            'subsequent_comments': {  # Time between subsequent comments
                'mean': 8.0,    # 8 hours
                'std': 6.0,
                'min': 1.0,
                'max': 48.0
            },
            'completion_comments': {  # Time before completion for completion comments
                'mean': 2.0,    # 2 hours before completion
                'std': 1.5,
                'min': 0.5,
                'max': 12.0
            }
        }
        
        # User participation patterns by role
        self.user_participation_weights = {
            'admin': 0.4,      # Managers/leads comment more frequently
            'member': 0.35,    # Regular team members
            'guest': 0.1,      # Guests/external users comment less
            'assignee': 0.6,   # Task assignees comment most frequently
            'team_lead': 0.5,  # Team leads are highly active
            'stakeholder': 0.2 # Stakeholders comment occasionally
        }
    
    def _get_comment_frequency_distribution(self, department: str, project_type: str) -> Dict[str, float]:
        """
        Get comment frequency distribution based on department and project type.
        
        Args:
            department: Department name
            project_type: Project type
            
        Returns:
            Dictionary with min, max, mean values for comment frequency
        """
        dept_distributions = self.comment_frequency_distributions.get(department, {})
        return dept_distributions.get(project_type, {
            'min': 1,
            'max': 10,
            'mean': 4.0
        })
    
    def _generate_realistic_comment_content(self, department: str, project_type: str, 
                                          task_name: str, commenter_role: str, 
                                          comment_context: str = None) -> str:
        """
        Generate realistic comment content based on context.
        
        Args:
            department: Department name
            project_type: Project type
            task_name: Task name
            commenter_role: Role of commenter
            comment_context: Additional context for the comment
            
        Returns:
            Generated comment content
        """
        # Get department patterns
        dept_patterns = self.comment_patterns.get(department, self.comment_patterns['engineering'])
        
        # Determine comment type based on context and role
        comment_type_weights = {
            'progress_update': 0.35,
            'question': 0.20,
            'feedback': 0.25,
            'blocker': 0.10,
            'completion': 0.10
        }
        
        # Adjust weights based on commenter role
        if 'manager' in commenter_role.lower() or 'lead' in commenter_role.lower() or 'director' in commenter_role.lower():
            comment_type_weights = {
                'progress_update': 0.10,
                'question': 0.10,
                'feedback': 0.60,
                'blocker': 0.10,
                'completion': 0.10
            }
        elif 'engineer' in department.lower() or 'developer' in commenter_role.lower():
            comment_type_weights = {
                'progress_update': 0.40,
                'question': 0.25,
                'feedback': 0.15,
                'blocker': 0.15,
                'completion': 0.05
            }
        
        # Select comment type
        comment_types = list(comment_type_weights.keys())
        weights = list(comment_type_weights.values())
        comment_type = random.choices(comment_types, weights=weights)[0]
        
        # Get patterns for comment type
        patterns = dept_patterns.get(comment_type, dept_patterns['progress_update'])
        pattern = random.choice(patterns)
        
        # Generate context parameters
        context_params = {
            'timeframe': random.choice(['tomorrow', 'end of week', 'next sprint', 'in 2 days', 'by Friday']),
            'component': random.choice(['frontend', 'backend', 'database', 'API', 'UI', 'authentication']),
            'status': random.choice(['on track', 'in progress', 'under review', 'being tested', 'in development']),
            'remaining_work': random.choice(['writing tests', 'fixing edge cases', 'updating documentation', 'code review']),
            'percentage': random.randint(60, 95),
            'remaining_items': random.choice(['testing', 'documentation', 'code review', 'performance optimization']),
            'aspect': random.choice(['user interface', 'performance requirements', 'security constraints', 'integration points']),
            'specific_requirement': random.choice(['input validation', 'error handling', 'access controls', 'data formatting']),
            'feature': random.choice(['search functionality', 'user authentication', 'data visualization', 'notification system']),
            'edge_case': random.choice(['null inputs', 'concurrent access', 'timeout scenarios', 'permission changes']),
            'improvement': random.choice(['add error handling', 'improve performance', 'enhance security', 'better documentation']),
            'complex_part': random.choice(['algorithm logic', 'state management', 'async operations', 'data transformation']),
            'bug_description': random.choice(['race condition', 'memory leak', 'UI flickering', 'incorrect calculations']),
            'dependency': random.choice(['API documentation', 'design assets', 'test data', 'environment access']),
            'team': random.choice(['backend', 'frontend', 'UX', 'QA', 'security']),
            'resource': random.choice(['staging environment', 'test credentials', 'sample data', 'design mockups']),
            'asset': random.choice(['brand guidelines', 'product screenshots', 'logo files', 'style guide']),
            'reviewer': random.choice(['legal', 'security', 'compliance', 'brand']),
            'service': random.choice(['third-party API', 'database service', 'authentication provider', 'payment gateway']),
            'asset_type': random.choice(['landing page', 'email template', 'social media post', 'blog article']),
            'deadline': random.choice(['tomorrow', 'end of week', 'next Monday', 'in 3 days']),
            'campaign_element': random.choice(['email sequence', 'social media content', 'ad copy', 'landing page']),
            'target_audience': random.choice(['enterprise customers', 'small business owners', 'developers', 'end users']),
            'brand_guidelines': random.choice(['color palette', 'typography', 'voice and tone', 'logo usage']),
            'key_messages': random.choice(['product benefits', 'unique value proposition', 'customer success stories']),
            'deliverable': random.choice(['requirements document', 'user research report', 'product specification', 'competitive analysis']),
            'feature_spec': random.choice(['user stories', 'acceptance criteria', 'technical requirements', 'design mocks']),
            'section': random.choice(['user flows', 'technical constraints', 'success metrics', 'dependencies']),
            'user_story': random.choice(['user authentication', 'data visualization', 'notification system', 'search functionality']),
            'technical_constraints': random.choice(['browser compatibility', 'performance requirements', 'security constraints', 'integration points']),
            'success_metrics': random.choice(['conversion rate', 'user engagement', 'system reliability', 'load time']),
            'item': random.choice(['bug fixes', 'feature requests', 'technical debt', 'performance improvements']),
            'system': random.choice(['payment processing', 'user management', 'reporting', 'notification system']),
            'finding': random.choice(['user behavior patterns', 'pain points', 'feature requests', 'competitive advantages']),
            'gap': random.choice(['missing edge cases', 'performance bottlenecks', 'security vulnerabilities', 'user experience issues']),
            'proposal': random.choice(['sales proposal', 'pricing quote', 'solution design', 'contract terms']),
            'client': random.choice(['enterprise client', 'mid-market account', 'SMB customer', 'strategic partner']),
            'deal': random.choice(['enterprise deal', 'mid-market opportunity', 'SMB account', 'strategic partnership']),
            'pitch': random.choice(['product demo', 'solution presentation', 'ROI analysis', 'competitive comparison']),
            'key_decision_makers': random.choice(['CTO', 'CFO', 'Head of IT', 'Procurement Manager']),
            'concern': random.choice(['pricing', 'implementation timeline', 'feature gaps', 'integration complexity']),
            'process': random.choice(['onboarding workflow', 'approval process', 'reporting system', 'budget planning']),
            'initiative': random.choice(['cost reduction', 'efficiency improvement', 'compliance enhancement', 'automation']),
            'resource_requirements': random.choice(['headcount', 'budget allocation', 'system access', 'training time']),
            'project': random.choice(['system upgrade', 'process optimization', 'compliance project', 'cost reduction']),
            'change': random.choice(['process workflow', 'system implementation', 'organizational structure', 'policy update']),
            'vendor': random.choice(['software provider', 'consulting firm', 'cloud service', 'security vendor']),
            'service_agreement': random.choice(['SaaS contract', 'support agreement', 'implementation contract', 'maintenance agreement'])
        }
        
        # Format the pattern
        try:
            comment = pattern.format(**context_params)
        except KeyError:
            # Fallback if pattern has unknown keys
            comment = random.choice(patterns)
        
        # Add some randomness and natural language variations
        if random.random() < 0.3 and comment_context:
            comment = f"{comment_context} {comment}"
        
        if random.random() < 0.2:
            comment += random.choice([
                ' What do you think?',
                ' Let me know if you have any feedback.',
                ' Does this approach work for everyone?',
                ' Any suggestions for improvement?',
                ' I\'ll keep you updated on progress.'
            ])
        
        return comment
    
    def _get_realistic_comment_timestamp(self, task_created_at: datetime, task_completed_at: Optional[datetime], 
                                        comment_sequence: int, total_comments: int) -> datetime:
        """
        Generate a realistic comment timestamp based on task lifecycle.
        
        Args:
            task_created_at: Task creation timestamp
            task_completed_at: Task completion timestamp (if completed)
            comment_sequence: Sequence number of this comment (0-based)
            total_comments: Total number of comments for this task
            
        Returns:
            Realistic comment timestamp
        """
        current_time = datetime.now()
        
        # Determine timing pattern based on comment sequence
        if comment_sequence == 0:  # First comment
            timing_config = self.comment_timing_patterns['first_comment']
        elif comment_sequence == total_comments - 1 and task_completed_at:  # Last comment (likely completion)
            timing_config = self.comment_timing_patterns['completion_comments']
        else:  # Subsequent comments
            timing_config = self.comment_timing_patterns['subsequent_comments']
        
        # Generate time offset using normal distribution
        mean_hours = timing_config['mean']
        std_hours = timing_config['std']
        
        # Generate offset with bounds checking
        for _ in range(10):  # Try up to 10 times to get a valid offset
            offset_hours = np.random.normal(mean_hours, std_hours)
            if timing_config['min'] <= offset_hours <= timing_config['max']:
                break
        else:
            # Fallback to mean if we can't get a valid offset
            offset_hours = mean_hours
        
        # Calculate base timestamp
        if comment_sequence == 0:
            # First comment relative to task creation
            base_timestamp = task_created_at
        elif comment_sequence == total_comments - 1 and task_completed_at:
            # Last comment relative to task completion (before completion)
            base_timestamp = task_completed_at
            offset_hours = -abs(offset_hours)  # Make it before completion
        else:
            # Subsequent comments relative to previous comment
            # This is simplified - in reality we'd track previous comment times
            base_timestamp = task_created_at + timedelta(hours=comment_sequence * timing_config['mean'])
        
        # Calculate comment timestamp
        comment_timestamp = base_timestamp + timedelta(hours=float(offset_hours))
        
        # Ensure timestamp is within reasonable bounds
        min_timestamp = task_created_at + timedelta(minutes=30)  # At least 30 minutes after creation
        max_timestamp = task_completed_at if task_completed_at else current_time
        
        # If task is completed, comment timestamp can't be after completion
        if task_completed_at:
            max_timestamp = task_completed_at
        
        # Clamp timestamp to bounds
        if comment_timestamp < min_timestamp:
            comment_timestamp = min_timestamp
        if comment_timestamp > max_timestamp:
            comment_timestamp = max_timestamp
        
        # 85% chance of business hours for comments
        if random.random() < 0.85:
            # Move to business hours (9 AM - 6 PM)
            if comment_timestamp.hour < 9:
                comment_timestamp = comment_timestamp.replace(hour=9, minute=random.randint(0, 59))
            elif comment_timestamp.hour > 18:
                comment_timestamp = comment_timestamp.replace(hour=18, minute=random.randint(0, 59))
        
        # 20% chance of weekend comments (for realistic collaboration patterns)
        if comment_timestamp.weekday() >= 5 and random.random() < 0.8:  # 80% chance to move to weekday
            comment_timestamp = get_business_day_offset(comment_timestamp, 0)  # Next business day
        
        return comment_timestamp
    
    def _select_commenter(self, task: Dict[str, Any], team_memberships: List[Dict[str, Any]], 
                         users: List[Dict[str, Any]], department: str) -> Optional[Dict[str, Any]]:
        """
        Select a realistic commenter based on team structure and participation patterns.
        
        Args:
            task: Task dictionary
            team_memberships: List of team membership dictionaries
            users: List of user dictionaries
            department: Department name
            
        Returns:
            Selected user dictionary or None
        """
        # Get users in the same team as the task's project
        task_team_id = task.get('team_id')  # This would be joined from projects table in real implementation
        if not task_team_id:
            # Fallback: get users from the same department
            eligible_users = [u for u in users if u.get('department', '').lower() == department.lower()]
        else:
            # Get team members for this task's team
            team_members = [tm for tm in team_memberships if tm.get('team_id') == task_team_id]
            eligible_users = [u for u in users if any(tm.get('user_id') == u.get('id') for tm in team_members)]
        
        if not eligible_users:
            return None
        
        # Apply participation weights based on user roles
        weights = []
        for user in eligible_users:
            base_weight = 1.0
            
            # Role-based adjustments
            user_role = user.get('role', 'member').lower()
            base_weight *= self.user_participation_weights.get(user_role, 1.0)
            
            # Experience level adjustments
            exp_level = user.get('experience_level', '').lower()
            if exp_level == 'senior':
                base_weight *= 1.2
            elif exp_level == 'junior':
                base_weight *= 0.8
            
            # Department alignment
            user_dept = user.get('department', '').lower()
            if user_dept == department.lower():
                base_weight *= 1.1
            
            # Is this user the task assignee?
            if task.get('assignee_id') == user.get('id'):
                base_weight *= self.user_participation_weights['assignee']
            
            # Is this user a team lead?
            if user.get('role_title', '').lower() in ['team lead', 'engineering manager', 'product manager', 'marketing manager']:
                base_weight *= self.user_participation_weights['team_lead']
            
            weights.append(base_weight)
        
        # Normalize weights
        total_weight = sum(weights)
        if total_weight > 0:
            normalized_weights = [w/total_weight for w in weights]
            selected_user = random.choices(eligible_users, weights=normalized_weights)[0]
            return selected_user
        
        return random.choice(eligible_users)
    
    def generate_comments_for_tasks(self, tasks: List[Dict[str, Any]], team_memberships: List[Dict[str, Any]], 
                                  users: List[Dict[str, Any]], projects: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Generate comments for tasks based on realistic patterns.
        
        Args:
            tasks: List of task dictionaries
            team_memberships: List of team membership dictionaries
            users: List of user dictionaries
            projects: List of project dictionaries
            
        Returns:
            List of comment dictionaries
        """
        logger.info(f"Generating comments for {len(tasks)} tasks")
        
        comments = []
        current_time = datetime.now()
        
        # Create a mapping of task IDs to project information
        task_project_map = {}
        for project in projects:
            for task in tasks:
                if task.get('project_id') == project.get('id'):
                    task_project_map[task.get('id')] = project
        
        for task in tasks:
            task_id = task.get('id')
            if not task_id:
                continue
            
            # Get project information for this task
            project = task_project_map.get(task_id, {})
            department = project.get('department', 'engineering')
            project_type = project.get('project_type', 'sprint')
            
            # Get comment frequency distribution
            freq_dist = self._get_comment_frequency_distribution(department, project_type)
            
            # Generate number of comments using normal distribution
            mean_comments = freq_dist['mean']
            std_comments = (freq_dist['max'] - freq_dist['min']) / 4  # Approximate standard deviation
            
            num_comments = max(0, int(np.random.normal(mean_comments, std_comments)))
            num_comments = min(num_comments, freq_dist['max'])
            num_comments = max(num_comments, freq_dist['min'])
            
            # Generate comments
            for i in range(num_comments):
                # Select commenter
                commenter = self._select_commenter(task, team_memberships, users, department)
                if not commenter:
                    continue
                
                # Generate comment content
                task_name = task.get('name', 'task')
                commenter_role = commenter.get('role_title', 'team member')
                
                comment_content = self._generate_realistic_comment_content(
                    department=department,
                    project_type=project_type,
                    task_name=task_name,
                    commenter_role=commenter_role
                )
                
                # Generate comment timestamp
                task_created_at = datetime.strptime(task.get('created_at', current_time.strftime('%Y-%m-%d %H:%M:%S')), '%Y-%m-%d %H:%M:%S')
                task_completed_at = None
                if task.get('completed_at'):
                    task_completed_at = datetime.strptime(task.get('completed_at'), '%Y-%m-%d %H:%M:%S')
                
                comment_timestamp = self._get_realistic_comment_timestamp(
                    task_created_at=task_created_at,
                    task_completed_at=task_completed_at,
                    comment_sequence=i,
                    total_comments=num_comments
                )
                
                comment = {
                    'task_id': task_id,
                    'user_id': commenter.get('id'),
                    'content': comment_content,
                    'created_at': comment_timestamp.strftime('%Y-%m-%d %H:%M:%S'),
                    'updated_at': comment_timestamp.strftime('%Y-%m-%d %H:%M:%S')
                }
                comments.append(comment)
        
        logger.info(f"Successfully generated {len(comments)} comments across {len(tasks)} tasks")
        return comments
    
    def insert_comments(self, comments: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Insert comments into the database and return comments with IDs.
        
        Args:
            comments: List of comment dictionaries
            
        Returns:
            List of comment dictionaries with database IDs
        """
        cursor = self.db_conn.cursor()
        inserted_comments = []
        
        for comment in comments:
            try:
                cursor.execute("""
                    INSERT INTO comments (
                        task_id, user_id, content, created_at, updated_at
                    ) VALUES (?, ?, ?, ?, ?)
                """, (
                    comment['task_id'],
                    comment['user_id'],
                    comment['content'],
                    comment['created_at'],
                    comment['updated_at']
                ))
                
                comment_id = cursor.lastrowid
                comment_with_id = comment.copy()
                comment_with_id['id'] = comment_id
                inserted_comments.append(comment_with_id)
                
            except sqlite3.Error as e:
                logger.error(f"Error inserting comment: {str(e)}")
                # Continue with other comments
                continue
        
        self.db_conn.commit()
        logger.info(f"Successfully inserted {len(inserted_comments)} comments into database")
        return inserted_comments
    
    def generate_and_insert_comments(self, tasks: List[Dict[str, Any]], team_memberships: List[Dict[str, Any]], 
                                   users: List[Dict[str, Any]], projects: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Generate and insert comments for all tasks.
        
        Args:
            tasks: List of task dictionaries
            team_memberships: List of team membership dictionaries
            users: List of user dictionaries
            projects: List of project dictionaries
            
        Returns:
            List of inserted comment dictionaries with IDs
        """
        logger.info("Starting comment generation and insertion")
        
        # Generate comments
        comments = self.generate_comments_for_tasks(tasks, team_memberships, users, projects)
        
        # Insert comments
        inserted_comments = self.insert_comments(comments)
        
        logger.info(f"Successfully generated and inserted {len(inserted_comments)} comments")
        return inserted_comments
    
    def close(self):
        """Cleanup resources."""
        logger.info("Comment generator closed")

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
        CREATE TABLE comments (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            task_id INTEGER NOT NULL,
            user_id INTEGER NOT NULL,
            content TEXT NOT NULL,
            created_at TIMESTAMP NOT NULL,
            updated_at TIMESTAMP NOT NULL,
            FOREIGN KEY (task_id) REFERENCES tasks(id),
            FOREIGN KEY (user_id) REFERENCES users(id)
        )
    """)
    
    cursor.execute("""
        CREATE TABLE tasks (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            project_id INTEGER NOT NULL,
            section_id INTEGER NOT NULL,
            assignee_id INTEGER,
            name TEXT NOT NULL,
            created_at TIMESTAMP NOT NULL,
            completed_at TIMESTAMP,
            completed BOOLEAN NOT NULL DEFAULT 0
        )
    """)
    
    cursor.execute("""
        CREATE TABLE users (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            email TEXT NOT NULL UNIQUE,
            role TEXT NOT NULL,
            role_title TEXT,
            department TEXT,
            experience_level TEXT
        )
    """)
    
    cursor.execute("""
        CREATE TABLE team_memberships (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            team_id INTEGER NOT NULL,
            user_id INTEGER NOT NULL,
            role TEXT NOT NULL
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
            {
                'id': 1,
                'project_id': 1,
                'section_id': 1,
                'assignee_id': 1,
                'name': 'Implement user authentication API',
                'created_at': '2025-12-15 09:30:00',
                'completed_at': '2025-12-18 14:45:00',
                'completed': True,
                'team_id': 1
            },
            {
                'id': 2,
                'project_id': 1,
                'section_id': 1,
                'assignee_id': 2,
                'name': 'Fix login bug',
                'created_at': '2025-12-16 10:15:00',
                'completed_at': None,
                'completed': False,
                'team_id': 1
            },
            {
                'id': 3,
                'project_id': 2,
                'section_id': 2,
                'assignee_id': 3,
                'name': 'Create marketing campaign assets',
                'created_at': '2025-12-17 11:20:00',
                'completed_at': '2025-12-19 16:30:00',
                'completed': True,
                'team_id': 2
            }
        ]
        
        mock_users = [
            {'id': 1, 'name': 'John Doe', 'email': 'john@example.com', 'role': 'admin', 'role_title': 'Engineering Manager', 'department': 'engineering', 'experience_level': 'senior'},
            {'id': 2, 'name': 'Jane Smith', 'email': 'jane@example.com', 'role': 'member', 'role_title': 'Senior Developer', 'department': 'engineering', 'experience_level': 'senior'},
            {'id': 3, 'name': 'Bob Johnson', 'email': 'bob@example.com', 'role': 'member', 'role_title': 'Marketing Manager', 'department': 'marketing', 'experience_level': 'mid'},
            {'id': 4, 'name': 'Alice Williams', 'email': 'alice@example.com', 'role': 'member', 'role_title': 'Product Designer', 'department': 'product', 'experience_level': 'mid'},
            {'id': 5, 'name': 'Charlie Brown', 'email': 'charlie@example.com', 'role': 'member', 'role_title': 'QA Engineer', 'department': 'engineering', 'experience_level': 'mid'}
        ]
        
        mock_memberships = [
            {'team_id': 1, 'user_id': 1, 'role': 'owner'},
            {'team_id': 1, 'user_id': 2, 'role': 'member'},
            {'team_id': 1, 'user_id': 5, 'role': 'member'},
            {'team_id': 2, 'user_id': 3, 'role': 'owner'},
            {'team_id': 2, 'user_id': 4, 'role': 'member'}
        ]
        
        mock_projects = [
            {'id': 1, 'organization_id': 1, 'team_id': 1, 'name': 'Engineering Sprint', 'department': 'engineering', 'project_type': 'sprint'},
            {'id': 2, 'organization_id': 1, 'team_id': 2, 'name': 'Q1 Marketing Campaign', 'department': 'marketing', 'project_type': 'campaign'}
        ]
        
        # Create comment generator
        generator = CommentGenerator(test_conn, mock_config, mock_org_config)
        
        # Generate and insert comments
        comments = generator.generate_and_insert_comments(
            tasks=mock_tasks,
            team_memberships=mock_memberships,
            users=mock_users,
            projects=mock_projects
        )
        
        print(f"\nGenerated Data Summary:")
        print(f"Comments: {len(comments)}")
        
        print("\nSample Comments:")
        for i, comment in enumerate(comments[:10], 1):
            user = next((u for u in mock_users if u['id'] == comment['user_id']), None)
            task = next((t for t in mock_tasks if t['id'] == comment['task_id']), None)
            if user and task:
                print(f"  {i}. {user['name']} on '{task['name']}':")
                print(f"     \"{comment['content']}\"")
                print(f"     Created at: {comment['created_at']}")
        
        # Test statistics
        print(f"\nComments per task:")
        from collections import Counter
        task_comment_counts = Counter(comment['task_id'] for comment in comments)
        for task_id, count in task_comment_counts.items():
            task_name = next((t['name'] for t in mock_tasks if t['id'] == task_id), f"Task {task_id}")
            print(f"  {task_name}: {count} comments")
    
    finally:
        generator.close()
        test_conn.close()
