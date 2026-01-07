

"""
Temporal generator module for creating realistic time-based patterns and distributions.
This module handles all time-aware data generation including business day logic,
temporal distributions, and realistic timestamp patterns based on enterprise workflows.

The generator is designed to be:
- Business-aware: Understands business days, holidays, and work hours
- Pattern-driven: Creates realistic temporal patterns based on real-world data
- Context-sensitive: Adapts time patterns to different departments and project types
- Consistent: Ensures temporal integrity across all generated data
- Configurable: Adjustable for different timezones and business calendars
"""

import logging
import random
import time
from datetime import datetime, timedelta, date
from typing import List, Dict, Optional, Tuple, Set, Any
import numpy as np
import holidays
from scipy.stats import truncnorm

from src.utils.logging import get_logger

logger = get_logger(__name__)

class TemporalGenerator:
    """
    Generator for creating realistic temporal patterns and time-based data.
    
    This class handles:
    1. Business day calculations and holiday awareness
    2. Realistic time distributions for task creation/completion
    3. Work hour patterns and timezone considerations
    4. Temporal correlations between related events
    5. Seasonal and cyclical patterns in enterprise workflows
    
    The generator uses statistical distributions and real-world patterns to ensure temporal realism.
    """
    
    def __init__(self, config: Dict[str, Any]):
        """
        Initialize the temporal generator.
        
        Args:
            config: Application configuration with timezone and temporal settings
        """
        self.config = config
        
        # Get timezone from config or use default
        self.timezone = config.get('simulation_timezone', 'America/Los_Angeles')
        
        # Set up holiday calendar (US holidays as default)
        self.holiday_calendar = holidays.US()
        
        # Work hour patterns (9 AM - 6 PM typical business hours)
        self.work_start_hour = 9
        self.work_end_hour = 18
        
        # Time distributions based on research data
        self.time_distributions = {
            'task_creation': {
                'weekday_weights': [0.05, 0.15, 0.20, 0.20, 0.30, 0.05, 0.05],  # Mon-Sun
                'hour_weights': [0.01] * 9 + [0.05] * 4 + [0.08] * 3 + [0.03] * 3 + [0.01] * 5,  # 9AM-2PM peak
            },
            'task_completion': {
                'weekday_weights': [0.02, 0.12, 0.18, 0.22, 0.35, 0.08, 0.03],  # Fri peak for weekend prep
                'hour_weights': [0.01] * 9 + [0.03] * 4 + [0.12] * 2 + [0.06] * 3 + [0.02] * 6,  # 1-3PM peak
            },
            'meeting_scheduling': {
                'weekday_weights': [0.10, 0.25, 0.30, 0.25, 0.10, 0.0, 0.0],  # Weekday only
                'hour_weights': [0.01] * 9 + [0.08] * 2 + [0.15] + [0.08] * 2 + [0.05] * 4 + [0.01] * 6,  # 10-11AM, 2-3PM peaks
            },
            'email_activity': {
                'weekday_weights': [0.05, 0.20, 0.25, 0.25, 0.20, 0.03, 0.02],
                'hour_weights': [0.02] * 7 + [0.08] * 2 + [0.12] * 2 + [0.06] * 2 + [0.04] * 3 + [0.02] * 8,  # Early morning and late afternoon peaks
            }
        }
        
        # Department-specific temporal patterns
        self.department_patterns = {
            'engineering': {
                'task_creation_peak': 'morning',  # 9-11AM
                'task_completion_peak': 'afternoon',  # 2-4PM
                'weekend_activity': 0.15,  # 15% weekend activity
                'evening_activity': 0.25   # 25% evening activity (6PM-12AM)
            },
            'product': {
                'task_creation_peak': 'midday',  # 11AM-1PM
                'task_completion_peak': 'midday',  # 11AM-2PM
                'weekend_activity': 0.10,
                'evening_activity': 0.20
            },
            'marketing': {
                'task_creation_peak': 'late_morning',  # 10AM-12PM
                'task_completion_peak': 'afternoon',  # 1-4PM
                'weekend_activity': 0.20,  # Higher weekend activity for campaigns
                'evening_activity': 0.30
            },
            'sales': {
                'task_creation_peak': 'early_morning',  # 8-10AM (prep for day)
                'task_completion_peak': 'late_afternoon',  # 3-6PM (follow-ups)
                'weekend_activity': 0.12,
                'evening_activity': 0.35
            },
            'operations': {
                'task_creation_peak': 'morning',  # 9-11AM
                'task_completion_peak': 'morning',  # 9-12PM
                'weekend_activity': 0.08,
                'evening_activity': 0.15
            }
        }
        
        # Project type temporal patterns
        self.project_type_patterns = {
            'sprint': {
                'daily_task_creation_std_dev': 2.5,  # More consistent daily creation
                'completion_acceleration': 1.3,      # Faster completion near sprint end
                'weekend_pause_factor': 0.3          # 70% reduction on weekends
            },
            'bug_tracking': {
                'daily_task_creation_std_dev': 4.0,  # More variable creation
                'completion_acceleration': 1.1,      # Steady completion
                'weekend_pause_factor': 0.6          # 40% reduction on weekends (critical bugs still fixed)
            },
            'feature_development': {
                'daily_task_creation_std_dev': 3.0,
                'completion_acceleration': 1.2,
                'weekend_pause_factor': 0.2
            },
            'campaign': {
                'daily_task_creation_std_dev': 3.5,
                'completion_acceleration': 1.4,      # High acceleration near launch
                'weekend_pause_factor': 0.7          # Minimal weekend pause for marketing
            },
            'research': {
                'daily_task_creation_std_dev': 5.0,  # Highly variable
                'completion_acceleration': 1.0,      # Steady pace
                'weekend_pause_factor': 0.8          # Minimal weekend pause
            }
        }
    
    def is_business_day(self, date_obj: date) -> bool:
        """
        Check if a date is a business day (not weekend or holiday).
        
        Args:
            date_obj: Date to check
            
        Returns:
            True if business day, False otherwise
        """
        # Check weekend (Saturday = 5, Sunday = 6)
        if date_obj.weekday() >= 5:
            return False
        
        # Check holiday
        return date_obj not in self.holiday_calendar
    
    def get_next_business_day(self, start_date: datetime) -> datetime:
        """
        Get the next business day from a given date.
        
        Args:
            start_date: Starting datetime
            
        Returns:
            Next business day datetime at same time
        """
        current_date = start_date + timedelta(days=1)
        while not self.is_business_day(current_date.date()):
            current_date += timedelta(days=1)
        return current_date
    
    def get_previous_business_day(self, start_date: datetime) -> datetime:
        """
        Get the previous business day from a given date.
        
        Args:
            start_date: Starting datetime
            
        Returns:
            Previous business day datetime at same time
        """
        current_date = start_date - timedelta(days=1)
        while not self.is_business_day(current_date.date()):
            current_date -= timedelta(days=1)
        return current_date
    
    def get_business_day_offset(self, base_date: datetime, offset_days: int) -> datetime:
        """
        Get a business day with a specific offset from base date.
        
        Args:
            base_date: Base datetime
            offset_days: Number of business days to offset (positive or negative)
            
        Returns:
            Business day datetime at same time
        """
        if offset_days == 0:
            return base_date
        
        current_date = base_date
        days_to_add = 1 if offset_days > 0 else -1
        remaining_days = abs(offset_days)
        
        while remaining_days > 0:
            current_date += timedelta(days=days_to_add)
            if self.is_business_day(current_date.date()):
                remaining_days -= 1
        
        return current_date
    
    def get_random_business_date(self, start_date: datetime, end_date: datetime) -> datetime:
        """
        Get a random business day between two dates.
        
        Args:
            start_date: Start datetime
            end_date: End datetime
            
        Returns:
            Random business day datetime
        """
        # Ensure start_date is before end_date
        if start_date > end_date:
            start_date, end_date = end_date, start_date
        
        # Get all business days in range
        business_days = []
        current_date = start_date
        
        while current_date <= end_date:
            if self.is_business_day(current_date.date()):
                business_days.append(current_date)
            current_date += timedelta(days=1)
        
        if not business_days:
            # Fallback to nearest business day
            return self.get_next_business_day(start_date)
        
        return random.choice(business_days)
    
    def get_random_time_in_work_hours(self, base_date: datetime, activity_type: str = 'task_creation') -> datetime:
        """
        Generate a random time within work hours based on activity patterns.
        
        Args:
            base_date: Base date to add time to
            activity_type: Type of activity for pattern selection
            
        Returns:
            Datetime with realistic time within work hours
        """
        # Get time distribution for activity type
        dist = self.time_distributions.get(activity_type, self.time_distributions['task_creation'])
        
        # Select random hour based on weights
        hours = list(range(24))
        hour_weights = dist['hour_weights']
        
        # Normalize weights
        total_weight = sum(hour_weights)
        normalized_weights = [w/total_weight for w in hour_weights] if total_weight > 0 else [1/24] * 24
        
        hour = random.choices(hours, weights=normalized_weights)[0]
        
        # Generate minutes (more likely at :00, :15, :30, :45)
        minute_weights = [0.4, 0.1, 0.1, 0.1, 0.1, 0.1, 0.1]  # :00, 1-14, 15-29, 30-44, 45-59
        minute_ranges = [
            (0, 1),    # :00
            (1, 15),   # 1-14
            (15, 30),  # 15-29
            (30, 45),  # 30-44
            (45, 60)   # 45-59
        ]
        
        range_idx = random.choices(range(len(minute_weights)), weights=minute_weights)[0]
        minute_range = minute_ranges[range_idx]
        minute = random.randint(minute_range[0], minute_range[1]-1)
        
        return datetime(
            base_date.year, base_date.month, base_date.day,
            hour, minute, random.randint(0, 59)
        )
    
    def generate_realistic_timestamp(self, activity_type: str = 'task_creation', 
                                    department: str = 'engineering', 
                                    project_type: str = 'sprint',
                                    base_date: Optional[datetime] = None) -> datetime:
        """
        Generate a realistic timestamp for an activity based on patterns.
        
        Args:
            activity_type: Type of activity ('task_creation', 'task_completion', etc.)
            department: Department name
            project_type: Project type
            base_date: Base date to start from (defaults to current time)
            
        Returns:
            Realistic datetime timestamp
        """
        if base_date is None:
            base_date = datetime.now()
        
        # Get department pattern
        dept_pattern = self.department_patterns.get(department, self.department_patterns['engineering'])
        
        # Determine if this should be during work hours or outside
        work_hours_probability = 0.85  # 85% chance during work hours
        
        # Adjust based on department evening/weekend patterns
        if base_date.weekday() >= 5:  # Weekend
            work_hours_probability *= (1 - dept_pattern['weekend_activity'])
        elif base_date.hour < self.work_start_hour or base_date.hour >= self.work_end_hour:  # Evening
            work_hours_probability *= (1 - dept_pattern['evening_activity'])
        
        # Generate timestamp
        if random.random() < work_hours_probability:
            # During work hours - use realistic time patterns
            timestamp = self.get_random_time_in_work_hours(base_date, activity_type)
        else:
            # Outside work hours - more random but still realistic
            if base_date.weekday() >= 5:  # Weekend
                # Weekend hours: 9AM-8PM more likely
                hour_weights = [0.01] * 9 + [0.08] * 11 + [0.01] * 4
                hour = random.choices(range(24), weights=hour_weights)[0]
            else:  # Evening
                # Evening hours: 6PM-11PM more likely
                hour_weights = [0.01] * 18 + [0.12] * 5 + [0.01] * 1
                hour = random.choices(range(24), weights=hour_weights)[0]
            
            minute = random.randint(0, 59)
            second = random.randint(0, 59)
            
            timestamp = datetime(
                base_date.year, base_date.month, base_date.day,
                hour, minute, second
            )
        
        # Adjust for project type patterns (e.g., weekend pause for sprints)
        project_pattern = self.project_type_patterns.get(project_type, self.project_type_patterns['sprint'])
        
        if timestamp.weekday() >= 5:  # Weekend
            # Apply weekend pause factor
            if random.random() < project_pattern['weekend_pause_factor']:
                # Still active, but likely during reasonable hours
                timestamp = timestamp.replace(
                    hour=min(max(timestamp.hour, 9), 20),  # 9AM-8PM
                    minute=random.randint(0, 59),
                    second=random.randint(0, 59)
                )
            else:
                # Skip to next business day
                timestamp = self.get_next_business_day(timestamp)
                timestamp = self.get_random_time_in_work_hours(timestamp, activity_type)
        
        return timestamp
    
    def generate_task_lifecycle(self, created_at: datetime, due_date: Optional[datetime], 
                              department: str = 'engineering', project_type: str = 'sprint') -> Dict[str, Any]:
        """
        Generate realistic task lifecycle timestamps.
        
        Args:
            created_at: Task creation timestamp
            due_date: Task due date (optional)
            department: Department name
            project_type: Project type
            
        Returns:
            Dictionary with lifecycle timestamps and metadata
        """
        lifecycle = {
            'created_at': created_at,
            'started_at': None,
            'completed_at': None,
            'cycle_time_days': None,
            'lead_time_days': None
        }
        
        # Determine if task will be completed
        completion_probability = self._get_completion_probability(department, project_type, created_at, due_date)
        
        if random.random() < completion_probability:
            # Generate completion timestamp
            completed_at = self._generate_completion_timestamp(created_at, due_date, department, project_type)
            lifecycle['completed_at'] = completed_at
            
            # Generate started_at timestamp (sometime between created and completed)
            lifecycle['started_at'] = self._generate_started_timestamp(created_at, completed_at, department, project_type)
            
            # Calculate metrics
            lifecycle['cycle_time_days'] = (completed_at - lifecycle['started_at']).total_seconds() / 86400
            lifecycle['lead_time_days'] = (completed_at - created_at).total_seconds() / 86400
        
        return lifecycle
    
    def _get_completion_probability(self, department: str, project_type: str, 
                                   created_at: datetime, due_date: Optional[datetime]) -> float:
        """
        Get realistic completion probability based on context.
        
        Args:
            department: Department name
            project_type: Project type
            created_at: Creation timestamp
            due_date: Due date
            
        Returns:
            Completion probability (0-1)
        """
        # Base completion rates by department (industry benchmarks)
        base_rates = {
            'engineering': 0.65,
            'product': 0.70,
            'marketing': 0.75,
            'sales': 0.60,
            'operations': 0.72
        }
        
        base_rate = base_rates.get(department, 0.68)
        
        # Adjust based on project type
        project_adjustments = {
            'sprint': 0.15,      # Higher completion for sprints
            'bug_tracking': 0.05,
            'feature_development': 0.0,
            'campaign': 0.10,    # Higher completion for campaigns
            'research': -0.20    # Lower completion for research
        }
        
        adjustment = project_adjustments.get(project_type, 0.0)
        rate = base_rate + adjustment
        
        # Adjust based on due date proximity
        if due_date:
            days_until_due = (due_date - created_at).days
            if days_until_due <= 0:
                rate *= 0.5  # Overdue tasks less likely to be completed
            elif days_until_due <= 3:
                rate *= 1.2  # Urgent tasks more likely to be completed
            elif days_until_due <= 7:
                rate *= 1.1
            elif days_until_due > 30:
                rate *= 0.9  # Long-term tasks less likely to be completed
        
        # Adjust based on day of week (tasks created on Friday less likely completed)
        if created_at.weekday() == 4:  # Friday
            rate *= 0.85
        elif created_at.weekday() == 5 or created_at.weekday() == 6:  # Weekend
            rate *= 0.7
        
        return max(0.1, min(0.95, rate))  # Clamp between 10% and 95%
    
    def _generate_started_timestamp(self, created_at: datetime, completed_at: datetime, 
                                   department: str, project_type: str) -> datetime:
        """
        Generate realistic task started timestamp.
        
        Args:
            created_at: Task creation timestamp
            completed_at: Task completion timestamp
            department: Department name
            project_type: Project type
            
        Returns:
            Realistic started timestamp
        """
        # Time between creation and start (in hours)
        # Most tasks are started within 1-24 hours of creation
        hours_until_start = np.random.lognormal(mean=1.5, sigma=0.8)
        hours_until_start = max(0.5, min(hours_until_start, 168))  # 0.5 hours to 1 week
        
        # Adjust based on department and project type
        dept_adjustments = {
            'engineering': 1.0,
            'product': 0.8,      # Faster start for product tasks
            'marketing': 1.2,    # Slower start for marketing tasks
            'sales': 0.7,        # Very fast start for sales tasks
            'operations': 1.1
        }
        
        project_adjustments = {
            'sprint': 0.6,       # Fast start for sprint tasks
            'bug_tracking': 0.4, # Very fast start for bugs
            'campaign': 1.5,     # Slower start for campaigns
            'research': 2.0      # Very slow start for research
        }
        
        factor = dept_adjustments.get(department, 1.0) * project_adjustments.get(project_type, 1.0)
        hours_until_start *= factor
        
        # Convert to seconds and add to created_at
        start_timestamp = created_at + timedelta(hours=float(hours_until_start))
        
        # Ensure start timestamp is before completion and within reasonable bounds
        max_start_time = completed_at - timedelta(minutes=30)
        if start_timestamp > max_start_time:
            start_timestamp = max_start_time
        
        min_start_time = created_at + timedelta(minutes=5)
        if start_timestamp < min_start_time:
            start_timestamp = min_start_time
        
        # Adjust to business hours if needed
        if self.is_business_day(start_timestamp.date()):
            start_timestamp = start_timestamp.replace(
                hour=min(max(start_timestamp.hour, self.work_start_hour), self.work_end_hour - 1),
                minute=random.randint(0, 59),
                second=random.randint(0, 59)
            )
        
        return start_timestamp
    
    def _generate_completion_timestamp(self, created_at: datetime, due_date: Optional[datetime], 
                                      department: str, project_type: str) -> datetime:
        """
        Generate realistic task completion timestamp.
        
        Args:
            created_at: Task creation timestamp
            due_date: Task due date (optional)
            department: Department name
            project_type: Project type
            
        Returns:
            Realistic completion timestamp
        """
        project_pattern = self.project_type_patterns.get(project_type, self.project_type_patterns['sprint'])
        
        # Calculate base duration (in hours) using log-normal distribution
        # Most tasks take 2-48 hours to complete
        base_hours = np.random.lognormal(mean=2.5, sigma=1.0)
        base_hours = max(1.0, min(base_hours, 336))  # 1 hour to 2 weeks
        
        # Apply department multiplier
        dept_multipliers = {
            'engineering': 1.2,   # Longer for engineering
            'product': 1.0,
            'marketing': 0.8,     # Faster for marketing
            'sales': 0.6,         # Much faster for sales
            'operations': 1.1
        }
        
        multiplier = dept_multipliers.get(department, 1.0)
        duration_hours = base_hours * multiplier
        
        # Apply project type acceleration (near due date)
        if due_date:
            days_until_due = (due_date - created_at).days
            if days_until_due > 0:
                # Acceleration factor increases as due date approaches
                acceleration = 1.0 + (project_pattern['completion_acceleration'] - 1.0) * (1 - min(1.0, days_until_due / 30))
                duration_hours /= acceleration
        
        # Convert to completion timestamp
        completion_timestamp = created_at + timedelta(hours=float(duration_hours))
        
        # Ensure completion is after creation and before due date if specified
        if due_date and completion_timestamp > due_date:
            # Complete before due date, but not too early
            max_completion = due_date
            min_completion = max(created_at + timedelta(hours=1), due_date - timedelta(days=3))
            
            if min_completion < max_completion:
                completion_timestamp = min_completion + (max_completion - min_completion) * random.random()
        
        # Adjust to business hours and realistic completion times
        if self.is_business_day(completion_timestamp.date()):
            completion_timestamp = self.get_random_time_in_work_hours(completion_timestamp, 'task_completion')
        else:
            # Weekend completion - adjust to reasonable hours
            completion_timestamp = completion_timestamp.replace(
                hour=min(max(completion_timestamp.hour, 9), 20),
                minute=random.randint(0, 59),
                second=random.randint(0, 59)
            )
        
        return completion_timestamp
    
    def generate_time_series_pattern(self, start_date: datetime, end_date: datetime, 
                                   pattern_type: str = 'daily_task_creation', 
                                   department: str = 'engineering', 
                                   project_type: str = 'sprint') -> List[Tuple[datetime, float]]:
        """
        Generate realistic time series patterns for analytics.
        
        Args:
            start_date: Start datetime
            end_date: End datetime
            pattern_type: Type of pattern ('daily_task_creation', 'weekly_completion_rate', etc.)
            department: Department name
            project_type: Project type
            
        Returns:
            List of (timestamp, value) tuples
        """
        time_series = []
        current_date = start_date
        
        # Get pattern parameters
        project_pattern = self.project_type_patterns.get(project_type, self.project_type_patterns['sprint'])
        
        while current_date <= end_date:
            # Base value based on pattern type
            if pattern_type == 'daily_task_creation':
                base_value = np.random.normal(loc=5.0, scale=project_pattern['daily_task_creation_std_dev'])
                base_value = max(0, base_value)
                
                # Weekend reduction
                if current_date.weekday() >= 5:
                    base_value *= project_pattern['weekend_pause_factor']
                
                # Department adjustment
                dept_adjustment = {
                    'engineering': 1.0,
                    'product': 0.8,
                    'marketing': 1.2,
                    'sales': 0.9,
                    'operations': 0.7
                }.get(department, 1.0)
                base_value *= dept_adjustment
                
            elif pattern_type == 'weekly_completion_rate':
                base_value = np.random.normal(loc=0.65, scale=0.1)
                base_value = max(0.2, min(0.95, base_value))
                
                # End of week boost
                if current_date.weekday() == 4:  # Friday
                    base_value *= 1.2
            
            else:
                base_value = np.random.normal(loc=1.0, scale=0.3)
                base_value = max(0, base_value)
            
            time_series.append((current_date.copy(), base_value))
            current_date += timedelta(days=1)
        
        return time_series
    
    def get_simulation_time_range(self, start_date_str: str, end_date_str: str) -> Dict[str, Any]:
        """
        Get simulation time range with business day awareness.
        
        Args:
            start_date_str: Start date string (YYYY-MM-DD)
            end_date_str: End date string (YYYY-MM-DD)
            
        Returns:
            Dictionary with time range information
        """
        try:
            start_date = datetime.strptime(start_date_str, '%Y-%m-%d')
            end_date = datetime.strptime(end_date_str, '%Y-%m-%d')
        except ValueError as e:
            logger.error(f"Invalid date format: {e}")
            # Use defaults if parsing fails
            start_date = datetime.now() - timedelta(days=180)
            end_date = datetime.now()
        
        # Ensure start_date is not after end_date
        if start_date > end_date:
            start_date, end_date = end_date, start_date
        
        # Calculate business days
        business_days = 0
        current = start_date
        while current <= end_date:
            if self.is_business_day(current.date()):
                business_days += 1
            current += timedelta(days=1)
        
        return {
            'start_date': start_date,
            'end_date': end_date,
            'total_days': (end_date - start_date).days + 1,
            'business_days': business_days,
            'weekend_days': (end_date - start_date).days + 1 - business_days,
            'timezone': self.timezone
        }
    
    def close(self):
        """Cleanup resources."""
        logger.info("Temporal generator closed")

# Example usage and testing
if __name__ == "__main__":
    # Setup logging for testing
    logging.basicConfig(level=logging.INFO)
    
    # Mock configuration
    mock_config = {
        'simulation_timezone': 'America/Los_Angeles',
        'debug_mode': True
    }
    
    # Create temporal generator
    generator = TemporalGenerator(mock_config)
    
    try:
        print("=== Temporal Generator Testing ===\n")
        
        # Test business day logic
        test_dates = [
            datetime(2026, 1, 1),  # New Year's Day (holiday)
            datetime(2026, 1, 2),  # Regular weekday
            datetime(2026, 1, 3),  # Friday
            datetime(2026, 1, 4),  # Saturday
            datetime(2026, 1, 5),  # Sunday
            datetime(2026, 1, 6)   # Monday
        ]
        
        print("Business Day Tests:")
        for date_obj in test_dates:
            is_business = generator.is_business_day(date_obj.date())
            holiday_name = generator.holiday_calendar.get(date_obj.date())
            print(f"  {date_obj.date()}: {'Business Day' if is_business else 'Non-Business Day'}")
            if holiday_name:
                print(f"    Holiday: {holiday_name}")
        
        print("\nBusiness Day Offset Tests:")
        base_date = datetime(2026, 1, 2, 10, 30, 0)  # Thursday
        for offset in [-2, -1, 0, 1, 2, 3]:
            result_date = generator.get_business_day_offset(base_date, offset)
            print(f"  {offset} business days from {base_date.date()}: {result_date.date()}")
        
        print("\nRealistic Timestamp Generation:")
        departments = ['engineering', 'marketing', 'sales']
        project_types = ['sprint', 'campaign', 'research']
        
        for dept in departments:
            for proj_type in project_types:
                timestamp = generator.generate_realistic_timestamp(
                    activity_type='task_creation',
                    department=dept,
                    project_type=proj_type
                )
                print(f"  {dept} {proj_type}: {timestamp.strftime('%Y-%m-%d %H:%M:%S')}")
        
        print("\nTask Lifecycle Generation:")
        created_at = datetime(2026, 1, 2, 9, 30, 0)
        due_date = datetime(2026, 1, 15, 17, 0, 0)
        
        lifecycle = generator.generate_task_lifecycle(
            created_at=created_at,
            due_date=due_date,
            department='engineering',
            project_type='sprint'
        )
        
        print(f"  Created at: {lifecycle['created_at'].strftime('%Y-%m-%d %H:%M:%S')}")
        if lifecycle['started_at']:
            print(f"  Started at: {lifecycle['started_at'].strftime('%Y-%m-%d %H:%M:%S')}")
        if lifecycle['completed_at']:
            print(f"  Completed at: {lifecycle['completed_at'].strftime('%Y-%m-%d %H:%M:%S')}")
            print(f"  Cycle time: {lifecycle['cycle_time_days']:.1f} days")
            print(f"  Lead time: {lifecycle['lead_time_days']:.1f} days")
        
        print("\nTime Series Pattern Generation:")
        start_date = datetime(2026, 1, 1)
        end_date = datetime(2026, 1, 14)
        
        pattern = generator.generate_time_series_pattern(
            start_date=start_date,
            end_date=end_date,
            pattern_type='daily_task_creation',
            department='engineering',
            project_type='sprint'
        )
        
        print("  Daily Task Creation Pattern:")
        for timestamp, value in pattern:
            print(f"    {timestamp.date()}: {value:.1f} tasks")
    
    finally:
        generator.close()