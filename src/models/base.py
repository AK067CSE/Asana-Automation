

"""
Base data models module for type safety and validation in the seed data generation pipeline.
This module provides the foundational data models, validation logic, and type definitions
that ensure data integrity and consistency throughout the generation process.

The models are designed to be:
- Type-safe: Strict type checking with Pydantic validation
- Extensible: Base classes that can be inherited and extended
- Validatable: Built-in validation logic for business rules
- Serializable: Easy JSON/database serialization
- Documented: Clear field descriptions and validation rules
"""

import logging
import re
from datetime import datetime, date, timedelta
from typing import Optional, Dict, List, Any, Union, Tuple, Set, TypeVar, Generic, Type
from enum import Enum, auto
from dataclasses import dataclass, field, asdict
from abc import ABC, abstractmethod
import json

from src.utils.logging import get_logger

logger = get_logger(__name__)

T = TypeVar('T')

# Base validation types and patterns
class ValidationLevel(Enum):
    """Validation severity levels."""
    CRITICAL = auto()    # Will raise exception
    WARNING = auto()     # Will log warning but continue
    INFO = auto()        # Will log informational message

class ValidationError(Exception):
    """Base exception for validation errors."""
    def __init__(self, message: str, field: str = None, level: ValidationLevel = ValidationLevel.CRITICAL):
        self.message = message
        self.field = field
        self.level = level
        super().__init__(message)

class BaseModel(ABC):
    """
    Base model class with validation and serialization capabilities.
    
    All domain models should inherit from this class to ensure:
    - Consistent validation
    - JSON serialization
    - Type safety
    - Error handling
    """
    
    def validate(self) -> List[Dict[str, Any]]:
        """
        Validate the model instance.
        
        Returns:
            List of validation results (empty if valid)
        """
        results = []
        try:
            self._validate_fields()
            self._validate_business_rules()
        except ValidationError as e:
            results.append({
                'field': e.field,
                'message': e.message,
                'level': e.level.name,
                'success': False
            })
        except Exception as e:
            results.append({
                'field': 'unknown',
                'message': f"Unexpected validation error: {str(e)}",
                'level': 'CRITICAL',
                'success': False
            })
        
        return results
    
    def is_valid(self) -> bool:
        """
        Check if the model is valid.
        
        Returns:
            True if valid, False otherwise
        """
        validation_results = self.validate()
        return not any(result['level'] == 'CRITICAL' for result in validation_results)
    
    def to_dict(self) -> Dict[str, Any]:
        """
        Convert model to dictionary.
        
        Returns:
            Dictionary representation of the model
        """
        return asdict(self)
    
    def to_json(self) -> str:
        """
        Convert model to JSON string.
        
        Returns:
            JSON string representation
        """
        return json.dumps(self.to_dict(), default=self._json_default)
    
    @staticmethod
    def _json_default(obj: Any) -> Any:
        """Default JSON serializer for non-serializable objects."""
        if isinstance(obj, (datetime, date)):
            return obj.isoformat()
        if isinstance(obj, Enum):
            return obj.value
        if hasattr(obj, 'to_dict'):
            return obj.to_dict()
        raise TypeError(f"Object of type {type(obj)} is not JSON serializable")
    
    @abstractmethod
    def _validate_fields(self):
        """Validate individual fields."""
        pass
    
    @abstractmethod
    def _validate_business_rules(self):
        """Validate business rules and relationships."""
        pass

@dataclass
class TimeRange(BaseModel):
    """
    Time range model for temporal data generation.
    
    This model represents a time range with validation for:
    - Start date before end date
    - Reasonable duration limits
    - Business day considerations
    """
    start_date: datetime
    end_date: datetime
    
    def _validate_fields(self):
        """Validate individual fields."""
        if not isinstance(self.start_date, datetime):
            raise ValidationError("start_date must be a datetime object", "start_date")
        if not isinstance(self.end_date, datetime):
            raise ValidationError("end_date must be a datetime object", "end_date")
    
    def _validate_business_rules(self):
        """Validate business rules."""
        if self.start_date > self.end_date:
            raise ValidationError(f"start_date ({self.start_date}) cannot be after end_date ({self.end_date})", "start_date")
        
        # Check reasonable duration (max 2 years for seed data)
        max_duration = timedelta(days=730)  # 2 years
        if (self.end_date - self.start_date) > max_duration:
            raise ValidationError(f"Time range duration exceeds maximum of 2 years", "duration")
    
    def get_business_days(self) -> int:
        """Get number of business days in the time range."""
        from src.utils.temporal import get_business_day_offset, is_business_day
        
        business_days = 0
        current_date = self.start_date
        
        while current_date <= self.end_date:
            if is_business_day(current_date.date()):
                business_days += 1
            current_date += timedelta(days=1)
        
        return business_days
    
    def get_random_date(self, include_time: bool = False) -> Union[datetime, date]:
        """Get a random date within the time range."""
        total_seconds = (self.end_date - self.start_date).total_seconds()
        random_seconds = random.uniform(0, total_seconds)
        random_date = self.start_date + timedelta(seconds=random_seconds)
        
        if include_time:
            return random_date
        return random_date.date()
    
    def __str__(self):
        return f"TimeRange({self.start_date.date()} to {self.end_date.date()})"

@dataclass
class ContactInfo(BaseModel):
    """
    Contact information model with validation.
    
    This model handles email, phone, and other contact details with:
    - Format validation
    - Business rule compliance
    - Privacy considerations
    """
    email: Optional[str] = None
    phone: Optional[str] = None
    slack_handle: Optional[str] = None
    
    def _validate_fields(self):
        """Validate individual fields."""
        if self.email:
            if not re.match(r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$', self.email):
                raise ValidationError(f"Invalid email format: {self.email}", "email")
        
        if self.phone:
            if not re.match(r'^\+?1?[-. (]*\d{3}[-. )]*\d{3}[-. ]*\d{4}$', self.phone):
                raise ValidationError(f"Invalid phone format: {self.phone}", "phone")
        
        if self.slack_handle:
            if not re.match(r'^[@a-zA-Z0-9._-]+$', self.slack_handle):
                raise ValidationError(f"Invalid Slack handle format: {self.slack_handle}", "slack_handle")
    
    def _validate_business_rules(self):
        """Validate business rules."""
        if not any([self.email, self.phone, self.slack_handle]):
            # This is a warning, not critical - users might not have all contact info
            logger.warning("ContactInfo has no contact methods specified")
    
    def get_primary_contact(self) -> Tuple[str, str]:
        """Get the primary contact method and value."""
        if self.email:
            return 'email', self.email
        if self.slack_handle:
            return 'slack', self.slack_handle
        if self.phone:
            return 'phone', self.phone
        return 'none', ''

@dataclass
class Metadata(BaseModel):
    """
    Generic metadata model for additional data attributes.
    
    This model handles flexible metadata with:
    - Type validation
    - Size limits
    - Security considerations
    """
    data: Dict[str, Any] = field(default_factory=dict)
    version: str = "1.0"
    created_at: datetime = field(default_factory=datetime.now)
    updated_at: datetime = field(default_factory=datetime.now)
    
    def _validate_fields(self):
        """Validate individual fields."""
        if not isinstance(self.data, dict):
            raise ValidationError("data must be a dictionary", "data")
        
        if not isinstance(self.version, str):
            raise ValidationError("version must be a string", "version")
        
        if len(json.dumps(self.data)) > 10000:  # 10KB limit
            raise ValidationError("metadata size exceeds 10KB limit", "data")
    
    def _validate_business_rules(self):
        """Validate business rules."""
        if self.updated_at < self.created_at:
            raise ValidationError("updated_at cannot be before created_at", "updated_at")
    
    def add(self, key: str, value: Any):
        """Add or update a metadata entry."""
        self.data[key] = value
        self.updated_at = datetime.now()
    
    def get(self, key: str, default: Any = None) -> Any:
        """Get a metadata entry."""
        return self.data.get(key, default)
    
    def remove(self, key: str):
        """Remove a metadata entry."""
        if key in self.data:
            del self.data[key]
            self.updated_at = datetime.now()

@dataclass
class Status(BaseModel):
    """
    Status model for tracking entity states.
    
    This model handles status transitions and validation:
    - Valid status transitions
    - Status history tracking
    - Business rule compliance
    """
    current: str
    history: List[Dict[str, Any]] = field(default_factory=list)
    last_updated: datetime = field(default_factory=datetime.now)
    
    VALID_STATUSES = {'active', 'completed', 'archived', 'blocked', 'pending'}
    STATUS_TRANSITIONS = {
        'active': {'completed', 'blocked', 'archived'},
        'completed': {'archived'},
        'blocked': {'active', 'archived'},
        'pending': {'active', 'archived'},
        'archived': set()  # No transitions from archived
    }
    
    def _validate_fields(self):
        """Validate individual fields."""
        if self.current not in self.VALID_STATUSES:
            raise ValidationError(f"Invalid status: {self.current}. Valid statuses are: {self.VALID_STATUSES}", "current")
        
        if not isinstance(self.history, list):
            raise ValidationError("history must be a list", "history")
    
    def _validate_business_rules(self):
        """Validate business rules."""
        if self.history:
            # Validate last status transition
            last_entry = self.history[-1]
            last_status = last_entry.get('status')
            if last_status and last_status != self.current:
                if self.current not in self.STATUS_TRANSITIONS.get(last_status, set()):
                    raise ValidationError(
                        f"Invalid status transition from {last_status} to {self.current}", 
                        "current"
                    )
    
    def update(self, new_status: str, reason: str = None, user: str = None):
        """Update status with transition validation."""
        if new_status not in self.VALID_STATUSES:
            raise ValidationError(f"Invalid status: {new_status}", "new_status")
        
        if new_status == self.current:
            return  # No change needed
        
        # Validate transition
        if self.current not in self.STATUS_TRANSITIONS or new_status not in self.STATUS_TRANSITIONS[self.current]:
            raise ValidationError(
                f"Invalid status transition from {self.current} to {new_status}", 
                "new_status"
            )
        
        # Record history
        self.history.append({
            'from': self.current,
            'to': new_status,
            'reason': reason,
            'user': user,
            'timestamp': self.last_updated.isoformat()
        })
        
        # Update status
        self.current = new_status
        self.last_updated = datetime.now()
    
    def get_duration_in_current_status(self) -> timedelta:
        """Get duration in current status."""
        if not self.history:
            return datetime.now() - self.last_updated
        return datetime.now() - datetime.fromisoformat(self.history[-1]['timestamp'])
    
    def __str__(self):
        return f"Status({self.current}, updated: {self.last_updated.strftime('%Y-%m-%d %H:%M:%S')})"

@dataclass
class ValidationConfig:
    """
    Configuration for validation behavior.
    
    This model configures how validation should behave:
    - Strictness levels
    - Error handling
    - Performance considerations
    """
    strict_mode: bool = True
    max_errors: int = 10
    log_warnings: bool = True
    fail_fast: bool = False
    
    def should_raise_error(self, error_count: int, level: ValidationLevel) -> bool:
        """Determine if an error should raise an exception."""
        if self.fail_fast:
            return True
        
        if level == ValidationLevel.CRITICAL:
            return True
        
        if not self.strict_mode and level == ValidationLevel.WARNING:
            return False
        
        return error_count < self.max_errors

class ValidatableCollection(Generic[T]):
    """
    Collection of validatable items with batch validation.
    
    This class handles collections of BaseModel items with:
    - Batch validation
    - Error aggregation
    - Performance optimization
    - Reporting capabilities
    """
    
    def __init__(self, items: List[T] = None, config: ValidationConfig = None):
        self.items = items or []
        self.config = config or ValidationConfig()
        self.validation_results = []
    
    def add(self, item: T):
        """Add an item to the collection."""
        self.items.append(item)
    
    def validate_all(self) -> Dict[str, Any]:
        """
        Validate all items in the collection.
        
        Returns:
            Dictionary with validation summary
        """
        self.validation_results = []
        total_items = len(self.items)
        valid_count = 0
        warning_count = 0
        error_count = 0
        
        for i, item in enumerate(self.items):
            if hasattr(item, 'validate'):
                try:
                    results = item.validate()
                    if not results:  # No validation errors
                        valid_count += 1
                    else:
                        for result in results:
                            if result['level'] == 'CRITICAL':
                                error_count += 1
                            elif result['level'] == 'WARNING':
                                warning_count += 1
                        
                        self.validation_results.append({
                            'item_index': i,
                            'item': str(item),
                            'results': results
                        })
                except Exception as e:
                    error_count += 1
                    self.validation_results.append({
                        'item_index': i,
                        'item': str(item),
                        'error': str(e)
                    })
        
        success_rate = valid_count / total_items if total_items > 0 else 0
        
        return {
            'total_items': total_items,
            'valid_count': valid_count,
            'warning_count': warning_count,
            'error_count': error_count,
            'success_rate': success_rate,
            'results': self.validation_results
        }
    
    def get_valid_items(self) -> List[T]:
        """Get only valid items from the collection."""
        valid_items = []
        
        for item in self.items:
            if hasattr(item, 'is_valid') and item.is_valid():
                valid_items.append(item)
        
        return valid_items
    
    def get_invalid_items(self) -> List[Tuple[T, List[Dict[str, Any]]]]:
        """Get invalid items with their validation results."""
        invalid_items = []
        
        for item in self.items:
            if hasattr(item, 'validate'):
                results = item.validate()
                if any(result['level'] == 'CRITICAL' for result in results):
                    invalid_items.append((item, results))
        
        return invalid_items
    
    def __len__(self):
        return len(self.items)
    
    def __iter__(self):
        return iter(self.items)

# Example usage and testing
if __name__ == "__main__":
    # Setup logging for testing
    logging.basicConfig(level=logging.INFO)
    
    print("=== Base Models Testing ===\n")
    
    try:
        # Test TimeRange model
        print("Testing TimeRange model:")
        start_date = datetime(2025, 7, 1)
        end_date = datetime(2026, 1, 7)
        
        time_range = TimeRange(start_date=start_date, end_date=end_date)
        validation_results = time_range.validate()
        
        print(f"  TimeRange: {time_range}")
        print(f"  Business days: {time_range.get_business_days()}")
        print(f"  Random date: {time_range.get_random_date()}")
        
        if validation_results:
            print("  Validation errors:")
            for result in validation_results:
                print(f"    - {result['message']} ({result['level']})")
        else:
            print("  ✓ Validation passed")
        
        # Test invalid TimeRange
        try:
            invalid_range = TimeRange(start_date=end_date, end_date=start_date)
            invalid_range.validate()
        except ValidationError as e:
            print(f"  ✓ Caught expected validation error: {e.message}")
        
        # Test ContactInfo model
        print("\nTesting ContactInfo model:")
        contact = ContactInfo(
            email="user@example.com",
            phone="+1 (555) 123-4567",
            slack_handle="@user"
        )
        print(f"  Contact: {contact}")
        print(f"  Primary contact: {contact.get_primary_contact()}")
        
        # Test invalid email
        try:
            invalid_contact = ContactInfo(email="invalid-email")
            invalid_contact.validate()
        except ValidationError as e:
            print(f"  ✓ Caught expected validation error: {e.message}")
        
        # Test Status model
        print("\nTesting Status model:")
        status = Status(current="active")
        print(f"  Initial status: {status}")
        
        # Test valid transition
        status.update("completed", reason="All tasks finished", user="user1")
        print(f"  After transition: {status}")
        
        # Test invalid transition
        try:
            status.update("active")  # Cannot go back from completed to active
        except ValidationError as e:
            print(f"  ✓ Caught expected validation error: {e.message}")
        
        # Test ValidatableCollection
        print("\nTesting ValidatableCollection:")
        items = [
            TimeRange(start_date=datetime(2025, 1, 1), end_date=datetime(2025, 12, 31)),
            TimeRange(start_date=datetime(2026, 1, 1), end_date=datetime(2025, 12, 31)),  # Invalid
            TimeRange(start_date=datetime(2025, 6, 1), end_date=datetime(2026, 6, 1))
        ]
        
        collection = ValidatableCollection(items)
        results = collection.validate_all()
        
        print(f"  Collection validation results:")
        print(f"    Total items: {results['total_items']}")
        print(f"    Valid items: {results['valid_count']}")
        print(f"    Error items: {results['error_count']}")
        print(f"    Success rate: {results['success_rate']:.1%}")
        
        # Test metadata
        print("\nTesting Metadata model:")
        metadata = Metadata()
        metadata.add("source", "seed_generator")
        metadata.add("version", "1.0")
        metadata.add("config", {"batch_size": 1000, "quality": "high"})
        
        print(f"  Meta {metadata.to_json()}")
        print(f"  Source: {metadata.get('source')}")
        print(f"  Non-existent field: {metadata.get('nonexistent', 'default_value')}")
        
        print("\n✅ All base model tests completed successfully!")
        
    except Exception as e:
        print(f"Test error: {str(e)}")
        import traceback
        traceback.print_exc()