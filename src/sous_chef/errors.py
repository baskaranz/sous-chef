from typing import Dict, List, Any
from dataclasses import dataclass

@dataclass
class ValidationError:
    """Structured validation error"""
    path: str  # JSON path to error location
    code: str  # Machine-readable error code
    message: str  # Human-readable message
    context: Dict[str, Any] = None  # Additional context

class SousChefError(Exception):
    """Base exception for SousChef errors"""
    def __init__(self, message: str, errors: List[ValidationError] = None):
        super().__init__(message)
        self.errors = errors or []
    
    def to_dict(self) -> Dict:
        """Convert to CI-friendly dictionary format"""
        return {
            "message": str(self),
            "errors": [
                {
                    "path": e.path,
                    "code": e.code,
                    "message": e.message,
                    "context": e.context
                }
                for e in self.errors
            ]
        }