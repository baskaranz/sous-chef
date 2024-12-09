import os
from typing import Dict, List, Any
from pathlib import Path
from datetime import timedelta
from .errors import ValidationError, SousChefError

class FeatureViewValidator:
    """Validates feature view configurations"""
    
    REQUIRED_FIELDS = ['source_name', 'entities', 'schema']
    OPTIONAL_FIELDS = ['ttl_days', 'online', 'description', 'tags']
    
    @classmethod
    def validate(cls, name: str, config: Dict[str, Any]) -> List[str]:
        """
        Validate feature view configuration
        
        Args:
            name: Feature view name
            config: Feature view configuration dictionary
            
        Returns:
            List of validation error messages, empty if valid
        """
        errors = []
        
        # Check required fields
        for field in cls.REQUIRED_FIELDS:
            if field not in config:
                errors.append(f"Missing required field '{field}' in feature view '{name}'")
        
        # Validate schema if present
        if 'schema' in config:
            schema_errors = cls._validate_schema(config['schema'])
            errors.extend(f"Schema error in '{name}': {err}" for err in schema_errors)
            
        # Validate ttl_days if present
        if 'ttl_days' in config:
            try:
                ttl = int(config['ttl_days'])
                if ttl <= 0:
                    errors.append(f"ttl_days must be positive in feature view '{name}'")
            except ValueError:
                errors.append(f"Invalid ttl_days value in feature view '{name}'")
                
        # Check for unknown fields
        valid_fields = set(cls.REQUIRED_FIELDS + cls.OPTIONAL_FIELDS)
        unknown = set(config.keys()) - valid_fields
        if unknown:
            errors.append(f"Unknown fields in feature view '{name}': {', '.join(unknown)}")
            
        return errors
    
    @staticmethod
    def _validate_schema(schema: List[Dict]) -> List[str]:
        """Validate feature schema configuration"""
        errors = []
        for idx, field in enumerate(schema):
            if 'name' not in field:
                errors.append(f"Missing 'name' in schema field {idx}")
            if 'dtype' not in field:
                errors.append(f"Missing 'dtype' in schema field {idx}")
        return errors

class ConfigValidator:
    """Validates configurations for CI safety"""
    
    @classmethod
    def validate_ci_safety(cls, config: Dict, base_path: Path) -> List[ValidationError]:
        """Validate configuration is safe for CI use"""
        errors = []
        
        # Validate paths are relative
        if 'data_sources' in config:
            for source_name, source in config['data_sources'].items():
                if 'path' in source and os.path.isabs(source['path']):
                    errors.append(ValidationError(
                        path=f"data_sources.{source_name}.path",
                        code="ABSOLUTE_PATH",
                        message="Absolute paths are not allowed in CI",
                        context={"path": source['path']}
                    ))
                    
        # Validate no environment variables in values
        cls._check_env_vars(config, "", errors)
        
        return errors
    
    @classmethod
    def _check_env_vars(cls, value: Any, path: str, errors: List[ValidationError]):
        """Recursively check for environment variable references"""
        if isinstance(value, str):
            if "${" in value or "$(" in value:
                errors.append(ValidationError(
                    path=path,
                    code="ENV_VAR_REFERENCE",
                    message="Environment variable references not allowed in CI",
                    context={"value": value}
                ))
        elif isinstance(value, dict):
            for k, v in value.items():
                cls._check_env_vars(v, f"{path}.{k}" if path else k, errors)
        elif isinstance(value, list):
            for i, v in enumerate(value):
                cls._check_env_vars(v, f"{path}[{i}]", errors)

class SQLValidator:
    """Validates SQL configurations in YAML"""

    @classmethod
    def validate_sql(cls, query: str) -> List[str]:
        """Validate SQL query structure"""
        errors = []
        
        # Normalize query
        query = ' '.join(query.strip().split())
        
        # Validate query has SELECT
        if 'SELECT' not in query.upper():
            errors.append("Query must contain SELECT statement")
            return errors
            
        # Split on SELECT to handle subqueries and CTEs
        parts = query.upper().split('SELECT')
        
        for part in parts[1:]:  # Skip first empty part
            if 'FROM' not in part:
                continue
                
            select_part = part.split('FROM')[0].strip()
            
            # Validate each column has alias
            for expr in select_part.split(','):
                expr = expr.strip()
                if expr and ' AS ' not in expr:
                    errors.append(f"Column expression missing alias: {expr}")

        return errors

    @classmethod 
    def validate_config(cls, config: Dict) -> List[str]:
        """Validate SQL source configuration"""
        errors = []
        
        # Required fields
        required = ['query', 'timestamp_field', 'database', 'schema']
        for field in required:
            if field not in config:
                errors.append(f"Missing required field: {field}")
                
        # Validate query if present
        if 'query' in config:
            query_errors = cls.validate_sql(config['query'])
            errors.extend(query_errors)
            
        return errors