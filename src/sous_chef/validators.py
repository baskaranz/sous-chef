from typing import Dict, List, Optional
from enum import Enum
import yaml
from pathlib import Path
from typing import Dict, List, Set

class ValidationErrorCode(Enum):
    """Enumeration of possible validation error codes"""
    INVALID_SQL = "INVALID_SQL"
    MISSING_FIELD = "MISSING_FIELD"

class SQLValidator:
    """SQL query validator with support for SELECT statements"""
    
    AGGREGATE_FUNCTIONS = {'COUNT', 'SUM', 'AVG', 'MAX', 'MIN', 'ARRAY_AGG', 'COLLECT_LIST'}
    WINDOW_FUNCTIONS = {'RANK', 'ROW_NUMBER', 'LAG', 'LEAD', 'FIRST_VALUE', 'LAST_VALUE', 'AVG', 'SUM'}
    
    @classmethod
    def validate_sql(cls, query: str) -> bool:
        """Validate SQL query and return True if valid, False otherwise"""
        try:
            # Clean query
            clean_query = ""
            current_line = ""
            
            # First parse preserving content between parentheses
            in_parens = 0
            for char in query:
                if char == '(':
                    in_parens += 1
                elif char == ')':
                    in_parens -= 1
                
                if char == '\n' and in_parens == 0:
                    if '--' in current_line:
                        current_line = current_line[:current_line.index('--')]
                    if current_line.strip():
                        clean_query += " " + current_line.strip()
                    current_line = ""
                else:
                    current_line += char
                    
            # Add last line
            if current_line.strip():
                if '--' in current_line:
                    current_line = current_line[:current_line.index('--')]
                clean_query += " " + current_line.strip()
                
            clean_query = clean_query.strip()
            
            # Basic validation
            if not clean_query.upper().startswith('SELECT'):
                return False

            # Find FROM clause (not in EXTRACT function)
            query_upper = clean_query.upper()
            in_extract = False
            from_pos = -1
            i = 0
            
            while i < len(query_upper):
                if query_upper[i:].startswith('EXTRACT'):
                    in_extract = True
                elif query_upper[i:].startswith('FROM') and not in_extract:
                    from_pos = i
                    break
                elif query_upper[i] == ')':
                    in_extract = False
                i += 1
                
            if from_pos == -1:
                return False
                
            # Get SELECT columns
            select_part = clean_query[6:from_pos].strip()
            if not select_part:
                return False

            # Parse and validate columns
            columns = []
            current = []
            parens = 0
            for char in select_part:
                if char == '(':
                    parens += 1
                    current.append(char)
                elif char == ')':
                    parens -= 1
                    current.append(char)
                elif char == ',' and parens == 0:
                    if current:
                        columns.append(''.join(current).strip())
                    current = []
                else:
                    current.append(char)
                    
            if current:
                columns.append(''.join(current).strip())

            # Check each column
            for col in columns:
                col = col.strip().upper()
                if not col:
                    continue
                    
                # Skip if already has alias
                if ' AS ' in col:
                    continue
                    
                # Skip simple column references
                if col.isalnum():
                    continue
                    
                # Skip qualified columns (table.column)
                if '.' in col and not col.endswith('.') and len(col.split('.')) == 2:
                    continue
                    
                # All other expressions need aliases
                if (
                    col.endswith('.') or
                    '(' in col or
                    any(op in col for op in ['+', '-', '*', '/']) or
                    any(fn in col for fn in ['CASE', 'EXTRACT'])
                ):
                    return False

            return True

        except Exception as e:
            print(f"Validation error: {str(e)}")
            return False

    @classmethod
    def validate_config(cls, config: Dict) -> bool:
        """Validate configuration"""
        # Check required fields
        if not all(k in config for k in ['query', 'timestamp_field', 'database']):
            return False
            
        # Check query
        return cls.validate_sql(config['query'])

    @classmethod
    def _split_columns(cls, select_part: str) -> List[str]:
        """Split SELECT columns handling nested expressions"""
        columns = []
        current = []
        parens = 0
        
        for char in select_part:
            if char == '(':
                parens += 1
            elif char == ')':
                parens -= 1
            elif char == ',' and parens == 0:
                if current:
                    columns.append(''.join(current).strip())
                current = []
                continue
            current.append(char)
            
        if current:
            columns.append(''.join(current).strip())
            
        return [col for col in columns if col]  # Filter out empty strings

class ConfigValidator:
    def __init__(self, metadata_rules: Optional[Dict] = None):
        """Initialize validator with optional custom metadata rules"""
        if metadata_rules:
            self.metadata_rules = metadata_rules
        else:
            rules_path = Path(__file__).parent / "config" / "metadata_rules.yaml"
            with open(rules_path) as f:
                self.metadata_rules = yaml.safe_load(f)['metadata_rules']

    def _get_required_tags(self, context_type: str) -> Set[str]:
        """Get required tags for a specific context"""
        rules = self.metadata_rules['required_tags']
        # Get global and context-specific required tags
        global_tags = set(rules.get('global', []))
        context_tags = set(rules.get(context_type, []))
        return global_tags | context_tags

    def _get_allowed_tags(self, context_type: str) -> Set[str]:
        """Get all allowed tags for a context"""
        # Start with optional tags
        allowed = set(self.metadata_rules['optional_tags'].get('global', []))
        # Add all required tags as allowed
        for section in self.metadata_rules['required_tags'].values():
            allowed.update(section)
        return allowed

    def validate_tags(self, tags: Dict, context: str, context_type: str) -> List[str]:
        """Validate tags structure and content"""
        if not isinstance(tags, dict):
            return [f"{context}: tags must be a dictionary"]

        # Get required and allowed tags
        required_tags = self._get_required_tags(context_type)
        allowed_tags = self._get_allowed_tags(context_type)

        errors = []
        
        # Check for invalid tags first
        invalid = set(tags.keys()) - allowed_tags
        if invalid:
            errors.append(f"{context}: unsupported tags found: {invalid}")

        # Check for missing required tags
        missing = required_tags - set(tags.keys())
        if missing:
            errors.append(f"{context}: missing required tags: {missing}")
            
        return errors

    @classmethod
    def validate(cls, config: Dict, metadata_rules: Optional[Dict] = None) -> List[str]:
        """Validate configuration with optional custom metadata rules"""
        validator = cls(metadata_rules=metadata_rules)
        errors = []
        
        if not isinstance(config, dict):
            errors.append("Configuration must be a dictionary")
            return errors
            
        if 'query' in config:
            sql_errors = SQLValidator.validate_sql(config['query'])
            errors.extend(sql_errors)
            
        # Validate feature views section
        if 'feature_views' in config:
            for name, view_config in config['feature_views'].items():
                if not isinstance(view_config, dict):
                    errors.append(f"Feature view '{name}' configuration must be a dictionary")
                    continue
                    
                required_fields = {'source_name', 'entities', 'schema'}
                missing = required_fields - set(view_config.keys())
                if missing:
                    errors.append(f"Feature view '{name}' missing required fields: {missing}")
                    
                if 'tags' in view_config:
                    errors.extend(validator.validate_tags(
                        view_config['tags'],
                        f"Feature view '{name}'",
                        'feature_view'
                    ))
                    
                # Validate feature-level tags
                if 'schema' in view_config:
                    for feature in view_config['schema']:
                        if 'tags' in feature:
                            errors.extend(validator.validate_tags(
                                feature['tags'],
                                f"Feature '{feature['name']}' in view '{name}'",
                                'feature'
                            ))

        # Validate feature services section
        if 'feature_services' in config:
            for name, service_config in config['feature_services'].items():
                if not isinstance(service_config, dict):
                    errors.append(f"Feature service '{name}' configuration must be a dictionary")
                    continue
                    
                # Check required fields
                if 'features' not in service_config:
                    errors.append(f"Feature service '{name}' missing required field: features")
                else:
                    # Validate features list
                    if not isinstance(service_config['features'], list):
                        errors.append(f"Feature service '{name}' features must be a list")
                    elif not service_config['features']:  # Check for empty list
                        errors.append(f"Feature service '{name}' features list cannot be empty")
                
                # Validate tags if present
                if 'tags' in service_config:
                    if not isinstance(service_config['tags'], dict):
                        errors.append(f"Feature service '{name}' tags must be a dictionary")
                    
                # Check if referenced feature views exist
                if 'features' in service_config and isinstance(service_config['features'], list):
                    for view_name in service_config['features']:
                        if view_name not in config.get('feature_views', {}):
                            errors.append(f"Feature service '{name}' references non-existent feature view: {view_name}")
                            
                if 'tags' in service_config:
                    errors.extend(validator.validate_tags(
                        service_config['tags'],
                        f"Feature service '{name}'",
                        'feature_service'
                    ))

        return errors