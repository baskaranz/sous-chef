from typing import Dict, List
from enum import Enum

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
    """Basic configuration validator"""
    
    @classmethod
    def validate(cls, config: Dict) -> List[str]:
        """Basic config validation"""
        errors = []
        
        if not isinstance(config, dict):
            errors.append("Configuration must be a dictionary")
            return errors
            
        if 'query' in config:
            sql_errors = SQLValidator.validate_sql(config['query'])
            errors.extend(sql_errors)
            
        return errors