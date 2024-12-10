from typing import Optional, List, Type, Dict
from feast.types import Float32, Int64, String
import re

class SQLSource:
    """Base class for SQL sources"""

    def validate_query(self, query: str) -> List[str]:
        """Validate SQL query syntax"""
        errors = []
        query = query.strip().upper()
        
        # Allow queries starting with WITH
        if not (query.startswith('SELECT') or query.startswith('WITH')):
            errors.append("Query must start with SELECT or WITH")
            
        return errors

    def infer_schema(self, query: str) -> List[Dict]:
        """Infer schema from SQL query"""
        try:
            # Basic validation
            query = query.strip()
            if not (query.upper().startswith('SELECT') or query.upper().startswith('WITH')):
                return []
                
            # Extract SELECT part
            select_stmt = self._find_main_select(query)
            if not select_stmt or 'FROM' not in select_stmt.upper():
                return []
                
            # Get columns
            select_part = select_stmt[select_stmt.upper().index('SELECT') + 6:]
            from_idx = select_part.upper().find('FROM')
            if from_idx == -1 or not (select_part := select_part[:from_idx].strip()):
                return []

            # Process columns
            schema = []
            for col in self._split_columns(select_part):
                col = col.strip()
                if '*' in col and 'COUNT(*)' not in col.upper():
                    continue
                    
                # Handle column naming
                col_upper = col.upper()
                name = None
                expr = col_upper

                # Extract alias
                if ' AS ' in col_upper:
                    parts = col_upper.split(' AS ')
                    expr, name = parts[0].strip(), parts[-1].strip().split()[0]
                    
                # Default to column reference if no alias
                if not name:
                    name = col_upper.split('.')[-1].strip()
                name = name.strip('"\'').strip()
                if not name:
                    continue
                    
                # Type inference
                dtype = (
                    'INT64' if any(fn in expr for fn in ['COUNT', 'SUM', 'RANK', 'ROW_NUMBER'])
                    else 'FLOAT' if any(fn in expr for fn in ['AVG', 'PERCENTILE', 'MEDIAN'])
                    else 'INT64' if 'ZEROIFNULL' in expr and any(agg in expr for agg in ['SUM', 'COUNT'])
                    else 'FLOAT' if 'ZEROIFNULL' in expr
                    else 'STRING'
                )
                
                schema.append({'name': name, 'dtype': dtype})
            
            return schema
            
        except Exception as e:
            print(f"Error inferring schema: {e}")
            return []

    def _find_main_select(self, query: str) -> Optional[str]:
        """Find the main SELECT statement from a query with CTEs"""
        try:
            # Normalize whitespace
            query = ' '.join(query.strip().split())
            
            # For WITH queries, find the last SELECT at top level
            if query.upper().startswith('WITH'):
                # Count parentheses to track nesting
                level = 0
                pos = 0
                last_select = None
                
                while pos < len(query):
                    if query[pos:pos+6].upper() == 'SELECT' and level == 0:
                        last_select = pos
                    elif query[pos] == '(':
                        level += 1
                    elif query[pos] == ')':
                        level -= 1
                    pos += 1
                        
                if last_select is not None:
                    return query[last_select:]
                    
            return query
            
        except Exception:
            return None

    def _split_columns(self, select_part: str) -> List[str]:
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
                columns.append(''.join(current).strip())
                current = []
                continue
            current.append(char)
            
        if current:
            columns.append(''.join(current).strip())
            
        return columns

class SnowflakeSource(SQLSource):
    """Snowflake SQL source implementation"""
    
    def _map_snowflake_type(self, sf_type: str) -> str:
        """Map Snowflake types to Feast types"""
        type_map = {
            'NUMBER': 'FLOAT',
            'FLOAT': 'FLOAT',
            'VARCHAR': 'STRING', 
            'ARRAY': 'STRING',
            'OBJECT': 'STRING',
            'VARIANT': 'STRING'
        }
        return type_map.get(sf_type.upper(), 'STRING')

    def infer_schema(self, query: str) -> List[Dict]:
        """Infer schema from Snowflake query"""
        schema = super().infer_schema(query)
        return [s for s in schema if not s['name'].startswith('SYS_')]

class TeradataSource(SQLSource):
    """Teradata SQL source implementation"""
    
    def infer_schema(self, query: str) -> List[Dict[str, str]]:
        """Infer schema from Teradata query"""
        # Use base class schema inference for validation and parsing
        schema = super().infer_schema(query)
        # Filter out Teradata system columns
        return [s for s in schema if not s['name'].startswith('TD_')]

    def _map_teradata_type(self, td_type: str) -> str:
        """Map Teradata types to Feast types"""
        type_map = {
            'INTEGER': 'INT64',
            'DECIMAL': 'FLOAT',
            'NUMBER': 'FLOAT',
            'FLOAT': 'FLOAT',
            'VARCHAR': 'STRING',
            'DATE': 'STRING',
            'TIMESTAMP': 'STRING'
        }
        base_type = td_type.split('(')[0].upper()
        return type_map.get(base_type, 'STRING')

    def _infer_type(self, expr: str) -> str:
        """Infer type from SQL expression"""
        expr = expr.upper()
        if any(f in expr for f in ['COUNT(', 'ROW_NUMBER(', 'RANK(']):
            return 'INT64'
        elif any(f in expr for f in ['SUM(', 'AVG(', 'MIN(', 'MAX(']):
            return 'FLOAT'
        return 'STRING'

class SQLSourceRegistry:
    """Registry for SQL source implementations"""
    
    _sources = {
        'snowflake': SnowflakeSource,
        'teradata': TeradataSource
    }
    
    @classmethod
    def get_source_class(cls, provider: str) -> Optional[Type[SQLSource]]:
        """Get SQL source class for provider"""
        return cls._sources.get(provider)
    
    @classmethod
    def validate_config(cls, provider: str, config: Dict) -> List[str]:
        """Validate SQL source configuration"""
        errors = []
        
        # Check provider exists
        if provider not in cls._sources:
            errors.append(f"Unsupported SQL provider: {provider}")
            return errors
            
        # Only require query/table and timestamp field
        if 'query' in config:
            required_fields = ['query', 'timestamp_field']
        elif 'table' in config:
            required_fields = ['table', 'timestamp_field']
        else:
            errors.append("Either 'query' or 'table' must be specified")
            return errors
        
        for field in required_fields:
            if field not in config:
                errors.append(f"Missing required field: {field}")
                
        # Validate query if present
        if 'query' in config:
            source_class = cls.get_source_class(provider)()
            query_errors = source_class.validate_query(config['query'])
            errors.extend(query_errors)
                
        return errors