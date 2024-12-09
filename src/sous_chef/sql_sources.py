from typing import Optional, List, Type, Dict, Tuple
from feast.types import Float32, Int64, String

class SQLSource:
    """Base class for SQL sources"""
    
    def infer_schema(self, query: str) -> List[Dict[str, str]]:
        """Base method for schema inference"""
        raise NotImplementedError

class SnowflakeSource(SQLSource):
    """Snowflake SQL source implementation"""

    def extract_features(self, query: str) -> List[str]:
        """Extract feature names from Snowflake query"""
        features = []
        
        # Normalize query
        query = ' '.join(query.strip().split())
        
        # Split on SELECT to handle CTEs
        parts = query.upper().split('SELECT')
        
        for part in parts[1:]:
            if 'FROM' not in part:
                continue
                
            select_part = part.split('FROM')[0].strip()
            
            # Extract aliases
            for expr in select_part.split(','):
                expr = expr.strip()
                if ' AS ' in expr:
                    alias = expr.split(' AS ')[-1].strip()
                    features.append(alias.split()[0].lower())
                    
        return list(dict.fromkeys(features))
        
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

class TeradataSource(SQLSource):
    """Teradata SQL source implementation"""
    supports_partitioning = True

    def extract_features(self, query: str) -> List[str]:
        """Extract feature names from Teradata query"""
        features = []
        
        # Normalize query by removing newlines and extra spaces
        query = ' '.join(query.strip().split())
        
        # Split on SELECT to handle subqueries and CTEs
        parts = query.upper().split('SELECT')
        
        # Process each SELECT clause
        for part in parts[1:]:  # Skip first empty part
            if 'FROM' not in part:
                continue
                
            select_part = part.split('FROM')[0].strip()
            
            # Extract column aliases
            for expr in select_part.split(','):
                expr = expr.strip()
                
                # Handle window functions and aggregates
                if 'OVER' in expr:
                    # Handle complex window function syntax
                    if ' AS ' in expr:
                        # Find the last AS clause (handles nested functions)
                        alias_parts = expr.split(' AS ')
                        alias = alias_parts[-1].strip()
                        # Clean up any parentheses or extra tokens
                        cleaned_alias = alias.split()[0].rstrip(')').lower()
                        features.append(cleaned_alias)
                    continue
                
                # Regular column aliases
                if ' AS ' in expr:
                    alias = expr.split(' AS ')[-1].strip()
                    features.append(alias.split()[0].lower())
        
        # Remove duplicates while preserving order
        return list(dict.fromkeys(features))
    
    def _map_teradata_type(self, td_type: str) -> str:
        """Map Teradata types to Feast types"""
        # Extract base type without precision/scale
        base_type = td_type.split('(')[0].upper()
        
        type_map = {
            'INTEGER': 'INT64',
            'DECIMAL': 'FLOAT',
            'NUMBER': 'FLOAT', 
            'FLOAT': 'FLOAT',
            'VARCHAR': 'STRING',
            'DATE': 'STRING',
            'TIMESTAMP': 'STRING'
        }
        return type_map.get(base_type, 'STRING')
        
    def infer_schema(self, query: str) -> List[Dict[str, str]]:
        """Infer schema from Teradata query"""
        features = self.extract_features(query)
        schema = []
        
        # Extract full expressions for each feature
        expressions = {}
        for expr in query.upper().split(','):
            if ' AS ' in expr:
                full_expr = expr.split(' AS ')[0].strip()
                alias = expr.split(' AS ')[1].strip().split()[0].lower()
                expressions[alias] = full_expr
        
        for feature in features:
            expr = expressions.get(feature, '')
            
            # Infer type based on expression
            if 'COUNT(' in expr:
                dtype = 'INT64'
            elif any(agg in expr for agg in ['SUM(', 'AVG(']):
                dtype = 'FLOAT'
            else:
                dtype = 'STRING'
                
            schema.append({
                'name': feature,
                'dtype': dtype
            })
            
        return schema

class SQLSourceRegistry:
    """Registry for SQL source implementations"""
    
    _sources = {
        'snowflake': SnowflakeSource,
        'teradata': TeradataSource
    }
    
    @classmethod
    def is_sql_source(cls, config: Dict) -> bool:
        """Check if source config is SQL-based"""
        return 'query' in config and config.get('type', '') in cls._sources
    
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
                
        return errors