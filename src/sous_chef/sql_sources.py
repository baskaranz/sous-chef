from typing import Optional, List, Type, Dict
from feast.types import Float32, Int64, String
import re

class SQLSource:
    """Base class for SQL sources"""

    def _validate_format(self, query: str) -> List[str]:
        """Validate SQL follows required format"""
        # First clean up query
        clean_lines = []
        query = query.strip()

        # Check for invalid characters that indicate bad syntax
        if any(c in query for c in [';', '`', '|']):
            raise ValueError("Invalid SELECT statement")
        
        # Handle single line queries by splitting on commas
        if '\n' not in query and ',' in query:
            parts = query.split(',')
            query = '\n'.join(parts)

        # Reject CTEs explicitly
        if query.upper().startswith('WITH'):
            raise ValueError("CTEs (WITH clauses) are not supported")

        for line in query.splitlines():
            line = line.strip()
            if not line:
                continue
            clean_lines.append(line)
                
        if not clean_lines:
            raise ValueError("Empty query")
            
        # Find SELECT and FROM
        select_line = None
        from_line = None
        
        for i, line in enumerate(clean_lines):
            if line.upper().startswith('SELECT'):
                select_line = i
            elif line.upper().startswith('FROM'):
                from_line = i
                break
                
        if select_line is None:
            raise ValueError("Query must start with SELECT")
        if from_line is None:
            raise ValueError("Query must contain FROM clause")
            
        return clean_lines[select_line:from_line]

    def validate_query(self, query: str) -> bool:
        """Validate SQL query format"""
        try:
            if '*' in query:
                return False
                
            if 'WITH' in query.upper():
                return False
                
            lines = self._validate_format(query)
            select_part = self._extract_select(lines)
            return bool(select_part and self._split_columns(select_part))
        except Exception:
            return False

    def _extract_select(self, lines: List[str]) -> Optional[str]:
        """Extract SELECT clause from query lines"""
        select_part = []
        for line in lines:
            # Skip the SELECT keyword
            if line.upper().startswith('SELECT'):
                line = line[6:].strip()
            select_part.append(line)
        return ' '.join(select_part).strip()

    def _clean_sql(self, sql: str) -> str:
        """Clean SQL by removing comments and normalizing whitespace"""
        # Remove comments and normalize whitespace
        clean_lines = []
        for line in sql.splitlines():
            if '--' in line:
                line = line[:line.index('--')]
            clean_lines.append(line.strip())
        return ' '.join(clean_lines)

    def _split_columns(self, select_part: str) -> List[str]:
        """Split columns using strict rules"""
        # Remove extra whitespace and linebreaks
        select_part = ' '.join(select_part.split())
        columns = []
        current = []
        parens = 0
        in_case = False
        
        for char in select_part:
            if char == '(':
                parens += 1
                current.append(char)
            elif char == ')':
                parens -= 1
                current.append(char)
            elif char == ',' and parens == 0 and not in_case:
                col = ''.join(current).strip()
                if col:
                    columns.append(col)
                current = []
            elif char.isspace() and not current:
                continue
            else:
                if 'CASE' in ''.join(current).upper():
                    in_case = True
                elif in_case and 'END' in ''.join(current).upper():
                    in_case = False
                current.append(char)
                
        if current:
            col = ''.join(current).strip()
            if col:
                columns.append(col)
            
        return [c.strip() for c in columns if c]

    def _parse_column(self, col: str) -> tuple[Optional[str], str]:
        """Parse column expression into (name, expression) tuple"""
        col = col.strip()
        
        # Handle explicit AS clauses first
        if ' AS ' in col.upper():
            parts = col.upper().split(' AS ', maxsplit=1) 
            if len(parts) == 2:
                expr, alias = parts
                return alias.strip(), expr.strip()
                
        # Handle qualified names (table.column)
        if '.' in col and not any(x in col.upper() for x in ['(', 'CASE', '+', '-', '*', '/']):
            parts = col.split('.')
            return parts[-1].strip().upper(), col.strip().upper()
            
        # Simple column without special chars
        if not any(x in col.upper() for x in ['(', 'CASE', '.', '+', '-', '*', '/', 'OVER']):
            return col.strip().upper(), col.strip().upper()
            
        return None, col.upper()

    def _find_main_select(self, query: str) -> str:
        """Find the main SELECT statement from a query"""
        query = query.strip()
        if query.upper().startswith('WITH'):
            raise ValueError("CTEs (WITH clauses) are not supported")
        return query

    def infer_schema(self, query: str) -> List[Dict]:
        """Infer schema from SQL query"""
        try:
            # Reject CTEs first 
            if query.strip().upper().startswith('WITH'):
                raise ValueError("CTEs (WITH clauses) are not supported")

            # Basic syntax validation
            if not query.strip().upper().startswith('SELECT'):
                raise ValueError("Query must start with SELECT")

            if 'FROM' not in query.upper():
                raise ValueError("Query must contain FROM clause")

            if any(c in query for c in [';', '`', '|']):
                raise ValueError("Invalid SELECT statement")

            # Get clean lines
            lines = self._validate_format(query)
            select_part = self._extract_select(lines)
            columns = self._split_columns(select_part)
            
            schema = []
            for col in columns:
                name, expr = self._parse_column(col)
                if name:
                    schema.append({
                        'name': name,
                        'dtype': self._infer_type(expr)
                    })

            return schema

        except ValueError as e:
            print(f"Error inferring schema: {str(e)}")
            raise
        except Exception as e:
            print(f"Error inferring schema: {str(e)}")
            return []

    def _requires_alias(self, expr: str) -> bool:
        """Check if expression requires an alias"""
        expr = expr.upper()
        
        # Special functions and expressions that require aliases
        if any(x in expr for x in [
            'COUNT(', 'SUM(', 'AVG(', 'MAX(', 'MIN(',  # aggregates
            'RANK(', 'ROW_NUMBER(',  # window functions
            'CASE',  # case statements
            '+', '-', '*', '/',  # arithmetic
            'CONCAT(', '||',  # string operations
            'COALESCE(', 'NVL(',  # null handling
            'CAST(', 'CONVERT('  # conversions
        ]):
            return True
        return False

    def _infer_type(self, expr: str) -> str:
        """Infer type from SQL expression"""
        expr = expr.upper()
        if any(f in expr for f in ['COUNT(', 'ROW_NUMBER(', 'RANK(']):
            return 'INT64'
        elif any(f in expr for f in ['SUM(', 'AVG(', 'MIN(', 'MAX(']):
            return 'FLOAT'
        return 'STRING'

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
    
    def validate_query(self, query: str) -> bool:
        """Validate Teradata SQL query format"""
        try:
            # Basic syntax check
            query = query.strip().upper()
            if not query.startswith('SELECT'):
                return False
                
            if 'FROM' not in query:
                return False
                
            # Check for valid column list between SELECT and FROM
            select_part = query[query.index('SELECT') + 6:query.index('FROM')].strip()
            if not select_part or select_part == '*':
                return False
                
            return True
            
        except Exception:
            return False

    def infer_schema(self, query: str) -> List[Dict[str, str]]:
        """Infer schema from Teradata query"""
        try:
            return super().infer_schema(query)
        except ValueError as e:
            print(f"Error inferring schema: {str(e)}")
            raise  # Re-raise the original error with the correct message

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

class SparkSqlEmrSource(SQLSource):
    """Spark SQL (EMR) source implementation"""
    
    def infer_schema(self, query: str) -> List[Dict[str, str]]:
        """Infer schema from Spark SQL (EMR) query"""
        schema = super().infer_schema(query)
        return schema

    def _map_spark_type(self, spark_type: str) -> str:
        """Map Spark SQL (EMR) types to Feast types"""
        type_map = {
            'INTEGER': 'INT64',
            'DOUBLE': 'FLOAT',
            'STRING': 'STRING',
            'ARRAY': 'STRING',
            'STRUCT': 'STRING'
        }
        return type_map.get(spark_type.upper(), 'STRING')

class SQLSourceRegistry:
    """Registry for SQL source implementations"""
    
    _sources = {
        'snowflake': SnowflakeSource,
        'teradata': TeradataSource,
        'spark_sql_emr': SparkSqlEmrSource  # Added Spark SQL (EMR)
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

        # Check for CTEs
        if 'query' in config:
            query = config['query'].strip().upper()
            if query.startswith('WITH'):
                errors.append("CTEs (WITH clauses) are not supported")
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

class SQLValidator:
    """Validates SQL queries and configurations"""
    
    @classmethod
    def validate_sql(cls, query: str) -> List[str]:
        """Validate SQL query syntax and requirements"""
        errors = []
        query = query.strip().upper()
        
        # Basic syntax validation
        if not (query.startswith('SELECT') or query.startswith('WITH')):
            errors.append("Query must start with SELECT or WITH")
            return errors

        # Extract SELECT columns
        try:
            select_part = query[query.index('SELECT') + 6:query.index('FROM')].strip()
            
            # Check each column for required alias
            for col in select_part.split(','):
                col = col.strip()
                if any(agg in col for agg in ['COUNT(', 'SUM(', 'AVG(', 'MAX(', 'MIN(']):
                    if ' AS ' not in col:
                        errors.append(f"Missing alias for aggregate function: {col}")
        except ValueError:
            errors.append("Invalid SELECT statement")
            
        return errors

    @classmethod 
    def validate_config(cls, config: Dict) -> List[str]:
        """Validate SQL configuration"""
        errors = []
        
        required = ['query', 'timestamp_field', 'database']
        for field in required:
            if field not in config:
                errors.append(f"Missing required field: {field}")
                
        if 'query' in config:
            sql_errors = cls.validate_sql(config['query'])
            errors.extend(sql_errors)
            
        return errors