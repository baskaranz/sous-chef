import pytest
from sous_chef.sql_sources import TeradataSource, SQLSourceRegistry

def test_sql_source_registry():
    """Test SQL source registry functionality"""
    # Test valid provider
    source_class = SQLSourceRegistry.get_source_class('snowflake')
    assert source_class.__name__ == 'SnowflakeSource'
    
    # Test invalid provider
    assert SQLSourceRegistry.get_source_class('invalid') is None

def test_sql_config_validation():
    """Test SQL configuration validation"""
    # Test query-based config
    config = {
        'query': 'SELECT * FROM table',
        'timestamp_field': 'ts'
    }
    errors = SQLSourceRegistry.validate_config('snowflake', config)
    assert not errors
    
    # Test table-based config
    config = {
        'table': 'my_table',
        'timestamp_field': 'ts'
    }
    errors = SQLSourceRegistry.validate_config('snowflake', config)
    assert not errors
    
    # Test missing required fields
    config = {'query': 'SELECT * FROM table'}
    errors = SQLSourceRegistry.validate_config('snowflake', config)
    assert len(errors) == 1
    assert 'timestamp_field' in errors[0]
    
    # Test missing both query and table
    config = {'timestamp_field': 'ts'}
    errors = SQLSourceRegistry.validate_config('snowflake', config)
    assert len(errors) == 1
    assert "Either 'query' or 'table' must be specified" in errors[0]
    
    # Test invalid provider
    errors = SQLSourceRegistry.validate_config('invalid', {})
    assert len(errors) == 1
    assert 'Unsupported SQL provider' in errors[0]

def test_schema_inference(complex_teradata_query):
    """Test schema inference from SQL query"""
    source = TeradataSource()
    schema = source.infer_schema(complex_teradata_query)
    
    # Verify schema structure
    assert isinstance(schema, list)
    assert all(isinstance(s, dict) for s in schema)
    assert all({'name', 'dtype'}.issubset(s.keys()) for s in schema)
    
    # Verify specific inferred types
    type_checks = {
        'transaction_count': 'INT64',
        'daily_amount': 'FLOAT',
        'customer_tier': 'STRING'
    }
    
    for feature in schema:
        if feature['name'] in type_checks:
            assert feature['dtype'] == type_checks[feature['name']]