import pytest
from sous_chef.sql_sources import SQLSourceRegistry

def test_sql_source_registry():
    """Test SQL source registry functionality"""
    source_class = SQLSourceRegistry.get_source_class('snowflake')
    assert source_class.__name__ == 'SnowflakeSource'
    assert SQLSourceRegistry.get_source_class('invalid') is None

def test_sql_config_validation():
    """Test SQL configuration validation"""
    config = {
        'query': 'SELECT * FROM table',
        'timestamp_field': 'ts'
    }
    errors = SQLSourceRegistry.validate_config('snowflake', config)
    assert not errors