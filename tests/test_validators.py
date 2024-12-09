import pytest
from pathlib import Path
import yaml
from typing import List, Dict
from sous_chef import SousChef
from sous_chef.validators import ConfigValidator, SQLValidator
from sous_chef.errors import SousChefError

class SQLValidator:
    """Validates SQL queries and configurations"""
    
    @classmethod
    def validate_sql(cls, query: str) -> List[str]:
        """Validate SQL query syntax and requirements"""
        errors = []
        
        # Check for missing aliases on aggregates
        query_upper = query.upper()
        for agg in ['COUNT(', 'SUM(', 'AVG(']:
            if agg in query_upper and ' AS ' not in query_upper:
                errors.append(f"Missing alias for {agg} aggregate")
                
        return errors
    
    @classmethod
    def validate_config(cls, config: Dict) -> List[str]:
        """Validate SQL configuration"""
        errors = []
        
        # Required fields
        required = ['query', 'timestamp_field', 'database', 'schema']
        for field in required:
            if field not in config:
                errors.append(f"Missing required field: {field}")
                
        # Validate query if present
        if 'query' in config:
            sql_errors = cls.validate_sql(config['query'])
            errors.extend(sql_errors)
            
        return errors

def test_ci_safety_validation():
    """Test CI safety validation"""
    config = {
        'data_sources': {
            'source1': {
                'path': '/absolute/path/data.parquet'  # Should fail
            },
            'source2': {
                'path': 'data/${ENV_VAR}/data.parquet'  # Should fail
            },
            'source3': {
                'path': 'data/valid.parquet'  # Should pass
            }
        }
    }
    
    errors = ConfigValidator.validate_ci_safety(config, Path("/dummy"))
    assert len(errors) == 2
    assert any(e.code == "ABSOLUTE_PATH" for e in errors)
    assert any(e.code == "ENV_VAR_REFERENCE" for e in errors)

def test_dry_run(test_repo, mocker, feast_config):
    """Test dry run functionality"""
    chef = SousChef(str(test_repo), feast_config=feast_config)
    
    # Mock store methods
    mock_source = mocker.Mock()
    mock_source.field_mapping = {}  # Add empty field mapping
    
    mock_get_source = mocker.patch.object(chef.store, 'get_data_source')
    mock_get_source.return_value = mock_source
    
    mock_entity = mocker.Mock()
    mock_entity.name = 'driver_id'
    mock_entity.join_key = 'driver_id'
    mock_entity.value_type = 'INT64'
    
    mock_get_entity = mocker.patch.object(chef.store, 'get_entity')
    mock_get_entity.return_value = mock_entity
    
    mock_apply = mocker.patch.object(chef.store, 'apply')
    
    feature_view_config = {
        'feature_views': {
            'test_view': {
                'source_name': 'driver_source',
                'entities': ['driver_id'],
                'schema': [{'name': 'feature1', 'dtype': 'INT64'}]
            }
        }
    }
    
    yaml_path = test_repo / "feature_views.yaml"
    with open(yaml_path, 'w') as f:
        yaml.dump(feature_view_config, f)
    
    # Dry run should not call apply
    feature_views = chef.create_from_yaml("feature_views.yaml", dry_run=True)
    assert len(feature_views) == 1
    assert not mock_apply.called

def test_ci_validation_absolute_paths():
    """Test validation catches absolute paths"""
    config = {
        'data_sources': {
            'source1': {
                'type': 'file',
                'path': '/absolute/path/data.parquet'
            }
        }
    }
    
    errors = ConfigValidator.validate_ci_safety(config, Path("/dummy"))
    assert any(e.code == "ABSOLUTE_PATH" for e in errors)

def test_ci_validation_env_vars():
    """Test validation catches environment variable usage"""
    config = {
        'data_sources': {
            'source1': {
                'type': 'file',
                'path': 'data/${ENV_VAR}/data.parquet'
            }
        }
    }
    
    errors = ConfigValidator.validate_ci_safety(config, Path("/dummy"))
    assert any(e.code == "ENV_VAR_REFERENCE" for e in errors)

def test_dry_run_mode(test_repo, feast_config, mocker):
    """Test dry run mode doesn't apply changes"""
    chef = SousChef(str(test_repo), feast_config=feast_config)
    
    # Mock store methods
    mocker.patch.object(chef.store, 'get_data_source')
    mocker.patch.object(chef.store, 'get_entity')
    mock_apply = mocker.patch.object(chef.store, 'apply')
    
    feature_view_config = {
        'feature_views': {
            'test_view': {
                'source_name': 'driver_source',
                'entities': ['driver_id'],
                'schema': [{'name': 'feature1', 'dtype': 'INT64'}]
            }
        }
    }
    
    yaml_path = test_repo / "feature_views.yaml"
    with open(yaml_path, 'w') as f:
        yaml.dump(feature_view_config, f)
    
    feature_views = chef.create_from_yaml("feature_views.yaml", dry_run=True)
    
    # Check feature views were created but not applied
    assert len(feature_views) == 1
    assert 'test_view' in feature_views
    assert not mock_apply.called

def test_error_format():
    """Test error format is CI-friendly"""
    config = {
        'data_sources': {
            'source1': {'path': '/absolute/path'},
            'source2': {'path': '${ENV_VAR}'}
        }
    }
    
    errors = ConfigValidator.validate_ci_safety(config, Path("/dummy"))
    error = SousChefError("Test error", errors)
    error_dict = error.to_dict()
    
    # Verify error format is structured for CI
    assert "message" in error_dict
    assert "errors" in error_dict
    assert all(
        {"path", "code", "message"}.issubset(e.keys()) 
        for e in error_dict["errors"]
    )

def test_sql_validation():
    """Test SQL validation"""
    # Test missing alias
    invalid_query = """
    SELECT customer_id, COUNT(*), SUM(amount)
    FROM orders 
    GROUP BY customer_id
    """
    errors = SQLValidator.validate_sql(invalid_query)
    assert len(errors) == 2
    assert "missing alias" in errors[0].lower()
    
    # Test valid query
    valid_query = """
    SELECT 
        customer_id,
        COUNT(*) as order_count,
        SUM(amount) as total_amount
    FROM orders
    GROUP BY customer_id
    """
    errors = SQLValidator.validate_sql(valid_query)
    assert not errors

def test_sql_config_validation():
    """Test SQL config validation"""
    # Test missing required fields
    invalid_config = {
        'query': 'SELECT * FROM table'
    }
    errors = SQLValidator.validate_config(invalid_config)
    assert len(errors) == 3
    
    # Test valid config
    valid_config = {
        'query': 'SELECT id as customer_id FROM customers',
        'timestamp_field': 'created_at',
        'database': 'analytics',
        'schema': 'public'
    }
    errors = SQLValidator.validate_config(valid_config)
    assert not errors