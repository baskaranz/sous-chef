import pytest
from sous_chef.validators import ConfigValidator, SQLValidator

def test_sql_validation():
    """Test SQL validation"""
    # Test invalid queries
    invalid_queries = [
        """
SELECT
    customer_id,
    COUNT(*),
    SUM(amount) / 100,
    MAX(order_date)
FROM orders
GROUP BY customer_id""",
        """
SELECT
    orders.
FROM orders""",
        """
SELECT
    amount + tax,
    EXTRACT(month FROM date)
FROM orders"""
    ]
    
    print("\nTesting invalid queries...")
    for query in invalid_queries:
        print(f"\nQuery:\n{query}")
        result = SQLValidator.validate_sql(query)
        print(f"Validation result: {result}")
        assert not result, "Expected validation failure"
    
    # Test valid queries - removed comments
    valid_queries = [
        """
        SELECT 
            customer_id,
            orders.order_id,
            COUNT(*) as order_count,
            SUM(amount) as total_amount,  
            amount + tax as total_with_tax,
            EXTRACT(month FROM date) as order_month
        FROM orders
        GROUP BY customer_id
        """,
        """
        SELECT 
            t.product_id,
            COUNT(DISTINCT order_id) as order_count,
            AVG(amount) as avg_amount
        FROM transactions t
        GROUP BY t.product_id
        """
    ]
    
    print("\nTesting valid queries...")
    for query in valid_queries:
        print(f"\nQuery:\n{query}")
        result = SQLValidator.validate_sql(query)
        print(f"Validation result: {result}")
        assert result, "Expected validation success"

def test_sql_config_validation():
    """Test SQL config validation"""
    invalid_config = {
        'query': 'SELECT * FROM table'
    }
    print("\nTesting invalid config:", invalid_config)
    result = SQLValidator.validate_config(invalid_config)
    print(f"Validation result: {result}")
    assert not result, "Expected validation failure"
    
    valid_config = {
        'query': 'SELECT id as customer_id FROM customers',
        'timestamp_field': 'created_at',
        'database': 'analytics'
    }
    print("\nTesting valid config:", valid_config)
    result = SQLValidator.validate_config(valid_config)
    print(f"Validation result: {result}")
    assert result, "Expected validation success"

def test_validate_feature_service_config():
    """Test complete feature service configuration validation"""
    config = {
        'feature_views': {
            'view1': {
                'source_name': 'source1',
                'entities': ['entity1'],
                'schema': [{'name': 'feature1', 'dtype': 'INT64'}]
            },
            'view2': {
                'source_name': 'source2',
                'entities': ['entity1'],
                'schema': [{'name': 'feature2', 'dtype': 'FLOAT'}]
            }
        },
        'feature_services': {
            'service1': {
                'features': ['view1', 'view2'],
                'description': 'Test service',
                'tags': {
                    'owner': 'data_team',
                    'version': '1.0'
                }
            }
        }
    }
    
    errors = ConfigValidator.validate(config)
    assert len(errors) == 0, f"Unexpected validation errors: {errors}"

def test_validate_feature_service_fields():
    """Test feature service field validation"""
    invalid_configs = [
        # Missing features list
        {
            'feature_views': {'view1': {'source_name': 'source1', 'entities': ['entity1'], 'schema': []}},
            'feature_services': {'service1': {'description': 'Test'}}
        },
        # Empty features list
        {
            'feature_views': {'view1': {'source_name': 'source1', 'entities': ['entity1'], 'schema': []}},
            'feature_services': {'service1': {'features': [], 'description': 'Test'}}
        },
        # Invalid tags format
        {
            'feature_views': {'view1': {'source_name': 'source1', 'entities': ['entity1'], 'schema': []}},
            'feature_services': {'service1': {'features': ['view1'], 'tags': 'invalid'}}
        }
    ]
    
    expected_errors = [
        "Feature service 'service1' missing required field: features",
        "Feature service 'service1' features list cannot be empty",
        "Feature service 'service1' tags must be a dictionary"
    ]
    
    for config, expected_error in zip(invalid_configs, expected_errors):
        errors = ConfigValidator.validate(config)
        assert any(expected_error in error for error in errors), f"Expected error '{expected_error}' not found in {errors}"

def test_validate_invalid_feature_service():
    config = {
        'feature_services': {
            'service1': {
                'description': 'Missing features field'
            }
        }
    }
    
    errors = ConfigValidator.validate(config)
    expected_error = "Feature service 'service1' missing required field: features"
    assert any(expected_error in error for error in errors), f"Expected error '{expected_error}' not found in {errors}"

def test_validate_nonexistent_feature_views():
    config = {
        'feature_services': {
            'service1': {
                'features': ['nonexistent_view']
            }
        },
        'feature_views': {}
    }
    
    errors = ConfigValidator.validate(config)
    expected_error = "Feature service 'service1' references non-existent feature view: nonexistent_view"
    assert any(expected_error in error for error in errors), f"Expected error '{expected_error}' not found in {errors}"