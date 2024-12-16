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
    # Add metadata rules for validator
    metadata_rules = {
        'required_tags': {
            'global': ['owner', 'version'],
            'feature_view': ['team', 'domain'],
            'feature': ['description', 'data_quality'],
            'feature_service': ['status', 'SLA']
        },
        'optional_tags': {
            'global': ['description', 'team', 'domain']
        }
    }
    
    config = {
        'feature_views': {
            'view1': {
                'source_name': 'source1',
                'entities': ['entity1'],
                'schema': [
                    {
                        'name': 'feature1', 
                        'dtype': 'INT64',
                        'tags': {
                            'owner': 'data_team',
                            'version': '1.0',
                            'description': 'Test feature',
                            'data_quality': 'verified'
                        }
                    }
                ],
                'tags': {
                    'owner': 'data_team',
                    'version': '1.0',
                    'domain': 'test_domain',
                    'team': 'test_team'
                }
            },
            'view2': {
                'source_name': 'source2',
                'entities': ['entity1'],
                'schema': [
                    {
                        'name': 'feature2', 
                        'dtype': 'FLOAT',
                        'tags': {
                            'owner': 'data_team',
                            'version': '1.0',
                            'description': 'Test feature',
                            'data_quality': 'verified'
                        }
                    }
                ],
                'tags': {
                    'owner': 'data_team',
                    'version': '1.0',
                    'domain': 'test_domain',
                    'team': 'test_team'
                }
            }
        },
        'feature_services': {
            'service1': {
                'features': ['view1', 'view2'],
                'description': 'Test service',
                'tags': {
                    'owner': 'data_team',
                    'version': '1.0',
                    'status': 'production',
                    'SLA': 'T+1'
                }
            }
        }
    }
    
    validator = ConfigValidator(metadata_rules=metadata_rules)  # Pass required rules
    errors = validator.validate(config)
    assert len(errors) == 0, f"Unexpected validation errors: {errors}"

# Update other test cases that use ConfigValidator to include metadata_rules
def test_validate_feature_service_fields():
    metadata_rules = {
        'required_tags': {
            'global': [],
            'feature_view': [],
            'feature': [],
            'feature_service': []
        },
        'optional_tags': {'global': []}
    }
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
        errors = ConfigValidator(metadata_rules=metadata_rules).validate(config)
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

def test_validate_tags():
    """Test tag validation with custom rules"""
    test_rules = {
        'required_tags': {
            'global': ['owner'],  # All entities require owner
            'entity': ['version'],  # Entities also require version
        },
        'optional_tags': {
            'global': ['domain', 'description']
        }
    }
    
    validator = ConfigValidator(metadata_rules=test_rules)
    
    invalid_tag_configs = [
        # Missing required global tag
        {
            'tags': {
                'version': '1.0'  # Missing required 'owner' tag
            }
        },
        # Invalid tag type
        {
            'tags': 'not_a_dict'
        },
        # Unknown tag
        {
            'tags': {
                'owner': 'team',
                'version': '1.0',
                'invalid_tag': 'value'
            }
        }
    ]
    
    for config in invalid_tag_configs:
        errors = validator.validate_tags(config['tags'], "Test context", "entity")
        assert len(errors) > 0, "Expected validation errors for invalid tags"

def test_validate_feature_view_tags():
    """Test feature view tag validation"""
    config = {
        'feature_views': {
            'test_view': {
                'source_name': 'source1',
                'entities': ['entity1'],
                'schema': [
                    {
                        'name': 'feature1',
                        'dtype': 'INT64',
                        'tags': {
                            'owner': 'data_team',
                            'version': '1.0',
                            'description': 'Test feature',
                            'data_quality': 'verified',
                            'domain': 'customer'
                        }
                    }
                ],
                'tags': {
                    'owner': 'data_team',
                    'version': '1.0',
                    'domain': 'customer',
                    'team': 'test_team'
                }
            }
        }
    }
    
    errors = ConfigValidator.validate(config)
    assert len(errors) == 0, f"Unexpected validation errors: {errors}"

def test_validate_feature_service_tags():
    """Test feature service tag validation"""
    config = {
        'feature_views': {
            'view1': {
                'source_name': 'source1',
                'entities': ['entity1'],
                'schema': []
            }
        },
        'feature_services': {
            'service1': {
                'features': ['view1'],
                'tags': {
                    'owner': 'data_science',
                    'version': '2.0',
                    'domain': 'customer_analytics',
                    'team': 'customer_insights',
                    'status': 'production',
                    'SLA': 'T+1'
                }
            }
        }
    }
    
    errors = ConfigValidator.validate(config)
    assert len(errors) == 0, f"Unexpected validation errors: {errors}"

def test_validate_configurable_tags():
    """Test configurable tag validation"""
    validator = ConfigValidator()
    
    # Test different contexts
    test_cases = [
        {
            'tags': {'owner': 'team1', 'version': '1.0'},  # Only global requirements
            'context': 'Test entity',
            'context_type': 'entity',
            'should_pass': True
        },
        {
            'tags': {
                'owner': 'team1', 
                'version': '1.0',
                'domain': 'customer',
                'team': 'data_science'
            },
            'context': 'Test feature view',
            'context_type': 'feature_view',
            'should_pass': True
        },
        {
            'tags': {
                'owner': 'team1',
                'version': '1.0',
                'status': 'production',
                'SLA': 'T+1'
            },
            'context': 'Test feature service',
            'context_type': 'feature_service',
            'should_pass': True
        }
    ]
    
    for tc in test_cases:
        errors = validator.validate_tags(tc['tags'], tc['context'], tc['context_type'])
        if tc['should_pass']:
            assert len(errors) == 0, f"Expected no errors but got: {errors}"
        else:
            assert len(errors) > 0, "Expected validation errors but got none"

def test_custom_metadata_rules():
    """Test validation with custom metadata rules"""
    custom_rules = {
        'required_tags': {
            'global': ['owner', 'version'],  # Added common required tags
            'feature_view': ['team', 'domain'],
            'feature': ['description', 'data_quality'],
            'feature_service': ['status', 'SLA']
        },
        'optional_tags': {
            'global': [
                'domain', 'data_quality', 'SLA',
                'team', 'description', 'status'
            ]
        }
    }

    # Updated config with all required tags
    config = {
        'feature_views': {
            'test_view': {
                'source_name': 'source1',
                'entities': ['entity1'],
                'schema': [
                    {
                        'name': 'feature1',
                        'dtype': 'INT64',
                        'tags': {
                            'owner': 'test_owner',
                            'version': '1.0',
                            'description': 'Test feature',
                            'data_quality': 'verified'
                        }
                    }
                ],
                'tags': {
                    'owner': 'test_owner',
                    'version': '1.0',
                    'team': 'test_team',
                    'domain': 'test_domain'
                }
            }
        },
        'feature_services': {
            'service1': {
                'features': ['test_view'],
                'tags': {
                    'owner': 'test_owner',
                    'version': '1.0',
                    'status': 'production',
                    'SLA': 'T+1'
                }
            }
        }
    }

    validator = ConfigValidator(metadata_rules=custom_rules)
    errors = validator.validate(config)
    assert len(errors) == 0, f"Unexpected validation errors: {errors}"

# Update test fixtures with required tags
@pytest.fixture
def sample_config():
    return {
        # ...existing config...
        'tags': {
            'owner': 'data_team',
            'version': '1.0'
        }
    }