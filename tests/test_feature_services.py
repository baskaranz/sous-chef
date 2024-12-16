import pytest
from feast import FeatureService, FeatureView
from sous_chef.sous_chef import SousChef
from datetime import timedelta
import yaml

@pytest.fixture
def feast_config():
    """Basic Feast configuration"""
    return {
        'project': 'test_project',
        'provider': 'local',
        'registry': 'registry.db',
        'online_store': {
            'type': 'sqlite',
            'path': 'online_store.db'
        },
        'offline_store': {
            'type': 'file'
        },
        'entity_key_serialization_version': 2
    }

@pytest.fixture
def sample_config():
    """Basic config with all required tags"""
    return {
        'feature_views': {
            'user_features': {
                'source_name': 'test_source',
                'entities': ['user'],
                'schema': [
                    {
                        'name': 'age',
                        'dtype': 'INT64',
                        'tags': {
                            'owner': 'data_team',
                            'version': '1.0',
                            'description': 'Age in years',
                            'data_quality': 'verified'
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
        },
        'feature_services': {
            'user_insights': {
                'features': ['user_features'],
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

@pytest.fixture
def sample_config_with_tags():
    """Config with comprehensive tags"""
    return {
        'feature_views': {
            'user_features': {
                'source_name': 'test_source',
                'entities': ['user'],
                'schema': [
                    {
                        'name': 'age',
                        'dtype': 'INT64',
                        'tags': {
                            'owner': 'data_team',
                            'version': '1.0',
                            'description': 'User age in years',
                            'data_quality': 'validated'
                        }
                    }
                ],
                'tags': {
                    'owner': 'data_team',
                    'version': '1.0',
                    'domain': 'user_profile',
                    'team': 'test_team'
                }
            }
        },
        'feature_services': {
            'user_insights': {
                'description': 'User profile insights',
                'features': ['user_features'],
                'tags': {
                    'owner': 'data_science',
                    'version': '1.0',
                    'status': 'production',
                    'SLA': 'T+1'
                }
            }
        }
    }

@pytest.fixture
def mock_feast_store(mocker):
    store = mocker.MagicMock()
    store.get_data_source.return_value = mocker.MagicMock()
    store.get_entity.return_value = mocker.MagicMock()
    return store

@pytest.fixture
def minimal_metadata_rules():
    """Fixture for minimal metadata rules used in testing"""
    return {
        'required_tags': {
            'global': [],  # No global requirements
            'feature_view': [],
            'feature': [],
            'feature_service': []
        },
        'optional_tags': {
            'global': [
                'owner', 'version', 'description', 'domain', 
                'team', 'status', 'SLA', 'data_quality',
                'freshness_sla', 'tier', 'validation_rules',
                'privacy_level', 'source_system', 'update_frequency'
            ]
        }
    }

def test_create_feature_service(tmp_path, sample_config, mock_feast_store, minimal_metadata_rules, feast_config):
    # Setup
    sous_chef = SousChef(str(tmp_path), feast_config, check_dirs=False, metadata_rules=minimal_metadata_rules)
    sous_chef.store = mock_feast_store

    # Write config to temp file
    config_path = tmp_path / "feature_config.yaml"
    with open(config_path, 'w') as f:
        yaml.dump(sample_config, f)

    # Create feature views and services
    created_objects = sous_chef.create_from_yaml(config_path, apply=False)

    # Verify no failures occurred and feature service was created correctly
    assert 'user_insights' in created_objects, "Failed to create feature service 'user_insights'"
    service = created_objects['user_insights']
    assert isinstance(service, FeatureService), "Created object is not a FeatureService"
    assert service.name == 'user_insights', "Feature service name mismatch"
    assert service.description == 'Test service', "Feature service description mismatch"
    assert service.tags == {
        'owner': 'data_team',
        'version': '1.0',
        'status': 'production',
        'SLA': 'T+1'
    }, "Feature service tags mismatch"

def test_create_feature_service_with_tags(tmp_path, sample_config_with_tags, mock_feast_store, minimal_metadata_rules, feast_config):
    """Test creation of feature service with tags at all levels"""
    sous_chef = SousChef(str(tmp_path), feast_config, check_dirs=False, metadata_rules=minimal_metadata_rules)
    sous_chef.store = mock_feast_store

    config_path = tmp_path / "config.yaml"
    with open(config_path, 'w') as f:
        yaml.dump(sample_config_with_tags, f)

    created_objects = sous_chef.create_from_yaml(config_path, dry_run=True)
    
    # Verify feature view tags
    feature_view = created_objects['user_features']
    assert feature_view.tags == sample_config_with_tags['feature_views']['user_features']['tags']
    
    # Verify feature-level tags
    feature_schema = feature_view.schema[0]
    assert feature_schema.tags == sample_config_with_tags['feature_views']['user_features']['schema'][0]['tags']
    
    # Verify feature service tags
    service = created_objects['user_insights']
    assert service.tags == sample_config_with_tags['feature_services']['user_insights']['tags']

def test_feature_service_validation(tmp_path, mocker, minimal_metadata_rules, feast_config):
    # Invalid config missing referenced feature view
    invalid_config = {
        'feature_views': {
            'user_features': {
                'source_name': 'test_source',
                'entities': ['user'],
                'schema': [{'name': 'age', 'dtype': 'INT64'}]
            }
        },
        'feature_services': {
            'invalid_service': {
                'features': ['nonexistent_view']
            }
        }
    }

    sous_chef = SousChef(str(tmp_path), feast_config, check_dirs=False, metadata_rules=minimal_metadata_rules)
    sous_chef.store = mocker.MagicMock()  # Initialize mock store
    sous_chef.store.get_data_source.return_value = mocker.MagicMock()
    sous_chef.store.get_entity.return_value = mocker.MagicMock()
    
    config_path = tmp_path / "invalid_config.yaml"
    with open(config_path, 'w') as f:
        yaml.dump(invalid_config, f)

    with pytest.raises(ValueError, match=".*references non-existent feature view: nonexistent_view.*"):
        # Changed regex pattern to match actual error
        sous_chef.create_from_yaml(config_path)

def test_feature_service_dry_run(tmp_path, sample_config, mock_feast_store, minimal_metadata_rules, feast_config):
    sous_chef = SousChef(str(tmp_path), feast_config, check_dirs=False, metadata_rules=minimal_metadata_rules)
    sous_chef.store = mock_feast_store

    config_path = tmp_path / "config.yaml"
    with open(config_path, 'w') as f:
        yaml.dump(sample_config, f)

    # Test dry run
    created_objects = sous_chef.create_from_yaml(config_path, dry_run=True)
    
    # Verify all required objects were created without failures
    assert 'user_features' in created_objects, "Failed to create user_features view"
    assert 'user_insights' in created_objects, "Failed to create user_insights service"
    mock_feast_store.apply.assert_not_called()

def test_feature_service_apply(tmp_path, sample_config, mock_feast_store, minimal_metadata_rules, feast_config):
    sous_chef = SousChef(str(tmp_path), feast_config, check_dirs=False, metadata_rules=minimal_metadata_rules)
    sous_chef.store = mock_feast_store

    config_path = tmp_path / "config.yaml"
    with open(config_path, 'w') as f:  # Fixed syntax error: removed extra ')'
        yaml.dump(sample_config, f)

    # Test apply
    created_objects = sous_chef.create_from_yaml(config_path, apply=True)
    
    # Verify all objects were created successfully
    assert 'user_features' in created_objects, "Failed to create user_features view"
    assert 'user_insights' in created_objects, "Failed to create user_insights service"
    
    # Verify apply was called with all created objects
    mock_feast_store.apply.assert_called_once()
    applied_objects = set(obj.name for obj in mock_feast_store.apply.call_args[0][0])
    created_names = set(obj.name for obj in created_objects.values())
    assert applied_objects == created_names, "Not all created objects were applied"