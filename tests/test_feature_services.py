import pytest
from feast import FeatureService, FeatureView
from sous_chef.sous_chef import SousChef
from datetime import timedelta
import yaml

@pytest.fixture
def sample_config():
    return {
        'feature_views': {
            'user_features': {
                'source_name': 'test_source',
                'entities': ['user'],
                'schema': [
                    {'name': 'age', 'dtype': 'INT64'},
                    {'name': 'total_orders', 'dtype': 'INT64'}
                ]
            },
            'order_features': {
                'source_name': 'test_source',
                'entities': ['user'],
                'schema': [
                    {'name': 'last_order_amount', 'dtype': 'FLOAT'},
                    {'name': 'order_frequency', 'dtype': 'FLOAT'}
                ]
            }
        },
        'feature_services': {
            'user_insights': {
                'description': 'Combined user and order features',
                'features': ['user_features', 'order_features'],
                'tags': {
                    'owner': 'data_team',
                    'version': '1.0'
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

def test_create_feature_service(tmp_path, sample_config, mock_feast_store):
    # Setup
    sous_chef = SousChef(str(tmp_path), check_dirs=False)
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
    assert service.description == 'Combined user and order features', "Feature service description mismatch"
    assert service.tags == {'owner': 'data_team', 'version': '1.0'}, "Feature service tags mismatch"

def test_feature_service_validation(tmp_path, mocker):  # Add mocker fixture
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

    sous_chef = SousChef(str(tmp_path), check_dirs=False)
    sous_chef.store = mocker.MagicMock()  # Initialize mock store
    sous_chef.store.get_data_source.return_value = mocker.MagicMock()
    sous_chef.store.get_entity.return_value = mocker.MagicMock()
    
    config_path = tmp_path / "invalid_config.yaml"
    with open(config_path, 'w') as f:
        yaml.dump(invalid_config, f)

    with pytest.raises(ValueError, match="Feature view 'nonexistent_view' not found"):
        sous_chef.create_from_yaml(config_path)

def test_feature_service_dry_run(tmp_path, sample_config, mock_feast_store):
    sous_chef = SousChef(str(tmp_path), check_dirs=False)
    sous_chef.store = mock_feast_store

    config_path = tmp_path / "config.yaml"
    with open(config_path, 'w') as f:
        yaml.dump(sample_config, f)

    # Test dry run
    created_objects = sous_chef.create_from_yaml(config_path, dry_run=True)
    
    # Verify all required objects were created without failures
    assert 'user_features' in created_objects, "Failed to create user_features view"
    assert 'order_features' in created_objects, "Failed to create order_features view"
    assert 'user_insights' in created_objects, "Failed to create user_insights service"
    mock_feast_store.apply.assert_not_called()

def test_feature_service_apply(tmp_path, sample_config, mock_feast_store):
    sous_chef = SousChef(str(tmp_path), check_dirs=False)
    sous_chef.store = mock_feast_store

    config_path = tmp_path / "config.yaml"
    with open(config_path, 'w') as f:  # Fixed syntax error: removed extra ')'
        yaml.dump(sample_config, f)

    # Test apply
    created_objects = sous_chef.create_from_yaml(config_path, apply=True)
    
    # Verify all objects were created successfully
    assert 'user_features' in created_objects, "Failed to create user_features view"
    assert 'order_features' in created_objects, "Failed to create order_features view"
    assert 'user_insights' in created_objects, "Failed to create user_insights service"
    
    # Verify apply was called with all created objects
    mock_feast_store.apply.assert_called_once()
    applied_objects = set(obj.name for obj in mock_feast_store.apply.call_args[0][0])
    created_names = set(obj.name for obj in created_objects.values())
    assert applied_objects == created_names, "Not all created objects were applied"