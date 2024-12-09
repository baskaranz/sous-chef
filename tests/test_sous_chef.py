import pytest
from pathlib import Path
import yaml
from datetime import timedelta

from feast import FeatureStore, Entity, Field, FeatureView
from feast.types import Float32, Int64
from sous_chef import SousChef

@pytest.fixture
def dummy_config():
    """Create a dummy feature store config"""
    return {
        'project': 'test',
        'registry': 'registry.db',
        'provider': 'local',
        'online_store': {'type': 'sqlite'}
    }

@pytest.fixture
def feast_config():
    """Create test feast configuration"""
    return {
        'project': 'test_project',
        'registry': 'data/registry.db',
        'provider': 'local',
        'entities': [{
            'name': 'driver_id',
            'join_key': 'driver_id',
            'description': 'test driver'
        }],
        'data_sources': {
            'driver_source': {
                'type': 'file',
                'path': 'data/test.parquet',
                'timestamp_field': 'event_timestamp'
            }
        }
    }

@pytest.fixture
def test_repo(tmp_path):
    """Create a temporary test repository"""
    repo_path = tmp_path / "test_repo"
    return repo_path

@pytest.fixture
def mock_feature_store(mocker):
    """Mock FeatureStore instance"""
    # Patch the actual FeatureStore class
    mock = mocker.patch('sous_chef.sous_chef.FeatureStore', autospec=True)
    # Create a mock instance
    mock_instance = mock.return_value
    # Setup required mock methods
    mock_instance.get_data_source = mocker.Mock()
    mock_instance.get_entity = mocker.Mock()
    mock_instance.apply = mocker.Mock()
    return mock

def test_init(test_repo, mock_feature_store, feast_config):
    """Test SousChef initialization"""
    chef = SousChef(str(test_repo), feast_config=feast_config)
    assert chef.repo_path == Path(test_repo)
    mock_feature_store.assert_called_once_with(repo_path=str(test_repo / "feature_repo"))

def test_resolve_path(test_repo, feast_config):
    """Test path resolution"""
    chef = SousChef(str(test_repo), feast_config=feast_config)
    path = "data/test.parquet"
    resolved = chef._resolve_path(path)
    assert str(test_repo / path) == resolved
    assert Path(resolved).parent.exists()

def test_filter_source_config():
    """Test source configuration filtering"""
    chef = SousChef("dummy_path", check_dirs=False)
    config = {
        'path': 'test.parquet',
        'timestamp_field': 'event_ts',
        'invalid_param': 'value'
    }
    filtered = chef._filter_source_config('file', config)
    assert 'path' in filtered
    assert 'timestamp_field' in filtered
    assert 'invalid_param' not in filtered

def test_import_source_class():
    """Test source class importing"""
    chef = SousChef("dummy_path", check_dirs=False)
    with pytest.raises(ImportError):
        chef._import_source_class('invalid_source')
    
    source_class = chef._import_source_class('file')
    assert source_class.__name__ == 'FileSource'

@pytest.mark.parametrize("source_type", ["file", "spark", "kafka", "redis"])
def test_supported_sources(source_type):
    """Test all supported source types"""
    chef = SousChef("dummy_path", check_dirs=False)
    assert source_type in chef.SOURCE_TYPE_MAP
    assert source_type in chef.SOURCE_PARAMS

def test_create_from_yaml(test_repo, mocker, feast_config):
    """Test feature view creation from YAML"""
    chef = SousChef(str(test_repo), feast_config=feast_config)
    
    # Mock feature store methods
    mocker.patch.object(chef.store, 'get_data_source')
    mocker.patch.object(chef.store, 'get_entity')
    mocker.patch.object(chef.store, 'apply')
    
    # Test feature view yaml
    feature_view_config = {
        'feature_views': {
            'driver_stats': {
                'source_name': 'driver_source',
                'entities': ['driver_id'],
                'ttl_days': 1,
                'schema': [
                    {'name': 'trips_today', 'dtype': 'INT64'},
                    {'name': 'rating', 'dtype': 'FLOAT'}
                ]
            }
        }
    }
    
    yaml_path = test_repo / "feature_views.yaml"
    with open(yaml_path, 'w') as f:
        yaml.dump(feature_view_config, f)
        
    feature_views = chef.create_from_yaml("feature_views.yaml")
    assert len(feature_views) == 1
    assert 'driver_stats' in feature_views
    assert chef.store.apply.called