import pytest
from pathlib import Path
import yaml
from datetime import timedelta

from feast import FeatureStore, Entity, Field, FeatureView
from feast.types import Float32, Int64
from sous_chef import SousChef

FIXTURES_DIR = Path(__file__).parent / "fixtures"
CONFIG_DIR = FIXTURES_DIR / "config"
FEATURES_DIR = FIXTURES_DIR / "features"
TEST_DATA_DIR = FIXTURES_DIR / "test_data"

@pytest.fixture
def dummy_config():
    """Load dummy feature store config from YAML"""
    with open(CONFIG_DIR / "dummy_config.yaml") as f:
        return yaml.safe_load(f)

@pytest.fixture
def feast_config():
    """Load test feast configuration from YAML"""
    with open(CONFIG_DIR / "feast_config.yaml") as f:
        return yaml.safe_load(f)

@pytest.fixture
def test_config():
    """Load test configuration with feature_repo structure from YAML"""
    with open(CONFIG_DIR / "test_config.yaml") as f:
        return yaml.safe_load(f)

@pytest.fixture
def complex_features():
    """Load complex feature configuration from YAML"""
    with open(FEATURES_DIR / "complex_features.yaml") as f:
        return yaml.safe_load(f)

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

@pytest.fixture
def metadata_rules():
    """Load metadata rules configurations"""
    with open(CONFIG_DIR / "metadata_rules.yaml") as f:
        return yaml.safe_load(f)

@pytest.fixture
def minimal_rules(metadata_rules):
    """Get minimal metadata rules"""
    return metadata_rules['minimal']

@pytest.fixture
def complex_rules(metadata_rules):
    """Get complex metadata rules"""
    return metadata_rules['complex']

@pytest.fixture
def standard_rules(metadata_rules):
    """Get standard metadata rules"""
    return metadata_rules['standard']

@pytest.fixture
def driver_stats():
    """Load driver stats feature view config"""
    with open(FEATURES_DIR / "driver_stats.yaml") as f:
        return yaml.safe_load(f)

@pytest.fixture
def descriptions():
    """Load test descriptions"""
    with open(TEST_DATA_DIR / "descriptions.yaml") as f:
        return yaml.safe_load(f)

@pytest.fixture
def source_configs():
    """Load source configurations"""
    with open(CONFIG_DIR / "source_configs.yaml") as f:
        return yaml.safe_load(f)

def test_init(test_repo, mock_feature_store, feast_config, minimal_rules):
    """Test SousChef initialization"""
    chef = SousChef(str(test_repo), feast_config=feast_config, metadata_rules=minimal_rules)
    assert chef.repo_path == Path(test_repo)
    mock_feature_store.assert_called_once_with(repo_path=str(test_repo / "feature_repo"))

def test_resolve_path(test_repo, feast_config, minimal_rules):
    """Test path resolution"""
    chef = SousChef(str(test_repo), feast_config=feast_config, metadata_rules=minimal_rules)
    path = "data/test.parquet"
    resolved = chef._resolve_path(path)
    assert str(test_repo / path) == resolved
    assert Path(resolved).parent.exists()

def test_filter_source_config(dummy_config, minimal_rules, source_configs):
    """Test source configuration filtering"""
    chef = SousChef("dummy_path", feast_config=dummy_config, metadata_rules=minimal_rules, check_dirs=False)
    filtered = chef._filter_source_config('file', source_configs['file_source'])
    assert 'path' in filtered
    assert 'timestamp_field' in filtered
    assert 'invalid_param' not in filtered

def test_import_source_class(dummy_config, minimal_rules):
    """Test source class importing"""
    chef = SousChef("dummy_path", feast_config=dummy_config, metadata_rules=minimal_rules, check_dirs=False)
    with pytest.raises(ImportError):
        chef._import_source_class('invalid_source')
    
    source_class = chef._import_source_class('file')
    assert source_class.__name__ == 'FileSource'

@pytest.mark.parametrize("source_type", ["file", "spark", "kafka", "redis"])
def test_supported_sources(source_type, dummy_config, minimal_rules):
    """Test all supported source types"""
    chef = SousChef("dummy_path", feast_config=dummy_config, metadata_rules=minimal_rules, check_dirs=False)
    assert source_type in chef.SOURCE_TYPE_MAP
    assert source_type in chef.SOURCE_PARAMS

def test_create_from_yaml(test_repo, mocker, feast_config, minimal_rules, driver_stats):
    """Test feature view creation from YAML"""
    chef = SousChef(str(test_repo), feast_config=feast_config, metadata_rules=minimal_rules)
    
    # Mock feature store methods
    mocker.patch.object(chef.store, 'get_data_source')
    mocker.patch.object(chef.store, 'get_entity')
    mocker.patch.object(chef.store, 'apply')
    
    # Test feature view yaml
    yaml_path = test_repo / "feature_views.yaml"
    with open(yaml_path, 'w') as f:
        yaml.dump(driver_stats, f)
        
    feature_views = chef.create_from_yaml("feature_views.yaml")
    assert len(feature_views) == 1
    assert 'driver_stats' in feature_views
    assert chef.store.apply.called

def test_init_requires_metadata_rules(dummy_config):
    """Test SousChef initialization requires metadata rules"""
    with pytest.raises(TypeError):
        SousChef("dummy_path", feast_config=dummy_config)

def test_init_with_metadata_rules(dummy_config, standard_rules):
    """Test SousChef initialization with metadata rules"""
    chef = SousChef(
        "dummy_path", 
        feast_config=dummy_config,
        metadata_rules=standard_rules,
        check_dirs=False
    )
    assert chef.metadata_rules == standard_rules

def test_init_requires_configs(minimal_rules):
    """Test SousChef initialization requires both configs"""
    with pytest.raises(ValueError, match="feast_config is required"):
        SousChef("dummy_path", feast_config=None, metadata_rules=minimal_rules)

    feast_config = {'project': 'test'}  # Valid feast_config
    with pytest.raises(ValueError, match="metadata_rules is required"):
        SousChef("dummy_path", feast_config=feast_config, metadata_rules=None)

def test_sous_chef_init(tmp_path, test_config):
    """Test SousChef initialization with feature_repo structure"""
    repo_path = tmp_path / "test_repo"
    repo_path.mkdir()
    feature_repo = repo_path / "feature_repo"
    feature_repo.mkdir()

def test_complex_feature_config(test_repo, mocker, feast_config, complex_features, complex_rules, descriptions):
    """Test feature creation with complex metadata and special characters"""
    chef = SousChef(str(test_repo), feast_config=feast_config, metadata_rules=complex_rules)
    
    # Setup mocks with proper source configuration
    mock_source = mocker.Mock()
    mock_source.field_mapping = {}  # Add empty field mapping
    mock_source.name = 'customer_source'
    mock_source.batch_source = None  # Ensure it's not a stream source
    
    mock_entity = mocker.Mock()
    mock_entity.name = 'customer_id'
    mock_entity.join_key = 'customer_id'
    mock_entity.value_type = 'INT64'
    
    mock_feature_view = mocker.Mock()
    mock_feature_service = mocker.Mock()
    
    mocker.patch.object(chef.store, 'get_data_source', return_value=mock_source)
    mocker.patch.object(chef.store, 'get_entity', return_value=mock_entity)
    mocker.patch.object(chef.store, 'get_feature_view', return_value=mock_feature_view)
    mocker.patch.object(chef.store, 'get_feature_service', return_value=mock_feature_service)
    mocker.patch.object(chef.store, 'apply')

    yaml_path = test_repo / "complex_features.yaml"
    with open(yaml_path, 'w') as f:
        yaml.dump(complex_features, f, allow_unicode=True)

    # Create and register features
    feature_objects = chef.create_from_yaml(yaml_path)
    assert len(feature_objects) == 2
    assert 'customer_360' in feature_objects
    assert chef.store.apply.called

    # Verify feature view metadata preservation
    mock_feature_view.name = 'customer_360'
    mock_feature_view.tags = complex_features['feature_views']['customer_360']['tags']
    retrieved_view = chef.store.get_feature_view('customer_360')
    assert retrieved_view.tags == mock_feature_view.tags
    assert retrieved_view.tags['description'] == descriptions['complex_feature_view']
    assert '🚀' in retrieved_view.tags['description']
    assert '©' in retrieved_view.tags['description']
    assert '†' in retrieved_view.tags['description']

    # Verify feature metadata preservation
    mock_feature_view.schema = [
        mocker.Mock(name='lifetime_value', tags=complex_features['feature_views']['customer_360']['schema'][0]['tags'])
    ]
    feature_tags = retrieved_view.schema[0].tags
    assert feature_tags['description'] == descriptions['feature_description']
    assert '💰' in feature_tags['description']
    assert '∑' in feature_tags['description']
    assert '∀' in feature_tags['description']

    # Verify feature service metadata preservation
    mock_feature_service.name = 'premium_insights'
    mock_feature_service.tags = complex_features['feature_services']['premium_insights']['tags']
    retrieved_service = chef.store.get_feature_service('premium_insights')
    assert retrieved_service.tags == mock_feature_service.tags