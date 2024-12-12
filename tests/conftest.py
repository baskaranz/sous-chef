import pytest
from pathlib import Path
import yaml
from feast import FeatureStore

@pytest.fixture
def test_repo(tmp_path):
    """Create a temporary test repository"""
    repo_path = tmp_path / "test_repo"
    return repo_path

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
def mock_feature_store(mocker):
    """Mock FeatureStore instance"""
    mock = mocker.patch('sous_chef.sous_chef.FeatureStore', autospec=True)
    mock_instance = mock.return_value
    mock_instance.get_data_source = mocker.Mock()
    mock_instance.get_entity = mocker.Mock()
    mock_instance.apply = mocker.Mock()
    return mock

@pytest.fixture
def complex_snowflake_query():
    """Complex Snowflake SQL query fixture"""
    return """
    WITH base AS (
        SELECT 
            driver_id,
            event_timestamp,
            COUNT(*) as trip_count
        FROM trips
        GROUP BY driver_id, event_timestamp
    )
    SELECT * FROM base
    """

@pytest.fixture
def complex_teradata_query():
    """Complex Teradata SQL query fixture"""
    return """
    SELECT 
        driver_id,
        event_timestamp,
        COUNT(*) OVER (PARTITION BY driver_id) as trip_count
    FROM trips
    """