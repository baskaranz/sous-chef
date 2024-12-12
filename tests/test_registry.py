import pytest
from sous_chef.registry import SourceRegistry

def test_source_registry():
    """Test SourceRegistry functionality"""
    # Test getting file source
    file_source = SourceRegistry.get_source('file')
    assert file_source.__name__ == 'FileSource'
    
    # Test caching
    assert 'file' in SourceRegistry._sources
    
    # Test invalid source
    with pytest.raises(ValueError):
        SourceRegistry.get_source('invalid_source')
    
    # Test import error
    with pytest.raises(ImportError):
        SourceRegistry.get_source('nonexistent_source')