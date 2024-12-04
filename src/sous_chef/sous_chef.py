from typing import Dict, List, Optional, Union
from pathlib import Path
import yaml
from datetime import timedelta
import os

from feast import FeatureStore, Feature, FeatureView, ValueType, Field
from feast.types import Float32, Int64  # Add these imports
from .registry import SourceRegistry
from feast.data_format import ParquetFormat

class SousChef:
    """
    A minimal wrapper for Feast that enables YAML-based feature view creation.
    """
    # Type mapping dictionary
    DTYPE_MAP = {
        'FLOAT': Float32,
        'INT64': Int64,
    }

    REQUIRED_CONFIG_FILES = [
        'feature_store.yaml',
        'features.yaml',
        'data_sources.yaml'
    ]

    def __init__(self, repo_path: str):
        """
        Initialize SousChef with a Feast repository path.
        
        Args:
            repo_path (str): Path to the Feast feature repository
        """
        self.repo_path = Path(repo_path)
        config_dir = self.repo_path / "feature_repo"
        
        if not config_dir.exists():
            raise ValueError("Config directory 'feature_repo' not found. Please create it and place config files inside.")
        
        # Validate required config files
        missing_files = []
        for config_file in self.REQUIRED_CONFIG_FILES:
            if not (config_dir / config_file).exists():
                missing_files.append(config_file)
                
        if missing_files:
            raise ValueError(
                f"Missing required configuration files in feature_repo/: {', '.join(missing_files)}"
            )
        
        self.store = FeatureStore(repo_path=str(config_dir))

    def create_from_yaml(self, yaml_path: Union[str, Path], apply: bool = True) -> Dict[str, FeatureView]:
        """
        Create feature views from a YAML configuration file.
        
        Args:
            yaml_path (Union[str, Path]): Path to YAML config file
            apply (bool): Whether to apply feature views to the feature store
            
        Returns:
            Dict[str, FeatureView]: Dictionary of created feature views
        """
        yaml_path = self.repo_path / yaml_path
        if not os.path.exists(yaml_path):
            raise FileNotFoundError(f"Config file not found: {yaml_path}")
            
        with open(yaml_path) as f:
            config = yaml.safe_load(f)
            
        if 'feature_views' not in config:
            raise ValueError("No feature_views section found in YAML")
            
        feature_views = {}
        for name, spec in config['feature_views'].items():
            # Get data source
            source = self.store.get_data_source(spec['source_name'])
            
            # Get entity objects
            entity_objects = []
            for entity_name in spec['entities']:
                entity = self.store.get_entity(entity_name)
                entity_objects.append(entity)
            
            # Convert dtype strings to actual types
            schema = [
                Field(
                    name=f['name'], 
                    dtype=self.DTYPE_MAP[f['dtype']]
                ) 
                for f in spec['schema']
            ]
            
            # Create feature view
            feature_view = FeatureView(
                name=name,
                entities=entity_objects,  # Use entity objects instead of names
                ttl=timedelta(days=spec.get('ttl_days', 1)),
                source=source,
                schema=schema
            )
            
            feature_views[name] = feature_view
            
        if apply:
            self.store.apply(list(feature_views.values()))
            
        return feature_views

    def create_data_source(self, config: dict):
        """Create data source from config"""
        source_type = config.get('type', 'file').lower()
        source_class = SourceRegistry.get_source(source_type)
        source_config = config.copy()
        source_config.pop('type', None)
        return source_class(**source_config)