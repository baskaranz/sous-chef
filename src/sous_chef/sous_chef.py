from typing import Dict, List, Optional, Union
from pathlib import Path
import yaml
from datetime import timedelta
import os
import importlib

from feast import FeatureStore, Feature, FeatureView, ValueType, Field, Entity
from feast.types import Float32, Int64

class SousChef:
    """
    A minimal wrapper for Feast that enables YAML-based feature view creation.
    """
    # Type mapping dictionary
    DTYPE_MAP = {
        'FLOAT': Float32,
        'INT64': Int64,
    }
    
    # Source type mapping with import paths
    SOURCE_TYPE_MAP = {
        'file': ('feast.infra.offline_stores.file_source', 'FileSource'),
        'spark': ('feast.infra.offline_stores.spark_source', 'SparkSource'),
        'kafka': ('feast.data_source', 'KafkaSource'),
        'redis': ('feast.data_source', 'RedisSource')
    }

    # Add source-specific parameter mappings
    SOURCE_PARAMS = {
        'file': ['path', 'timestamp_field', 'created_timestamp_column', 'field_mapping'],
        'spark': ['path', 'timestamp_field', 'table', 'query', 'field_mapping'],
        'kafka': ['bootstrap_servers', 'topic', 'timestamp_field', 'message_format'],
        'redis': ['connection_string', 'key_ttl']
    }

    def __init__(self, repo_path: str):
        """
        Initialize SousChef with a Feast repository path.
        
        Args:
            repo_path (str): Path to the Feast feature repository
        """
        self.repo_path = Path(repo_path)
        if not (self.repo_path / "feature_repo").exists():
            raise ValueError("Config directory 'feature_repo' not found. Please create it and place config files inside.")
        
        self.store = FeatureStore(repo_path=str(self.repo_path / "feature_repo"))
        
        # Initialize data sources from feature_store.yaml
        self._init_data_sources()
    
    def _resolve_path(self, path: str) -> str:
        """Resolve path relative to the repository root"""
        abs_path = self.repo_path / path
        # Create parent directories if they don't exist
        abs_path.parent.mkdir(parents=True, exist_ok=True)
        return str(abs_path)

    def _import_source_class(self, source_type: str):
        """Dynamically import the source class"""
        if source_type not in self.SOURCE_TYPE_MAP:
            raise ValueError(f"Unsupported source type: {source_type}")
            
        module_path, class_name = self.SOURCE_TYPE_MAP[source_type]
        try:
            module = importlib.import_module(module_path)
            return getattr(module, class_name)
        except ImportError as e:
            raise ImportError(f"Failed to import {source_type} source. You may need to install additional dependencies: {str(e)}")

    def _filter_source_config(self, source_type: str, config: dict) -> dict:
        """Filter configuration parameters based on source type"""
        if source_type not in self.SOURCE_PARAMS:
            return config
        
        allowed_params = self.SOURCE_PARAMS[source_type]
        return {k: v for k, v in config.items() if k in allowed_params}

    def _init_data_sources(self):
        """Initialize data sources and entities from feature_store.yaml"""
        config_path = self.repo_path / "feature_repo" / "feature_store.yaml"
        with open(config_path) as f:
            config = yaml.safe_load(f)
        
        # Initialize entities first
        if 'entities' in config:
            entities = []
            for entity_config in config['entities']:
                entity = Entity(
                    name=entity_config['name'],
                    join_keys=[entity_config['join_key']],  # Convert single join_key to list of join_keys
                    description=entity_config.get('description', '')
                )
                entities.append(entity)
            self.store.apply(entities)
        
        # Initialize data sources
        if 'data_sources' in config:
            for name, source_config in config['data_sources'].items():
                source_type = source_config.get('type', 'file').lower()
                
                # Dynamically import the source class
                source_class = self._import_source_class(source_type)
                
                # Remove type from config and resolve path if it exists
                source_config = source_config.copy()
                source_config.pop('type', None)
                if 'path' in source_config:
                    source_config['path'] = self._resolve_path(source_config['path'])
                
                # Filter source config based on source type
                filtered_config = self._filter_source_config(source_type, source_config)
                
                # Create source instance with filtered config
                source = source_class(name=name, **filtered_config)
                self.store.apply([source])

    @property
    def feature_store(self) -> FeatureStore:
        """Get the underlying Feast feature store instance"""
        return self.store

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