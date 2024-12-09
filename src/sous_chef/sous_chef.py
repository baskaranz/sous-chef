from typing import Dict, List, Optional, Union
from pathlib import Path
import yaml
from datetime import timedelta
import os
import importlib

from feast import FeatureStore, Feature, FeatureView, ValueType, Field, Entity
from feast.types import Float32, Int64

from .errors import SousChefError
from .validators import ConfigValidator

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

    def __init__(self, repo_path: str, feast_config: Optional[Dict] = None, check_dirs: bool = True):
        """
        Initialize SousChef with a Feast repository path and optional config.
        
        Args:
            repo_path (str): Path to the Feast feature repository
            feast_config (Optional[Dict]): Feast configuration dictionary
            check_dirs (bool): Whether to verify directory structure exists
        """
        self.repo_path = Path(repo_path)
        feature_repo = self.repo_path / "feature_repo"
        
        if check_dirs:
            if not feature_repo.exists():
                feature_repo.mkdir(parents=True)
                
            # Write feast config if provided
            if feast_config:
                self.offline_store_type = feast_config.get('offline_store', {}).get('type', 'file')
                with open(feature_repo / "feature_store.yaml", 'w') as f:
                    yaml.dump(feast_config, f)
                    
            self.store = FeatureStore(repo_path=str(feature_repo))
            
            # Initialize data sources if config provided
            if feast_config:
                self._init_data_sources(feast_config)
        else:
            # In test mode, just set attributes without initialization
            self.store = None
            self.offline_store_type = None

    def _resolve_path(self, path: str) -> str:
        """Resolve path relative to the repository root"""
        abs_path = self.repo_path / path
        # Create parent directories if they don't exist
        abs_path.parent.mkdir(parents=True, exist_ok=True)
        return str(abs_path)

    def _import_source_class(self, source_type: str):
        """Dynamically import the source class"""
        if source_type not in self.SOURCE_TYPE_MAP:
            raise ImportError(f"Source type '{source_type}' is not supported. Available types: {list(self.SOURCE_TYPE_MAP.keys())}")
            
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

    def _init_data_sources(self, config: Dict):
        """Initialize data sources and entities from config"""
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

    def create_from_yaml(self, yaml_path: Union[str, Path], apply: bool = True, dry_run: bool = False) -> Dict[str, FeatureView]:
        """
        Create feature views from a YAML configuration file.
        
        Args:
            yaml_path: Path to YAML config file
            apply: Whether to apply feature views to the feature store
            dry_run: If True, validate and return changes without applying
            
        Returns:
            Dict[str, FeatureView]: Dictionary of created feature views
        """
        yaml_path = self.repo_path / yaml_path
        if not os.path.exists(yaml_path):
            raise FileNotFoundError(f"Config file not found: {yaml_path}")
            
        with open(yaml_path) as f:
            config = yaml.safe_load(f)
        
        # Validate CI safety
        errors = ConfigValidator.validate_ci_safety(config, self.repo_path)
        if errors:
            raise SousChefError("Configuration validation failed", errors)
            
        if 'feature_views' not in config:
            raise ValueError("No feature_views section found in YAML")
        
        # Create feature views
        feature_views = {}
        for name, spec in config['feature_views'].items():
            source_name = spec['source_name']
            
            # Get data source from feature store instead of config
            source = self.store.get_data_source(source_name)
            if source is None:
                raise ValueError(f"Data source '{source_name}' not found")
            
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
                entities=entity_objects,
                ttl=timedelta(days=spec.get('ttl_days', 1)),
                source=source,
                schema=schema
            )
            
            feature_views[name] = feature_view
            
        if apply and not dry_run:
            self.store.apply(list(feature_views.values()))
            
        return feature_views

    def _create_sql_source(self, name: str, config: Dict):
        """Create SQL-based data source"""
        source_class = SQLSourceRegistry.get_source_class(self.offline_store_type)
        
        if source_class is None:
            raise ValueError(f"Unsupported SQL source type: {self.offline_store_type}")
        
        # Get database/schema from offline store config
        offline_config = self.store.config.offline_store
        
        return source_class(
            name=name,
            database=offline_config.get('database'),
            schema=offline_config.get('schema'),
            query=config.get('query'),
            table=config.get('table'),
            timestamp_field=config['timestamp_field']
        )