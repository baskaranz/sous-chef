from typing import Dict, List, Optional, Union
from pathlib import Path
import yaml
from datetime import timedelta
import os

from feast import FeatureStore, Feature, FeatureView, ValueType, Field
from feast.types import Float32, Int64  # Add these imports

class SousChef:
    """
    A minimal wrapper for Feast that enables YAML-based feature view creation.
    """
    # Type mapping dictionary
    DTYPE_MAP = {
        'FLOAT': Float32,
        'INT64': Int64,
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