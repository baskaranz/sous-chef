from importlib import import_module
from typing import Dict, Type

class SourceRegistry:
    """Registry for data source types"""
    _sources: Dict[str, Type] = {}
    
    @classmethod
    def get_source(cls, source_type: str):
        """Get or import source class based on type"""
        if source_type not in cls._sources:
            try:
                # Map source types to module paths
                module_map = {
                    'file': ('feast', 'FileSource'),
                    'spark': ('feast.infra.offline_stores.contrib.spark_offline_store.spark_source', 'SparkSource'),
                    'teradata': ('feast_teradata', 'TeradataSource'),
                }
                
                if source_type not in module_map:
                    raise ValueError(f"Unsupported source type: {source_type}")
                    
                module_name, class_name = module_map[source_type]
                module = import_module(module_name)
                cls._sources[source_type] = getattr(module, class_name)
                
            except ImportError as e:
                raise ImportError(f"Could not import {source_type} source. "
                                f"Make sure required dependencies are installed: {e}")
                
        return cls._sources[source_type]