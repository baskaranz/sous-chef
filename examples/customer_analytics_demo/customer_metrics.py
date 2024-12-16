"""
Simple example demonstrating basic SousChef usage.
"""
from pathlib import Path
import yaml
from datetime import datetime
import pandas as pd
import numpy as np
import logging

from sous_chef import SousChef
from feast import FileSource, Entity, ValueType

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("sous_chef")

def create_sample_data():
    """Create sample customer data"""
    DATA_DIR = Path(__file__).parent / "data"
    DATA_DIR.mkdir(exist_ok=True)
    
    # Create sample data with consistent timestamps
    start_date = datetime(2024, 1, 1)
    data = pd.DataFrame({
        'event_timestamp': pd.date_range(start_date, periods=100, freq='D'),
        'customer_id': np.random.choice([1, 2, 3], 100),  # Limited set of customers
        'total_purchases': np.random.uniform(10, 1000, 100).astype('float32'),
        'purchase_count': np.random.randint(1, 50, 100)
    })
    
    data_file = DATA_DIR / "customer_data.parquet"
    data.to_parquet(data_file)
    logger.info(f"Created sample data at {data_file}")
    logger.info(f"Sample data range: {data['event_timestamp'].min()} to {data['event_timestamp'].max()}")
    return data

def run_simple_example():
    """Run simple example with basic feature store setup"""
    try:
        EXAMPLE_DIR = Path(__file__).parent
        FEATURE_REPO_DIR = EXAMPLE_DIR / "feature_repo"  # Changed from config
        DATA_DIR = EXAMPLE_DIR / "data"
        
        # Generate sample data
        sample_data = create_sample_data()

        with open(FEATURE_REPO_DIR / "features.yaml") as f:
            features_config = yaml.safe_load(f)
        
        # Direct configuration without feature repo
        feast_config = {
            'project': 'customer_analytics',
            'provider': 'local',
            'registry': str(DATA_DIR / "registry.db"),
            'offline_store': {'type': 'file'},
            'online_store': {
                'type': 'sqlite',
                'path': str(DATA_DIR / "online_store.db")
            }
        }

        # Initialize SousChef with direct config
        metadata_rules = {
            'required_tags': {
                'global': ['owner', 'version', 'domain'],
                'feature_view': ['team', 'data_quality'],
                'feature': ['description', 'freshness_sla'],
                'feature_service': ['status', 'SLA', 'tier']
            }
        }

        chef = SousChef(
            repo_path=str(EXAMPLE_DIR),
            feast_config=feast_config,
            metadata_rules=metadata_rules,
            log_level="INFO"
        )

        # Create and register entity first
        customer_entity = Entity(
            name="customer",
            value_type=ValueType.INT64,
            join_keys=["customer_id"],
            tags={
                "owner": "data_team",
                "version": "1.0",
                "domain": "customer"
            }
        )

        # Create data source
        data_source = FileSource(
            name="customer_source",
            path=str(DATA_DIR / "customer_data.parquet"),
            timestamp_field="event_timestamp",
            tags={
                "owner": "data_team",
                "version": "1.0",
                "domain": "customer"
            }
        )
        
        # Register both entity and data source
        chef.store.apply([customer_entity, data_source])
        logger.info("Registered entity: customer")
        logger.info("Registered data source: customer_source")

        # Create feature views and services from YAML
        objects = chef.create_from_yaml(FEATURE_REPO_DIR / "features.yaml")

        logger.info("\nFeature creation successful!")
        logger.info("Created feature views: %s", list(objects.keys()))

        # Create entity DataFrame with timestamp in range
        entity_df = pd.DataFrame({
            "customer_id": [1, 2],  # Must match customer IDs in sample data
            "event_timestamp": [
                datetime(2024, 1, 15),  # Timestamp within sample data range
                datetime(2024, 1, 15),
            ],
        })
        
        logger.info("\nQuerying features for entities:")
        logger.info("\n%s", entity_df.to_string())

        # Get historical features
        features = chef.feature_store.get_historical_features(
            entity_df=entity_df,
            features=chef.feature_store.get_feature_service("customer_insights")
        ).to_df()

        logger.info("\nRetrieved Features:")
        logger.info("\n%s", features.to_string())

        logger.info("\nDemo completed successfully!")
        
    except Exception as e:
        logger.error("Error running demo: %s", str(e))
        raise

if __name__ == "__main__":
    run_simple_example()