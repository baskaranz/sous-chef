"""
Retail domain example demonstrating SousChef usage with customer features.
"""
from pathlib import Path
import yaml
from datetime import datetime
import pandas as pd
import numpy as np
import logging
import tempfile
from feast import FileSource, Entity, ValueType

from sous_chef import SousChef

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("sous_chef")

# Initialize paths
EXAMPLE_DIR = Path(__file__).parent.absolute()
FEATURE_REPO_DIR = EXAMPLE_DIR / "feature_repo"
DATA_DIR = EXAMPLE_DIR / "data"

def create_sample_data():
    """Create sample retail data"""
    DATA_DIR.mkdir(exist_ok=True)
    data_file = DATA_DIR / "retail_data.parquet"
    
    if not data_file.exists():
        # Generate sample data
        dates = pd.date_range('2024-01-01', periods=100, freq='D')
        data = []
        for customer_id in [1001, 1002]:
            for date in dates:
                data.append({
                    'event_timestamp': date,
                    'customer_id': customer_id,
                    'total_purchases': float(np.random.randint(50, 200)),
                    'purchase_frequency': float(np.random.randint(1, 5)),
                    'customer_segment': np.random.randint(1, 4)
                })
        
        df = pd.DataFrame(data)
        df.to_parquet(data_file)
        logger.info(f"Created sample data at {data_file}")
        return df
    return pd.read_parquet(data_file)

def main():
    """Run retail example"""
    tmp_files = []
    try:
        # Create sample data first
        sample_data = create_sample_data()
        logger.info("Starting retail feature store demo...")
        
        # Use absolute paths for configuration
        with open(FEATURE_REPO_DIR / "features.yaml") as f:
            features_config = yaml.safe_load(f)

        feast_config = {
            'project': 'retail_feature_store',
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
                'feature': ['description', 'data_quality'],
                'feature_service': ['status', 'SLA']
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
            join_keys=["customer_id"],  # Fixed: changed join_key to join_keys and make it a list
            tags={
                "owner": "retail_analytics",
                "version": "1.0",
                "domain": "retail"
            }
        )

        # Create data source
        data_source = FileSource(
            name="retail_transactions",
            path=str(DATA_DIR / "retail_data.parquet"),
            timestamp_field="event_timestamp",
            tags={
                "owner": "retail_analytics",
                "version": "1.0",
                "domain": "retail"
            }
        )
        
        # Register both entity and data source
        chef.store.apply([customer_entity, data_source])
        logger.info("Registered entity: customer")
        logger.info("Registered data source: retail_transactions")

        # Create feature views with updated config
        objects = chef.create_from_yaml(FEATURE_REPO_DIR / "features.yaml")

        # Query data for specific timestamp
        query_timestamp = datetime(2024, 1, 15)
        entity_df = pd.DataFrame({
            "customer_id": [1001, 1002],
            "event_timestamp": [query_timestamp, query_timestamp],
        })

        logger.info(f"\nQuerying features for timestamp: {query_timestamp}")
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
    finally:
        # Clean up temporary files
        for tmp_file in tmp_files:
            try:
                Path(tmp_file).unlink(missing_ok=True)
            except Exception as e:
                logger.warning("Failed to clean up %s: %s", tmp_file, str(e))

if __name__ == "__main__":
    main()
