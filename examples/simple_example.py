from pathlib import Path
import yaml
from datetime import datetime
import pandas as pd
import logging

from sous_chef import SousChef

logger = logging.getLogger("sous_chef")

# Initialize paths
EXAMPLE_DIR = Path(__file__).parent
CONFIG_DIR = EXAMPLE_DIR / "config"

# Load configurations
with open(CONFIG_DIR / "feature_store.yaml") as f:
    feast_config = yaml.safe_load(f)

with open(CONFIG_DIR / "features.yaml") as f:
    features_config = yaml.safe_load(f)

# Update feast_config with entities and data sources
feast_config.update({
    'entities': features_config['entities'],
    'data_sources': features_config['data_sources']
})

# Initialize SousChef with debug logging
chef = SousChef(
    repo_path=str(EXAMPLE_DIR),
    feast_config=feast_config,
    log_level="INFO"  # Set to DEBUG for more verbose output
)

# Create feature views from YAML
feature_views = chef.create_from_yaml("config/features.yaml")

# Get feature store
store = chef.feature_store

# Example: Get features for specific customers
entity_df = pd.DataFrame({
    "customer_id": [1, 2],
    "event_timestamp": [
        datetime(2023, 1, 2, 12, 0),
        datetime(2023, 1, 2, 12, 0),
    ],
})

# Get historical features
entity_df = store.get_historical_features(
    entity_df=entity_df,
    features=[
        "customer_stats:amount",
        "customer_stats:transaction_count",
    ],
).to_df()

logger.info("\nRetrieved Features:")
logger.info("\n%s", entity_df.to_string())