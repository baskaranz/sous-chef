# Sous Chef

A simplified feature store management system built on top of Feast, with advanced SQL source support.

## Features

- YAML-based feature view configuration
- Advanced SQL source support for Snowflake and Teradata
- Automatic schema inference from SQL queries
- Support for complex SQL patterns:
  - Common Table Expressions (CTEs)
  - Window functions
  - Array and Object aggregations
  - Nested queries
  - Complex date operations
- Type mapping from source databases to Feast types

## Installation

```bash
# From source
git clone https://github.com/your-org/sous-chef.git
cd sous-chef
pip install -e .
```

## Configuration

```yaml
project: my_project
registry: data/registry.db
provider: local

offline_store:
    type: snowflake  # or teradata
    database: MY_DB
    schema: PUBLIC
    warehouse: COMPUTE_WH  # for snowflake

feature_views:
  customer_metrics:
    source_name: snowflake_source
    entities: ["customer_id"]
    schema:
      - name: total_spend
        dtype: FLOAT
      - name: order_count
        dtype: INT64
    ttl_days: 7

data_sources:
  snowflake_source:
    type: snowflake
    database: MY_DB
    schema: PUBLIC
    query: |
      SELECT 
        customer_id,
        SUM(amount) as total_spend,
        COUNT(*) as order_count
      FROM transactions
      GROUP BY customer_id
```

## Usage

```python
from sous_chef import SousChef

# Initialize
chef = SousChef(".")

# Create feature views
feature_views = chef.create_from_yaml("feature_views.yaml")

# Dry run mode
chef.create_from_yaml("feature_views.yaml", dry_run=True)
```

## Development
```python
# Install dev dependencies
pip install -e ".[dev]"

# Run tests
pytest

# Run with specific SQL provider tests
pytest tests/test_snowflake_sources.py
pytest tests/test_teradata_sources.py
```
