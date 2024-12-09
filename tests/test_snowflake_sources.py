import pytest
from sous_chef.sql_sources import SQLSourceRegistry, SnowflakeSource

@pytest.fixture
def complex_snowflake_query():
    return """
    WITH customer_stats AS (
        SELECT 
            customer_id,
            COUNT(DISTINCT order_id) as order_count,
            SUM(amount) as total_spend,
            AVG(amount) as avg_order_value,
            PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY amount) as median_order,
            FIRST_VALUE(transaction_date) OVER (PARTITION BY customer_id ORDER BY transaction_date) as first_purchase,
            LAST_VALUE(transaction_date) OVER (PARTITION BY customer_id ORDER BY transaction_date) as last_purchase
        FROM transactions
        WHERE transaction_date >= DATEADD(month, -6, CURRENT_DATE())
        GROUP BY customer_id
    ),
    product_affinity AS (
        SELECT 
            t1.customer_id,
            LISTAGG(DISTINCT p.category, ',') WITHIN GROUP (ORDER BY p.category) as preferred_categories,
            COUNT(DISTINCT p.category) as category_count
        FROM transactions t1
        JOIN products p ON t1.product_id = p.product_id
        GROUP BY t1.customer_id
    )
    SELECT 
        cs.*,
        pa.preferred_categories,
        pa.category_count,
        DATEDIFF(day, cs.first_purchase, cs.last_purchase) as customer_lifetime_days,
        cs.total_spend / NULLIF(cs.order_count, 0) as calculated_aov,
        CASE 
            WHEN cs.total_spend > 1000 THEN 'HIGH'
            WHEN cs.total_spend > 500 THEN 'MEDIUM'
            ELSE 'LOW'
        END as customer_segment
    FROM customer_stats cs
    LEFT JOIN product_affinity pa ON cs.customer_id = pa.customer_id
    WHERE cs.order_count >= 2
    QUALIFY ROW_NUMBER() OVER (PARTITION BY customer_segment ORDER BY total_spend DESC) <= 1000
    """

@pytest.fixture
def nested_cte_query():
    return """
    WITH user_metrics AS (
        WITH daily_stats AS (
            SELECT user_id, 
                   DATE_TRUNC('day', event_ts) as day,
                   COUNT(*) as events
            FROM events
            GROUP BY user_id, DATE_TRUNC('day', event_ts)
        )
        SELECT user_id,
               AVG(events) as avg_daily_events,
               PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY events) as median_events
        FROM daily_stats
        GROUP BY user_id
    )
    SELECT 
        u.user_id,
        u.avg_daily_events,
        u.median_events,
        IFF(u.avg_daily_events > 100, 'high', 'low') as user_activity_segment
    FROM user_metrics u
    """

@pytest.fixture
def array_agg_query():
    return """
    SELECT 
        user_id,
        ARRAY_AGG(DISTINCT category) WITHIN GROUP (ORDER BY category) as categories,
        ARRAY_AGG(OBJECT_CONSTRUCT('product', product_name, 'qty', quantity))
            as purchase_details
    FROM purchases
    GROUP BY user_id
    """

def test_snowflake_complex_query(complex_snowflake_query):
    """Test complex Snowflake query validation"""
    config = {
        'query': complex_snowflake_query,
        'timestamp_field': 'event_timestamp'
    }
    errors = SQLSourceRegistry.validate_config('snowflake', config)
    assert not errors

def test_snowflake_nested_ctes(nested_cte_query):
    """Test feature extraction from nested CTEs"""
    source = SnowflakeSource()
    features = source.extract_features(nested_cte_query)
    assert 'avg_daily_events' in features
    assert 'median_events' in features
    assert 'user_activity_segment' in features

def test_snowflake_array_aggs(array_agg_query):
    """Test handling of array aggregations"""
    source = SnowflakeSource()
    features = source.extract_features(array_agg_query)
    assert 'categories' in features
    assert 'purchase_details' in features

def test_snowflake_type_mapping():
    """Test Snowflake type mapping"""
    source = SnowflakeSource()
    type_map = {
        'NUMBER': 'FLOAT',
        'FLOAT': 'FLOAT', 
        'VARCHAR': 'STRING',
        'ARRAY': 'STRING',
        'OBJECT': 'STRING',
        'VARIANT': 'STRING'
    }
    for sf_type, feast_type in type_map.items():
        assert source._map_snowflake_type(sf_type) == feast_type

def test_snowflake_config_validation():
    """Test Snowflake configuration validation"""
    # Query source
    config = {
        'query': 'SELECT * FROM table',
        'timestamp_field': 'ts'
    }
    errors = SQLSourceRegistry.validate_config('snowflake', config)
    assert not errors

    # Table source
    config = {
        'table': 'my_table',
        'timestamp_field': 'ts'
    }
    errors = SQLSourceRegistry.validate_config('snowflake', config)
    assert not errors

    # Missing required fields
    invalid_config = {'query': 'SELECT * FROM table'}
    errors = SQLSourceRegistry.validate_config('snowflake', invalid_config)
    assert len(errors) == 1
    assert 'timestamp_field' in errors[0]