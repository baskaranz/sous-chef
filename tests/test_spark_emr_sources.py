import pytest
from sous_chef.sql_sources import SQLSourceRegistry, SparkSqlEmrSource

@pytest.fixture
def complex_spark_emr_query():
    return """
    SELECT 
        customer_id,
        COUNT(DISTINCT order_id) as order_count,
        SUM(amount) as total_spend,
        AVG(amount) as avg_order_value,
        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY amount) as median_order,
        FIRST_VALUE(transaction_date) OVER (PARTITION BY customer_id ORDER BY transaction_date) as first_purchase,
        LAST_VALUE(transaction_date) OVER (PARTITION BY customer_id ORDER BY transaction_date) as last_purchase,
        DATEDIFF(day, FIRST_VALUE(transaction_date) OVER (PARTITION BY customer_id ORDER BY transaction_date), 
                 LAST_VALUE(transaction_date) OVER (PARTITION BY customer_id ORDER BY transaction_date)) as customer_lifetime_days,
        SUM(amount) / NULLIF(COUNT(DISTINCT order_id), 0) as calculated_aov,
        CASE 
            WHEN SUM(amount) > 1000 THEN 'HIGH'
            WHEN SUM(amount) > 500 THEN 'MEDIUM'
            ELSE 'LOW'
        END as customer_segment
    FROM transactions
    WHERE transaction_date >= DATEADD(month, -6, CURRENT_DATE())
    GROUP BY customer_id
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
        IF(u.avg_daily_events > 100, 'high', 'low') as user_activity_segment
    FROM user_metrics u
    """

@pytest.fixture
def array_agg_query():
    return """
    SELECT 
        user_id,
        COLLECT_LIST(DISTINCT category) as categories,
        COLLECT_LIST(NAMED_STRUCT('product', product_name, 'qty', quantity)) as purchase_details
    FROM purchases
    GROUP BY user_id
    """

def test_spark_emr_complex_query(complex_spark_emr_query):
    """Test complex Spark EMR query validation"""
    config = {
        'query': complex_spark_emr_query,
        'timestamp_field': 'event_timestamp'
    }
    errors = SQLSourceRegistry.validate_config('spark_sql_emr', config)
    assert not errors

def test_spark_emr_nested_ctes(nested_cte_query):
    """Test schema inference rejects nested CTEs"""
    source = SparkSqlEmrSource()
    with pytest.raises(ValueError, match="CTEs .* not supported"):
        source.infer_schema(nested_cte_query)

def test_spark_emr_array_aggs(array_agg_query):
    """Test handling of array aggregations"""
    source = SparkSqlEmrSource()
    schema = source.infer_schema(array_agg_query)
    features = [f['name'] for f in schema]
    assert 'CATEGORIES' in features
    assert 'PURCHASE_DETAILS' in features
    assert all(f['dtype'] == 'STRING' for f in schema)  # Arrays stored as strings

def test_spark_emr_type_mapping():
    """Test Spark EMR type mapping"""
    source = SparkSqlEmrSource()
    assert source._map_spark_type('INTEGER') == 'INT64'
    assert source._map_spark_type('DOUBLE') == 'FLOAT'
    assert source._map_spark_type('STRING') == 'STRING'

def test_spark_emr_config_validation():
    """Test Spark EMR configuration validation"""
    # Query source
    config = {
        'query': 'SELECT * FROM table',
        'timestamp_field': 'ts'
    }
    errors = SQLSourceRegistry.validate_config('spark_sql_emr', config)
    assert not errors

    # Table source
    config = {
        'table': 'my_table',
        'timestamp_field': 'ts'
    }
    errors = SQLSourceRegistry.validate_config('spark_sql_emr', config)
    assert not errors

    # Missing required fields
    invalid_config = {'query': 'SELECT * FROM table'}
    errors = SQLSourceRegistry.validate_config('spark_sql_emr', invalid_config)
    assert len(errors) == 1
    assert 'timestamp_field' in errors[0]

def test_spark_emr_query_validation():
    """Test Spark EMR query validation"""
    source = SparkSqlEmrSource()
    assert not source.validate_query("SELECT * FROM table")
    assert not source.validate_query("WITH cte AS (...) SELECT * FROM cte")