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

@pytest.fixture
def complex_emr_query():
    return """
    SELECT 
        c.customer_id,
        COUNT(DISTINCT o.order_id) as order_count,
        SUM(o.amount) as total_amount,
        percentile_approx(o.amount, 0.5) as median_amount,
        collect_list(DISTINCT p.category) as categories
    FROM customers c
    LEFT JOIN orders o ON c.customer_id = o.customer_id
    LEFT JOIN products p ON o.product_id = p.product_id
    WHERE o.order_date >= date_sub(current_date(), 90)
    GROUP BY c.customer_id
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

def test_emr_query_validation():
    """Test EMR query validation rules"""
    source = SparkSqlEmrSource()
    
    # Invalid EMR-specific commands
    invalid_queries = [
        "ADD JAR s3://my-bucket/udf.jar",
        "SET LOCATION 's3://bucket/path'",
        "SELECT * FROM table CLUSTERED BY (id)",
        "SELECT * FROM table DISTRIBUTE BY id"
    ]
    for query in invalid_queries:
        assert not source.validate_query(query), f"Should reject: {query}"

def test_emr_schema_inference():
    """Test EMR schema inference"""
    source = SparkSqlEmrSource()
    schema = source.infer_schema("""
        SELECT 
            COUNT(*) as event_count,
            SUM(amount) as total_amount,
            collect_set(category) as unique_categories
        FROM events
    """)
    
    expected_types = {
        'EVENT_COUNT': 'INT64',
        'TOTAL_AMOUNT': 'FLOAT',
        'UNIQUE_CATEGORIES': 'STRING'  # Arrays converted to strings
    }
    
    assert {f['name']: f['dtype'] for f in schema} == expected_types

def test_emr_complex_query(complex_emr_query):
    """Test complex EMR query handling"""
    source = SparkSqlEmrSource()
    schema = source.infer_schema(complex_emr_query)
    
    # Verify all expected columns are present
    names = {f['name'] for f in schema}
    assert names == {'CUSTOMER_ID', 'ORDER_COUNT', 'TOTAL_AMOUNT', 
                    'MEDIAN_AMOUNT', 'CATEGORIES'}
    
    # Verify types
    types = {f['name']: f['dtype'] for f in schema}
    assert types['ORDER_COUNT'] == 'INT64'
    assert types['TOTAL_AMOUNT'] == 'FLOAT'
    assert types['CATEGORIES'] == 'STRING'

def test_emr_type_mapping():
    """Test EMR type mapping"""
    source = SparkSqlEmrSource()
    type_tests = [
        ('TINYINT', 'INT64'),
        ('INT', 'INT64'),
        ('BIGINT', 'INT64'),
        ('DECIMAL(10,2)', 'FLOAT'),
        ('DOUBLE', 'FLOAT'),
        ('STRING', 'STRING'),
        ('ARRAY<INT>', 'STRING'),
        ('MAP<STRING,INT>', 'STRING'),
        ('STRUCT<name:STRING,age:INT>', 'STRING')
    ]
    
    for spark_type, expected_type in type_tests:
        assert source._map_spark_type(spark_type) == expected_type