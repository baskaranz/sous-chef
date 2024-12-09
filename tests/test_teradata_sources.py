import pytest
from sous_chef.sql_sources import SQLSourceRegistry, TeradataSource

@pytest.fixture
def complex_teradata_query():
    return """
    WITH RECURSIVE date_spine AS (
        SELECT CAST('2023-01-01' AS DATE) as date_key
        UNION ALL
        SELECT date_key + INTERVAL '1' DAY 
        FROM date_spine 
        WHERE date_key < CAST('2024-01-01' AS DATE)
    ),
    customer_metrics AS (
        SELECT 
            c.customer_id,
            c.segment_code,
            c.region_id,
            TRIM(BOTH FROM c.customer_name) as customer_name,
            COALESCE(
                CASE_N(
                    WHEN t.total_amount > 10000 THEN 'PREMIUM',
                    WHEN t.total_amount > 5000 THEN 'GOLD',
                    WHEN t.total_amount > 1000 THEN 'SILVER'
                ),
                'BRONZE'
            ) as customer_tier,
            RANK() OVER (
                PARTITION BY c.segment_code 
                ORDER BY t.total_amount DESC
            ) as segment_rank,
            QUALIFY ROW_NUMBER() OVER (
                PARTITION BY c.customer_id 
                ORDER BY t.transaction_date DESC
            ) = 1
        FROM customer_dim c
        LEFT JOIN (
            SELECT 
                customer_id,
                MAX(transaction_date) as last_transaction,
                SUM(amount) as total_amount,
                COUNT(*) as transaction_count,
                AVG(amount) as avg_transaction
            FROM transactions
            WHERE transaction_date >= ADD_MONTHS(CURRENT_DATE, -12)
            GROUP BY customer_id
        ) t ON c.customer_id = t.customer_id
    )
    SELECT 
        ds.date_key,
        cm.customer_id,
        cm.segment_code,
        cm.customer_tier,
        SUM(ZEROIFNULL(t.daily_transactions)) as transaction_count,
        SUM(ZEROIFNULL(t.daily_amount)) as daily_amount,
        MAX(cm.segment_rank) as customer_rank,
        STATS_MODE(p.product_category) as preferred_category
    FROM date_spine ds
    CROSS JOIN customer_metrics cm
    LEFT JOIN (
        SELECT 
            transaction_date,
            customer_id,
            COUNT(*) as daily_transactions,
            SUM(amount) as daily_amount
        FROM transactions 
        GROUP BY transaction_date, customer_id
    ) t ON ds.date_key = t.transaction_date 
        AND cm.customer_id = t.customer_id
    LEFT JOIN products p ON t.product_id = p.product_id
    WHERE cm.segment_rank <= 1000
    GROUP BY ds.date_key, cm.customer_id, cm.segment_code, cm.customer_tier
    HAVING transaction_count > 0
    """

@pytest.fixture
def simple_teradata_query():
    return """
    SELECT 
        customer_id,
        COUNT(*) as order_count,
        SUM(amount) as total_amount,
        MAX(order_date) as last_order
    FROM orders
    GROUP BY customer_id
    """

@pytest.fixture 
def window_teradata_query():
    return """
    SELECT
        customer_id,
        order_date,
        amount,
        AVG(amount) OVER (PARTITION BY customer_id ORDER BY order_date 
            ROWS BETWEEN 30 PRECEDING AND CURRENT ROW) as moving_avg_amount,
        RANK() OVER (PARTITION BY customer_id ORDER BY amount DESC) as amount_rank
    FROM orders
    """

def test_teradata_complex_query(complex_teradata_query):
    """Test complex Teradata query validation"""
    config = {
        'query': complex_teradata_query,
        'timestamp_field': 'event_timestamp'
    }
    errors = SQLSourceRegistry.validate_config('teradata', config)
    assert not errors

def test_teradata_table_source():
    """Test Teradata table source configuration"""
    config = {
        'table': 'transactions',
        'timestamp_field': 'event_timestamp'
    }
    errors = SQLSourceRegistry.validate_config('teradata', config)
    assert not errors

def test_teradata_query_features(complex_teradata_query):
    """Test Teradata query feature extraction"""
    source = TeradataSource()
    features = source.extract_features(complex_teradata_query)
    expected_features = [
        'transaction_count', 'daily_amount', 'customer_rank', 'preferred_category'
    ]
    for feature in expected_features:
        assert feature in features

def test_teradata_simple_query(simple_teradata_query):
    """Test feature extraction from simple query"""
    source = TeradataSource()
    features = source.extract_features(simple_teradata_query)
    assert features == ['order_count', 'total_amount', 'last_order']
    
    schema = source.infer_schema(simple_teradata_query)
    assert {'name': 'order_count', 'dtype': 'INT64'} in schema
    assert {'name': 'total_amount', 'dtype': 'FLOAT'} in schema
    assert {'name': 'last_order', 'dtype': 'STRING'} in schema

def test_teradata_window_functions(window_teradata_query):
    """Test feature extraction from window functions"""
    source = TeradataSource()
    features = source.extract_features(window_teradata_query)
    assert 'moving_avg_amount' in features
    assert 'amount_rank' in features
    
    schema = source.infer_schema(window_teradata_query)
    moving_avg = next(s for s in schema if s['name'] == 'moving_avg_amount')
    assert moving_avg['dtype'] == 'FLOAT'

def test_teradata_type_mapping():
    """Test Teradata type mapping"""
    source = TeradataSource()
    assert source._map_teradata_type('INTEGER') == 'INT64'
    assert source._map_teradata_type('DECIMAL(10,2)') == 'FLOAT'
    assert source._map_teradata_type('VARCHAR(100)') == 'STRING'
    assert source._map_teradata_type('UNKNOWN_TYPE') == 'STRING'

def test_teradata_invalid_query():
    """Test handling of invalid queries"""
    source = TeradataSource()
    invalid_query = "INVALID SQL SYNTAX"
    features = source.extract_features(invalid_query)
    assert features == []
    
    schema = source.infer_schema(invalid_query)
    assert schema == []