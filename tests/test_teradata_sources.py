import pytest
from typing import List, Dict, Optional
from sous_chef.sql_sources import SQLSourceRegistry, TeradataSource

@pytest.fixture
def complex_teradata_query():
    return """
    SELECT 
        ds.date_key,
        cm.customer_id,
        cm.segment_code,
        SUM(ZEROIFNULL(t.daily_transactions)) as transaction_count,
        SUM(ZEROIFNULL(t.daily_amount)) as daily_amount,
        MAX(cm.segment_rank) as customer_rank
    FROM (
        SELECT CAST('2023-01-01' AS DATE) as date_key
        UNION ALL
        SELECT date_key + INTERVAL '1' DAY 
        FROM date_spine 
        WHERE date_key < CAST('2024-01-01' AS DATE)
    ) ds
    CROSS JOIN (
        SELECT 
            c.customer_id,
            c.segment_code,
            RANK() OVER (PARTITION BY c.segment_code ORDER BY t.total_amount DESC) as segment_rank
        FROM customer_dim c
        LEFT JOIN (
            SELECT customer_id, SUM(amount) as total_amount
            FROM transactions
            GROUP BY customer_id
        ) t ON c.customer_id = t.customer_id
    ) cm
    LEFT JOIN (
        SELECT 
            transaction_date,
            customer_id,
            COUNT(*) as daily_transactions,
            SUM(amount) as daily_amount
        FROM transactions 
        GROUP BY transaction_date, customer_id
    ) t ON ds.date_key = t.transaction_date AND cm.customer_id = t.customer_id
    GROUP BY ds.date_key, cm.customer_id, cm.segment_code
    """

@pytest.fixture
def simple_teradata_query():
    """Simplified query without comments or CTEs"""
    return """
SELECT 
    customer_id,
    COUNT(*) AS order_count,
    SUM(amount) AS total_amount,
    MAX(order_date) AS last_order
FROM orders
GROUP BY customer_id"""

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
        'timestamp_field': 'transaction_date'
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
    """Test Teradata schema inference"""
    source = TeradataSource()
    schema = source.infer_schema(complex_teradata_query)
    features = {f['name'] for f in schema}
    expected = {
        'DATE_KEY', 'CUSTOMER_ID', 'SEGMENT_CODE',
        'TRANSACTION_COUNT', 'DAILY_AMOUNT', 'CUSTOMER_RANK'
    }
    assert features == expected

def test_teradata_simple_query(simple_teradata_query):
    """Test schema inference from simple query"""
    source = TeradataSource()
    schema = source.infer_schema(simple_teradata_query)
    features = [f['name'] for f in schema]
    assert 'ORDER_COUNT' in features
    assert 'TOTAL_AMOUNT' in features
    assert 'LAST_ORDER' in features

def test_teradata_window_functions(window_teradata_query):
    """Test schema inference from window functions"""
    source = TeradataSource()
    schema = source.infer_schema(window_teradata_query)
    features = {f['name'] for f in schema}
    expected = {'CUSTOMER_ID', 'ORDER_DATE', 'AMOUNT', 
                'MOVING_AVG_AMOUNT', 'AMOUNT_RANK'}
    assert features == expected
    
    # Check types are correctly inferred
    types = {f['name']: f['dtype'] for f in schema}
    assert types['AMOUNT_RANK'] == 'INT64'  # RANK() returns INT64
    assert types['MOVING_AVG_AMOUNT'] == 'FLOAT'  # AVG() returns FLOAT

def test_teradata_type_mapping():
    """Test Teradata type mapping"""
    source = TeradataSource()
    assert source._map_teradata_type('INTEGER') == 'INT64'
    assert source._map_teradata_type('DECIMAL(10,2)') == 'FLOAT'
    assert source._map_teradata_type('VARCHAR(255)') == 'STRING'
    assert source._map_teradata_type('DATE') == 'STRING'
    assert source._map_teradata_type('TIMESTAMP') == 'STRING'
    assert source._map_teradata_type('NUMBER') == 'FLOAT'
    assert source._map_teradata_type('UNKNOWN_TYPE') == 'STRING'

def test_teradata_invalid_query():
    """Test handling of invalid queries"""
    source = TeradataSource()
    with pytest.raises(ValueError, match="Query must start with SELECT"):
        source.infer_schema("INVALID SQL SYNTAX")
    with pytest.raises(ValueError, match="Query must contain FROM clause"):  # Updated error message
        source.infer_schema("SELECT FROM")

def test_teradata_query_validation():
    """Test Teradata query validation"""
    source = TeradataSource()
    # Basic valid queries should pass
    assert source.validate_query("SELECT customer_id FROM customers") == True  # Added explicit True comparison
    # Invalid queries should fail
    assert not source.validate_query("SELECT FROM")
    assert not source.validate_query("INSERT INTO table")