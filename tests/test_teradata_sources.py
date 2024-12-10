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
    """Test Teradata schema inference"""
    source = TeradataSource()
    schema = source.infer_schema(complex_teradata_query)
    features = [f['name'] for f in schema]
    assert 'TRANSACTION_COUNT' in features
    assert 'DAILY_AMOUNT' in features
    assert 'CUSTOMER_RANK' in features

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
    features = [f['name'] for f in schema]
    assert 'MOVING_AVG_AMOUNT' in features
    assert 'AMOUNT_RANK' in features

def test_teradata_type_mapping():
    """Test Teradata type mapping"""
    source = TeradataSource()
    assert source._map_teradata_type('INTEGER') == 'INT64'
    assert source._map_teradata_type('DECIMAL(10,2)') == 'FLOAT'

def test_teradata_invalid_query():
    """Test handling of invalid queries"""
    source = TeradataSource()
    invalid_query = "INVALID SQL SYNTAX"
    schema = source.infer_schema(invalid_query)
    assert schema == []

def test_teradata_query_validation():
    """Test Teradata query validation"""
    source = TeradataSource()
    assert not source.validate_query("SELECT * FROM table")
    assert not source.validate_query("WITH RECURSIVE dates AS (...)")