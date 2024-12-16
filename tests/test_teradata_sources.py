import pytest
from typing import List, Dict, Optional
from sous_chef.sql_sources import SQLSourceRegistry, TeradataSource

@pytest.fixture
def complex_teradata_query():
    return """
    SELECT 
        c.customer_id,
        COUNT(DISTINCT o.order_id) as order_count,
        SUM(o.amount) as total_amount,
        QUALIFY ROW_NUMBER() OVER (
            PARTITION BY c.segment 
            ORDER BY SUM(o.amount) DESC
        ) <= 100 as high_value_flag,
        CASE 
            WHEN SUM(o.amount) > 10000 THEN 'HIGH'
            WHEN SUM(o.amount) > 5000 THEN 'MEDIUM'
            ELSE 'LOW'
        END as customer_tier
    FROM customers c
    LEFT JOIN orders o ON c.customer_id = o.customer_id
    WHERE o.order_date >= ADD_MONTHS(CURRENT_DATE, -3)
    GROUP BY c.customer_id, c.segment
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
    """Test complex Teradata query handling"""
    source = TeradataSource()
    schema = source.infer_schema(complex_teradata_query)
    
    # Verify expected columns
    names = {f['name'] for f in schema}
    assert names == {'CUSTOMER_ID', 'ORDER_COUNT', 'TOTAL_AMOUNT', 
                    'HIGH_VALUE_FLAG', 'CUSTOMER_TIER'}
    
    # Verify types
    types = {f['name']: f['dtype'] for f in schema}
    assert types['ORDER_COUNT'] == 'INT64'
    assert types['TOTAL_AMOUNT'] == 'FLOAT'
    assert types['CUSTOMER_TIER'] == 'STRING'

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
        'CUSTOMER_ID', 'ORDER_COUNT', 'TOTAL_AMOUNT', 
        'HIGH_VALUE_FLAG', 'CUSTOMER_TIER'
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
    type_tests = [
        ('BYTEINT', 'INT64'),
        ('INTEGER', 'INT64'),
        ('DECIMAL(10,2)', 'FLOAT'),
        ('NUMBER', 'FLOAT'),
        ('VARCHAR(255)', 'STRING'),
        ('CLOB', 'STRING'),
        ('DATE', 'STRING'),
        ('TIMESTAMP', 'STRING')
    ]
    
    for td_type, expected_type in type_tests:
        assert source._map_teradata_type(td_type) == expected_type

def test_teradata_invalid_query():
    """Test handling of invalid queries"""
    source = TeradataSource()
    with pytest.raises(ValueError, match="Query must start with SELECT"):
        source.infer_schema("INVALID SQL SYNTAX")
    with pytest.raises(ValueError, match="Query must contain FROM clause"):  # Updated error message
        source.infer_schema("SELECT FROM")

def test_teradata_query_validation():
    """Test Teradata-specific query validation"""
    source = TeradataSource()
    
    # Test invalid Teradata commands
    invalid_queries = [
        "FASTLOAD INTO table",
        ".LOGON tdprod",
        "COLLECT STATISTICS ON customers",
        "SELECT * FROM table QUALIFY rank() > 5"  # QUALIFY without ROW_NUMBER
    ]
    
    for query in invalid_queries:
        assert not source.validate_query(query), f"Should reject: {query}"

    # Basic valid queries should pass
    assert source.validate_query("SELECT customer_id FROM customers") == True  # Added explicit True comparison
    # Invalid queries should fail
    assert not source.validate_query("SELECT FROM")
    assert not source.validate_query("INSERT INTO table")

def test_teradata_schema_inference():
    """Test Teradata schema inference"""
    source = TeradataSource()
    schema = source.infer_schema("""
        SELECT 
            customer_id,
            ZEROIFNULL(COUNT(*)) as visit_count,
            SUM(amount) as total_spend,
            MAX(CASE WHEN amount > 1000 THEN 1 ELSE 0 END) as high_value_flag
        FROM transactions
        GROUP BY customer_id
    """)
    
    expected_types = {
        'CUSTOMER_ID': 'STRING',
        'VISIT_COUNT': 'INT64',
        'TOTAL_SPEND': 'FLOAT',
        'HIGH_VALUE_FLAG': 'INT64'
    }
    
    assert {f['name']: f['dtype'] for f in schema} == expected_types

def test_teradata_fastload_validation():
    """Test Teradata FastLoad rejection"""
    source = TeradataSource()
    
    fastload_query = """
    .LOGON tdprod/user,pass;
    FASTLOAD INTO customer_stage
    SELECT * FROM customer_source;
    .LOGOFF;
    """
    
    assert not source.validate_query(fastload_query)

def test_teradata_qualify_clause():
    """Test Teradata QUALIFY clause handling"""
    source = TeradataSource()
    query = """
    SELECT 
        product_id,
        SUM(sales) as total_sales
    FROM sales
    GROUP BY product_id
    QUALIFY ROW_NUMBER() OVER (ORDER BY SUM(sales) DESC) <= 10
    """
    
    # Should warn about QUALIFY but not fail
    assert source.validate_query(query)