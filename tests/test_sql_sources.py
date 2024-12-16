import pytest
from sous_chef.sql_sources import SQLSource, SQLSourceRegistry

@pytest.fixture
def base_sql_source():
    return SQLSource()

def test_sql_source_registry():
    """Test SQL source registry functionality"""
    source_class = SQLSourceRegistry.get_source_class('snowflake')
    assert source_class.__name__ == 'SnowflakeSource'
    assert SQLSourceRegistry.get_source_class('invalid') is None

def test_sql_config_validation():
    """Test SQL configuration validation"""
    config = {
        'query': 'SELECT * FROM table',
        'timestamp_field': 'ts'
    }
    errors = SQLSourceRegistry.validate_config('snowflake', config)
    assert not errors

def test_sql_format_validation():
    """Test SQL format validation rules"""
    source = SQLSource()
    
    # Test valid format - one column per line
    valid_query = """
    SELECT
        id,
        users.name,
        orders.status,
        COUNT(*) AS order_count,
        SUM(amount) AS total,
        CASE WHEN amount > 100 THEN 'high' ELSE 'low' END AS category
    FROM orders"""

    schema = source.infer_schema(valid_query)
    expected_columns = ['ID', 'NAME', 'STATUS', 'ORDER_COUNT', 'TOTAL', 'CATEGORY']
    assert [s['name'] for s in schema] == expected_columns

    # Test invalid formats
    with pytest.raises(ValueError, match="CTEs .* not supported"):
        source.infer_schema("""
    WITH t AS (SELECT id FROM users)
    SELECT * FROM t""")
        
    with pytest.raises(ValueError, match="Query must contain FROM clause"):
        source.infer_schema("SELECT id, name")  # Missing FROM clause

    with pytest.raises(ValueError, match="Invalid SELECT statement"):
        source.infer_schema("SELECT id; name FROM users")  # Invalid syntax

def test_schema_inference():
    """Test SQL schema inference with aliasing requirements"""
    source = SQLSource()
    
    query = """
    SELECT
        id,
        users.name,
        orders.status,
        COUNT(*) AS order_count,
        SUM(amount) AS total,
        CASE
            WHEN amount > 100 THEN 'high'
            ELSE 'low'
        END AS category
    FROM orders
    JOIN users ON users.id = orders.user_id
    GROUP BY id, users.name, orders.status"""
    
    schema = source.infer_schema(query)
    expected_columns = ['ID', 'NAME', 'STATUS', 'ORDER_COUNT', 'TOTAL', 'CATEGORY']
    assert [s['name'] for s in schema] == expected_columns

    # Test query with missing required aliases
    invalid_query = """
    SELECT
        id,
        orders.status,
        COUNT(*),
        SUM(amount),
        CASE WHEN amount > 100 THEN 'high' ELSE 'low' END
    FROM orders
    GROUP BY id, orders.status"""
    
    schema = source.infer_schema(invalid_query)
    assert len(schema) == 2  # Only id and status should be included
    assert sorted([s['name'] for s in schema]) == ['ID', 'STATUS']

def test_column_parsing():
    """Test column name/expression parsing"""
    source = SQLSource()
    
    # Simple columns should work without aliases
    assert source._parse_column("customer_id") == ("CUSTOMER_ID", "CUSTOMER_ID")
    
    # Qualified columns should work without aliases
    assert source._parse_column("orders.status") == ("STATUS", "ORDERS.STATUS")
    assert source._parse_column("public.users.email") == ("EMAIL", "PUBLIC.USERS.EMAIL")
    
    # These expressions require aliases
    assert source._parse_column("COUNT(*)") == (None, "COUNT(*)")
    assert source._parse_column("user_id + 1") == (None, "USER_ID + 1")
    
    # Proper aliases should work for all types
    assert source._parse_column("orders.status AS order_status") == ("ORDER_STATUS", "ORDERS.STATUS")
    assert source._parse_column("COUNT(*) AS total_orders") == ("TOTAL_ORDERS", "COUNT(*)")
    
    # Complex expressions
    assert source._parse_column("CASE WHEN amount > 100 THEN 'high' ELSE 'low' END AS category") == ("CATEGORY", "CASE WHEN AMOUNT > 100 THEN 'HIGH' ELSE 'LOW' END")

def test_base_sql_validation():
    """Test base SQL validation rules"""
    source = SQLSource()
    
    # Valid queries
    assert source.validate_query("SELECT col1, col2 FROM table")
    assert source.validate_query("""
        SELECT 
            COUNT(*) as count,
            SUM(amount) as total
        FROM table
        GROUP BY date
    """)
    
    # Invalid queries
    assert not source.validate_query("SELECT * FROM table")
    assert not source.validate_query("WITH cte AS (SELECT 1) SELECT * FROM cte")
    assert not source.validate_query("INSERT INTO table VALUES (1)")

def test_sql_column_parsing():
    """Test SQL column name parsing"""
    source = SQLSource()
    
    # Test simple columns
    assert source._parse_column("column1") == ("COLUMN1", "COLUMN1")
    
    # Test aliased columns
    assert source._parse_column("sum(amount) as total") == ("TOTAL", "SUM(AMOUNT)")
    
    # Test complex expressions
    assert source._parse_column("CASE WHEN x > 0 THEN 1 ELSE 0 END as flag") == ("FLAG", "CASE WHEN X > 0 THEN 1 ELSE 0 END")

def test_sql_type_inference():
    """Test SQL type inference"""
    source = SQLSource()
    
    # Test aggregates
    assert source._infer_type("COUNT(*)") == "INT64"
    assert source._infer_type("SUM(amount)") == "FLOAT"
    assert source._infer_type("AVG(price)") == "FLOAT"
    
    # Test window functions
    assert source._infer_type("ROW_NUMBER()") == "INT64"
    assert source._infer_type("RANK()") == "INT64"
    
    # Default type
    assert source._infer_type("some_column") == "STRING"