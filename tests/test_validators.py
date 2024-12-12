import pytest
from sous_chef.validators import SQLValidator

def test_sql_validation():
    """Test SQL validation"""
    # Test invalid queries
    invalid_queries = [
        """
SELECT
    customer_id,
    COUNT(*),
    SUM(amount) / 100,
    MAX(order_date)
FROM orders
GROUP BY customer_id""",
        """
SELECT
    orders.
FROM orders""",
        """
SELECT
    amount + tax,
    EXTRACT(month FROM date)
FROM orders"""
    ]
    
    print("\nTesting invalid queries...")
    for query in invalid_queries:
        print(f"\nQuery:\n{query}")
        result = SQLValidator.validate_sql(query)
        print(f"Validation result: {result}")
        assert not result, "Expected validation failure"
    
    # Test valid queries - removed comments
    valid_queries = [
        """
        SELECT 
            customer_id,
            orders.order_id,
            COUNT(*) as order_count,
            SUM(amount) as total_amount,  
            amount + tax as total_with_tax,
            EXTRACT(month FROM date) as order_month
        FROM orders
        GROUP BY customer_id
        """,
        """
        SELECT 
            t.product_id,
            COUNT(DISTINCT order_id) as order_count,
            AVG(amount) as avg_amount
        FROM transactions t
        GROUP BY t.product_id
        """
    ]
    
    print("\nTesting valid queries...")
    for query in valid_queries:
        print(f"\nQuery:\n{query}")
        result = SQLValidator.validate_sql(query)
        print(f"Validation result: {result}")
        assert result, "Expected validation success"

def test_sql_config_validation():
    """Test SQL config validation"""
    invalid_config = {
        'query': 'SELECT * FROM table'
    }
    print("\nTesting invalid config:", invalid_config)
    result = SQLValidator.validate_config(invalid_config)
    print(f"Validation result: {result}")
    assert not result, "Expected validation failure"
    
    valid_config = {
        'query': 'SELECT id as customer_id FROM customers',
        'timestamp_field': 'created_at',
        'database': 'analytics'
    }
    print("\nTesting valid config:", valid_config)
    result = SQLValidator.validate_config(valid_config)
    print(f"Validation result: {result}")
    assert result, "Expected validation success"