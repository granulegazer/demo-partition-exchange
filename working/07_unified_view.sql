-- ========================================
-- UNIFIED VIEW FOR SEAMLESS QUERYING
-- Oracle 19.26 Optimized
-- ========================================

/*
    Purpose: 
        Provides a unified view of both active and archived sales data,
        allowing queries to seamlessly access all historical data regardless
        of storage location. The view adds a data_source column to identify
        whether data comes from the active or archived table.
    
    Performance Notes:
        - UNION ALL is used (no deduplication) for better performance
        - Queries are pushed down to respective tables for partition pruning
        - Index usage preserved for both source tables
        - Use WHERE clauses with sale_date for optimal partition elimination
    
    Usage Examples:
        See queries below demonstrating common access patterns
*/

-- Create view combining active and archived data
CREATE OR REPLACE VIEW sales_complete AS
SELECT 
    sale_id, sale_date, customer_id, product_id,
    amount, quantity, region, status,
    'ACTIVE' AS data_source
FROM sales
UNION ALL
SELECT 
    sale_id, sale_date, customer_id, product_id,
    amount, quantity, region, status,
    'ARCHIVED' AS data_source
FROM sales_archive;

COMMENT ON TABLE sales_complete IS 'Unified view of active and archived sales data for seamless querying across all historical records';

-- ========================================
-- Example Queries Using Unified View
-- ========================================

-- Example 1: Customer order history across all data
PROMPT
PROMPT ========================================
PROMPT Example 1: Customer Order History
PROMPT ========================================
SELECT 
    customer_id,
    COUNT(*) AS total_orders,
    SUM(amount) AS total_spent,
    MIN(sale_date) AS first_order,
    MAX(sale_date) AS last_order
FROM sales_complete
WHERE customer_id = 1234
GROUP BY customer_id;

-- Example 2: Date range query with data source breakdown
PROMPT
PROMPT ========================================
PROMPT Example 2: Date Range with Source Breakdown
PROMPT ========================================

-- Find all orders for a date range (regardless of active/archived)
SELECT 
    sale_date,
    data_source,
    COUNT(*) AS order_count,
    ROUND(SUM(amount), 2) AS daily_total
FROM sales_complete
WHERE sale_date BETWEEN DATE '2024-01-01' AND DATE '2024-03-31'
GROUP BY sale_date, data_source
ORDER BY sale_date, data_source;

-- Example 3: Product sales analysis across all data
PROMPT
PROMPT ========================================
PROMPT Example 3: Product Sales Analysis
PROMPT ========================================

SELECT 
    product_id,
    COUNT(*) AS total_orders,
    SUM(quantity) AS total_quantity,
    ROUND(SUM(amount), 2) AS total_revenue,
    ROUND(AVG(amount), 2) AS avg_order_value,
    COUNT(DISTINCT customer_id) AS unique_customers,
    SUM(CASE WHEN data_source = 'ACTIVE' THEN 1 ELSE 0 END) AS active_orders,
    SUM(CASE WHEN data_source = 'ARCHIVED' THEN 1 ELSE 0 END) AS archived_orders
FROM sales_complete
WHERE sale_date >= DATE '2024-01-01'
GROUP BY product_id
ORDER BY total_revenue DESC
FETCH FIRST 10 ROWS ONLY;

-- Example 4: Regional performance summary
PROMPT
PROMPT ========================================
PROMPT Example 4: Regional Performance
PROMPT ========================================

SELECT 
    region,
    data_source,
    COUNT(*) AS order_count,
    ROUND(SUM(amount), 2) AS total_revenue,
    ROUND(AVG(amount), 2) AS avg_order_value,
    COUNT(DISTINCT customer_id) AS unique_customers,
    MIN(sale_date) AS earliest_sale,
    MAX(sale_date) AS latest_sale
FROM sales_complete
GROUP BY region, data_source
ORDER BY region, data_source;

-- Example 5: Monthly trend analysis
PROMPT
PROMPT ========================================
PROMPT Example 5: Monthly Trend Analysis
PROMPT ========================================

SELECT 
    TO_CHAR(sale_date, 'YYYY-MM') AS month,
    COUNT(*) AS total_orders,
    ROUND(SUM(amount), 2) AS total_revenue,
    SUM(CASE WHEN data_source = 'ACTIVE' THEN 1 ELSE 0 END) AS active_count,
    SUM(CASE WHEN data_source = 'ARCHIVED' THEN 1 ELSE 0 END) AS archived_count,
    ROUND(AVG(amount), 2) AS avg_order_value
FROM sales_complete
GROUP BY TO_CHAR(sale_date, 'YYYY-MM')
ORDER BY month;

-- Example 6: Data distribution check
PROMPT
PROMPT ========================================
PROMPT Example 6: Data Distribution Summary
PROMPT ========================================

SELECT 
    data_source,
    COUNT(*) AS total_records,
    COUNT(DISTINCT sale_date) AS unique_dates,
    MIN(sale_date) AS oldest_date,
    MAX(sale_date) AS newest_date,
    ROUND(SUM(amount), 2) AS total_amount,
    ROUND(AVG(amount), 2) AS avg_amount,
    COUNT(DISTINCT customer_id) AS unique_customers,
    COUNT(DISTINCT product_id) AS unique_products
FROM sales_complete
GROUP BY data_source
ORDER BY data_source;

-- ========================================
-- Performance Tips
-- ========================================
/*
    1. Always include sale_date in WHERE clause for partition pruning:
       WHERE sale_date BETWEEN DATE '2024-01-01' AND DATE '2024-12-31'
    
    2. Filter by data_source if you only need active or archived:
       WHERE data_source = 'ACTIVE'
    
    3. Use indexes on customer_id, product_id, region for best performance
    
    4. For very large date ranges, consider querying tables separately
       and combining results in application layer
    
    5. Statistics should be kept current on both tables for optimal
       query plan selection
*/
