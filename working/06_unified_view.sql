-- ========================================
-- UNIFIED VIEW FOR SEAMLESS QUERYING
-- ========================================

-- Create view combining active and archived data
CREATE OR REPLACE VIEW sales_complete AS
SELECT 
    sale_id, sale_date, customer_id, product_id,
    amount, quantity, region, status,
    'ACTIVE' AS data_source,
    CAST(NULL AS DATE) AS archive_date,
    CAST(NULL AS VARCHAR2(50)) AS archived_by
FROM sales
UNION ALL
SELECT 
    sale_id, sale_date, customer_id, product_id,
    amount, quantity, region, status,
    'ARCHIVED' AS data_source,
    archive_date,
    archived_by
FROM sales_archive;

-- Example queries using unified view:

-- Query seamlessly across active and archived
SELECT 
    customer_id,
    COUNT(*) AS total_orders,
    SUM(amount) AS total_spent,
    MIN(sale_date) AS first_order,
    MAX(sale_date) AS last_order
FROM sales_complete
WHERE customer_id = 1234
GROUP BY customer_id;

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