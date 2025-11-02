-- ========================================
-- MONITORING QUERIES
-- ========================================

-- Check current partition distribution
SELECT 
    TO_CHAR(
        TO_DATE(
            TRIM(BOTH '''' FROM REGEXP_SUBSTR(high_value, '''[^'']+''')),
            'YYYY-MM-DD'
        ) - 1,
        'YYYY-MM'
    ) AS month,
    COUNT(*) AS active_partitions
FROM user_tab_partitions
WHERE table_name = 'SALES'
  AND partition_name != 'P_INITIAL'
GROUP BY TO_CHAR(
    TO_DATE(
        TRIM(BOTH '''' FROM REGEXP_SUBSTR(high_value, '''[^'']+''')),
        'YYYY-MM-DD'
    ) - 1,
    'YYYY-MM'
)
ORDER BY month;

-- Compare active vs archived data
SELECT 'ACTIVE' AS data_location, 
       COUNT(*) AS total_records,
       COUNT(DISTINCT sale_date) AS unique_dates,
       MIN(sale_date) AS oldest_date,
       MAX(sale_date) AS newest_date,
       ROUND(SUM(amount), 2) AS total_amount
FROM sales
UNION ALL
SELECT 'ARCHIVED' AS data_location,
       COUNT(*) AS total_records,
       COUNT(DISTINCT sale_date) AS unique_dates,
       MIN(sale_date) AS oldest_date,
       MAX(sale_date) AS newest_date,
       ROUND(SUM(amount), 2) AS total_amount
FROM sales_archive;

-- Monthly summary of active vs archived
SELECT 
    TO_CHAR(sale_date, 'YYYY-MM') AS month,
    COUNT(*) AS active_records
FROM sales
GROUP BY TO_CHAR(sale_date, 'YYYY-MM')
UNION ALL
SELECT 
    TO_CHAR(sale_date, 'YYYY-MM') AS month,
    -COUNT(*) AS archived_records  -- Negative to distinguish
FROM sales_archive
GROUP BY TO_CHAR(sale_date, 'YYYY-MM')
ORDER BY month, active_records DESC;

-- View archived data summary
SELECT 
    TO_CHAR(sale_date, 'YYYY-MM-DD') AS archived_date,
    COUNT(*) AS records_archived
FROM sales_archive
GROUP BY sale_date
ORDER BY sale_date;

-- Check specific dates status (active or archived)
WITH date_list AS (
    SELECT DATE '2024-01-15' AS check_date FROM dual UNION ALL
    SELECT DATE '2024-02-01' FROM dual UNION ALL
    SELECT DATE '2024-03-10' FROM dual
)
SELECT 
    dl.check_date,
    CASE 
        WHEN a.cnt > 0 THEN 'ARCHIVED (' || a.cnt || ' records)'
        WHEN s.cnt > 0 THEN 'ACTIVE (' || s.cnt || ' records)'
        ELSE 'NO DATA'
    END AS status
FROM date_list dl
LEFT JOIN (SELECT sale_date, COUNT(*) cnt FROM sales GROUP BY sale_date) s 
    ON dl.check_date = s.sale_date
LEFT JOIN (SELECT sale_date, COUNT(*) cnt FROM sales_archive GROUP BY sale_date) a
    ON dl.check_date = a.sale_date
ORDER BY dl.check_date;