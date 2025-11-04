-- ========================================
-- MONITORING QUERIES
-- Oracle 19.26 Optimized
-- ========================================

SET LINESIZE 200
SET PAGESIZE 50000

-- ========================================
-- 1. Configuration Status
-- ========================================
PROMPT
PROMPT ========================================
PROMPT Configuration Status
PROMPT ========================================

SELECT 
    source_table_name,
    archive_table_name,
    staging_table_name,
    is_active,
    validate_before_exchange AS validate_before,
    gather_stats_after_exchange AS gather_stats,
    enable_compression AS compression,
    compression_type AS comp_type,
    TO_CHAR(created_date, 'YYYY-MM-DD HH24:MI:SS') AS created,
    TO_CHAR(updated_date, 'YYYY-MM-DD HH24:MI:SS') AS updated,
    created_by,
    updated_by
FROM snparch_cnf_partition_archive
ORDER BY source_table_name;

-- ========================================
-- 2. Recent Execution History
-- ========================================
PROMPT
PROMPT ========================================
PROMPT Recent Partition Exchange Executions
PROMPT ========================================

SELECT 
    execution_id,
    TO_CHAR(execution_date, 'YYYY-MM-DD HH24:MI:SS') AS executed_at,
    source_table_name,
    archive_table_name,
    source_partition_name,
    archive_partition_name,
    TO_CHAR(partition_date, 'YYYY-MM-DD') AS data_date,
    records_archived,
    partition_size_mb,
    is_compressed,
    compression_type,
    ROUND(exchange_duration_seconds, 3) AS exchange_sec,
    ROUND(stats_gather_duration_seconds, 2) AS stats_sec,
    validation_status,
    executed_by
FROM snparch_ctl_execution_log
ORDER BY execution_id DESC
FETCH FIRST 20 ROWS ONLY;  -- Oracle 19c syntax

-- ========================================
-- 3. Execution Summary by Table
-- ========================================
PROMPT
PROMPT ========================================
PROMPT Execution Summary by Source Table
PROMPT ========================================

SELECT 
    source_table_name,
    COUNT(*) AS total_exchanges,
    SUM(records_archived) AS total_records,
    ROUND(SUM(partition_size_mb), 2) AS total_size_mb,
    SUM(CASE WHEN is_compressed = 'Y' THEN 1 ELSE 0 END) AS compressed_partitions,
    ROUND(AVG(exchange_duration_seconds), 3) AS avg_exchange_sec,
    ROUND(AVG(stats_gather_duration_seconds), 2) AS avg_stats_sec,
    MIN(execution_date) AS first_execution,
    MAX(execution_date) AS last_execution
FROM snparch_ctl_execution_log
GROUP BY source_table_name
ORDER BY source_table_name;

-- ========================================
-- 4. Index Status Check
-- ========================================
PROMPT
PROMPT ========================================
PROMPT Index Status
PROMPT ========================================

SELECT 
    table_name,
    index_name,
    index_type,
    uniqueness,
    status,
    locality,
    partitioned
FROM user_indexes
WHERE table_name IN ('SALES', 'SALES_ARCHIVE')
ORDER BY table_name, index_name;

-- ========================================
-- 5. Invalid Indexes (Should be empty)
-- ========================================
PROMPT
PROMPT ========================================
PROMPT Invalid Indexes (Should be empty)
PROMPT ========================================

SELECT 
    table_name,
    index_name,
    status,
    'ALTER INDEX ' || index_name || ' REBUILD;' AS rebuild_statement
FROM user_indexes
WHERE table_name IN ('SALES', 'SALES_ARCHIVE')
  AND status != 'VALID'
ORDER BY table_name, index_name;

-- ========================================
-- 6. Table Statistics Status
-- ========================================
PROMPT
PROMPT ========================================
PROMPT Table Statistics Status
PROMPT ========================================

SELECT 
    table_name,
    num_rows,
    blocks,
    avg_row_len,
    TO_CHAR(last_analyzed, 'YYYY-MM-DD HH24:MI:SS') AS last_analyzed,
    CASE 
        WHEN last_analyzed IS NULL THEN 'NEVER ANALYZED'
        WHEN last_analyzed < SYSDATE - 7 THEN 'STALE (>7 days)'
        WHEN last_analyzed < SYSDATE - 1 THEN 'OLD (>1 day)'
        ELSE 'CURRENT'
    END AS stats_status
FROM user_tables
WHERE table_name IN ('SALES', 'SALES_ARCHIVE', 'SNPARCH_CNF_PARTITION_ARCHIVE', 'SNPARCH_CTL_EXECUTION_LOG')
ORDER BY table_name;

-- ========================================
-- 7. Partition Statistics Status
-- ========================================
PROMPT
PROMPT ========================================
PROMPT Partition Statistics Status
PROMPT ========================================

SELECT 
    table_name,
    partition_name,
    num_rows,
    blocks,
    TO_CHAR(last_analyzed, 'YYYY-MM-DD HH24:MI:SS') AS last_analyzed,
    CASE 
        WHEN last_analyzed IS NULL THEN 'NEVER'
        WHEN last_analyzed < SYSDATE - 7 THEN 'STALE'
        ELSE 'CURRENT'
    END AS stats_status
FROM user_tab_partitions
WHERE table_name IN ('SALES', 'SALES_ARCHIVE')
ORDER BY table_name, partition_position DESC;

-- ========================================
-- 8. Compression Status by Partition
-- ========================================
PROMPT
PROMPT ========================================
PROMPT Partition Compression Status
PROMPT ========================================

SELECT 
    table_name,
    partition_name,
    compression,
    compress_for,
    num_rows,
    blocks,
    ROUND(bytes / 1024 / 1024, 2) AS size_mb
FROM user_tab_partitions p
JOIN user_segments s ON (
    s.segment_name = p.table_name 
    AND s.partition_name = p.partition_name
)
WHERE table_name IN ('SALES', 'SALES_ARCHIVE')
ORDER BY table_name, partition_position DESC
FETCH FIRST 20 ROWS ONLY;  -- Oracle 19c syntax

-- ========================================
-- 9. Check current partition distribution
-- ========================================
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

-- ========================================
-- 7. Compare active vs archived data
-- ========================================
PROMPT
PROMPT ========================================
PROMPT Active vs Archived Data Summary
PROMPT ========================================

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

-- ========================================
-- 8. Monthly summary of active vs archived
-- ========================================
PROMPT
PROMPT ========================================
PROMPT Monthly Summary (Active vs Archived)
PROMPT ========================================

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

-- ========================================
-- 9. View archived data summary
-- ========================================
PROMPT
PROMPT ========================================
PROMPT Archived Data by Date
PROMPT ========================================

SELECT 
    TO_CHAR(sale_date, 'YYYY-MM-DD') AS archived_date,
    COUNT(*) AS records_archived
FROM sales_archive
GROUP BY sale_date
ORDER BY sale_date;

-- ========================================
-- 10. Check specific dates status (active or archived)
-- ========================================
PROMPT
PROMPT ========================================
PROMPT Specific Dates Status Check
PROMPT ========================================

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