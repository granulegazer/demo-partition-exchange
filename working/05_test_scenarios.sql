-- ========================================
-- TEST SCENARIOS
-- Oracle 19.26 Compatible
-- ========================================

SET SERVEROUTPUT ON SIZE UNLIMITED

-- Scenario 1: Archive specific dates (3 dates)
DECLARE
    v_dates date_array_type := date_array_type(
        DATE '2024-01-15',
        DATE '2024-01-20',
        DATE '2024-01-25'
    );
BEGIN
    archive_partitions_by_dates(
        p_table_name => 'SALES',
        p_dates => v_dates
    );
END;
/

-- Scenario 2: Archive an entire week
DECLARE
    v_dates date_array_type := date_array_type();
    v_start_date DATE := DATE '2024-02-01';
BEGIN
    -- Build list of 7 consecutive dates
    FOR i IN 0..6 LOOP
        v_dates.EXTEND;
        v_dates(v_dates.COUNT) := v_start_date + i;
    END LOOP;
    
    archive_partitions_by_dates(
        p_table_name => 'SALES',
        p_dates => v_dates
    );
END;
/

-- Scenario 3: Archive first day of each month (Jan-Jun)
DECLARE
    v_dates date_array_type := date_array_type(
        DATE '2024-01-01',
        DATE '2024-02-01',
        DATE '2024-03-01',
        DATE '2024-04-01',
        DATE '2024-05-01',
        DATE '2024-06-01'
    );
BEGIN
    archive_partitions_by_dates(
        p_table_name => 'SALES',
        p_dates => v_dates
    );
END;
/

-- Scenario 4: Archive random dates across multiple months
DECLARE
    v_dates date_array_type := date_array_type(
        DATE '2024-01-10',
        DATE '2024-02-14',
        DATE '2024-03-17',
        DATE '2024-04-22',
        DATE '2024-05-30'
    );
BEGIN
    archive_partitions_by_dates(
        p_table_name => 'SALES',
        p_dates => v_dates
    );
END;
/

-- ========================================
-- NEW: Scenario 5 - Test with compression enabled
-- ========================================
-- First enable compression in config
UPDATE snparch_cnf_partition_archive
SET enable_compression = 'Y',
    compression_type = 'OLTP',
    updated_date = SYSTIMESTAMP,
    updated_by = USER
WHERE source_table_name = 'SALES';
COMMIT;

-- Then archive with compression
DECLARE
    v_dates date_array_type := date_array_type(
        DATE '2024-07-01',
        DATE '2024-07-02'
    );
BEGIN
    archive_partitions_by_dates(
        p_table_name => 'SALES',
        p_dates => v_dates
    );
END;
/

-- Reset compression to disabled
UPDATE snparch_cnf_partition_archive
SET enable_compression = 'N',
    compression_type = NULL,
    updated_date = SYSTIMESTAMP,
    updated_by = USER
WHERE source_table_name = 'SALES';
COMMIT;

-- ========================================
-- NEW: Scenario 6 - Fast mode (no validation, no stats)
-- ========================================
-- Temporarily disable validation and stats
UPDATE snparch_cnf_partition_archive
SET validate_before_exchange = 'N',
    gather_stats_after_exchange = 'N',
    updated_date = SYSTIMESTAMP,
    updated_by = USER
WHERE source_table_name = 'SALES';
COMMIT;

DECLARE
    v_dates date_array_type := date_array_type(
        DATE '2024-08-01'
    );
BEGIN
    archive_partitions_by_dates(
        p_table_name => 'SALES',
        p_dates => v_dates
    );
END;
/

-- Re-enable validation and stats (recommended)
UPDATE snparch_cnf_partition_archive
SET validate_before_exchange = 'Y',
    gather_stats_after_exchange = 'Y',
    updated_date = SYSTIMESTAMP,
    updated_by = USER
WHERE source_table_name = 'SALES';
COMMIT;

-- ========================================
-- NEW: View execution results
-- ========================================
PROMPT
PROMPT ========================================
PROMPT Recent Execution Results
PROMPT ========================================

SELECT 
    execution_id,
    TO_CHAR(execution_date, 'YYYY-MM-DD HH24:MI:SS') AS executed_at,
    source_partition_name,
    archive_partition_name,
    TO_CHAR(partition_date, 'YYYY-MM-DD') AS data_date,
    records_archived,
    partition_size_mb,
    is_compressed,
    compression_type,
    ROUND(exchange_duration_seconds, 3) AS exchange_sec,
    ROUND(stats_gather_duration_seconds, 2) AS stats_sec,
    validation_status
FROM snparch_ctl_execution_log
ORDER BY execution_id DESC
FETCH FIRST 10 ROWS ONLY;

PROMPT
PROMPT ========================================
PROMPT Execution Summary
PROMPT ========================================

SELECT 
    COUNT(*) AS total_executions,
    SUM(records_archived) AS total_records_archived,
    ROUND(SUM(partition_size_mb), 2) AS total_size_mb,
    SUM(CASE WHEN is_compressed = 'Y' THEN 1 ELSE 0 END) AS compressed_count,
    ROUND(AVG(exchange_duration_seconds), 3) AS avg_exchange_sec,
    ROUND(AVG(stats_gather_duration_seconds), 2) AS avg_stats_sec
FROM snparch_ctl_execution_log;
