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
-- NEW: View execution results with enhanced metrics
-- ========================================
PROMPT
PROMPT ========================================
PROMPT Recent Execution Results (Enhanced Metrics)
PROMPT ========================================

SELECT 
    execution_id,
    TO_CHAR(execution_date, 'YYYY-MM-DD HH24:MI:SS') AS executed_at,
    source_partition_name,
    archive_partition_name,
    TO_CHAR(partition_date, 'YYYY-MM-DD') AS data_date,
    records_archived,
    partition_size_mb,
    source_index_count,
    archive_index_count,
    ROUND(source_index_size_mb, 2) AS src_idx_mb,
    ROUND(archive_index_size_mb, 2) AS arch_idx_mb,
    invalid_indexes_before,
    invalid_indexes_after,
    data_validation_status,
    record_count_match,
    is_compressed,
    compression_type,
    ROUND(exchange_duration_seconds, 3) AS exchange_sec,
    ROUND(stats_gather_duration_seconds, 2) AS stats_sec,
    ROUND(total_duration_seconds, 2) AS total_sec,
    validation_status
FROM snparch_ctl_execution_log
ORDER BY execution_id DESC
FETCH FIRST 10 ROWS ONLY;

PROMPT
PROMPT ========================================
PROMPT Execution Summary with Index Metrics
PROMPT ========================================

SELECT 
    COUNT(*) AS total_executions,
    SUM(records_archived) AS total_records_archived,
    ROUND(SUM(partition_size_mb), 2) AS total_size_mb,
    ROUND(AVG(source_index_count), 1) AS avg_source_indexes,
    ROUND(AVG(archive_index_count), 1) AS avg_archive_indexes,
    ROUND(SUM(source_index_size_mb), 2) AS total_src_idx_mb,
    ROUND(SUM(archive_index_size_mb), 2) AS total_arch_idx_mb,
    SUM(CASE WHEN is_compressed = 'Y' THEN 1 ELSE 0 END) AS compressed_count,
    SUM(CASE WHEN data_validation_status = 'PASS' THEN 1 ELSE 0 END) AS validation_pass,
    SUM(CASE WHEN data_validation_status = 'FAIL' THEN 1 ELSE 0 END) AS validation_fail,
    SUM(CASE WHEN validation_status = 'WARNING' THEN 1 ELSE 0 END) AS warning_count,
    ROUND(AVG(exchange_duration_seconds), 3) AS avg_exchange_sec,
    ROUND(AVG(stats_gather_duration_seconds), 2) AS avg_stats_sec,
    ROUND(AVG(total_duration_seconds), 2) AS avg_total_sec
FROM snparch_ctl_execution_log;

PROMPT
PROMPT ========================================
PROMPT Data Validation Status Check
PROMPT ========================================

SELECT 
    execution_id,
    TO_CHAR(partition_date, 'YYYY-MM-DD') AS data_date,
    source_records_before,
    source_records_after,
    archive_records_before,
    archive_records_after,
    records_archived,
    record_count_match,
    data_validation_status,
    validation_status
FROM snparch_ctl_execution_log
ORDER BY execution_id DESC
FETCH FIRST 10 ROWS ONLY;

PROMPT
PROMPT ========================================
PROMPT Index Health Check
PROMPT ========================================

SELECT 
    execution_id,
    TO_CHAR(partition_date, 'YYYY-MM-DD') AS data_date,
    source_index_count,
    archive_index_count,
    invalid_indexes_before,
    invalid_indexes_after,
    CASE 
        WHEN invalid_indexes_after > invalid_indexes_before THEN 'DEGRADED'
        WHEN invalid_indexes_after < invalid_indexes_before THEN 'IMPROVED'
        ELSE 'UNCHANGED'
    END AS index_health_trend
FROM snparch_ctl_execution_log
ORDER BY execution_id DESC
FETCH FIRST 10 ROWS ONLY;

