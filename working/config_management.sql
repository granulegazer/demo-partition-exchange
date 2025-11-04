-- ========================================
-- CONFIGURATION MANAGEMENT SCRIPTS
-- Oracle 19.26 Optimized
-- ========================================
-- Quick reference for managing snparch_cnf_partition_archive table

-- ========================================
-- View Current Configuration
-- ========================================
SELECT 
    source_table_name,
    archive_table_name,
    staging_table_name,
    is_active,
    validate_before_exchange,
    gather_stats_after_exchange,
    enable_compression,
    compression_type,
    TO_CHAR(created_date, 'YYYY-MM-DD HH24:MI:SS') AS created,
    TO_CHAR(updated_date, 'YYYY-MM-DD HH24:MI:SS') AS updated,
    created_by,
    updated_by
FROM snparch_cnf_partition_archive
ORDER BY source_table_name;

-- ========================================
-- Add New Table Configuration
-- ========================================
/*
INSERT INTO snparch_cnf_partition_archive (
    source_table_name,
    archive_table_name,
    staging_table_name,
    is_active,
    validate_before_exchange,
    gather_stats_after_exchange,
    enable_compression,
    compression_type
) VALUES (
    'YOUR_TABLE',              -- Source partitioned table
    'YOUR_TABLE_ARCHIVE',      -- Archive partitioned table
    'YOUR_TABLE_STAGING',      -- Staging table name (will be created/dropped)
    'Y',                       -- Active flag
    'Y',                       -- Validate indexes before/after
    'Y',                       -- Gather statistics after exchange
    'N',                       -- Enable compression (Y/N)
    NULL                       -- Compression type: BASIC, OLTP, QUERY LOW/HIGH, ARCHIVE LOW/HIGH
);
COMMIT;
*/

-- ========================================
-- Enable/Disable Archival for a Table
-- ========================================

-- Disable archival (keep configuration)
/*
UPDATE snparch_cnf_partition_archive
SET is_active = 'N',
    updated_date = SYSTIMESTAMP,
    updated_by = USER
WHERE source_table_name = 'SALES';
COMMIT;
*/

-- Enable archival
/*
UPDATE snparch_cnf_partition_archive
SET is_active = 'Y',
    updated_date = SYSTIMESTAMP,
    updated_by = USER
WHERE source_table_name = 'SALES';
COMMIT;
*/

-- ========================================
-- Toggle Validation Options
-- ========================================

-- Skip index validation (faster but riskier)
/*
UPDATE snparch_cnf_partition_archive
SET validate_before_exchange = 'N',
    updated_date = SYSTIMESTAMP,
    updated_by = USER
WHERE source_table_name = 'SALES';
COMMIT;
*/

-- Skip statistics gathering (much faster)
/*
UPDATE snparch_cnf_partition_archive
SET gather_stats_after_exchange = 'N',
    updated_date = SYSTIMESTAMP,
    updated_by = USER
WHERE source_table_name = 'SALES';
COMMIT;
*/

-- Enable all validations (recommended for production)
/*
UPDATE snparch_cnf_partition_archive
SET validate_before_exchange = 'Y',
    gather_stats_after_exchange = 'Y',
    updated_date = SYSTIMESTAMP,
    updated_by = USER
WHERE source_table_name = 'SALES';
COMMIT;
*/

-- ========================================
-- Configure Compression
-- ========================================

-- Enable OLTP compression (Oracle 19c - good balance)
/*
UPDATE snparch_cnf_partition_archive
SET enable_compression = 'Y',
    compression_type = 'OLTP',
    updated_date = SYSTIMESTAMP,
    updated_by = USER
WHERE source_table_name = 'SALES';
COMMIT;
*/

-- Enable ARCHIVE HIGH compression (Oracle 19c - maximum compression)
/*
UPDATE snparch_cnf_partition_archive
SET enable_compression = 'Y',
    compression_type = 'ARCHIVE HIGH',
    updated_date = SYSTIMESTAMP,
    updated_by = USER
WHERE source_table_name = 'SALES';
COMMIT;
*/

-- Disable compression
/*
UPDATE snparch_cnf_partition_archive
SET enable_compression = 'N',
    compression_type = NULL,
    updated_date = SYSTIMESTAMP,
    updated_by = USER
WHERE source_table_name = 'SALES';
COMMIT;
*/

-- ========================================
-- Update Configuration
-- ========================================

-- Change staging table name
/*
UPDATE snparch_cnf_partition_archive
SET staging_table_name = 'NEW_STAGING_NAME',
    updated_date = SYSTIMESTAMP,
    updated_by = USER
WHERE source_table_name = 'SALES';
COMMIT;
*/

-- Change archive table name
/*
UPDATE snparch_cnf_partition_archive
SET archive_table_name = 'NEW_ARCHIVE_TABLE',
    updated_date = SYSTIMESTAMP,
    updated_by = USER
WHERE source_table_name = 'SALES';
COMMIT;
*/

-- ========================================
-- Delete Configuration
-- ========================================
/*
DELETE FROM snparch_cnf_partition_archive
WHERE source_table_name = 'SALES'
  AND archive_table_name = 'SALES_ARCHIVE';
COMMIT;
*/

-- ========================================
-- Bulk Configuration Setup
-- ========================================
-- Example: Add multiple tables at once
/*
BEGIN
    -- Table 1: With compression
    INSERT INTO snparch_cnf_partition_archive (
        source_table_name, archive_table_name, staging_table_name,
        is_active, validate_before_exchange, gather_stats_after_exchange,
        enable_compression, compression_type
    ) VALUES (
        'ORDERS', 'ORDERS_ARCHIVE', 'ORDERS_STAGING_TEMP',
        'Y', 'Y', 'Y', 'Y', 'OLTP'
    );
    
    -- Table 2: High compression for old data
    INSERT INTO snparch_cnf_partition_archive (
        source_table_name, archive_table_name, staging_table_name,
        is_active, validate_before_exchange, gather_stats_after_exchange,
        enable_compression, compression_type
    ) VALUES (
        'TRANSACTIONS', 'TRANSACTIONS_ARCHIVE', 'TRANS_STAGING_TEMP',
        'Y', 'Y', 'Y', 'Y', 'ARCHIVE HIGH'
    );
    
    -- Table 3: Fast mode - no validation or compression
    INSERT INTO snparch_cnf_partition_archive (
        source_table_name, archive_table_name, staging_table_name,
        is_active, validate_before_exchange, gather_stats_after_exchange,
        enable_compression, compression_type
    ) VALUES (
        'LOGS', 'LOGS_ARCHIVE', 'LOGS_STAGING_TEMP',
        'Y', 'N', 'N', 'N', NULL
    );
    
    COMMIT;
END;
/
*/

-- ========================================
-- Configuration Validation Query
-- ========================================
-- Check if configuration exists before running archival
/*
DECLARE
    v_count NUMBER;
BEGIN
    SELECT COUNT(*)
    INTO v_count
    FROM snparch_cnf_partition_archive
    WHERE source_table_name = 'SALES'
      AND is_active = 'Y';
      
    IF v_count = 0 THEN
        DBMS_OUTPUT.PUT_LINE('ERROR: No active configuration found for SALES');
    ELSE
        DBMS_OUTPUT.PUT_LINE('Configuration found and active for SALES');
    END IF;
END;
/
*/

-- ========================================
-- Audit Configuration Changes
-- ========================================
-- Show recent configuration changes (Oracle 19c syntax)
SELECT 
    source_table_name,
    archive_table_name,
    is_active,
    validate_before_exchange,
    gather_stats_after_exchange,
    enable_compression,
    compression_type,
    TO_CHAR(updated_date, 'YYYY-MM-DD HH24:MI:SS') AS last_updated,
    updated_by,
    ROUND(EXTRACT(DAY FROM (SYSTIMESTAMP - updated_date)) * 24 + 
          EXTRACT(HOUR FROM (SYSTIMESTAMP - updated_date)), 2) AS hours_since_update
FROM snparch_cnf_partition_archive
WHERE updated_date > SYSTIMESTAMP - INTERVAL '30' DAY  -- Oracle 19c syntax
ORDER BY updated_date DESC;

-- ========================================
-- Configuration Performance Settings
-- ========================================

-- Fast mode: Skip all validations (not recommended for production)
/*
UPDATE snparch_cnf_partition_archive
SET validate_before_exchange = 'N',
    gather_stats_after_exchange = 'N',
    updated_date = SYSTIMESTAMP,
    updated_by = USER
WHERE source_table_name = 'SALES';
COMMIT;
*/

-- Safe mode: Full validation and stats (recommended for production)
/*
UPDATE snparch_cnf_partition_archive
SET validate_before_exchange = 'Y',
    gather_stats_after_exchange = 'Y',
    updated_date = SYSTIMESTAMP,
    updated_by = USER
WHERE source_table_name = 'SALES';
COMMIT;
*/

-- Balanced mode: Validate but skip stats (good for frequent archival)
/*
UPDATE snparch_cnf_partition_archive
SET validate_before_exchange = 'Y',
    gather_stats_after_exchange = 'N',
    updated_date = SYSTIMESTAMP,
    updated_by = USER
WHERE source_table_name = 'SALES';
COMMIT;
*/
