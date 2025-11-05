-- ========================================
-- DEMO CONFIGURATION DATA
-- ========================================
-- Inserts configuration for SALES/SALES_ARCHIVE demo tables
-- Run this AFTER demo_tables_setup.sql
-- For production: modify this for your own tables
-- ========================================

SET ECHO ON
SET FEEDBACK ON

PROMPT
PROMPT ========================================
PROMPT Inserting Demo Configuration Data
PROMPT ========================================
PROMPT

-- Clear any existing demo configuration
DELETE FROM snparch_cnf_partition_archive 
WHERE source_table_name = 'SALES' 
  AND archive_table_name = 'SALES_ARCHIVE';

COMMIT;

-- Insert configuration for SALES table archival
-- This configures the partition exchange framework to:
-- 1. Archive data from SALES to SALES_ARCHIVE
-- 2. Use SALES_STAGING_TEMPLATE for exchange operations
-- 3. Enable all validations and statistics gathering
-- 4. Optionally enable compression (set to 'N' by default)
INSERT INTO snparch_cnf_partition_archive (
    source_table_name,
    archive_table_name,
    staging_table_name,
    is_active,
    validate_before_exchange,
    gather_stats_after_exchange,
    enable_compression,
    compression_type,
    created_date,
    updated_date,
    created_by,
    updated_by
) VALUES (
    'SALES',                        -- Source table
    'SALES_ARCHIVE',                -- Archive table
    'SALES_STAGING_TEMPLATE',       -- Staging table for exchange
    'Y',                            -- Active
    'Y',                            -- Validate before exchange
    'Y',                            -- Gather statistics after exchange
    'N',                            -- Compression disabled (set to 'Y' to enable)
    NULL,                           -- No compression type (use 'QUERY LOW', 'ARCHIVE LOW', etc. if compression enabled)
    SYSTIMESTAMP,
    SYSTIMESTAMP,
    USER,
    USER
);

COMMIT;

PROMPT Demo configuration inserted

-- Verify the configuration
PROMPT
PROMPT ========================================
PROMPT Configuration Verification
PROMPT ========================================
PROMPT

SET LINESIZE 200
SET PAGESIZE 1000
COLUMN source_table_name FORMAT A20
COLUMN archive_table_name FORMAT A20
COLUMN staging_table_name FORMAT A30
COLUMN is_active FORMAT A8
COLUMN validate_before_exchange FORMAT A10 HEADING 'VALIDATE'
COLUMN gather_stats_after_exchange FORMAT A10 HEADING 'STATS'
COLUMN enable_compression FORMAT A11 HEADING 'COMPRESSION'
COLUMN compression_type FORMAT A15

SELECT 
    source_table_name,
    archive_table_name,
    staging_table_name,
    is_active,
    validate_before_exchange,
    gather_stats_after_exchange,
    enable_compression,
    compression_type
FROM snparch_cnf_partition_archive
WHERE source_table_name = 'SALES'
ORDER BY source_table_name;

PROMPT
PROMPT ========================================
PROMPT Demo Configuration Complete
PROMPT ========================================
PROMPT
PROMPT Configuration Created:
PROMPT   - Source: SALES
PROMPT   - Archive: SALES_ARCHIVE
PROMPT   - Staging: SALES_STAGING_TEMPLATE
PROMPT   - Validation: Enabled
PROMPT   - Statistics: Enabled
PROMPT   - Compression: Disabled (can be enabled in config table)
PROMPT
PROMPT Next Steps:
PROMPT   1. Generate test data: Run 03_data_generator.sql
PROMPT   2. Create archive procedure: Run 04_archive_procedure.sql
PROMPT   3. Test the archival: Run 05_test_scenarios.sql
PROMPT
PROMPT To enable compression for archived partitions:
PROMPT   UPDATE snparch_cnf_partition_archive
PROMPT   SET enable_compression = 'Y',
PROMPT       compression_type = 'QUERY LOW'  -- or 'ARCHIVE LOW', 'BASIC', etc.
PROMPT   WHERE source_table_name = 'SALES';
PROMPT ========================================
