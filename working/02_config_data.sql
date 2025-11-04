-- ========================================
-- CONFIGURATION DATA
-- ========================================
-- Insert configuration records for partition archival
-- This script populates the SNPARCH_CNF_PARTITION_ARCHIVE table

-- Insert configuration for SALES table
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
    'SALES',
    'SALES_ARCHIVE',
    'SALES_STAGING_TEMP',
    'Y',
    'Y',
    'Y',
    'N',
    NULL
);

COMMIT;

-- Display configuration
SET LINESIZE 200
SET PAGESIZE 50000
COLUMN source_table_name FORMAT A20
COLUMN archive_table_name FORMAT A20
COLUMN staging_table_name FORMAT A25
COLUMN is_active FORMAT A8
COLUMN validate_before_exchange FORMAT A15
COLUMN gather_stats_after_exchange FORMAT A18

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
ORDER BY source_table_name;

PROMPT
PROMPT Configuration data loaded successfully
PROMPT
