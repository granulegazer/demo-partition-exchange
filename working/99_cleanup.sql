-- ========================================
-- CLEANUP SCRIPTS
-- Oracle 19.26 Compatible
-- ========================================

-- Step 1: Drop the view
DROP VIEW sales_complete;

-- Step 2: Drop helper function
DROP FUNCTION get_date_list;

-- Step 2a: Drop partition name function
DROP FUNCTION get_partition_name_by_date;

-- Step 3: Drop archive procedure
DROP PROCEDURE archive_partitions_by_dates;

-- Step 4: Drop custom type
DROP TYPE date_array_type;

-- Step 5: Drop tables
DROP TABLE sales PURGE;
DROP TABLE sales_archive PURGE;
DROP TABLE snparch_cnf_partition_archive PURGE;
DROP TABLE snparch_ctl_execution_log PURGE;

-- Step 6: Verify all objects are dropped
SELECT object_name, object_type 
FROM user_objects 
WHERE object_name IN (
    'SALES',
    'SALES_ARCHIVE',
    'SALES_COMPLETE',
    'SNPARCH_CNF_PARTITION_ARCHIVE',
    'SNPARCH_CTL_EXECUTION_LOG',
    'GET_DATE_LIST',
    'GET_PARTITION_NAME_BY_DATE',
    'ARCHIVE_PARTITIONS_BY_DATES',
    'DATE_ARRAY_TYPE'
);

-- Note: The PURGE option is used to completely remove the tables
-- without placing them in the recycle bin, ensuring a clean rollback.
-- Remove PURGE if you want the option to flashback the tables.

-- ========================================
-- SELECTIVE CLEANUP
-- ========================================
/*
-- Reset tables (keep structure, remove data)
TRUNCATE TABLE sales;
TRUNCATE TABLE sales_archive;

-- Remove specific partition from sales
-- Replace partition_name with actual partition name
ALTER TABLE sales DROP PARTITION partition_name;

-- Remove specific partition from archive
-- Replace partition_name with actual partition name
ALTER TABLE sales_archive DROP PARTITION partition_name;

-- Rebuild indexes if needed
ALTER INDEX idx_sales_date REBUILD;
ALTER INDEX idx_sales_customer REBUILD;
ALTER INDEX idx_sales_region REBUILD;
ALTER INDEX idx_archive_date REBUILD;
ALTER INDEX idx_archive_customer REBUILD;
ALTER INDEX idx_archive_archdate REBUILD;

-- Regather statistics after major changes
BEGIN
    DBMS_STATS.GATHER_TABLE_STATS(
        ownname => USER,
        tabname => 'SALES',
        estimate_percent => DBMS_STATS.AUTO_SAMPLE_SIZE,
        cascade => TRUE
    );
    
    DBMS_STATS.GATHER_TABLE_STATS(
        ownname => USER,
        tabname => 'SALES_ARCHIVE',
        estimate_percent => DBMS_STATS.AUTO_SAMPLE_SIZE,
        cascade => TRUE
    );
END;
/
*/