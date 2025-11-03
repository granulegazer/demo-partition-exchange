-- ========================================
-- MANUAL PARTITION EXCHANGE - PURE SQL WITH FUNCTIONS
-- ========================================
-- This script performs partition exchange using the get_partition_name_by_date function
-- No complex partition name lookups - just specify the date
--
-- INSTRUCTIONS:
-- 1. Update the target date below
-- 2. Run the script
-- ========================================

SET ECHO ON
SET FEEDBACK ON
SET LINESIZE 200
SET SERVEROUTPUT ON

PROMPT ===========================================
PROMPT MANUAL PARTITION EXCHANGE
PROMPT ===========================================

-- *** CONFIGURE THESE VARIABLES ***
DEFINE v_target_date = '2024-01-15'
DEFINE v_source_table = 'SALES'
DEFINE v_archive_table = 'SALES_ARCHIVE'
DEFINE v_staging_table = 'SALES_STAGING_TEMP'

PROMPT Target Date: &v_target_date
PROMPT Source Table: &v_source_table
PROMPT Archive Table: &v_archive_table
PROMPT

-- Display initial table stats
PROMPT ===========================================
PROMPT Initial Table Statistics
PROMPT ===========================================

SELECT 
    '&v_source_table' AS table_name,
    f_defrag_get_table_size_stats_util('&v_source_table') AS table_stats
FROM dual
UNION ALL
SELECT 
    '&v_archive_table' AS table_name,
    f_defrag_get_table_size_stats_util('&v_archive_table') AS table_stats
FROM dual;

PROMPT

PAUSE Press Enter to continue or Ctrl+C to exit...

PROMPT ===========================================
PROMPT STEP 1: Get partition name using function
PROMPT ===========================================

COLUMN partition_name FORMAT A30
COLUMN data_for_date FORMAT A15

SELECT 
    get_partition_name_by_date('&v_source_table', DATE '&v_target_date') AS partition_name,
    '&v_target_date' AS data_for_date
FROM dual;

PROMPT Note the partition name from above
PROMPT

PAUSE Press Enter to continue...

PROMPT ===========================================
PROMPT STEP 2: Verify partition and count records
PROMPT ===========================================

-- Store partition name in a variable using a query
COLUMN pname NEW_VALUE source_partition NOPRINT
SELECT get_partition_name_by_date('&v_source_table', DATE '&v_target_date') AS pname FROM dual;

SELECT COUNT(*) AS record_count
FROM &v_source_table PARTITION (&source_partition);

PROMPT Records to be archived from partition: &source_partition
PROMPT

PAUSE Press Enter to continue...

PROMPT ===========================================
PROMPT STEP 3: Create staging table
PROMPT ===========================================

-- Clean up if exists
BEGIN
    EXECUTE IMMEDIATE 'DROP TABLE &v_staging_table PURGE';
EXCEPTION WHEN OTHERS THEN NULL;
END;
/

CREATE TABLE &v_staging_table AS 
SELECT * FROM &v_source_table WHERE 1=0;

PROMPT Staging table created

PAUSE Press Enter to continue...

PROMPT ===========================================
PROMPT STEP 4: Create indexes on staging table
PROMPT ===========================================

CREATE INDEX idx_staging_date ON &v_staging_table(sale_date);
CREATE INDEX idx_staging_customer ON &v_staging_table(customer_id);
CREATE INDEX idx_staging_region ON &v_staging_table(region);

PROMPT Indexes created

PAUSE Press Enter to continue...

PROMPT ===========================================
PROMPT STEP 5: Exchange partition from source to staging
PROMPT ===========================================
PROMPT This is INSTANT - metadata only operation
PROMPT Partition: &source_partition

ALTER TABLE &v_source_table 
EXCHANGE PARTITION &source_partition
WITH TABLE &v_staging_table 
INCLUDING INDEXES WITHOUT VALIDATION;

PROMPT Exchange from &v_source_table to staging completed

PAUSE Press Enter to continue...

PROMPT ===========================================
PROMPT STEP 6: Get archive partition name using function
PROMPT ===========================================

COLUMN aname NEW_VALUE archive_partition NOPRINT
SELECT get_partition_name_by_date('&v_archive_table', DATE '&v_target_date') AS aname FROM dual;

-- Display the partition name
SELECT 
    CASE 
        WHEN get_partition_name_by_date('&v_archive_table', DATE '&v_target_date') IS NULL 
        THEN 'Partition does not exist - will be created'
        ELSE 'Partition exists: ' || get_partition_name_by_date('&v_archive_table', DATE '&v_target_date')
    END AS archive_partition_status
FROM dual;

PAUSE Press Enter to continue...

PROMPT ===========================================
PROMPT STEP 7: Create archive partition if needed
PROMPT ===========================================

DECLARE
    v_archive_partition VARCHAR2(128);
BEGIN
    v_archive_partition := get_partition_name_by_date('&v_archive_table', DATE '&v_target_date');
    
    IF v_archive_partition IS NULL THEN
        DBMS_OUTPUT.PUT_LINE('Creating archive partition by inserting test row...');
        
        -- Insert one row to trigger interval partition creation
        EXECUTE IMMEDIATE 'INSERT INTO &v_archive_table SELECT * FROM &v_staging_table WHERE ROWNUM = 1';
        COMMIT;
        
        -- Get the newly created partition name
        v_archive_partition := get_partition_name_by_date('&v_archive_table', DATE '&v_target_date');
        DBMS_OUTPUT.PUT_LINE('Archive partition created: ' || v_archive_partition);
        
        -- Delete the test row
        EXECUTE IMMEDIATE 'DELETE FROM &v_archive_table PARTITION (' || v_archive_partition || ')';
        COMMIT;
        
        DBMS_OUTPUT.PUT_LINE('Test row removed');
    ELSE
        DBMS_OUTPUT.PUT_LINE('Archive partition already exists: ' || v_archive_partition);
    END IF;
END;
/

PAUSE Press Enter to continue...

PROMPT ===========================================
PROMPT STEP 8: Refresh archive partition name
PROMPT ===========================================

COLUMN aname NEW_VALUE archive_partition NOPRINT
SELECT get_partition_name_by_date('&v_archive_table', DATE '&v_target_date') AS aname FROM dual;

SELECT 
    get_partition_name_by_date('&v_archive_table', DATE '&v_target_date') AS archive_partition_name
FROM dual;

PAUSE Press Enter to continue...

PROMPT ===========================================
PROMPT STEP 9: Exchange partition from staging to archive
PROMPT ===========================================
PROMPT This is INSTANT - metadata only operation
PROMPT Partition: &archive_partition

ALTER TABLE &v_archive_table 
EXCHANGE PARTITION &archive_partition
WITH TABLE &v_staging_table 
INCLUDING INDEXES WITHOUT VALIDATION;

PROMPT Exchange from staging to &v_archive_table completed

PAUSE Press Enter to continue...

PROMPT ===========================================
PROMPT STEP 10: Drop staging table
PROMPT ===========================================

DROP TABLE &v_staging_table PURGE;

PROMPT Staging table dropped

PAUSE Press Enter to continue...

PROMPT ===========================================
PROMPT STEP 11: Drop empty partition from source
PROMPT ===========================================
PROMPT Partition: &source_partition

ALTER TABLE &v_source_table 
DROP PARTITION &source_partition;

PROMPT Empty partition dropped from &v_source_table

PAUSE Press Enter to continue...

PROMPT ===========================================
PROMPT STEP 12: Verify the exchange
PROMPT ===========================================

SELECT 
    '&v_archive_table' AS table_name,
    '&archive_partition' AS partition_name,
    COUNT(*) AS record_count
FROM &v_archive_table 
PARTITION (&archive_partition);

PROMPT ===========================================
PROMPT Final Table Statistics
PROMPT ===========================================

SELECT 
    '&v_source_table' AS table_name,
    f_defrag_get_table_size_stats_util('&v_source_table') AS table_stats
FROM dual
UNION ALL
SELECT 
    '&v_archive_table' AS table_name,
    f_defrag_get_table_size_stats_util('&v_archive_table') AS table_stats
FROM dual;

PROMPT ===========================================
PROMPT EXCHANGE COMPLETED SUCCESSFULLY
PROMPT ===========================================
PROMPT Date archived: &v_target_date
PROMPT Source partition: &source_partition
PROMPT Archive partition: &archive_partition
PROMPT ===========================================

-- Reset SQL*Plus settings
UNDEFINE v_target_date
UNDEFINE v_source_table
UNDEFINE v_archive_table
UNDEFINE v_staging_table
UNDEFINE source_partition
UNDEFINE archive_partition
