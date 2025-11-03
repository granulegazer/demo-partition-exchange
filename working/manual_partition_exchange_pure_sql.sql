-- ========================================
-- MANUAL PARTITION EXCHANGE - PURE SQL WITH FUNCTIONS
-- ========================================
-- This script performs partition exchange using the get_partition_name_by_date function
-- Simple hardcoded version - just update the date and run
--
-- INSTRUCTIONS:
-- 1. Update the date in the queries below (search for '2024-01-15')
-- 2. Run the script
-- ========================================

SET ECHO ON
SET FEEDBACK ON
SET LINESIZE 200
SET SERVEROUTPUT ON

PROMPT ===========================================
PROMPT MANUAL PARTITION EXCHANGE
PROMPT ===========================================
PROMPT Target Date: 2024-01-15
PROMPT Source Table: SALES
PROMPT Archive Table: SALES_ARCHIVE
PROMPT

PAUSE Press Enter to continue or Ctrl+C to exit...

-- Display initial table stats
PROMPT ===========================================
PROMPT Initial Table Statistics
PROMPT ===========================================

SELECT 
    'SALES' AS table_name,
    f_degrag_get_table_size_stats_util('SALES') AS table_stats
FROM dual
UNION ALL
SELECT 
    'SALES_ARCHIVE' AS table_name,
    f_degrag_get_table_size_stats_util('SALES_ARCHIVE') AS table_stats
FROM dual;

PROMPT

PAUSE Press Enter to continue...

PROMPT ===========================================
PROMPT STEP 1: Get partition name using function
PROMPT ===========================================

SELECT 
    get_partition_name_by_date('SALES', DATE '2024-01-15') AS partition_name,
    '2024-01-15' AS data_for_date
FROM dual;

PROMPT

PAUSE Press Enter to continue...

PROMPT ===========================================
PROMPT STEP 2: Verify partition and count records
PROMPT ===========================================

SELECT COUNT(*) AS record_count
FROM sales PARTITION (SYS_P21);  -- *** REPLACE WITH YOUR PARTITION NAME ***

PROMPT

PAUSE Press Enter to continue...

PROMPT ===========================================
PROMPT STEP 3: Create staging table
PROMPT ===========================================

-- Clean up if exists
DROP TABLE sales_staging_temp PURGE;

CREATE TABLE sales_staging_temp AS 
SELECT * FROM sales WHERE 1=0;

PROMPT Staging table created

PAUSE Press Enter to continue...

PROMPT ===========================================
PROMPT STEP 4: Create indexes on staging table
PROMPT ===========================================

CREATE INDEX idx_staging_date ON sales_staging_temp(sale_date);
CREATE INDEX idx_staging_customer ON sales_staging_temp(customer_id);
CREATE INDEX idx_staging_region ON sales_staging_temp(region);

PROMPT Indexes created

PAUSE Press Enter to continue...

PROMPT ===========================================
PROMPT STEP 5: Exchange partition from source to staging
PROMPT ===========================================
PROMPT This is INSTANT - metadata only operation

ALTER TABLE sales 
EXCHANGE PARTITION SYS_P21  -- *** REPLACE WITH YOUR PARTITION NAME ***
WITH TABLE sales_staging_temp 
INCLUDING INDEXES WITHOUT VALIDATION;

PROMPT Exchange from SALES to staging completed

PAUSE Press Enter to continue...

PROMPT ===========================================
PROMPT STEP 6: Get archive partition name using function
PROMPT ===========================================

SELECT 
    get_partition_name_by_date('SALES_ARCHIVE', DATE '2024-01-15') AS archive_partition_name
FROM dual;

PAUSE Press Enter to continue...

PROMPT ===========================================
PROMPT STEP 7: Create archive partition if needed
PROMPT ===========================================

DECLARE
    v_archive_partition VARCHAR2(128);
BEGIN
    v_archive_partition := get_partition_name_by_date('SALES_ARCHIVE', DATE '2024-01-15');
    
    IF v_archive_partition IS NULL THEN
        DBMS_OUTPUT.PUT_LINE('Creating archive partition by inserting test row...');
        
        -- Insert one row to trigger interval partition creation
        EXECUTE IMMEDIATE 'INSERT INTO sales_archive SELECT * FROM sales_staging_temp WHERE ROWNUM = 1';
        COMMIT;
        
        -- Get the newly created partition name
        v_archive_partition := get_partition_name_by_date('SALES_ARCHIVE', DATE '2024-01-15');
        DBMS_OUTPUT.PUT_LINE('Archive partition created: ' || v_archive_partition);
        
        -- Delete the test row
        EXECUTE IMMEDIATE 'DELETE FROM sales_archive PARTITION (' || v_archive_partition || ')';
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

SELECT 
    get_partition_name_by_date('SALES_ARCHIVE', DATE '2024-01-15') AS archive_partition_name
FROM dual;

PAUSE Press Enter to continue...

PROMPT ===========================================
PROMPT STEP 9: Exchange partition from staging to archive
PROMPT ===========================================
PROMPT This is INSTANT - metadata only operation

ALTER TABLE sales_archive 
EXCHANGE PARTITION SYS_P21  -- *** REPLACE WITH YOUR ARCHIVE PARTITION NAME ***
WITH TABLE sales_staging_temp 
INCLUDING INDEXES WITHOUT VALIDATION;

PROMPT Exchange from staging to SALES_ARCHIVE completed

PAUSE Press Enter to continue...

PROMPT ===========================================
PROMPT STEP 10: Drop staging table
PROMPT ===========================================

DROP TABLE sales_staging_temp PURGE;

PROMPT Staging table dropped

PAUSE Press Enter to continue...

PROMPT ===========================================
PROMPT STEP 11: Drop empty partition from source
PROMPT ===========================================

ALTER TABLE sales 
DROP PARTITION SYS_P21;  -- *** REPLACE WITH YOUR SOURCE PARTITION NAME ***

PROMPT Empty partition dropped from SALES

PAUSE Press Enter to continue...

PROMPT ===========================================
PROMPT STEP 12: Verify the exchange
PROMPT ===========================================

SELECT 
    'SALES_ARCHIVE' AS table_name,
    'SYS_P21' AS partition_name,  -- *** REPLACE WITH YOUR ARCHIVE PARTITION NAME ***
    COUNT(*) AS record_count
FROM sales_archive 
PARTITION (SYS_P21);  -- *** REPLACE WITH YOUR ARCHIVE PARTITION NAME ***

PROMPT ===========================================
PROMPT Final Table Statistics
PROMPT ===========================================

SELECT 
    'SALES' AS table_name,
    f_degrag_get_table_size_stats_util('SALES') AS table_stats
FROM dual
UNION ALL
SELECT 
    'SALES_ARCHIVE' AS table_name,
    f_degrag_get_table_size_stats_util('SALES_ARCHIVE') AS table_stats
FROM dual;

PROMPT ===========================================
PROMPT EXCHANGE COMPLETED SUCCESSFULLY
PROMPT ===========================================
PROMPT Date archived: 2024-01-15
PROMPT ===========================================
