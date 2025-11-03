-- ========================================
-- MANUAL PARTITION EXCHANGE - PURE SQL
-- ========================================
-- This script performs partition exchange using only SQL statements
-- No PL/SQL blocks - just straight SQL commands
--
-- INSTRUCTIONS:
-- 1. Run Step 1 to find the partition name for your target date
-- 2. Note the partition name (e.g., SYS_P12345)
-- 3. Replace PARTITION_NAME_HERE with the actual partition name in subsequent steps
-- 4. Run each step in sequence
-- ========================================

SET ECHO ON
SET FEEDBACK ON
SET LINESIZE 200

PROMPT ===========================================
PROMPT STEP 1: Find partition name for target date
PROMPT ===========================================
PROMPT Replace the date below with your target date
PROMPT Example: WHERE ... = DATE '2024-01-15' + 1

SELECT 
    partition_name,
    high_value,
    TO_DATE(
        TRIM(BOTH '''' FROM REGEXP_SUBSTR(high_value, '''[^'']+''')),
        'YYYY-MM-DD'
    ) - 1 AS data_date
FROM user_tab_partitions
WHERE table_name = 'SALES'
  AND partition_name != 'SALES_OLD'
  AND TO_DATE(
        TRIM(BOTH '''' FROM REGEXP_SUBSTR(high_value, '''[^'']+''')),
        'YYYY-MM-DD'
      ) = DATE '2024-01-15' + 1  -- *** CHANGE THIS DATE ***
ORDER BY partition_position;

PROMPT
PROMPT Note the PARTITION_NAME from above (e.g., SYS_P12345)
PROMPT

PAUSE Press Enter to continue or Ctrl+C to exit...

PROMPT ===========================================
PROMPT STEP 2: Count records in source partition
PROMPT ===========================================
PROMPT Replace PARTITION_NAME_HERE below

SELECT COUNT(*) AS record_count
FROM sales PARTITION (PARTITION_NAME_HERE);  -- *** REPLACE PARTITION_NAME_HERE ***

PAUSE Press Enter to continue...

PROMPT ===========================================
PROMPT STEP 3: Create staging table
PROMPT ===========================================

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
PROMPT STEP 5: Exchange partition from SALES to staging
PROMPT ===========================================
PROMPT This is INSTANT - metadata only operation

ALTER TABLE sales 
EXCHANGE PARTITION PARTITION_NAME_HERE  -- *** REPLACE PARTITION_NAME_HERE ***
WITH TABLE sales_staging_temp 
INCLUDING INDEXES WITHOUT VALIDATION;

PROMPT Exchange from SALES to staging completed

PAUSE Press Enter to continue...

PROMPT ===========================================
PROMPT STEP 6: Check if archive partition exists
PROMPT ===========================================

SELECT 
    partition_name,
    high_value
FROM user_tab_partitions
WHERE table_name = 'SALES_ARCHIVE'
  AND partition_name != 'SALES_OLD'
  AND TO_DATE(
        TRIM(BOTH '''' FROM REGEXP_SUBSTR(high_value, '''[^'']+''')),
        'YYYY-MM-DD'
      ) = DATE '2024-01-15' + 1  -- *** CHANGE THIS DATE ***
ORDER BY partition_position;

PROMPT If no rows returned, partition doesn't exist - continue to Step 7
PROMPT If partition exists, note the name and skip to Step 9

PAUSE Press Enter to continue (or skip to Step 9 if partition exists)...

PROMPT ===========================================
PROMPT STEP 7: Create archive partition (if needed)
PROMPT ===========================================
PROMPT Insert one row to trigger interval partition creation

INSERT INTO sales_archive 
SELECT * FROM sales_staging_temp WHERE ROWNUM = 1;

COMMIT;

PROMPT Partition created

PAUSE Press Enter to continue...

PROMPT ===========================================
PROMPT STEP 8: Find the newly created archive partition
PROMPT ===========================================

SELECT 
    partition_name,
    high_value
FROM user_tab_partitions
WHERE table_name = 'SALES_ARCHIVE'
  AND partition_name != 'SALES_OLD'
  AND TO_DATE(
        TRIM(BOTH '''' FROM REGEXP_SUBSTR(high_value, '''[^'']+''')),
        'YYYY-MM-DD'
      ) = DATE '2024-01-15' + 1  -- *** CHANGE THIS DATE ***
ORDER BY partition_position;

PROMPT Note the ARCHIVE_PARTITION_NAME from above

PAUSE Press Enter to continue...

PROMPT ===========================================
PROMPT STEP 8b: Delete the test row
PROMPT ===========================================

DELETE FROM sales_archive 
PARTITION (ARCHIVE_PARTITION_NAME);  -- *** REPLACE ARCHIVE_PARTITION_NAME ***

COMMIT;

PAUSE Press Enter to continue...

PROMPT ===========================================
PROMPT STEP 9: Exchange partition from staging to SALES_ARCHIVE
PROMPT ===========================================
PROMPT This is INSTANT - metadata only operation

ALTER TABLE sales_archive 
EXCHANGE PARTITION ARCHIVE_PARTITION_NAME  -- *** REPLACE ARCHIVE_PARTITION_NAME ***
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
PROMPT STEP 11: Drop empty partition from SALES
PROMPT ===========================================

ALTER TABLE sales 
DROP PARTITION PARTITION_NAME_HERE;  -- *** REPLACE PARTITION_NAME_HERE ***

PROMPT Empty partition dropped from SALES table

PAUSE Press Enter to continue...

PROMPT ===========================================
PROMPT STEP 12: Verify the exchange
PROMPT ===========================================

SELECT 'Records in archive partition:' AS description, COUNT(*) AS count
FROM sales_archive 
PARTITION (ARCHIVE_PARTITION_NAME);  -- *** REPLACE ARCHIVE_PARTITION_NAME ***

PROMPT ===========================================
PROMPT EXCHANGE COMPLETED
PROMPT ===========================================

-- ========================================
-- ALTERNATIVE: Single-shot version
-- Copy these commands, replace the placeholders, and run them all at once
-- ========================================
/*
-- Step 1: Find partition (run first, then copy partition name)
SELECT partition_name, high_value
FROM user_tab_partitions
WHERE table_name = 'SALES'
  AND TO_DATE(TRIM(BOTH '''' FROM REGEXP_SUBSTR(high_value, '''[^'']+''')), 'YYYY-MM-DD') = DATE '2024-01-15' + 1;

-- Step 2-12: Replace PARTITION_NAME and ARCHIVE_PARTITION_NAME then run all

DROP TABLE sales_staging_temp PURGE;
CREATE TABLE sales_staging_temp AS SELECT * FROM sales WHERE 1=0;
CREATE INDEX idx_staging_date ON sales_staging_temp(sale_date);
CREATE INDEX idx_staging_customer ON sales_staging_temp(customer_id);
CREATE INDEX idx_staging_region ON sales_staging_temp(region);

ALTER TABLE sales EXCHANGE PARTITION SYS_P12345 WITH TABLE sales_staging_temp INCLUDING INDEXES WITHOUT VALIDATION;

INSERT INTO sales_archive SELECT * FROM sales_staging_temp WHERE ROWNUM = 1;
COMMIT;

SELECT partition_name FROM user_tab_partitions 
WHERE table_name = 'SALES_ARCHIVE' 
  AND TO_DATE(TRIM(BOTH '''' FROM REGEXP_SUBSTR(high_value, '''[^'']+''')), 'YYYY-MM-DD') = DATE '2024-01-15' + 1;

DELETE FROM sales_archive PARTITION (SYS_P67890);
COMMIT;

ALTER TABLE sales_archive EXCHANGE PARTITION SYS_P67890 WITH TABLE sales_staging_temp INCLUDING INDEXES WITHOUT VALIDATION;

DROP TABLE sales_staging_temp PURGE;
ALTER TABLE sales DROP PARTITION SYS_P12345;

SELECT COUNT(*) FROM sales_archive PARTITION (SYS_P67890);
*/
