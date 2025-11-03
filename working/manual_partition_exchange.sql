-- ========================================
-- MANUAL PARTITION EXCHANGE SCRIPT
-- ========================================
-- This script performs partition exchange using straight SQL statements
-- without using procedures or functions
--
-- INSTRUCTIONS:
-- 1. Identify the partition name and date you want to archive
-- 2. Update the variables in the DECLARE block
-- 3. Run the script
-- ========================================

SET SERVEROUTPUT ON
SET ECHO ON

DECLARE
    -- *** CONFIGURE THESE VARIABLES ***
    v_date_to_archive DATE := DATE '2024-01-15';  -- Change this to your target date
    v_source_table VARCHAR2(30) := 'SALES';
    v_archive_table VARCHAR2(30) := 'SALES_ARCHIVE';
    v_staging_table VARCHAR2(30) := 'SALES_STAGING_TEMP';
    
    -- Working variables
    v_partition_name VARCHAR2(128);
    v_archive_partition_name VARCHAR2(128);
    v_count NUMBER;
BEGIN
    DBMS_OUTPUT.PUT_LINE('===========================================');
    DBMS_OUTPUT.PUT_LINE('Starting partition archiving for ' || p_dates.COUNT || ' dates');
    DBMS_OUTPUT.PUT_LINE('Source table stats: ' || f_degrag_get_table_size_stats_util('SALES'));
    DBMS_OUTPUT.PUT_LINE('Archive table stats: ' || f_degrag_get_table_size_stats_util('SALES_ARCHIVE'));
    DBMS_OUTPUT.PUT_LINE('===========================================');
    
    -- Step 1: Find the partition name for the source table
    DBMS_OUTPUT.PUT_LINE('Step 1: Finding partition name...');
    SELECT partition_name
    INTO v_partition_name
    FROM user_tab_partitions
    WHERE table_name = v_source_table
      AND partition_name != 'SALES_OLD'
      AND TO_DATE(
            TRIM(BOTH '''' FROM REGEXP_SUBSTR(high_value, '''[^'']+''')),
            'YYYY-MM-DD'
          ) = v_date_to_archive + 1;
    
    DBMS_OUTPUT.PUT_LINE('   Found partition: ' || v_partition_name);
    
    -- Step 2: Check if partition has data
    DBMS_OUTPUT.PUT_LINE('Step 2: Checking partition data...');
    EXECUTE IMMEDIATE 'SELECT COUNT(*) FROM ' || v_source_table || 
                      ' PARTITION (' || v_partition_name || ')'
    INTO v_count;
    DBMS_OUTPUT.PUT_LINE('   Records in partition: ' || v_count);
    
    IF v_count = 0 THEN
        DBMS_OUTPUT.PUT_LINE('   WARNING: Partition is empty. No exchange needed.');
        RETURN;
    END IF;
    
    -- Step 3: Create staging table
    DBMS_OUTPUT.PUT_LINE('Step 3: Creating staging table...');
    EXECUTE IMMEDIATE 'CREATE TABLE ' || v_staging_table || 
                      ' AS SELECT * FROM ' || v_source_table || ' WHERE 1=0';
    DBMS_OUTPUT.PUT_LINE('   Staging table created');
    
    -- Step 4: Create indexes on staging table
    DBMS_OUTPUT.PUT_LINE('Step 4: Creating indexes on staging table...');
    EXECUTE IMMEDIATE 'CREATE INDEX idx_staging_date ON ' || v_staging_table || '(sale_date)';
    EXECUTE IMMEDIATE 'CREATE INDEX idx_staging_customer ON ' || v_staging_table || '(customer_id)';
    EXECUTE IMMEDIATE 'CREATE INDEX idx_staging_region ON ' || v_staging_table || '(region)';
    DBMS_OUTPUT.PUT_LINE('   Indexes created');
    
    -- Step 5: Exchange partition from source to staging
    DBMS_OUTPUT.PUT_LINE('Step 5: Exchange partition from ' || v_source_table || ' to staging...');
    EXECUTE IMMEDIATE 'ALTER TABLE ' || v_source_table || 
                      ' EXCHANGE PARTITION ' || v_partition_name || 
                      ' WITH TABLE ' || v_staging_table || 
                      ' INCLUDING INDEXES WITHOUT VALIDATION';
    DBMS_OUTPUT.PUT_LINE('   ✓ Exchange completed (instant)');
    
    -- Step 6: Check if archive partition exists
    DBMS_OUTPUT.PUT_LINE('Step 6: Checking archive partition...');
    BEGIN
        SELECT partition_name
        INTO v_archive_partition_name
        FROM user_tab_partitions
        WHERE table_name = v_archive_table
          AND partition_name != 'SALES_OLD'
          AND TO_DATE(
                TRIM(BOTH '''' FROM REGEXP_SUBSTR(high_value, '''[^'']+''')),
                'YYYY-MM-DD'
              ) = v_date_to_archive + 1;
        
        DBMS_OUTPUT.PUT_LINE('   Archive partition exists: ' || v_archive_partition_name);
    EXCEPTION
        WHEN NO_DATA_FOUND THEN
            DBMS_OUTPUT.PUT_LINE('   Archive partition does not exist. Creating it...');
            
            -- Insert one row to trigger partition creation
            EXECUTE IMMEDIATE 'INSERT INTO ' || v_archive_table || 
                              ' SELECT * FROM ' || v_staging_table || ' WHERE ROWNUM = 1';
            COMMIT;
            
            -- Get the newly created partition name
            SELECT partition_name
            INTO v_archive_partition_name
            FROM user_tab_partitions
            WHERE table_name = v_archive_table
              AND partition_name != 'SALES_OLD'
              AND TO_DATE(
                    TRIM(BOTH '''' FROM REGEXP_SUBSTR(high_value, '''[^'']+''')),
                    'YYYY-MM-DD'
                  ) = v_date_to_archive + 1;
            
            -- Delete the test row
            EXECUTE IMMEDIATE 'DELETE FROM ' || v_archive_table || 
                              ' PARTITION (' || v_archive_partition_name || ')';
            COMMIT;
            
            DBMS_OUTPUT.PUT_LINE('   Created partition: ' || v_archive_partition_name);
    END;
    
    -- Step 7: Exchange partition from staging to archive
    DBMS_OUTPUT.PUT_LINE('Step 7: Exchange partition from staging to ' || v_archive_table || '...');
    EXECUTE IMMEDIATE 'ALTER TABLE ' || v_archive_table || 
                      ' EXCHANGE PARTITION ' || v_archive_partition_name || 
                      ' WITH TABLE ' || v_staging_table || 
                      ' INCLUDING INDEXES WITHOUT VALIDATION';
    DBMS_OUTPUT.PUT_LINE('   ✓ Exchange completed (instant)');
    
    -- Step 8: Drop staging table
    DBMS_OUTPUT.PUT_LINE('Step 8: Dropping staging table...');
    EXECUTE IMMEDIATE 'DROP TABLE ' || v_staging_table;
    DBMS_OUTPUT.PUT_LINE('   Staging table dropped');
    
    -- Step 9: Drop the empty partition from source table
    DBMS_OUTPUT.PUT_LINE('Step 9: Dropping empty partition from ' || v_source_table || '...');
    EXECUTE IMMEDIATE 'ALTER TABLE ' || v_source_table || ' DROP PARTITION ' || v_partition_name;
    DBMS_OUTPUT.PUT_LINE('   Partition ' || v_partition_name || ' dropped');
    
    -- Step 10: Verify the exchange
    DBMS_OUTPUT.PUT_LINE('Step 10: Verifying exchange...');
    EXECUTE IMMEDIATE 'SELECT COUNT(*) FROM ' || v_archive_table || 
                      ' PARTITION (' || v_archive_partition_name || ')'
    INTO v_count;
    DBMS_OUTPUT.PUT_LINE('   Records in archive partition: ' || v_count);
    
    COMMIT;
    
    DBMS_OUTPUT.PUT_LINE('===========================================');
    DBMS_OUTPUT.PUT_LINE('✓ PARTITION EXCHANGE COMPLETED SUCCESSFULLY');
    DBMS_OUTPUT.PUT_LINE('   Date archived: ' || TO_CHAR(v_date_to_archive, 'YYYY-MM-DD'));
    DBMS_OUTPUT.PUT_LINE('   Records archived: ' || v_count);
    DBMS_OUTPUT.PUT_LINE('Source table stats: ' || f_degrag_get_table_size_stats_util('SALES'));
    DBMS_OUTPUT.PUT_LINE('Archive table stats: ' || f_degrag_get_table_size_stats_util('SALES_ARCHIVE'));
    DBMS_OUTPUT.PUT_LINE('===========================================');
    
EXCEPTION
    WHEN OTHERS THEN
        DBMS_OUTPUT.PUT_LINE('ERROR: ' || SQLERRM);
        -- Cleanup staging table if it exists
        BEGIN
            EXECUTE IMMEDIATE 'DROP TABLE ' || v_staging_table;
            DBMS_OUTPUT.PUT_LINE('Cleanup: Staging table dropped');
        EXCEPTION WHEN OTHERS THEN NULL;
        END;
        ROLLBACK;
        RAISE;
END;
/

-- ========================================
-- SIMPLIFIED VERSION - Direct SQL Statements
-- ========================================
-- Uncomment and modify the section below to run direct SQL commands
-- without PL/SQL block (requires manual partition name lookup)
-- ========================================

/*
-- 1. Find partition name manually
SELECT partition_name, high_value
FROM user_tab_partitions
WHERE table_name = 'SALES'
  AND partition_name != 'SALES_OLD'
ORDER BY partition_position;

-- 2. Create staging table
CREATE TABLE sales_staging_temp AS SELECT * FROM sales WHERE 1=0;

-- 3. Create indexes on staging
CREATE INDEX idx_staging_date ON sales_staging_temp(sale_date);
CREATE INDEX idx_staging_customer ON sales_staging_temp(customer_id);
CREATE INDEX idx_staging_region ON sales_staging_temp(region);

-- 4. Exchange partition from sales to staging (replace SYS_P123 with actual partition name)
ALTER TABLE sales 
EXCHANGE PARTITION SYS_P123 
WITH TABLE sales_staging_temp 
INCLUDING INDEXES WITHOUT VALIDATION;

-- 5. Create archive partition if needed (insert one row to trigger interval partition)
INSERT INTO sales_archive SELECT * FROM sales_staging_temp WHERE ROWNUM = 1;
COMMIT;

-- 6. Find archive partition name
SELECT partition_name, high_value
FROM user_tab_partitions
WHERE table_name = 'SALES_ARCHIVE'
  AND partition_name != 'SALES_OLD'
ORDER BY partition_position;

-- 7. Clear the test row
DELETE FROM sales_archive PARTITION (SYS_P456);  -- replace with actual partition name
COMMIT;

-- 8. Exchange partition from staging to archive (replace SYS_P456 with actual partition name)
ALTER TABLE sales_archive 
EXCHANGE PARTITION SYS_P456 
WITH TABLE sales_staging_temp 
INCLUDING INDEXES WITHOUT VALIDATION;

-- 9. Drop staging table
DROP TABLE sales_staging_temp;

-- 10. Drop empty partition from sales (replace SYS_P123 with actual partition name)
ALTER TABLE sales DROP PARTITION SYS_P123;

-- 11. Verify
SELECT COUNT(*) FROM sales_archive PARTITION (SYS_P456);  -- replace with actual partition name
*/
