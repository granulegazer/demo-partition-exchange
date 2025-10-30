-- ============================================
-- PARTITION ARCHIVAL - COMPLETE SOLUTION
-- ============================================

-- SCENARIO: Main table with daily partitions, archive partitions older than 12 months

-- ============================================
-- STEP 1: CREATE MAIN PARTITIONED TABLE
-- ============================================
CREATE TABLE sales_main (
    sale_id NUMBER,
    sale_date DATE,
    amount NUMBER,
    customer_id NUMBER,
    product_id NUMBER,
    CONSTRAINT pk_sales PRIMARY KEY (sale_id, sale_date)
)
PARTITION BY RANGE (sale_date)
INTERVAL (NUMTODSINTERVAL(1, 'DAY'))  -- Auto-create partition per day
(
    PARTITION p_initial VALUES LESS THAN (DATE '2023-01-01')
)
NOCOMPRESS;

-- Create indexes
CREATE INDEX idx_sales_date ON sales_main(sale_date) LOCAL;
CREATE INDEX idx_sales_customer ON sales_main(customer_id) LOCAL;

-- ============================================
-- STEP 2: CREATE ARCHIVE PARTITIONED TABLE
-- ============================================
-- Archive table has SAME structure and partitioning scheme
CREATE TABLE sales_archive (
    sale_id NUMBER,
    sale_date DATE,
    amount NUMBER,
    customer_id NUMBER,
    product_id NUMBER,
    CONSTRAINT pk_sales_archive PRIMARY KEY (sale_id, sale_date)
)
PARTITION BY RANGE (sale_date)
INTERVAL (NUMTODSINTERVAL(1, 'DAY'))  -- Auto-create partition per day
(
    PARTITION p_archive_initial VALUES LESS THAN (DATE '2023-01-01')
)
COMPRESS BASIC;  -- Using basic compression (available in all editions)

-- Create matching indexes
CREATE INDEX idx_sales_archive_date ON sales_archive(sale_date) LOCAL;
CREATE INDEX idx_sales_archive_customer ON sales_archive(customer_id) LOCAL;

-- ============================================
-- STEP 3: CREATE STAGING TABLE FOR EXCHANGE
-- ============================================
-- Staging table matches main table structure (no partitioning)
CREATE TABLE sales_staging (
    sale_id NUMBER,
    sale_date DATE,
    amount NUMBER,
    customer_id NUMBER,
    product_id NUMBER,
    CONSTRAINT pk_sales_staging PRIMARY KEY (sale_id, sale_date)
)
NOCOMPRESS;

-- Create matching indexes
CREATE INDEX idx_sales_staging_date ON sales_staging(sale_date);
CREATE INDEX idx_sales_staging_customer ON sales_staging(customer_id);

-- ============================================
-- STEP 4: POPULATE TEST DATA
-- ============================================
-- Generate data for 180 days with 10-15 records per day
DECLARE
    v_sale_id NUMBER := 1;
    v_records_per_day NUMBER;
    v_start_date DATE := DATE '2023-01-01';  -- Start from initial partition
    v_days NUMBER := 720;  -- 2 years of data
    v_regions SYS.ODCIVARCHAR2LIST := SYS.ODCIVARCHAR2LIST('North', 'South', 'East', 'West', 'Central');
    v_statuses SYS.ODCIVARCHAR2LIST := SYS.ODCIVARCHAR2LIST('Completed', 'Pending', 'Shipped', 'Delivered');
BEGIN
    DBMS_OUTPUT.PUT_LINE('Starting data generation for ' || v_days || ' days...');
    
    -- Loop through each day
    FOR day_offset IN 0..v_days-1 LOOP
        -- Random 10-15 records per day
        v_records_per_day := TRUNC(DBMS_RANDOM.VALUE(10, 16));
        
        -- Insert records for this day
        FOR rec IN 1..v_records_per_day LOOP
            INSERT INTO sales_main VALUES (
                v_sale_id,
                v_start_date + day_offset,
                TRUNC(DBMS_RANDOM.VALUE(1000, 5000)),  -- customer_id
                TRUNC(DBMS_RANDOM.VALUE(100, 999)),    -- product_id
                ROUND(DBMS_RANDOM.VALUE(50, 5000), 2)  -- amount
            );
            
            v_sale_id := v_sale_id + 1;
        END LOOP;
        
        -- Commit every 30 days
        IF MOD(day_offset, 30) = 0 THEN
            COMMIT;
            DBMS_OUTPUT.PUT_LINE('Committed data for day ' || TO_CHAR(v_start_date + day_offset, 'YYYY-MM-DD'));
        END IF;
    END LOOP;
    
    COMMIT;
    DBMS_OUTPUT.PUT_LINE('Data generation completed!');
    DBMS_OUTPUT.PUT_LINE('Total records inserted: ' || (v_sale_id - 1));
END;
/

-- Gather statistics
EXEC DBMS_STATS.GATHER_TABLE_STATS(USER, 'SALES_MAIN');

-- Verify data distribution
SELECT 
    TO_CHAR(sale_date, 'YYYY-MM') AS month,
    COUNT(*) AS total_records,
    COUNT(DISTINCT sale_date) AS days_with_data,
    ROUND(AVG(daily_count), 2) AS avg_records_per_day,
    MIN(daily_count) AS min_records_per_day,
    MAX(daily_count) AS max_records_per_day
FROM (
    SELECT sale_date, COUNT(*) AS daily_count
    FROM sales_main
    GROUP BY sale_date
)
GROUP BY TO_CHAR(sale_date, 'YYYY-MM')
ORDER BY month;

-- Check partition details (first 10)
SELECT 
    partition_name,
    partition_position,
    high_value,
    num_rows
FROM user_tab_partitions
WHERE table_name = 'SALES_MAIN'
  AND ROWNUM <= 10
ORDER BY partition_position;

-- ============================================
-- STEP 5: ARCHIVE PROCEDURE - THE EFFECTIVE APPROACH
-- ============================================
CREATE OR REPLACE PROCEDURE archive_old_partitions(
    p_main_table VARCHAR2,
    p_archive_table VARCHAR2,
    p_staging_table VARCHAR2,
    p_months_to_keep NUMBER DEFAULT 12,
    p_compress BOOLEAN DEFAULT TRUE
) IS
    v_cutoff_date DATE;
    v_partition_name VARCHAR2(128);
    v_high_value_str VARCHAR2(32767);
    v_high_date DATE;
    v_cursor INTEGER;
    v_result INTEGER;
    v_length INTEGER;
    v_rowcount NUMBER;
    v_archive_partition VARCHAR2(128);
    
    TYPE t_partitions IS TABLE OF VARCHAR2(128);
    v_partitions_to_archive t_partitions := t_partitions();
    
BEGIN
    DBMS_OUTPUT.PUT_LINE('========================================');
    DBMS_OUTPUT.PUT_LINE('Starting Archive Process: ' || TO_CHAR(SYSDATE, 'YYYY-MM-DD HH24:MI:SS'));
    DBMS_OUTPUT.PUT_LINE('========================================');
    
    -- Calculate cutoff date
    v_cutoff_date := ADD_MONTHS(TRUNC(SYSDATE, 'MM'), -p_months_to_keep);
    DBMS_OUTPUT.PUT_LINE('Cutoff Date: ' || TO_CHAR(v_cutoff_date, 'YYYY-MM-DD'));
    DBMS_OUTPUT.PUT_LINE('Archiving partitions older than: ' || TO_CHAR(v_cutoff_date, 'YYYY-MM-DD'));
    DBMS_OUTPUT.PUT_LINE('');
    
    -- Find partitions to archive
    v_cursor := DBMS_SQL.OPEN_CURSOR;
    
    DBMS_SQL.PARSE(v_cursor,
        'SELECT partition_name, high_value FROM user_tab_partitions ' ||
        'WHERE table_name = :tname ORDER BY partition_position',
        DBMS_SQL.NATIVE);
    
    DBMS_SQL.BIND_VARIABLE(v_cursor, ':tname', UPPER(p_main_table));
    DBMS_SQL.DEFINE_COLUMN(v_cursor, 1, v_partition_name, 128);
    DBMS_SQL.DEFINE_COLUMN_LONG(v_cursor, 2);
    
    v_result := DBMS_SQL.EXECUTE(v_cursor);
    
    -- Collect partitions older than cutoff
    WHILE DBMS_SQL.FETCH_ROWS(v_cursor) > 0 LOOP
        DBMS_SQL.COLUMN_VALUE(v_cursor, 1, v_partition_name);
        DBMS_SQL.COLUMN_VALUE_LONG(v_cursor, 2, 32767, 0, v_high_value_str, v_length);
        
        BEGIN
            EXECUTE IMMEDIATE 'SELECT ' || v_high_value_str || ' FROM DUAL' INTO v_high_date;
            
            IF v_high_date <= v_cutoff_date THEN
                v_partitions_to_archive.EXTEND;
                v_partitions_to_archive(v_partitions_to_archive.COUNT) := v_partition_name;
                DBMS_OUTPUT.PUT_LINE('Found partition to archive: ' || v_partition_name || 
                                   ' (high_value: ' || TO_CHAR(v_high_date, 'YYYY-MM-DD') || ')');
            END IF;
        EXCEPTION
            WHEN OTHERS THEN
                DBMS_OUTPUT.PUT_LINE('Skipping partition ' || v_partition_name || ' (MAXVALUE or error)');
        END;
    END LOOP;
    
    DBMS_SQL.CLOSE_CURSOR(v_cursor);
    
    DBMS_OUTPUT.PUT_LINE('');
    DBMS_OUTPUT.PUT_LINE('Total partitions to archive: ' || v_partitions_to_archive.COUNT);
    DBMS_OUTPUT.PUT_LINE('');
    
    -- Process each partition
    FOR i IN 1..v_partitions_to_archive.COUNT LOOP
        v_partition_name := v_partitions_to_archive(i);
        
        DBMS_OUTPUT.PUT_LINE('----------------------------------------');
        DBMS_OUTPUT.PUT_LINE('Processing partition: ' || v_partition_name);
        DBMS_OUTPUT.PUT_LINE('----------------------------------------');
        
        -- Step 1: Ensure staging table is empty
        EXECUTE IMMEDIATE 'TRUNCATE TABLE ' || p_staging_table;
        DBMS_OUTPUT.PUT_LINE('1. Staging table truncated');
        
        -- Step 2: Exchange partition from main to staging (INSTANT - metadata only)
        EXECUTE IMMEDIATE 'ALTER TABLE ' || p_main_table || 
                         ' EXCHANGE PARTITION ' || v_partition_name ||
                         ' WITH TABLE ' || p_staging_table ||
                         ' INCLUDING INDEXES WITHOUT VALIDATION';
        DBMS_OUTPUT.PUT_LINE('2. Partition exchanged to staging (instant)');
        
        -- Step 3: Get row count before archive
        EXECUTE IMMEDIATE 'SELECT COUNT(*) FROM ' || p_staging_table INTO v_rowcount;
        DBMS_OUTPUT.PUT_LINE('3. Rows to archive: ' || v_rowcount);
        
        -- Step 4: Exchange with archive partition
        -- Note: With interval partitioning, the partition will be auto-created
        -- The partition in archive will have the same name as source
        v_archive_partition := v_partition_name;
        DBMS_OUTPUT.PUT_LINE('4. Exchanging with archive partition: ' || v_archive_partition);
        
        -- Step 5: Exchange staging to archive partition (INSTANT)
        -- The partition will be auto-created in archive table due to interval partitioning
        EXECUTE IMMEDIATE 'ALTER TABLE ' || p_archive_table ||
                         ' EXCHANGE PARTITION ' || v_archive_partition ||
                         ' WITH TABLE ' || p_staging_table ||
                         ' INCLUDING INDEXES WITHOUT VALIDATION';
        DBMS_OUTPUT.PUT_LINE('5. Data exchanged to archive partition (instant)');
        
        -- Verify archive count matches
        EXECUTE IMMEDIATE 'SELECT COUNT(*) FROM ' || p_archive_table || 
                         ' PARTITION (' || v_archive_partition || ')' INTO v_rowcount;
        DBMS_OUTPUT.PUT_LINE('   Verified ' || v_rowcount || ' rows in archive partition');
        
        -- Step 6: Compress the archive partition (THIS TAKES TIME)
        IF p_compress THEN
            DBMS_OUTPUT.PUT_LINE('6. Compressing archive partition...');
            EXECUTE IMMEDIATE 'ALTER TABLE ' || p_archive_table ||
                             ' MOVE PARTITION ' || v_archive_partition ||
                             ' COMPRESS BASIC';
            DBMS_OUTPUT.PUT_LINE('   Compression complete');
        END IF;
        
        -- Step 7: Rebuild indexes on archive partition
        DBMS_OUTPUT.PUT_LINE('7. Rebuilding indexes on archive partition...');
        FOR idx IN (SELECT index_name FROM user_indexes 
                    WHERE table_name = UPPER(p_archive_table)) LOOP
            BEGIN
                EXECUTE IMMEDIATE 'ALTER INDEX ' || idx.index_name ||
                                 ' REBUILD PARTITION ' || v_archive_partition || ' ONLINE';
            EXCEPTION
                WHEN OTHERS THEN
                    DBMS_OUTPUT.PUT_LINE('   Warning: Could not rebuild ' || idx.index_name || ': ' || SQLERRM);
            END;
        END LOOP;
        DBMS_OUTPUT.PUT_LINE('   Index rebuild complete');
        
        -- Step 8: Drop the now-empty partition from main table
        EXECUTE IMMEDIATE 'ALTER TABLE ' || p_main_table ||
                         ' DROP PARTITION ' || v_partition_name;
        DBMS_OUTPUT.PUT_LINE('8. Empty partition dropped from main table');
        
        DBMS_OUTPUT.PUT_LINE('✓ Partition ' || v_partition_name || ' archived successfully (' || v_rowcount || ' rows)');
        DBMS_OUTPUT.PUT_LINE('');
        
        COMMIT;
    END LOOP;
    
    DBMS_OUTPUT.PUT_LINE('========================================');
    DBMS_OUTPUT.PUT_LINE('Archive Process Complete!');
    DBMS_OUTPUT.PUT_LINE('Total partitions archived: ' || v_partitions_to_archive.COUNT);
    DBMS_OUTPUT.PUT_LINE('Completed: ' || TO_CHAR(SYSDATE, 'YYYY-MM-DD HH24:MI:SS'));
    DBMS_OUTPUT.PUT_LINE('========================================');
    
EXCEPTION
    WHEN OTHERS THEN
        IF DBMS_SQL.IS_OPEN(v_cursor) THEN
            DBMS_SQL.CLOSE_CURSOR(v_cursor);
        END IF;
        DBMS_OUTPUT.PUT_LINE('ERROR: ' || SQLERRM);
        DBMS_OUTPUT.PUT_LINE('BACKTRACE: ' || DBMS_UTILITY.FORMAT_ERROR_BACKTRACE);
        RAISE;
END archive_old_partitions;
/

-- ============================================
-- STEP 5: EXECUTE THE ARCHIVAL
-- ============================================
SET SERVEROUTPUT ON SIZE UNLIMITED

-- Execute archival (archive partitions older than 12 months)
BEGIN
    archive_old_partitions(
        p_main_table => 'SALES_MAIN',
        p_archive_table => 'SALES_ARCHIVE',
        p_staging_table => 'SALES_STAGING',
        p_months_to_keep => 12,
        p_compress => TRUE
    );
END;
/

-- ============================================
-- STEP 6: VERIFY RESULTS
-- ============================================
-- Check main table partitions
SELECT table_name, partition_name, high_value, num_rows
FROM user_tab_partitions
WHERE table_name = 'SALES_MAIN'
ORDER BY partition_position;

-- Check archive table partitions
SELECT table_name, partition_name, high_value, num_rows, compression
FROM user_tab_partitions
WHERE table_name = 'SALES_ARCHIVE'
ORDER BY partition_position;

-- ============================================
-- STEP 7: CREATE VIEW FOR UNIFIED ACCESS
-- ============================================
CREATE OR REPLACE VIEW sales_unified AS
SELECT 'ACTIVE' as data_source, s.* FROM sales_main s
UNION ALL
SELECT 'ARCHIVE' as data_source, s.* FROM sales_archive s;

-- Query from both tables seamlessly
SELECT data_source, COUNT(*), MIN(sale_date), MAX(sale_date)
FROM sales_unified
GROUP BY data_source;

-- ============================================
-- EXAMPLE PROCEDURE CALL
-- ============================================
/*
-- Archive partitions older than 12 months
BEGIN
    archive_old_partitions(
        p_main_table => 'SALES_MAIN',
        p_archive_table => 'SALES_ARCHIVE',
        p_staging_table => 'SALES_STAGING',
        p_months_to_keep => 12,
        p_compress => TRUE
    );
END;
/
*/