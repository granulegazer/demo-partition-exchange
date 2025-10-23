-- ========================================
-- PART 1: CREATE PARTITIONED TABLE
-- ========================================

-- Clean up if exists
BEGIN
    EXECUTE IMMEDIATE 'DROP TABLE sales PURGE';
EXCEPTION
    WHEN OTHERS THEN NULL;
END;
/

BEGIN
    EXECUTE IMMEDIATE 'DROP TABLE sales_archive PURGE';
EXCEPTION
    WHEN OTHERS THEN NULL;
END;
/

-- Create interval partitioned table (daily partitions created automatically)
CREATE TABLE sales (
    sale_id NUMBER,
    sale_date DATE NOT NULL,
    customer_id NUMBER,
    product_id NUMBER,
    amount NUMBER(10,2),
    quantity NUMBER,
    region VARCHAR2(50),
    status VARCHAR2(20),
    CONSTRAINT pk_sales PRIMARY KEY (sale_id, sale_date)
)
PARTITION BY RANGE (sale_date)
INTERVAL (NUMTODSINTERVAL(1, 'DAY'))  -- Auto-create partition per day
(
    PARTITION p_initial VALUES LESS THAN (TO_DATE('2024-01-01', 'YYYY-MM-DD'))
);

-- Create local indexes
CREATE INDEX idx_sales_date ON sales(sale_date) LOCAL;
CREATE INDEX idx_sales_customer ON sales(customer_id) LOCAL;
CREATE INDEX idx_sales_region ON sales(region) LOCAL;

-- Create archive table with compression
CREATE TABLE sales_archive 
COMPRESS FOR OLTP
AS SELECT 
    s.*,
    SYSDATE AS archive_date,
    USER AS archived_by
FROM sales s
WHERE 1=0;

-- Create indexes on archive
CREATE INDEX idx_archive_date ON sales_archive(sale_date);
CREATE INDEX idx_archive_customer ON sales_archive(customer_id);
CREATE INDEX idx_archive_archdate ON sales_archive(archive_date);

-- Verify table creation
SELECT 'Sales table created with interval partitioning' AS status FROM dual;
SELECT 'Archive table created with compression' AS status FROM dual;

-- Check initial partition count
SELECT 
    table_name,
    COUNT(*) AS partition_count
FROM user_tab_partitions
WHERE table_name = 'SALES'
GROUP BY table_name;


-- ========================================
-- PART 2: DATA SETUP (150+ Days, 5+ Records/Day)
-- ========================================

-- Generate data for 180 days with 10-15 records per day
DECLARE
    v_sale_id NUMBER := 1;
    v_records_per_day NUMBER;
    v_start_date DATE := DATE '2024-01-01';
    v_days NUMBER := 180;
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
            INSERT INTO sales VALUES (
                v_sale_id,
                v_start_date + day_offset,
                TRUNC(DBMS_RANDOM.VALUE(1000, 5000)),  -- customer_id
                TRUNC(DBMS_RANDOM.VALUE(100, 999)),    -- product_id
                ROUND(DBMS_RANDOM.VALUE(50, 5000), 2), -- amount
                TRUNC(DBMS_RANDOM.VALUE(1, 50)),       -- quantity
                v_regions(TRUNC(DBMS_RANDOM.VALUE(1, 6))), -- region
                v_statuses(TRUNC(DBMS_RANDOM.VALUE(1, 5))) -- status
            );
            
            v_sale_id := v_sale_id + 1;
        END LOOP;
        
        -- Commit every 10 days
        IF MOD(day_offset, 10) = 0 THEN
            COMMIT;
            DBMS_OUTPUT.PUT_LINE('Committed data for day ' || day_offset);
        END IF;
    END LOOP;
    
    COMMIT;
    DBMS_OUTPUT.PUT_LINE('Data generation completed!');
    DBMS_OUTPUT.PUT_LINE('Total records inserted: ' || (v_sale_id - 1));
END;
/

-- Gather statistics
EXEC DBMS_STATS.GATHER_TABLE_STATS(USER, 'SALES');

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
    FROM sales
    GROUP BY sale_date
)
GROUP BY TO_CHAR(sale_date, 'YYYY-MM')
ORDER BY month;

-- Check partition count (should be 180+)
SELECT 
    'Total partitions created: ' || COUNT(*) AS partition_info
FROM user_tab_partitions
WHERE table_name = 'SALES'
  AND partition_name != 'P_INITIAL';

-- Show sample of daily data distribution
SELECT 
    TO_CHAR(sale_date, 'YYYY-MM-DD') AS sale_day,
    COUNT(*) AS record_count,
    ROUND(SUM(amount), 2) AS total_amount
FROM sales
WHERE sale_date BETWEEN DATE '2024-01-01' AND DATE '2024-01-10'
GROUP BY sale_date
ORDER BY sale_date;

-- Show partition details (first 10)
SELECT 
    partition_name,
    partition_position,
    TO_DATE(
        TRIM(BOTH '''' FROM REGEXP_SUBSTR(high_value, '''[^'']+''')),
        'YYYY-MM-DD'
    ) AS partition_date,
    num_rows
FROM user_tab_partitions
WHERE table_name = 'SALES'
  AND partition_name != 'P_INITIAL'
  AND ROWNUM <= 10
ORDER BY partition_position;


-- ========================================
-- PART 3: PARTITION EXCHANGE DEMO
-- ========================================

-- ========================================
-- 3.1: CREATE ARCHIVE PROCEDURE (Multiple Dates)
-- ========================================

CREATE OR REPLACE PROCEDURE archive_partitions_by_dates (
    p_dates IN SYS.ODCIDATELIST  -- Array of dates to archive
) AS
    v_partition_name VARCHAR2(128);
    v_partition_date DATE;
    v_sql VARCHAR2(4000);
    v_count NUMBER;
    v_total_archived NUMBER := 0;
    v_partitions_archived NUMBER := 0;
    
    -- Find partitions matching the supplied dates
    CURSOR c_partitions IS
        SELECT DISTINCT
            p.partition_name,
            TO_DATE(
                TRIM(BOTH '''' FROM REGEXP_SUBSTR(p.high_value, '''[^'']+''')),
                'YYYY-MM-DD'
            ) AS partition_date
        FROM user_tab_partitions p,
             TABLE(p_dates) d
        WHERE p.table_name = 'SALES'
          AND p.partition_name != 'P_INITIAL'
          AND TO_DATE(
                TRIM(BOTH '''' FROM REGEXP_SUBSTR(p.high_value, '''[^'']+''')),
                'YYYY-MM-DD'
              ) = d.COLUMN_VALUE + 1  -- Partition high_value is next day
        ORDER BY partition_date;
BEGIN
    DBMS_OUTPUT.PUT_LINE('===========================================');
    DBMS_OUTPUT.PUT_LINE('Starting partition archiving for ' || p_dates.COUNT || ' dates');
    DBMS_OUTPUT.PUT_LINE('===========================================');
    
    -- Show dates to be archived
    FOR i IN 1..p_dates.COUNT LOOP
        DBMS_OUTPUT.PUT_LINE('Date ' || i || ': ' || TO_CHAR(p_dates(i), 'YYYY-MM-DD'));
    END LOOP;
    DBMS_OUTPUT.PUT_LINE('-------------------------------------------');
    
    -- Process each matching partition
    FOR rec IN c_partitions LOOP
        BEGIN
            DBMS_OUTPUT.PUT_LINE('Processing partition: ' || rec.partition_name);
            DBMS_OUTPUT.PUT_LINE('Partition date: ' || TO_CHAR(rec.partition_date - 1, 'YYYY-MM-DD'));
            
            -- Count records in partition
            v_sql := 'SELECT COUNT(*) FROM sales PARTITION (' || rec.partition_name || ')';
            EXECUTE IMMEDIATE v_sql INTO v_count;
            DBMS_OUTPUT.PUT_LINE('Records found: ' || v_count);
            
            IF v_count > 0 THEN
                -- Create temporary staging table using CTAS
                EXECUTE IMMEDIATE 'CREATE TABLE sales_staging_temp AS SELECT * FROM sales WHERE 1=0';
                
                -- Create matching indexes for exchange
                EXECUTE IMMEDIATE 'CREATE INDEX idx_staging_date ON sales_staging_temp(sale_date)';
                EXECUTE IMMEDIATE 'CREATE INDEX idx_staging_customer ON sales_staging_temp(customer_id)';
                EXECUTE IMMEDIATE 'CREATE INDEX idx_staging_region ON sales_staging_temp(region)';
                
                -- EXCHANGE: Partition data moves to staging (instant)
                v_sql := 'ALTER TABLE sales EXCHANGE PARTITION ' || rec.partition_name || 
                         ' WITH TABLE sales_staging_temp INCLUDING INDEXES WITHOUT VALIDATION';
                EXECUTE IMMEDIATE v_sql;
                
                -- Move data from staging to archive
                INSERT INTO sales_archive (
                    sale_id, sale_date, customer_id, product_id, 
                    amount, quantity, region, status
                )
                SELECT 
                    sale_id, sale_date, customer_id, product_id,
                    amount, quantity, region, status
                FROM sales_staging_temp;
                
                v_total_archived := v_total_archived + SQL%ROWCOUNT;
                COMMIT;
                
                DBMS_OUTPUT.PUT_LINE('Archived ' || v_count || ' records');
                
                -- Drop staging table
                EXECUTE IMMEDIATE 'DROP TABLE sales_staging_temp';
            END IF;
            
            -- Drop the now-empty partition
            v_sql := 'ALTER TABLE sales DROP PARTITION ' || rec.partition_name;
            EXECUTE IMMEDIATE v_sql;
            DBMS_OUTPUT.PUT_LINE('Dropped partition: ' || rec.partition_name);
            DBMS_OUTPUT.PUT_LINE('-------------------------------------------');
            
            v_partitions_archived := v_partitions_archived + 1;
            
        EXCEPTION
            WHEN OTHERS THEN
                DBMS_OUTPUT.PUT_LINE('ERROR processing partition ' || rec.partition_name || ': ' || SQLERRM);
                -- Clean up if staging exists
                BEGIN
                    EXECUTE IMMEDIATE 'DROP TABLE sales_staging_temp';
                EXCEPTION WHEN OTHERS THEN NULL;
                END;
                ROLLBACK;
        END;
    END LOOP;
    
    DBMS_OUTPUT.PUT_LINE('===========================================');
    DBMS_OUTPUT.PUT_LINE('Archiving completed successfully!');
    DBMS_OUTPUT.PUT_LINE('Partitions archived: ' || v_partitions_archived);
    DBMS_OUTPUT.PUT_LINE('Total records archived: ' || v_total_archived);
    DBMS_OUTPUT.PUT_LINE('===========================================');
    
EXCEPTION
    WHEN OTHERS THEN
        DBMS_OUTPUT.PUT_LINE('FATAL ERROR: ' || SQLERRM);
        RAISE;
END;
/


-- ========================================
-- 3.2: TEST SCENARIOS
-- ========================================

SET SERVEROUTPUT ON SIZE UNLIMITED

-- Scenario 1: Archive specific dates (3 dates)
DECLARE
    v_dates SYS.ODCIDATELIST := SYS.ODCIDATELIST(
        DATE '2024-01-15',
        DATE '2024-01-20',
        DATE '2024-01-25'
    );
BEGIN
    archive_partitions_by_dates(v_dates);
END;
/

-- Check results
SELECT 'Active partitions remaining: ' || COUNT(*) AS status
FROM user_tab_partitions
WHERE table_name = 'SALES'
  AND partition_name != 'P_INITIAL';

SELECT 'Archived records: ' || COUNT(*) AS status 
FROM sales_archive;

SELECT 
    TO_CHAR(sale_date, 'YYYY-MM-DD') AS archived_date,
    COUNT(*) AS record_count,
    ROUND(SUM(amount), 2) AS total_amount
FROM sales_archive
GROUP BY sale_date
ORDER BY sale_date;


-- Scenario 2: Archive an entire week
DECLARE
    v_dates SYS.ODCIDATELIST := SYS.ODCIDATELIST();
    v_start_date DATE := DATE '2024-02-01';
BEGIN
    -- Build list of 7 consecutive dates
    FOR i IN 0..6 LOOP
        v_dates.EXTEND;
        v_dates(v_dates.COUNT) := v_start_date + i;
    END LOOP;
    
    archive_partitions_by_dates(v_dates);
END;
/


-- Scenario 3: Archive first day of each month (Jan-Jun)
DECLARE
    v_dates SYS.ODCIDATELIST := SYS.ODCIDATELIST(
        DATE '2024-01-01',
        DATE '2024-02-01',
        DATE '2024-03-01',
        DATE '2024-04-01',
        DATE '2024-05-01',
        DATE '2024-06-01'
    );
BEGIN
    archive_partitions_by_dates(v_dates);
END;
/


-- Scenario 4: Archive random dates across multiple months
DECLARE
    v_dates SYS.ODCIDATELIST := SYS.ODCIDATELIST(
        DATE '2024-01-10',
        DATE '2024-02-14',
        DATE '2024-03-17',
        DATE '2024-04-22',
        DATE '2024-05-30'
    );
BEGIN
    archive_partitions_by_dates(v_dates);
END;
/


-- ========================================
-- 3.3: MONITORING & VERIFICATION QUERIES
-- ========================================

-- Check current partition distribution
SELECT 
    TO_CHAR(
        TO_DATE(
            TRIM(BOTH '''' FROM REGEXP_SUBSTR(high_value, '''[^'']+''')),
            'YYYY-MM-DD'
        ) - 1,
        'YYYY-MM'
    ) AS month,
    COUNT(*) AS active_partitions
FROM user_tab_partitions
WHERE table_name = 'SALES'
  AND partition_name != 'P_INITIAL'
GROUP BY TO_CHAR(
    TO_DATE(
        TRIM(BOTH '''' FROM REGEXP_SUBSTR(high_value, '''[^'']+''')),
        'YYYY-MM-DD'
    ) - 1,
    'YYYY-MM'
)
ORDER BY month;

-- Compare active vs archived data
SELECT 'ACTIVE' AS data_location, 
       COUNT(*) AS total_records,
       COUNT(DISTINCT sale_date) AS unique_dates,
       MIN(sale_date) AS oldest_date,
       MAX(sale_date) AS newest_date,
       ROUND(SUM(amount), 2) AS total_amount
FROM sales
UNION ALL
SELECT 'ARCHIVED' AS data_location,
       COUNT(*) AS total_records,
       COUNT(DISTINCT sale_date) AS unique_dates,
       MIN(sale_date) AS oldest_date,
       MAX(sale_date) AS newest_date,
       ROUND(SUM(amount), 2) AS total_amount
FROM sales_archive;

-- Monthly summary of active vs archived
SELECT 
    TO_CHAR(sale_date, 'YYYY-MM') AS month,
    COUNT(*) AS active_records
FROM sales
GROUP BY TO_CHAR(sale_date, 'YYYY-MM')
UNION ALL
SELECT 
    TO_CHAR(sale_date, 'YYYY-MM') AS month,
    -COUNT(*) AS archived_records  -- Negative to distinguish
FROM sales_archive
GROUP BY TO_CHAR(sale_date, 'YYYY-MM')
ORDER BY month, active_records DESC;

-- View archived data by archive date
SELECT 
    TO_CHAR(archive_date, 'YYYY-MM-DD HH24:MI:SS') AS when_archived,
    archived_by,
    COUNT(*) AS records_archived,
    COUNT(DISTINCT sale_date) AS dates_archived
FROM sales_archive
GROUP BY TO_CHAR(archive_date, 'YYYY-MM-DD HH24:MI:SS'), archived_by
ORDER BY when_archived DESC;

-- Check specific dates status (active or archived)
WITH date_list AS (
    SELECT DATE '2024-01-15' AS check_date FROM dual UNION ALL
    SELECT DATE '2024-02-01' FROM dual UNION ALL
    SELECT DATE '2024-03-10' FROM dual
)
SELECT 
    dl.check_date,
    CASE 
        WHEN a.cnt > 0 THEN 'ARCHIVED (' || a.cnt || ' records)'
        WHEN s.cnt > 0 THEN 'ACTIVE (' || s.cnt || ' records)'
        ELSE 'NO DATA'
    END AS status
FROM date_list dl
LEFT JOIN (SELECT sale_date, COUNT(*) cnt FROM sales GROUP BY sale_date) s 
    ON dl.check_date = s.sale_date
LEFT JOIN (SELECT sale_date, COUNT(*) cnt FROM sales_archive GROUP BY sale_date) a
    ON dl.check_date = a.sale_date
ORDER BY dl.check_date;


-- ========================================
-- 3.4: UNIFIED VIEW FOR SEAMLESS QUERYING
-- ========================================

-- Create view combining active and archived data
CREATE OR REPLACE VIEW sales_complete AS
SELECT 
    sale_id, sale_date, customer_id, product_id,
    amount, quantity, region, status,
    'ACTIVE' AS data_source,
    CAST(NULL AS DATE) AS archive_date,
    CAST(NULL AS VARCHAR2(50)) AS archived_by
FROM sales
UNION ALL
SELECT 
    sale_id, sale_date, customer_id, product_id,
    amount, quantity, region, status,
    'ARCHIVED' AS data_source,
    archive_date,
    archived_by
FROM sales_archive;

-- Query seamlessly across active and archived
SELECT 
    customer_id,
    COUNT(*) AS total_orders,
    SUM(amount) AS total_spent,
    MIN(sale_date) AS first_order,
    MAX(sale_date) AS last_order
FROM sales_complete
WHERE customer_id = 1234
GROUP BY customer_id;

-- Find all orders for a date range (regardless of active/archived)
SELECT 
    sale_date,
    data_source,
    COUNT(*) AS order_count,
    ROUND(SUM(amount), 2) AS daily_total
FROM sales_complete
WHERE sale_date BETWEEN DATE '2024-01-01' AND DATE '2024-03-31'
GROUP BY sale_date, data_source
ORDER BY sale_date, data_source;


-- ========================================
-- 3.5: HELPER PROCEDURE - LIST DATES IN RANGE
-- ========================================

-- Helper to generate date list for a range
CREATE OR REPLACE FUNCTION get_date_list(
    p_start_date DATE,
    p_end_date DATE
) RETURN SYS.ODCIDATELIST IS
    v_dates SYS.ODCIDATELIST := SYS.ODCIDATELIST();
    v_current_date DATE := p_start_date;
BEGIN
    WHILE v_current_date <= p_end_date LOOP
        v_dates.EXTEND;
        v_dates(v_dates.COUNT) := v_current_date;
        v_current_date := v_current_date + 1;
    END LOOP;
    RETURN v_dates;
END;
/

-- Example: Archive entire January 2024 using helper
DECLARE
    v_dates SYS.ODCIDATELIST;
BEGIN
    v_dates := get_date_list(DATE '2024-01-01', DATE '2024-01-31');
    archive_partitions_by_dates(v_dates);
END;
/


-- ========================================
-- CLEANUP (Optional)
-- ========================================
/*
DROP TABLE sales PURGE;
DROP TABLE sales_archive PURGE;
DROP PROCEDURE archive_partitions_by_dates;
DROP FUNCTION get_date_list;
DROP VIEW sales_complete;
*/


-- ========================================
-- USAGE SUMMARY
-- ========================================
/*
PART 1: CREATE TABLES
- Run once to set up interval partitioned table
- Archive table with compression
- All indexes created

PART 2: DATA SETUP
- Generates 180 days of data
- 10-15 records per day (total ~2000+ records)
- Creates 180+ partitions automatically

PART 3: ARCHIVE BY DATES
- Supply array of specific dates to archive
- All partitions for those dates are archived and dropped
- Examples:
  1. Archive specific dates: 3 dates
  2. Archive a week: 7 consecutive dates
  3. Archive monthly: First day of each month
  4. Archive random dates: Any dates you want
  
CALL EXAMPLES:
-- Archive 3 specific dates
EXEC archive_partitions_by_dates(SYS.ODCIDATELIST(DATE '2024-01-15', DATE '2024-01-20', DATE '2024-01-25'));

-- Archive date range using helper
EXEC archive_partitions_by_dates(get_date_list(DATE '2024-02-01', DATE '2024-02-07'));

BENEFITS:
- Full control over which dates to archive
- Pass multiple dates in one call
- Instant archiving via partition exchange
- Zero downtime
- Compressed archive saves space
- Unified view for seamless querying
*/