-- ========================================
-- DEMO: CTAS WITH INTERVAL PARTITIONING
-- ========================================

-- Cleanup existing objects
BEGIN
    EXECUTE IMMEDIATE 'DROP TABLE source_orders PURGE';
EXCEPTION WHEN OTHERS THEN NULL;
END;
/

BEGIN
    EXECUTE IMMEDIATE 'DROP TABLE part_orders_daily PURGE';
EXCEPTION WHEN OTHERS THEN NULL;
END;
/

BEGIN
    EXECUTE IMMEDIATE 'DROP TABLE part_orders_monthly PURGE';
EXCEPTION WHEN OTHERS THEN NULL;
END;
/

-- Create a source table with some data
CREATE TABLE source_orders (
    order_id NUMBER,
    order_date DATE,
    customer_id NUMBER,
    amount NUMBER(10,2)
);

-- Insert test data spanning multiple months
INSERT INTO source_orders
SELECT 
    LEVEL as order_id,
    DATE '2024-01-01' + TRUNC(DBMS_RANDOM.VALUE(0, 90)) as order_date,
    TRUNC(DBMS_RANDOM.VALUE(1000, 9999)) as customer_id,
    ROUND(DBMS_RANDOM.VALUE(100, 1000), 2) as amount
FROM dual
CONNECT BY LEVEL <= 1000;

COMMIT;

-- ========================================
-- Method 1: CTAS with Daily Interval Partitioning
-- ========================================
CREATE TABLE part_orders_daily (
    CONSTRAINT pk_orders_daily PRIMARY KEY(order_id, order_date)
)
PARTITION BY RANGE (order_date)
INTERVAL (NUMTODSINTERVAL(1, 'DAY'))
(
    PARTITION p_init_daily VALUES LESS THAN (DATE '2024-01-01')
)
AS 
SELECT * FROM source_orders;

-- Create local indexes
CREATE INDEX idx_daily_date ON part_orders_daily(order_date) LOCAL;

-- ========================================
-- Method 2: CTAS with Monthly Interval Partitioning
-- ========================================
CREATE TABLE part_orders_monthly (
    CONSTRAINT pk_orders_monthly PRIMARY KEY(order_id, order_date)
)
PARTITION BY RANGE (order_date)
INTERVAL (NUMTODSINTERVAL(1, 'MONTH'))
(
    PARTITION p_init_monthly VALUES LESS THAN (DATE '2024-01-01')
)
AS 
SELECT * FROM source_orders;

-- Create local indexes
CREATE INDEX idx_monthly_date ON part_orders_monthly(order_date) LOCAL;

-- ========================================
-- Verify Daily Partitions
-- ========================================
SELECT 'DAILY PARTITIONS' as partition_type, 
       partition_name,
       high_value,
       num_rows
FROM user_tab_partitions
WHERE table_name = 'PART_ORDERS_DAILY'
ORDER BY partition_position;

-- ========================================
-- Verify Monthly Partitions
-- ========================================
SELECT 'MONTHLY PARTITIONS' as partition_type,
       partition_name,
       high_value,
       num_rows
FROM user_tab_partitions
WHERE table_name = 'PART_ORDERS_MONTHLY'
ORDER BY partition_position;

-- ========================================
-- Compare Data Distribution
-- ========================================
-- Daily distribution
SELECT 'DAILY' as table_type,
       TO_CHAR(order_date, 'YYYY-MM-DD') as partition_date,
       COUNT(*) as row_count
FROM part_orders_daily
GROUP BY order_date
ORDER BY order_date;

-- Monthly distribution
SELECT 'MONTHLY' as table_type,
       TO_CHAR(TRUNC(order_date, 'MM'), 'YYYY-MM') as partition_month,
       COUNT(*) as row_count
FROM part_orders_monthly
GROUP BY TRUNC(order_date, 'MM')
ORDER BY TRUNC(order_date, 'MM');

-- ========================================
-- Test Adding New Data
-- ========================================
-- Add future data to see automatic partition creation
INSERT INTO part_orders_daily
SELECT 
    order_id + 2000,
    DATE '2024-05-01' + LEVEL - 1,
    customer_id,
    amount
FROM source_orders
WHERE ROWNUM <= 10;

INSERT INTO part_orders_monthly
SELECT 
    order_id + 3000,
    DATE '2024-06-15' + (LEVEL * 30),
    customer_id,
    amount
FROM source_orders
WHERE ROWNUM <= 10;

COMMIT;

-- Verify new partitions were created automatically
SELECT table_name, partition_name, high_value, num_rows
FROM user_tab_partitions
WHERE table_name IN ('PART_ORDERS_DAILY', 'PART_ORDERS_MONTHLY')
  AND partition_name NOT LIKE '%INIT%'
ORDER BY table_name, partition_position;

-- ========================================
-- NOTES
-- ========================================
/*
Key Points about CTAS with Interval Partitioning:

1. You can combine CTAS with interval partitioning
2. The partitioning scheme must be defined in the CREATE TABLE statement
3. Initial partition is required but can be empty
4. Data from source table is automatically distributed to correct partitions
5. New partitions are created automatically as needed
6. Works with both daily and monthly intervals
7. Can include constraints in the CREATE TABLE statement
8. Local indexes can be added after table creation
9. Interval must be specified using NUMTODSINTERVAL
10. System generates partition names automatically
*/

-- ========================================
-- ROLLBACK SCRIPTS
-- ========================================
/*
-- Step 1: Drop all tables created by this script
DROP TABLE source_orders PURGE;
DROP TABLE part_orders_daily PURGE;
DROP TABLE part_orders_monthly PURGE;

-- Step 2: Verify all objects are dropped
SELECT object_name, object_type 
FROM user_objects 
WHERE object_name IN (
    'SOURCE_ORDERS',
    'PART_ORDERS_DAILY',
    'PART_ORDERS_MONTHLY'
);

-- Note: The PURGE option is used to completely remove the tables
-- without placing them in the recycle bin, ensuring a clean rollback.
-- Remove PURGE if you want the option to flashback the tables.
*/

-- ========================================
-- SELECTIVE CLEANUP SCRIPTS
-- ========================================
/*
-- Reset source table data only
TRUNCATE TABLE source_orders;

-- Remove specific partition from daily partitioned table
-- Replace partition_name with actual partition name
ALTER TABLE part_orders_daily DROP PARTITION partition_name;

-- Remove specific partition from monthly partitioned table
-- Replace partition_name with actual partition name
ALTER TABLE part_orders_monthly DROP PARTITION partition_name;

-- Rebuild indexes if needed
ALTER INDEX idx_daily_date REBUILD;
ALTER INDEX idx_monthly_date REBUILD;

-- Regather statistics after major changes
BEGIN
    DBMS_STATS.GATHER_TABLE_STATS(
        ownname => USER,
        tabname => 'PART_ORDERS_DAILY',
        estimate_percent => DBMS_STATS.AUTO_SAMPLE_SIZE,
        cascade => TRUE
    );
    
    DBMS_STATS.GATHER_TABLE_STATS(
        ownname => USER,
        tabname => 'PART_ORDERS_MONTHLY',
        estimate_percent => DBMS_STATS.AUTO_SAMPLE_SIZE,
        cascade => TRUE
    );
END;
/
*/