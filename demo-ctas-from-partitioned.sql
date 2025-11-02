-- ========================================
-- DEMO: CTAS FROM PARTITIONED TABLE
-- ========================================

-- Cleanup existing objects
BEGIN
    EXECUTE IMMEDIATE 'DROP TABLE source_orders PURGE';
EXCEPTION WHEN OTHERS THEN NULL;
END;
/

BEGIN
    EXECUTE IMMEDIATE 'DROP TABLE target_orders PURGE';
EXCEPTION WHEN OTHERS THEN NULL;
END;
/

-- Create source partitioned table
CREATE TABLE source_orders (
    order_id NUMBER,
    order_date DATE,
    customer_id NUMBER,
    amount NUMBER(10,2),
    CONSTRAINT pk_source_orders PRIMARY KEY(order_id, order_date)
)
PARTITION BY RANGE (order_date)
INTERVAL (NUMTODSINTERVAL(1, 'DAY'))
(
    PARTITION p_init VALUES LESS THAN (DATE '2024-01-01')
);

-- Create local indexes on source
CREATE INDEX idx_source_date ON source_orders(order_date) LOCAL;

-- Insert test data
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
-- Method 1: CTAS keeping same partitioning
-- ========================================
-- Create target table with identical partitioning
CREATE TABLE target_orders
PARTITION BY RANGE (order_date)
INTERVAL (NUMTODSINTERVAL(1, 'DAY'))
(
    PARTITION p_init VALUES LESS THAN (DATE '2024-01-01')
)
AS 
SELECT * FROM source_orders;

-- Add constraints after copy
ALTER TABLE target_orders ADD CONSTRAINT pk_target_orders 
PRIMARY KEY(order_id, order_date);

-- Add matching local indexes
CREATE INDEX idx_target_date ON target_orders(order_date) LOCAL;

-- ========================================
-- Method 2: CTAS with Subset of Data
-- ========================================
-- Create table with specific partition range
CREATE TABLE target_orders_subset
PARTITION BY RANGE (order_date)
INTERVAL (NUMTODSINTERVAL(1, 'DAY'))
(
    PARTITION p_init VALUES LESS THAN (DATE '2024-01-01')
)
AS 
SELECT * FROM source_orders 
WHERE order_date BETWEEN DATE '2024-01-01' AND DATE '2024-01-31';

-- ========================================
-- Method 3: CTAS with Partition Selection
-- ========================================
-- Create table from specific partition
CREATE TABLE target_orders_oneday
AS 
SELECT * FROM source_orders PARTITION
FOR (DATE '2024-01-15');

-- ========================================
-- Verify Results
-- ========================================
-- Check source partitions
SELECT 'SOURCE' as table_type,
       partition_name,
       high_value,
       num_rows
FROM user_tab_partitions
WHERE table_name = 'SOURCE_ORDERS'
ORDER BY partition_position;

-- Check target partitions
SELECT 'TARGET' as table_type,
       partition_name,
       high_value,
       num_rows
FROM user_tab_partitions
WHERE table_name = 'TARGET_ORDERS'
ORDER BY partition_position;

-- Compare row counts by date
SELECT 'SOURCE' as table_type,
       TO_CHAR(order_date, 'YYYY-MM-DD') as order_date,
       COUNT(*) as row_count
FROM source_orders
GROUP BY order_date
UNION ALL
SELECT 'TARGET',
       TO_CHAR(order_date, 'YYYY-MM-DD'),
       COUNT(*)
FROM target_orders
GROUP BY order_date
ORDER BY order_date, table_type;

-- Check subset table
SELECT MIN(order_date) as min_date,
       MAX(order_date) as max_date,
       COUNT(*) as row_count
FROM target_orders_subset;

-- Check single partition copy
SELECT MIN(order_date) as min_date,
       MAX(order_date) as max_date,
       COUNT(*) as row_count
FROM target_orders_oneday;

-- ========================================
-- NOTES
-- ========================================
/*
Key Points about CTAS from Partitioned Tables:

1. Can maintain same partitioning scheme in target
2. System automatically distributes rows to correct partitions
3. Can copy specific partitions using PARTITION FOR clause
4. Constraints must be added after CTAS
5. Local indexes must be recreated
6. Can filter data during copy while maintaining partitioning
7. Partition names in target are system-generated
8. Original partition names are not preserved
9. Interval partitioning continues to work in target
10. Storage attributes can be specified in target if needed
*/

-- ========================================
-- ROLLBACK SCRIPTS
-- ========================================
/*
-- Step 1: Drop all tables created by this script
DROP TABLE source_orders PURGE;
DROP TABLE target_orders PURGE;
DROP TABLE target_orders_subset PURGE;
DROP TABLE target_orders_oneday PURGE;

-- Step 2: Verify all objects are dropped
SELECT object_name, object_type 
FROM user_objects 
WHERE object_name IN (
    'SOURCE_ORDERS',
    'TARGET_ORDERS',
    'TARGET_ORDERS_SUBSET',
    'TARGET_ORDERS_ONEDAY'
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

-- Remove specific partition from source table
-- Replace partition_name with actual partition name
ALTER TABLE source_orders DROP PARTITION partition_name;

-- Rebuild indexes if needed
ALTER INDEX idx_source_date REBUILD;
ALTER INDEX idx_target_date REBUILD;

-- Regather statistics after major changes
BEGIN
    DBMS_STATS.GATHER_TABLE_STATS(
        ownname => USER,
        tabname => 'SOURCE_ORDERS',
        estimate_percent => DBMS_STATS.AUTO_SAMPLE_SIZE,
        cascade => TRUE
    );
    
    DBMS_STATS.GATHER_TABLE_STATS(
        ownname => USER,
        tabname => 'TARGET_ORDERS',
        estimate_percent => DBMS_STATS.AUTO_SAMPLE_SIZE,
        cascade => TRUE
    );
END;
/
*/