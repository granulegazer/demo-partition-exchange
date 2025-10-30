-- ========================================
-- DEMO: CREATE TABLE AS SELECT VARIANTS
-- ========================================

-- Cleanup existing objects
BEGIN
    EXECUTE IMMEDIATE 'DROP TABLE source_table PURGE';
EXCEPTION WHEN OTHERS THEN NULL;
END;
/

BEGIN
    EXECUTE IMMEDIATE 'DROP TABLE ctas_basic PURGE';
EXCEPTION WHEN OTHERS THEN NULL;
END;
/

BEGIN
    EXECUTE IMMEDIATE 'DROP TABLE ctas_empty PURGE';
EXCEPTION WHEN OTHERS THEN NULL;
END;
/

BEGIN
    EXECUTE IMMEDIATE 'DROP TABLE ctas_full PURGE';
EXCEPTION WHEN OTHERS THEN NULL;
END;
/

-- Create a source table for our examples
CREATE TABLE source_table (
    id NUMBER PRIMARY KEY,
    name VARCHAR2(100) NOT NULL,
    created_date DATE DEFAULT SYSDATE,
    status VARCHAR2(10) CHECK (status IN ('ACTIVE', 'INACTIVE')),
    amount NUMBER(10,2)
);

-- Create some indexes
CREATE INDEX idx_source_date ON source_table(created_date);
CREATE INDEX idx_source_status ON source_table(status);

-- Insert some sample data
INSERT INTO source_table (id, name, status, amount)
SELECT 
    LEVEL,
    'Name ' || LEVEL,
    CASE WHEN MOD(LEVEL, 2) = 0 THEN 'ACTIVE' ELSE 'INACTIVE' END,
    ROUND(DBMS_RANDOM.VALUE(100, 1000), 2)
FROM dual
CONNECT BY LEVEL <= 10;

COMMIT;

-- ========================================
-- VARIANT 1: Basic CTAS
-- ========================================
-- Simply copies structure and data
CREATE TABLE ctas_basic
AS SELECT * FROM source_table;

-- Note: This copies:
-- - Table structure
-- - Data
-- But does NOT copy:
-- - Constraints
-- - Indexes
-- - Default values
-- - Triggers

-- ========================================
-- VARIANT 2: CTAS with NO DATA
-- ========================================
-- Creates empty table with same structure
CREATE TABLE ctas_empty
AS SELECT * FROM source_table
WHERE 1=0;

-- Note: Useful for:
-- - Creating staging tables
-- - Testing environments
-- - When you need identical structure but no data

-- ========================================
-- VARIANT 3: CTAS with INCLUDING CLAUSES
-- ========================================
-- Creates table with constraints and indexes
CREATE TABLE ctas_full
INCLUDING PRIMARY KEYS
INCLUDING UNIQUE INDEXES
INCLUDING INDEXES
INCLUDING CONSTRAINTS
AS SELECT * FROM source_table;

-- Note: This copies:
-- - Table structure
-- - Data
-- - Primary keys
-- - Unique indexes
-- - Non-unique indexes
-- - Check constraints
-- But still does NOT copy:
-- - Default values
-- - Triggers
-- - Foreign key constraints

-- ========================================
-- VERIFY THE DIFFERENCES
-- ========================================

-- Check table structures
SELECT table_name, column_name, data_type, nullable
FROM user_tab_columns
WHERE table_name IN ('SOURCE_TABLE', 'CTAS_BASIC', 'CTAS_EMPTY', 'CTAS_FULL')
ORDER BY table_name, column_id;

-- Check constraints
SELECT table_name, constraint_name, constraint_type, search_condition
FROM user_constraints
WHERE table_name IN ('SOURCE_TABLE', 'CTAS_BASIC', 'CTAS_EMPTY', 'CTAS_FULL')
ORDER BY table_name, constraint_name;

-- Check indexes
SELECT table_name, index_name, uniqueness
FROM user_indexes
WHERE table_name IN ('SOURCE_TABLE', 'CTAS_BASIC', 'CTAS_EMPTY', 'CTAS_FULL')
ORDER BY table_name, index_name;

-- Check row counts
SELECT 'SOURCE_TABLE' as table_name, COUNT(*) as row_count FROM source_table
UNION ALL
SELECT 'CTAS_BASIC', COUNT(*) FROM ctas_basic
UNION ALL
SELECT 'CTAS_EMPTY', COUNT(*) FROM ctas_empty
UNION ALL
SELECT 'CTAS_FULL', COUNT(*) FROM ctas_full
ORDER BY table_name;

-- ========================================
-- COMMON USE CASES
-- ========================================

-- 1. Create backup/archive table with data
CREATE TABLE orders_backup
AS SELECT * FROM orders;

-- 2. Create empty staging table for ETL
CREATE TABLE staging_orders
AS SELECT * FROM orders WHERE 1=0;

-- 3. Create test table with subset of data
CREATE TABLE test_orders
AS SELECT * FROM orders
WHERE created_date >= TRUNC(SYSDATE) - 30;

-- 4. Create table with different structure
CREATE TABLE order_summary
AS
SELECT 
    TRUNC(created_date) AS order_date,
    COUNT(*) AS total_orders,
    SUM(amount) AS total_amount
FROM orders
GROUP BY TRUNC(created_date);

-- 5. Create table with all constraints for data integrity
CREATE TABLE orders_with_rules
INCLUDING ALL CONSTRAINTS
INCLUDING ALL INDEXES
AS SELECT * FROM orders;

-- ========================================
-- VARIANT 4: CTAS WITH EXCHANGE
-- ========================================

-- Cleanup exchange demo objects
BEGIN
    EXECUTE IMMEDIATE 'DROP TABLE part_orders PURGE';
EXCEPTION WHEN OTHERS THEN NULL;
END;
/

-- Create a partitioned table
CREATE TABLE part_orders (
    order_id NUMBER,
    order_date DATE,
    amount NUMBER(10,2),
    CONSTRAINT pk_part_orders PRIMARY KEY(order_id, order_date)
)
PARTITION BY RANGE (order_date)
INTERVAL (NUMTODSINTERVAL(1, 'DAY'))
(
    PARTITION p_init VALUES LESS THAN (DATE '2024-01-01')
);

-- Create local index
CREATE INDEX idx_part_orders_date ON part_orders(order_date) LOCAL;

-- Insert some test data
INSERT INTO part_orders
SELECT 
    LEVEL as order_id,
    DATE '2024-01-01' + TRUNC(DBMS_RANDOM.VALUE(0, 30)) as order_date,
    ROUND(DBMS_RANDOM.VALUE(100, 1000), 2) as amount
FROM dual
CONNECT BY LEVEL <= 100;

COMMIT;

-- Method 1: Create exchange table with exact structure using CTAS
CREATE TABLE exch_jan15
AS SELECT * FROM part_orders 
WHERE order_date = DATE '2024-01-15';

-- Add matching primary key
ALTER TABLE exch_jan15 
ADD CONSTRAINT pk_exch_jan15 
PRIMARY KEY(order_id, order_date);

-- Add matching index
CREATE INDEX idx_exch_jan15_date ON exch_jan15(order_date);

-- Exchange the partition
ALTER TABLE part_orders
EXCHANGE PARTITION 
FOR (DATE '2024-01-15')
WITH TABLE exch_jan15
INCLUDING INDEXES
WITHOUT VALIDATION;

-- Method 2: Create empty exchange table with WHERE 1=0
CREATE TABLE exch_jan16
AS SELECT * FROM part_orders WHERE 1=0;

-- Add matching constraints and indexes
ALTER TABLE exch_jan16 
ADD CONSTRAINT pk_exch_jan16 
PRIMARY KEY(order_id, order_date);

CREATE INDEX idx_exch_jan16_date ON exch_jan16(order_date);

-- Insert specific data
INSERT INTO exch_jan16
SELECT * FROM part_orders 
WHERE order_date = DATE '2024-01-16';

-- Exchange the partition
ALTER TABLE part_orders
EXCHANGE PARTITION 
FOR (DATE '2024-01-16')
WITH TABLE exch_jan16
INCLUDING INDEXES
WITHOUT VALIDATION;

-- Method 3: Direct exchange with newly created table
CREATE TABLE exch_jan17
AS SELECT * FROM part_orders 
WHERE order_date = DATE '2024-01-17';

-- Add exact matching constraints/indexes in one transaction
BEGIN
    -- Add PK
    EXECUTE IMMEDIATE 'ALTER TABLE exch_jan17 ADD CONSTRAINT pk_exch_jan17 PRIMARY KEY(order_id, order_date)';
    -- Add index
    EXECUTE IMMEDIATE 'CREATE INDEX idx_exch_jan17_date ON exch_jan17(order_date)';
    -- Perform exchange
    EXECUTE IMMEDIATE 'ALTER TABLE part_orders EXCHANGE PARTITION FOR (DATE ''2024-01-17'') WITH TABLE exch_jan17 INCLUDING INDEXES WITHOUT VALIDATION';
END;
/

-- Verify the exchanges
SELECT partition_name, num_rows, high_value
FROM user_tab_partitions
WHERE table_name = 'PART_ORDERS'
ORDER BY partition_position;

-- Check data distribution
SELECT TO_CHAR(order_date, 'YYYY-MM-DD') as date_partition,
       COUNT(*) as row_count
FROM part_orders
GROUP BY order_date
ORDER BY order_date;

-- Check exchanged tables
SELECT 'EXCH_JAN15' as table_name, COUNT(*) as rows FROM exch_jan15
UNION ALL
SELECT 'EXCH_JAN16', COUNT(*) FROM exch_jan16
UNION ALL
SELECT 'EXCH_JAN17', COUNT(*) FROM exch_jan17;

-- ========================================
-- CLEANUP
-- ========================================
/*
DROP TABLE source_table PURGE;
DROP TABLE ctas_basic PURGE;
DROP TABLE ctas_empty PURGE;
DROP TABLE ctas_full PURGE;
DROP TABLE part_orders PURGE;
DROP TABLE exch_jan15 PURGE;
DROP TABLE exch_jan16 PURGE;
DROP TABLE exch_jan17 PURGE;
*/

-- ========================================
-- NOTES
-- ========================================
/*
Key Points to Remember:
1. Basic CTAS only copies structure and data
2. WHERE 1=0 creates empty table
3. INCLUDING clauses help preserve constraints and indexes
4. Some objects never copy (triggers, default values)
5. CTAS is usually faster than CREATE TABLE + INSERT
6. CTAS automatically determines correct column types
7. CTAS can be used to change structure during copy
8. INCLUDING clauses may not work in all Oracle versions
*/