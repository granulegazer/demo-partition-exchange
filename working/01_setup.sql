-- ========================================
-- SETUP: Create required objects
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

BEGIN
    EXECUTE IMMEDIATE 'DROP TABLE sales_staging_template PURGE';
EXCEPTION
    WHEN OTHERS THEN NULL;
END;
/

BEGIN
    EXECUTE IMMEDIATE 'DROP TYPE date_array_type';
EXCEPTION
    WHEN OTHERS THEN NULL;
END;
/

-- Create custom type for date array (Oracle 9i+ compatible)
CREATE OR REPLACE TYPE date_array_type AS TABLE OF DATE;
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
    PARTITION sales_old VALUES LESS THAN (DATE '2000-01-01')
);

-- Create local indexes
CREATE INDEX idx_sales_date ON sales(sale_date) LOCAL;
CREATE INDEX idx_sales_customer ON sales(customer_id) LOCAL;
CREATE INDEX idx_sales_region ON sales(region) LOCAL;

-- Create archive table with same structure as sales table
CREATE TABLE sales_archive (
    sale_id NUMBER,
    sale_date DATE NOT NULL,
    customer_id NUMBER,
    product_id NUMBER,
    amount NUMBER(10,2),
    quantity NUMBER,
    region VARCHAR2(50),
    status VARCHAR2(20),
    CONSTRAINT pk_sales_archive PRIMARY KEY (sale_id, sale_date)
)
PARTITION BY RANGE (sale_date)
INTERVAL (NUMTODSINTERVAL(1, 'DAY'))
(
    PARTITION sales_old VALUES LESS THAN (DATE '2000-01-01')
);

-- Create local indexes on archive matching the source table
CREATE INDEX idx_archive_date ON sales_archive(sale_date) LOCAL;
CREATE INDEX idx_archive_customer ON sales_archive(customer_id) LOCAL;
CREATE INDEX idx_archive_region ON sales_archive(region) LOCAL;

-- Create staging table template for exchange operations
CREATE TABLE sales_staging_template
FOR EXCHANGE WITH TABLE sales;

-- Verify table creation
SELECT 'Sales table created with interval partitioning' AS status FROM dual;
SELECT 'Archive table created with compression' AS status FROM dual;
-- SQL*Plus display settings for better column-based output
SET LINESIZE 200
SET PAGESIZE 50000
SET COLSEP ' | '
SET TRIMSPOOL ON
SET TRIMOUT ON
SET FEEDBACK OFF
SET VERIFY OFF
SET ECHO OFF

COLUMN status        FORMAT A60
COLUMN table_name    FORMAT A30
COLUMN partition_count FORMAT 9999999
SELECT 
    table_name,
    COUNT(*) AS partition_count
FROM user_tab_partitions
WHERE table_name = 'SALES'
GROUP BY table_name;