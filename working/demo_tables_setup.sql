-- ========================================
-- DEMO TABLES SETUP
-- ========================================
-- Creates SALES and SALES_ARCHIVE tables for demonstration purposes
-- This is NOT required for the partition exchange framework
-- Only run this if you want to test with demo data
-- ========================================

SET ECHO ON
SET FEEDBACK ON

PROMPT
PROMPT ========================================
PROMPT Creating Demo Tables (SALES and SALES_ARCHIVE)
PROMPT ========================================
PROMPT

-- Clean up demo tables if they exist
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

-- Create local indexes (PK on sale_id, sale_date creates its own index automatically)
-- No need for separate index on sale_date - partition key provides efficient access
CREATE INDEX idx_sales_customer ON sales(customer_id) LOCAL;
CREATE INDEX idx_sales_region ON sales(region) LOCAL;

PROMPT Sales table created with interval partitioning

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

-- Create local indexes on archive (PK on sale_id, sale_date creates its own index automatically)
-- No need for separate index on sale_date - partition key provides efficient access
CREATE INDEX idx_archive_customer ON sales_archive(customer_id) LOCAL;
CREATE INDEX idx_archive_region ON sales_archive(region) LOCAL;

PROMPT Archive table created with interval partitioning

-- Create staging table template for exchange operations
CREATE TABLE sales_staging_template
FOR EXCHANGE WITH TABLE sales;

PROMPT Staging table template created

-- Verify table creation
PROMPT
PROMPT ========================================
PROMPT Verification
PROMPT ========================================

SET LINESIZE 200
SET PAGESIZE 50000
SET COLSEP ' | '
SET TRIMSPOOL ON
SET TRIMOUT ON
SET FEEDBACK ON
SET VERIFY OFF

COLUMN table_name    FORMAT A30
COLUMN partition_count FORMAT 9999999

SELECT 
    table_name,
    COUNT(*) AS partition_count
FROM user_tab_partitions
WHERE table_name IN ('SALES', 'SALES_ARCHIVE')
GROUP BY table_name
ORDER BY table_name;

PROMPT
PROMPT ========================================
PROMPT Demo Tables Created Successfully
PROMPT ========================================
PROMPT Next Step: Run demo_config_data.sql to configure archival
PROMPT ========================================
