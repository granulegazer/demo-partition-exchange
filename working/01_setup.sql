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
    PARTITION p_initial VALUES LESS THAN (TO_DATE('2024-01-01', 'YYYY-MM-DD'))
);

-- Create local indexes
CREATE INDEX idx_sales_date ON sales(sale_date) LOCAL;
CREATE INDEX idx_sales_customer ON sales(customer_id) LOCAL;
CREATE INDEX idx_sales_region ON sales(region) LOCAL;

-- Create archive table with compression
CREATE TABLE sales_archive 
COMPRESS BASIC  -- Using basic compression available in all editions
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