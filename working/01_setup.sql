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

BEGIN
    EXECUTE IMMEDIATE 'DROP TABLE snparch_cnf_partition_archive PURGE';
EXCEPTION
    WHEN OTHERS THEN NULL;
END;
/

BEGIN
    EXECUTE IMMEDIATE 'DROP TABLE snparch_ctl_execution_log PURGE';
EXCEPTION
    WHEN OTHERS THEN NULL;
END;
/

-- Create custom type for date array (Oracle 9i+ compatible)
CREATE OR REPLACE TYPE date_array_type AS TABLE OF DATE;
/

-- ========================================
-- CONFIGURATION TABLE
-- ========================================
-- Stores configuration for partition archival process
-- Oracle 19c optimized syntax
CREATE TABLE snparch_cnf_partition_archive (
    source_table_name VARCHAR2(128) NOT NULL,
    archive_table_name VARCHAR2(128) NOT NULL,
    staging_table_name VARCHAR2(128) NOT NULL,
    is_active VARCHAR2(1) DEFAULT 'Y' NOT NULL,
    validate_before_exchange VARCHAR2(1) DEFAULT 'Y' NOT NULL,
    gather_stats_after_exchange VARCHAR2(1) DEFAULT 'Y' NOT NULL,
    enable_compression VARCHAR2(1) DEFAULT 'N' NOT NULL,
    compression_type VARCHAR2(30) DEFAULT NULL,
    created_date TIMESTAMP(6) DEFAULT SYSTIMESTAMP NOT NULL,
    updated_date TIMESTAMP(6) DEFAULT SYSTIMESTAMP NOT NULL,
    created_by VARCHAR2(128) DEFAULT USER NOT NULL,
    updated_by VARCHAR2(128) DEFAULT USER NOT NULL,
    CONSTRAINT pk_snparch_cnf_part_arch PRIMARY KEY (source_table_name, archive_table_name),
    CONSTRAINT chk_snparch_active CHECK (is_active IN ('Y', 'N')),
    CONSTRAINT chk_snparch_validate CHECK (validate_before_exchange IN ('Y', 'N')),
    CONSTRAINT chk_snparch_gather_stats CHECK (gather_stats_after_exchange IN ('Y', 'N')),
    CONSTRAINT chk_snparch_compression CHECK (enable_compression IN ('Y', 'N')),
    CONSTRAINT chk_snparch_comp_type CHECK (compression_type IN (NULL, 'BASIC', 'OLTP', 'QUERY LOW', 'QUERY HIGH', 'ARCHIVE LOW', 'ARCHIVE HIGH'))
) SEGMENT CREATION IMMEDIATE;

-- Add comments using Oracle 19c style
COMMENT ON TABLE snparch_cnf_partition_archive IS 'Configuration table for partition archival process - Oracle 19.26';
COMMENT ON COLUMN snparch_cnf_partition_archive.source_table_name IS 'Source partitioned table name';
COMMENT ON COLUMN snparch_cnf_partition_archive.archive_table_name IS 'Archive partitioned table name';
COMMENT ON COLUMN snparch_cnf_partition_archive.staging_table_name IS 'Temporary staging table name for exchange';
COMMENT ON COLUMN snparch_cnf_partition_archive.is_active IS 'Y/N flag to enable/disable archival for this table pair';
COMMENT ON COLUMN snparch_cnf_partition_archive.validate_before_exchange IS 'Y/N flag to validate indexes before exchange';
COMMENT ON COLUMN snparch_cnf_partition_archive.gather_stats_after_exchange IS 'Y/N flag to gather statistics after exchange';
COMMENT ON COLUMN snparch_cnf_partition_archive.enable_compression IS 'Y/N flag to enable compression on archived partitions';
COMMENT ON COLUMN snparch_cnf_partition_archive.compression_type IS 'Compression type: BASIC, OLTP, QUERY LOW/HIGH, ARCHIVE LOW/HIGH';

-- Insert configuration for SALES table
INSERT INTO snparch_cnf_partition_archive (
    source_table_name,
    archive_table_name,
    staging_table_name,
    is_active,
    validate_before_exchange,
    gather_stats_after_exchange,
    enable_compression,
    compression_type
) VALUES (
    'SALES',
    'SALES_ARCHIVE',
    'SALES_STAGING_TEMP',
    'Y',
    'Y',
    'Y',
    'N',
    NULL
);

COMMIT;

-- ========================================
-- EXECUTION LOG TABLE (CONTROL TABLE)
-- ========================================
-- Tracks each partition exchange execution with detailed metadata
-- Oracle 19c optimized with improved data types
CREATE TABLE snparch_ctl_execution_log (
    execution_id NUMBER GENERATED ALWAYS AS IDENTITY (START WITH 1 INCREMENT BY 1) NOT NULL,
    execution_date TIMESTAMP(6) DEFAULT SYSTIMESTAMP NOT NULL,
    source_table_name VARCHAR2(128) NOT NULL,
    archive_table_name VARCHAR2(128) NOT NULL,
    source_partition_name VARCHAR2(128) NOT NULL,
    archive_partition_name VARCHAR2(128) NOT NULL,
    partition_date DATE NOT NULL,
    records_archived NUMBER NOT NULL,
    partition_size_mb NUMBER(12,2),
    is_compressed VARCHAR2(1) DEFAULT 'N' NOT NULL,
    compression_type VARCHAR2(30),
    compression_ratio NUMBER(5,2),
    exchange_duration_seconds NUMBER(10,3),
    stats_gather_duration_seconds NUMBER(10,3),
    validation_status VARCHAR2(20) DEFAULT 'SUCCESS' NOT NULL,
    error_code NUMBER,
    error_message VARCHAR2(4000),
    executed_by VARCHAR2(128) DEFAULT USER NOT NULL,
    session_id NUMBER DEFAULT SYS_CONTEXT('USERENV', 'SESSIONID'),
    CONSTRAINT pk_snparch_ctl_exec_log PRIMARY KEY (execution_id),
    CONSTRAINT chk_snparch_compressed CHECK (is_compressed IN ('Y', 'N')),
    CONSTRAINT chk_snparch_val_status CHECK (validation_status IN ('SUCCESS', 'WARNING', 'ERROR'))
) SEGMENT CREATION IMMEDIATE;

-- Create indexes for common queries (Oracle 19c optimized)
CREATE INDEX idx_snparch_ctl_exec_date ON snparch_ctl_execution_log(execution_date);
CREATE INDEX idx_snparch_ctl_src_table ON snparch_ctl_execution_log(source_table_name, execution_date);
CREATE INDEX idx_snparch_ctl_part_date ON snparch_ctl_execution_log(partition_date);

COMMENT ON TABLE snparch_ctl_execution_log IS 'Execution log for partition exchange operations - Oracle 19.26';
COMMENT ON COLUMN snparch_ctl_execution_log.execution_id IS 'Unique execution identifier (auto-generated identity column)';
COMMENT ON COLUMN snparch_ctl_execution_log.execution_date IS 'Timestamp when partition exchange was executed';
COMMENT ON COLUMN snparch_ctl_execution_log.source_table_name IS 'Source table from which partition was archived';
COMMENT ON COLUMN snparch_ctl_execution_log.archive_table_name IS 'Archive table to which partition was moved';
COMMENT ON COLUMN snparch_ctl_execution_log.source_partition_name IS 'Name of source partition that was exchanged';
COMMENT ON COLUMN snparch_ctl_execution_log.archive_partition_name IS 'Name of archive partition that received data';
COMMENT ON COLUMN snparch_ctl_execution_log.partition_date IS 'Business date of the partition data';
COMMENT ON COLUMN snparch_ctl_execution_log.records_archived IS 'Number of records moved to archive';
COMMENT ON COLUMN snparch_ctl_execution_log.partition_size_mb IS 'Size of partition in megabytes';
COMMENT ON COLUMN snparch_ctl_execution_log.is_compressed IS 'Y if partition is compressed in archive, N otherwise';
COMMENT ON COLUMN snparch_ctl_execution_log.compression_type IS 'Type of compression applied (BASIC, OLTP, QUERY, ARCHIVE)';
COMMENT ON COLUMN snparch_ctl_execution_log.compression_ratio IS 'Compression ratio achieved (if compressed)';
COMMENT ON COLUMN snparch_ctl_execution_log.exchange_duration_seconds IS 'Duration of partition exchange operation in seconds';
COMMENT ON COLUMN snparch_ctl_execution_log.stats_gather_duration_seconds IS 'Duration of statistics gathering in seconds';
COMMENT ON COLUMN snparch_ctl_execution_log.validation_status IS 'Status: SUCCESS, WARNING, or ERROR';
COMMENT ON COLUMN snparch_ctl_execution_log.error_code IS 'Oracle error code if operation failed';
COMMENT ON COLUMN snparch_ctl_execution_log.error_message IS 'Error message if operation failed';
COMMENT ON COLUMN snparch_ctl_execution_log.session_id IS 'Oracle session ID that executed the operation';

COMMIT;

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