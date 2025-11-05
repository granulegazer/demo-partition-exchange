-- ========================================
-- SETUP: Create required objects
-- ========================================
-- Creates core partition exchange framework objects
-- Does NOT include demo tables (SALES, SALES_ARCHIVE)
-- For demo tables, run: demo_tables_setup.sql
-- ========================================

SET ECHO ON
SET FEEDBACK ON

PROMPT
PROMPT ========================================
PROMPT Creating Partition Exchange Framework
PROMPT ========================================
PROMPT

-- Clean up framework tables if they exist
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

PROMPT Date array type created

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
COMMENT ON TABLE snparch_cnf_partition_archive IS 'Configuration table for partition archival process';
COMMENT ON COLUMN snparch_cnf_partition_archive.source_table_name IS 'Source partitioned table name';
COMMENT ON COLUMN snparch_cnf_partition_archive.archive_table_name IS 'Archive partitioned table name';
COMMENT ON COLUMN snparch_cnf_partition_archive.staging_table_name IS 'Temporary staging table name for exchange';
COMMENT ON COLUMN snparch_cnf_partition_archive.is_active IS 'Y/N flag to enable/disable archival for this table pair';
COMMENT ON COLUMN snparch_cnf_partition_archive.validate_before_exchange IS 'Y/N flag to validate indexes before exchange';
COMMENT ON COLUMN snparch_cnf_partition_archive.gather_stats_after_exchange IS 'Y/N flag to gather statistics after exchange';
COMMENT ON COLUMN snparch_cnf_partition_archive.enable_compression IS 'Y/N flag to enable compression on archived partitions';
COMMENT ON COLUMN snparch_cnf_partition_archive.compression_type IS 'Compression type: BASIC, OLTP, QUERY LOW/HIGH, ARCHIVE LOW/HIGH';

PROMPT Configuration table created

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
    -- Index information
    source_index_count NUMBER,
    archive_index_count NUMBER,
    source_index_size_mb NUMBER(12,2),
    archive_index_size_mb NUMBER(12,2),
    invalid_indexes_before NUMBER DEFAULT 0,
    invalid_indexes_after NUMBER DEFAULT 0,
    -- Validation details
    data_validation_status VARCHAR2(20),
    record_count_match VARCHAR2(1) DEFAULT 'Y',
    source_records_before NUMBER,
    source_records_after NUMBER,
    archive_records_before NUMBER,
    archive_records_after NUMBER,
    -- Compression information
    is_compressed VARCHAR2(1) DEFAULT 'N' NOT NULL,
    compression_type VARCHAR2(30),
    compression_ratio NUMBER(5,2),
    -- Performance metrics
    exchange_duration_seconds NUMBER(10,3),
    stats_gather_duration_seconds NUMBER(10,3),
    total_duration_seconds NUMBER(10,3),
    -- Status and error handling
    validation_status VARCHAR2(20) DEFAULT 'SUCCESS' NOT NULL,
    error_code NUMBER,
    error_message VARCHAR2(4000),
    -- Audit fields
    executed_by VARCHAR2(128) DEFAULT USER NOT NULL,
    session_id NUMBER DEFAULT SYS_CONTEXT('USERENV', 'SESSIONID'),
    CONSTRAINT pk_snparch_ctl_exec_log PRIMARY KEY (execution_id),
    CONSTRAINT chk_snparch_compressed CHECK (is_compressed IN ('Y', 'N')),
    CONSTRAINT chk_snparch_val_status CHECK (validation_status IN ('SUCCESS', 'WARNING', 'ERROR')),
    CONSTRAINT chk_snparch_data_val CHECK (data_validation_status IN ('PASS', 'FAIL', 'SKIPPED', NULL)),
    CONSTRAINT chk_snparch_rec_match CHECK (record_count_match IN ('Y', 'N', NULL))
) SEGMENT CREATION IMMEDIATE;

-- Create indexes for common queries (Oracle 19c optimized)
CREATE INDEX idx_snparch_ctl_exec_date ON snparch_ctl_execution_log(execution_date);
CREATE INDEX idx_snparch_ctl_src_table ON snparch_ctl_execution_log(source_table_name, execution_date);
CREATE INDEX idx_snparch_ctl_part_date ON snparch_ctl_execution_log(partition_date);

COMMENT ON TABLE snparch_ctl_execution_log IS 'Execution log for partition exchange operations';
COMMENT ON COLUMN snparch_ctl_execution_log.execution_id IS 'Unique execution identifier (auto-generated identity column)';
COMMENT ON COLUMN snparch_ctl_execution_log.execution_date IS 'Timestamp when partition exchange was executed';
COMMENT ON COLUMN snparch_ctl_execution_log.source_table_name IS 'Source table from which partition was archived';
COMMENT ON COLUMN snparch_ctl_execution_log.archive_table_name IS 'Archive table to which partition was moved';
COMMENT ON COLUMN snparch_ctl_execution_log.source_partition_name IS 'Name of source partition that was exchanged';
COMMENT ON COLUMN snparch_ctl_execution_log.archive_partition_name IS 'Name of archive partition that received data';
COMMENT ON COLUMN snparch_ctl_execution_log.partition_date IS 'Business date of the partition data';
COMMENT ON COLUMN snparch_ctl_execution_log.records_archived IS 'Number of records moved to archive';
COMMENT ON COLUMN snparch_ctl_execution_log.partition_size_mb IS 'Size of partition in megabytes';
COMMENT ON COLUMN snparch_ctl_execution_log.source_index_count IS 'Number of indexes on source table';
COMMENT ON COLUMN snparch_ctl_execution_log.archive_index_count IS 'Number of indexes on archive table';
COMMENT ON COLUMN snparch_ctl_execution_log.source_index_size_mb IS 'Total size of indexes on source table in MB';
COMMENT ON COLUMN snparch_ctl_execution_log.archive_index_size_mb IS 'Total size of indexes on archive table in MB';
COMMENT ON COLUMN snparch_ctl_execution_log.invalid_indexes_before IS 'Number of invalid indexes before exchange';
COMMENT ON COLUMN snparch_ctl_execution_log.invalid_indexes_after IS 'Number of invalid indexes after exchange';
COMMENT ON COLUMN snparch_ctl_execution_log.data_validation_status IS 'Data validation result: PASS, FAIL, SKIPPED';
COMMENT ON COLUMN snparch_ctl_execution_log.record_count_match IS 'Y if record counts match after exchange, N otherwise';
COMMENT ON COLUMN snparch_ctl_execution_log.source_records_before IS 'Record count in source table before exchange';
COMMENT ON COLUMN snparch_ctl_execution_log.source_records_after IS 'Record count in source table after exchange';
COMMENT ON COLUMN snparch_ctl_execution_log.archive_records_before IS 'Record count in archive table before exchange';
COMMENT ON COLUMN snparch_ctl_execution_log.archive_records_after IS 'Record count in archive table after exchange';
COMMENT ON COLUMN snparch_ctl_execution_log.is_compressed IS 'Y if partition is compressed in archive, N otherwise';
COMMENT ON COLUMN snparch_ctl_execution_log.compression_type IS 'Type of compression applied (BASIC, OLTP, QUERY, ARCHIVE)';
COMMENT ON COLUMN snparch_ctl_execution_log.compression_ratio IS 'Compression ratio achieved (if compressed)';
COMMENT ON COLUMN snparch_ctl_execution_log.exchange_duration_seconds IS 'Duration of partition exchange operation in seconds';
COMMENT ON COLUMN snparch_ctl_execution_log.stats_gather_duration_seconds IS 'Duration of statistics gathering in seconds';
COMMENT ON COLUMN snparch_ctl_execution_log.total_duration_seconds IS 'Total duration of entire archival operation in seconds';
COMMENT ON COLUMN snparch_ctl_execution_log.validation_status IS 'Status: SUCCESS, WARNING, or ERROR';
COMMENT ON COLUMN snparch_ctl_execution_log.error_code IS 'Oracle error code if operation failed';
COMMENT ON COLUMN snparch_ctl_execution_log.error_message IS 'Error message if operation failed';
COMMENT ON COLUMN snparch_ctl_execution_log.session_id IS 'Oracle session ID that executed the operation';

PROMPT Execution log table created
PROMPT Indexes on execution log table created

COMMIT;

PROMPT
PROMPT ========================================
PROMPT Framework Setup Complete
PROMPT ========================================
PROMPT
PROMPT Objects Created:
PROMPT   - Type: DATE_ARRAY_TYPE
PROMPT   - Table: SNPARCH_CNF_PARTITION_ARCHIVE (configuration)
PROMPT   - Table: SNPARCH_CTL_EXECUTION_LOG (execution log)
PROMPT   - Indexes: 3 indexes on execution log table
PROMPT
PROMPT Next Steps:
PROMPT   1. Run demo_tables_setup.sql (optional - for demo only)
PROMPT   2. Run demo_config_data.sql (for demo) OR configure your own tables
PROMPT   3. Run 03_data_generator.sql (optional - for demo only)
PROMPT   4. Run 04_archive_procedure.sql
PROMPT ========================================

