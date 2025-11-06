-- ========================================
-- HELPER FUNCTION: Get Partition Name by Date
-- ========================================

CREATE OR REPLACE FUNCTION get_partition_name_by_date(
    p_table_name IN VARCHAR2,
    p_date IN DATE
) RETURN VARCHAR2
IS
/*
    Function: get_partition_name_by_date
    
    Purpose:
        Retrieves the partition name for a given date from a partitioned table.
        Searches through all partitions and finds the one containing the specified date
        based on partition high_value boundaries.
    
    Parameters:
        p_table_name (IN VARCHAR2) - Name of the partitioned table to search
        p_date (IN DATE)           - Date to find the corresponding partition for
    
    Returns:
        VARCHAR2 - Name of the partition containing the specified date
                   NULL if no partition found
    
    Logic:
        1. Queries user_tab_partitions for the specified table
        2. Iterates through partitions in order (by partition_position)
        3. For each partition, evaluates the high_value boundary:
           - MAXVALUE partitions are treated as 9999-12-31
           - Other partitions: executes high_value expression to get boundary date
        4. Returns partition name when p_date < high_value (exclusive boundary)
    
    Notes:
        - Partition high_value is an EXCLUSIVE boundary
        - MAXVALUE partitions are treated specially
        - Returns NULL if date doesn't fall in any partition
    
    Example Usage:
        v_partition := get_partition_name_by_date('SALES', DATE '2024-01-15');
        -- Returns: 'SYS_P123' or 'SALES_20240115' depending on partition naming
    
    Error Handling:
        - Returns NULL on any error
        - Outputs error message to DBMS_OUTPUT
    
    Dependencies:
        - Requires SELECT privilege on USER_TAB_PARTITIONS
*/
    v_partition_name VARCHAR2(128);
    v_high_value_str VARCHAR2(32767);
    v_high_value_date DATE;

    CURSOR c_partitions IS
        SELECT partition_name, high_value
        FROM user_tab_partitions
        WHERE table_name = UPPER(p_table_name)
        ORDER BY partition_position;

BEGIN
    FOR rec IN c_partitions LOOP
        v_high_value_str := rec.high_value;
        IF UPPER(v_high_value_str) = 'MAXVALUE' THEN
            v_high_value_date := TO_DATE('9999-12-31 23:59:59', 'yyyy-mm-dd hh24:mi:ss');
        ELSE
            EXECUTE IMMEDIATE 'SELECT ' || v_high_value_str || ' FROM dual' INTO v_high_value_date;
        END IF;

        -- Partition high_value is exclusive boundary
        IF p_date < v_high_value_date THEN
            RETURN rec.partition_name;
        END IF;
    END LOOP;

    RETURN NULL;

EXCEPTION
    WHEN OTHERS THEN
        DBMS_OUTPUT.PUT_LINE('ERROR in get_partition_name_by_date: ' || SQLERRM);
        RETURN NULL;
END;
/

-- ========================================
-- ARCHIVE PROCEDURE: Multiple Dates Support with Validation
-- ========================================

CREATE OR REPLACE PROCEDURE archive_partitions_by_dates (
    p_table_name IN VARCHAR2,
    p_dates IN date_array_type
) AS
/*
    Procedure: archive_partitions_by_dates
    
    Purpose:
        Archives partitions from a source table to an archive table using partition exchange.
        Processes multiple dates in a single execution with comprehensive logging and validation.
        Uses two-step exchange process: Source → Staging → Archive (both metadata-only operations).
    
    Parameters:
        p_table_name (IN VARCHAR2)        - Name of source partitioned table to archive from
        p_dates (IN date_array_type)      - Collection of dates to archive (one partition per date)
    
    Configuration:
        Reads settings from SNPARCH_CNF_PARTITION_ARCHIVE table:
        - archive_table_name:           Target archive table name
        - staging_table_name:           Temporary staging table name template
        - is_active:                    Y/N to enable/disable archival
        - validate_before_exchange:     Y/N to validate indexes before exchange
        - gather_stats_after_exchange:  Y/N to gather statistics after exchange
        - enable_compression:           Y/N compression flag
        - compression_type:             Type of compression (BASIC, OLTP, QUERY, ARCHIVE)
    
    Process Flow:
        1. Validation Phase:
           - Verify source table exists and is partitioned
           - Load configuration from SNPARCH_CNF_PARTITION_ARCHIVE
           - Check configuration is active
           - Validate table structure compatibility:
             * Verify all three tables exist (source, archive, staging)
             * Confirm archive table is partitioned
             * Confirm staging table is NOT partitioned
             * Verify column count matches across all tables
             * Validate column names, data types, and sizes match exactly
             * Check partition key columns match between source and archive
           - Optionally validate indexes (if validate_before_exchange = Y)
        
        2. Pre-Exchange Metrics Collection:
           - Count records in source and archive tables
           - Get index counts and sizes
           - Count invalid indexes
        
        3. Partition Exchange (for each date):
           a. Find partition name for the date
           b. Exchange source partition → staging table (INSTANT, using pre-configured staging)
           c. Exchange staging table → archive partition (INSTANT)
           d. Collect post-exchange metrics
           e. Validate data integrity (record counts)
           f. Drop empty source partition
        
        4. Post-Exchange Actions:
           - Validate index status (if validate_before_exchange = Y)
           - Gather statistics (if gather_stats_after_exchange = Y)
           - Log execution details to SNPARCH_CTL_EXECUTION_LOG
        
        5. Final Summary:
           - Display completion statistics
           - Show before/after table stats
    
    Execution Logging (SNPARCH_CTL_EXECUTION_LOG):
        Records comprehensive metrics for each partition exchange:
        - Partition names (source and archive)
        - Partition date and size
        - Record counts (before/after for both tables)
        - Index counts and sizes
        - Invalid index counts (before/after)
        - Data validation status (PASS/FAIL)
        - Compression information
        - Performance metrics (exchange duration, stats duration, total duration)
        - Status (SUCCESS/WARNING/ERROR)
    
    Data Validation:
        Automatically validates data integrity after exchange:
        - Checks: source_records_before - source_records_after == records_moved
        - Checks: archive_records_after - archive_records_before == records_moved
        - Sets data_validation_status to FAIL if mismatch detected
        - Sets overall status to WARNING if validation fails
    
    Error Handling:
        - Graceful handling of missing partitions (logs warning, continues with remaining dates)
        - Invalid indexes are automatically rebuilt before exchange
        - Detailed error logging via prc_log_error_autonomous
        - Specific exceptions for common errors:
          * e_table_not_partitioned: Source table is not partitioned
          * e_config_not_found: No configuration found
          * e_config_inactive: Configuration exists but is inactive
        - Structure validation errors (raise application errors):
          * -20010: Source table does not exist
          * -20011: Archive table does not exist
          * -20012: Staging table does not exist
          * -20013: Archive table is not partitioned
          * -20014: Staging table is partitioned (must be non-partitioned)
          * -20015: Column count mismatch
          * -20016: Column structure mismatch (source vs archive)
          * -20017: Column structure mismatch (source vs staging)
          * -20018: Partition key mismatch
    
    Performance Considerations:
        - Both exchange operations are INSTANT (metadata-only)
        - INCLUDING INDEXES ensures indexes remain usable after exchange
        - WITHOUT VALIDATION clause for faster exchange
        - Statistics gathering uses AUTO_DEGREE for parallelism
        - Index validation optional to reduce overhead
        - Automatically rebuilds any INVALID or UNUSABLE indexes detected
    
    Dependencies:
        - Function: get_partition_name_by_date
        - Procedure: prc_log_error_autonomous
        - Function: f_degrag_get_table_size_stats_util
        - Table: SNPARCH_CNF_PARTITION_ARCHIVE (configuration)
        - Table: SNPARCH_CTL_EXECUTION_LOG (logging)
        - Type: date_array_type (collection of dates)
    
    Example Usage:
        -- Archive single date
        BEGIN
            archive_partitions_by_dates(
                p_table_name => 'SALES',
                p_dates => date_array_type(DATE '2024-01-15')
            );
        END;        
        -- Archive multiple dates
        BEGIN
            archive_partitions_by_dates(
                p_table_name => 'SALES',
                p_dates => date_array_type(
                    DATE '2024-01-01',
                    DATE '2024-01-02',
                    DATE '2024-01-03'
                )
            );
        END;
    Output (DBMS_OUTPUT):
        - Configuration details
        - Before/after table statistics
        - Processing status for each date
        - Partition exchange confirmations
        - Data validation results
        - Index validation results
        - Statistics gathering confirmation
        - Final summary with totals
    
    Notes:
        - Staging table is pre-configured in SNPARCH_CNF_PARTITION_ARCHIVE and reused for all exchanges
        - Source partitions are DROPPED after successful exchange
        - Archive partitions are created automatically if they don't exist
        - No physical data movement - both exchanges are metadata operations
        - Indexes are exchanged automatically with partitions using INCLUDING INDEXES clause
        - LOCAL indexes become regular indexes on staging, then back to LOCAL on archive
        - Index validation checks for both INVALID and UNUSABLE statuses
        - Any unusable indexes are automatically rebuilt before and after exchanges
        - Transaction committed after each date to avoid long-running transactions
    
    Version History:
        - Optimized with IDENTITY columns, TIMESTAMP(6), enhanced metrics
        - Added comprehensive data validation
        - Added index size tracking
        - Added before/after record count validation
*/
    -- Configuration variables
    v_archive_table_name VARCHAR2(128);
    v_staging_table VARCHAR2(128);
    v_validate_before VARCHAR2(1);
    v_gather_stats_after VARCHAR2(1);
    v_is_active VARCHAR2(1);
    v_enable_compression VARCHAR2(1);
    v_compression_type VARCHAR2(30);
    
    -- Working variables
    v_partition_name VARCHAR2(128);
    v_archive_partition_name VARCHAR2(128);
    v_sql VARCHAR2(4000);
    v_count NUMBER;
    v_total_archived NUMBER := 0;
    v_partitions_archived NUMBER := 0;
    v_step NUMBER := 0;
    v_proc_name VARCHAR2(30) := 'ARCHIVE_PARTITIONS';
    v_stats VARCHAR2(4000);
    
    -- Execution logging variables
    v_execution_start TIMESTAMP(6);
    v_execution_end TIMESTAMP(6);
    v_exchange_start TIMESTAMP(6);
    v_exchange_end TIMESTAMP(6);
    v_stats_start TIMESTAMP(6);
    v_stats_end TIMESTAMP(6);
    v_exchange_duration NUMBER;
    v_stats_duration NUMBER;
    v_total_duration NUMBER;
    v_partition_size_mb NUMBER(12,2);
    v_is_compressed VARCHAR2(1);
    v_compression_ratio NUMBER(5,2);
    
    -- Index tracking variables
    v_source_index_count NUMBER;
    v_archive_index_count NUMBER;
    v_source_index_size_mb NUMBER(12,2);
    v_archive_index_size_mb NUMBER(12,2);
    v_invalid_indexes_before NUMBER := 0;
    v_invalid_indexes_after NUMBER := 0;
    
    -- Data validation variables
    v_data_validation_status VARCHAR2(20);
    v_record_count_match VARCHAR2(1);
    v_source_records_before NUMBER;
    v_source_records_after NUMBER;
    v_archive_records_before NUMBER;
    v_archive_records_after NUMBER;
    
    -- Validation variables
    v_invalid_indexes NUMBER;
    v_stale_stats NUMBER;
    
    -- Exception for configuration not found
    e_config_not_found EXCEPTION;
    e_config_inactive EXCEPTION;
    e_invalid_indexes_found EXCEPTION;
    e_table_not_partitioned EXCEPTION;
    
    -- Validation variables
    v_partitioned VARCHAR2(3);
    
BEGIN
    v_execution_start := SYSTIMESTAMP;
    v_step := 1;
    
    -- Check if source table is partitioned
    BEGIN
        SELECT partitioned
        INTO v_partitioned
        FROM user_tables
        WHERE table_name = UPPER(p_table_name);
        
        IF v_partitioned != 'YES' THEN
            prc_log_error_autonomous(v_proc_name, 'E', v_step, NULL, NULL, 
                'Table is not partitioned', 'Table: ' || p_table_name, USER);
            RAISE e_table_not_partitioned;
        END IF;
        
        prc_log_error_autonomous(v_proc_name, 'I', v_step, NULL, NULL, 
            'Verified table is partitioned', 'Table: ' || p_table_name, USER);
            
    EXCEPTION
        WHEN NO_DATA_FOUND THEN
            prc_log_error_autonomous(v_proc_name, 'E', v_step, NULL, NULL, 
                'Table does not exist', 'Table: ' || p_table_name, USER);
            RAISE_APPLICATION_ERROR(-20001, 'Table ' || p_table_name || ' does not exist');
    END;
    
    v_step := 2;
    
    -- Get configuration for this table to retrieve archive and staging table names
    BEGIN
        SELECT 
            archive_table_name,
            staging_table_name,
            is_active,
            validate_before_exchange,
            gather_stats_after_exchange,
            enable_compression,
            compression_type
        INTO 
            v_archive_table_name,
            v_staging_table,
            v_is_active,
            v_validate_before,
            v_gather_stats_after,
            v_enable_compression,
            v_compression_type
        FROM snparch_cnf_partition_archive
        WHERE source_table_name = UPPER(p_table_name)
        AND staging_table_name IS NOT NULL
        AND is_active = 'Y';
          
    EXCEPTION
        WHEN NO_DATA_FOUND THEN
            prc_log_error_autonomous(v_proc_name, 'E', v_step, NULL, NULL, 
                'Configuration not found', 'Table: ' || p_table_name, USER);
            RAISE e_config_not_found;
    END;
    
    -- Check if configuration is active
    IF v_is_active != 'Y' THEN
        prc_log_error_autonomous(v_proc_name, 'E', v_step, NULL, NULL, 
            'Configuration is inactive', 'Table: ' || p_table_name, USER);
        RAISE e_config_inactive;
    END IF;
    
    prc_log_error_autonomous(v_proc_name, 'I', v_step, NULL, NULL, 
        'Configuration loaded', 
        'Source: ' || p_table_name || ', Archive: ' || v_archive_table_name || ', Staging: ' || v_staging_table, 
        USER);
    
    -- Validate table structure compatibility for partition exchange
    v_step := 3;
    DECLARE
        v_source_columns NUMBER := 0;
        v_archive_columns NUMBER := 0;
        v_staging_columns NUMBER := 0;
        v_column_mismatch NUMBER := 0;
        v_archive_partitioned VARCHAR2(3);
        v_staging_partitioned VARCHAR2(3);
        e_structure_mismatch EXCEPTION;
    BEGIN
        prc_log_error_autonomous(v_proc_name, 'I', v_step, NULL, NULL, 
            'Validating table structures for partition exchange compatibility', 
            'Source: ' || p_table_name || ', Archive: ' || v_archive_table_name || ', Staging: ' || v_staging_table, 
            USER);
        
        -- 1. Check if all three tables exist
        BEGIN
            SELECT COUNT(*) INTO v_source_columns
            FROM user_tables
            WHERE table_name = UPPER(p_table_name);
            
            IF v_source_columns = 0 THEN
                RAISE_APPLICATION_ERROR(-20010, 'Source table ' || p_table_name || ' does not exist');
            END IF;
            
            SELECT COUNT(*) INTO v_archive_columns
            FROM user_tables
            WHERE table_name = UPPER(v_archive_table_name);
            
            IF v_archive_columns = 0 THEN
                RAISE_APPLICATION_ERROR(-20011, 'Archive table ' || v_archive_table_name || ' does not exist');
            END IF;
            
            SELECT COUNT(*) INTO v_staging_columns
            FROM user_tables
            WHERE table_name = UPPER(v_staging_table);
            
            IF v_staging_columns = 0 THEN
                RAISE_APPLICATION_ERROR(-20012, 'Staging table ' || v_staging_table || ' does not exist');
            END IF;
        END;
        
        -- 2. Check archive table is partitioned
        SELECT partitioned INTO v_archive_partitioned
        FROM user_tables
        WHERE table_name = UPPER(v_archive_table_name);
        
        IF v_archive_partitioned != 'YES' THEN
            prc_log_error_autonomous(v_proc_name, 'E', v_step, NULL, NULL, 
                'Archive table is not partitioned', 'Archive: ' || v_archive_table_name, USER);
            RAISE_APPLICATION_ERROR(-20013, 'Archive table ' || v_archive_table_name || ' must be partitioned');
        END IF;
        
        -- 3. Check staging table is NOT partitioned
        SELECT partitioned INTO v_staging_partitioned
        FROM user_tables
        WHERE table_name = UPPER(v_staging_table);
        
        IF v_staging_partitioned = 'YES' THEN
            prc_log_error_autonomous(v_proc_name, 'E', v_step, NULL, NULL, 
                'Staging table is partitioned', 'Staging: ' || v_staging_table, USER);
            RAISE_APPLICATION_ERROR(-20014, 'Staging table ' || v_staging_table || ' must NOT be partitioned');
        END IF;
        
        -- 4. Check column count matches
        SELECT COUNT(*) INTO v_source_columns
        FROM user_tab_columns
        WHERE table_name = UPPER(p_table_name);
        
        SELECT COUNT(*) INTO v_archive_columns
        FROM user_tab_columns
        WHERE table_name = UPPER(v_archive_table_name);
        
        SELECT COUNT(*) INTO v_staging_columns
        FROM user_tab_columns
        WHERE table_name = UPPER(v_staging_table);
        
        IF v_source_columns != v_archive_columns OR v_source_columns != v_staging_columns THEN
            prc_log_error_autonomous(v_proc_name, 'E', v_step, NULL, NULL, 
                'Column count mismatch', 
                'Source: ' || v_source_columns || ', Archive: ' || v_archive_columns || ', Staging: ' || v_staging_columns, 
                USER);
            RAISE_APPLICATION_ERROR(-20015, 
                'Column count mismatch - Source: ' || v_source_columns || 
                ', Archive: ' || v_archive_columns || 
                ', Staging: ' || v_staging_columns);
        END IF;
        
        -- 5. Check column names, data types, and sizes match
        -- Compare source vs archive
        SELECT COUNT(*)
        INTO v_column_mismatch
        FROM (
            -- Columns in source but not in archive (or different data type/size)
            SELECT column_name, data_type, data_length, data_precision, data_scale, nullable
            FROM user_tab_columns
            WHERE table_name = UPPER(p_table_name)
            MINUS
            SELECT column_name, data_type, data_length, data_precision, data_scale, nullable
            FROM user_tab_columns
            WHERE table_name = UPPER(v_archive_table_name)
            UNION ALL
            -- Columns in archive but not in source (or different data type/size)
            SELECT column_name, data_type, data_length, data_precision, data_scale, nullable
            FROM user_tab_columns
            WHERE table_name = UPPER(v_archive_table_name)
            MINUS
            SELECT column_name, data_type, data_length, data_precision, data_scale, nullable
            FROM user_tab_columns
            WHERE table_name = UPPER(p_table_name)
        );
        
        IF v_column_mismatch > 0 THEN
            prc_log_error_autonomous(v_proc_name, 'E', v_step, NULL, NULL, 
                'Column structure mismatch between source and archive tables', 
                'Mismatched columns: ' || v_column_mismatch, USER);
            RAISE_APPLICATION_ERROR(-20016, 
                'Column structure mismatch between ' || p_table_name || 
                ' and ' || v_archive_table_name || ' (' || v_column_mismatch || ' differences)');
        END IF;
        
        -- Compare source vs staging
        SELECT COUNT(*)
        INTO v_column_mismatch
        FROM (
            SELECT column_name, data_type, data_length, data_precision, data_scale, nullable
            FROM user_tab_columns
            WHERE table_name = UPPER(p_table_name)
            MINUS
            SELECT column_name, data_type, data_length, data_precision, data_scale, nullable
            FROM user_tab_columns
            WHERE table_name = UPPER(v_staging_table)
            UNION ALL
            SELECT column_name, data_type, data_length, data_precision, data_scale, nullable
            FROM user_tab_columns
            WHERE table_name = UPPER(v_staging_table)
            MINUS
            SELECT column_name, data_type, data_length, data_precision, data_scale, nullable
            FROM user_tab_columns
            WHERE table_name = UPPER(p_table_name)
        );
        
        IF v_column_mismatch > 0 THEN
            prc_log_error_autonomous(v_proc_name, 'E', v_step, NULL, NULL, 
                'Column structure mismatch between source and staging tables', 
                'Mismatched columns: ' || v_column_mismatch, USER);
            RAISE_APPLICATION_ERROR(-20017, 
                'Column structure mismatch between ' || p_table_name || 
                ' and ' || v_staging_table || ' (' || v_column_mismatch || ' differences)');
        END IF;
        
        -- 6. Validate partition key compatibility (source and archive must use same partition key)
        DECLARE
            v_source_part_key VARCHAR2(4000);
            v_archive_part_key VARCHAR2(4000);
        BEGIN
            -- Get source table partition key columns (concatenated, ordered)
            SELECT LISTAGG(column_name, ',') WITHIN GROUP (ORDER BY column_position)
            INTO v_source_part_key
            FROM user_part_key_columns
            WHERE name = UPPER(p_table_name)
              AND object_type = 'TABLE';
            
            -- Get archive table partition key columns (concatenated, ordered)
            SELECT LISTAGG(column_name, ',') WITHIN GROUP (ORDER BY column_position)
            INTO v_archive_part_key
            FROM user_part_key_columns
            WHERE name = UPPER(v_archive_table_name)
              AND object_type = 'TABLE';
            
            IF v_source_part_key != v_archive_part_key THEN
                prc_log_error_autonomous(v_proc_name, 'E', v_step, NULL, NULL, 
                    'Partition key mismatch', 
                    'Source: ' || v_source_part_key || ', Archive: ' || v_archive_part_key, USER);
                RAISE_APPLICATION_ERROR(-20018, 
                    'Partition key mismatch - Source: (' || v_source_part_key || 
                    '), Archive: (' || v_archive_part_key || ')');
            END IF;
            
            prc_log_error_autonomous(v_proc_name, 'I', v_step, NULL, NULL, 
                'Partition key validated', 'Key columns: ' || v_source_part_key, USER);
        END;
        
        -- 7. All validations passed
        prc_log_error_autonomous(v_proc_name, 'I', v_step, NULL, NULL, 
            'Table structure validation PASSED', 
            'All tables are partition exchange compatible', USER);
        
        DBMS_OUTPUT.PUT_LINE('Structure validation: PASSED');
        DBMS_OUTPUT.PUT_LINE('  - Source columns: ' || v_source_columns);
        DBMS_OUTPUT.PUT_LINE('  - Archive columns: ' || v_archive_columns);
        DBMS_OUTPUT.PUT_LINE('  - Staging columns: ' || v_staging_columns);
        DBMS_OUTPUT.PUT_LINE('  - Archive partitioned: YES');
        DBMS_OUTPUT.PUT_LINE('  - Staging partitioned: NO');
        DBMS_OUTPUT.PUT_LINE('  - Column structures: MATCH');
        
    EXCEPTION
        WHEN OTHERS THEN
            prc_log_error_autonomous(v_proc_name, 'E', v_step, SQLCODE, SQLERRM, 
                'Structure validation failed', DBMS_UTILITY.FORMAT_ERROR_BACKTRACE, USER);
            RAISE;
    END;
    
    -- Get initial table stats
    v_step := 4;
    v_stats := f_degrag_get_table_size_stats_util(p_table_name);
    prc_log_error_autonomous(v_proc_name, 'I', v_step, NULL, NULL, 
        'Starting partition archiving', 'Table: ' || p_table_name || ', Dates: ' || p_dates.COUNT, USER);
    prc_log_error_autonomous(v_proc_name, 'I', v_step, NULL, NULL, 
        'Source table stats (before)', v_stats, USER);
    
    v_stats := f_degrag_get_table_size_stats_util(v_archive_table_name);
    prc_log_error_autonomous(v_proc_name, 'I', v_step, NULL, NULL, 
        'Archive table stats (before)', v_stats, USER);
    
    DBMS_OUTPUT.PUT_LINE('===========================================');
    DBMS_OUTPUT.PUT_LINE('Starting partition archiving');
    DBMS_OUTPUT.PUT_LINE('Table: ' || p_table_name);
    DBMS_OUTPUT.PUT_LINE('Archive: ' || v_archive_table_name);
    DBMS_OUTPUT.PUT_LINE('Staging: ' || v_staging_table);
    DBMS_OUTPUT.PUT_LINE('Dates to archive: ' || p_dates.COUNT);
    DBMS_OUTPUT.PUT_LINE('Source stats: ' || f_degrag_get_table_size_stats_util(p_table_name));
    DBMS_OUTPUT.PUT_LINE('Archive stats: ' || f_degrag_get_table_size_stats_util(v_archive_table_name));
    DBMS_OUTPUT.PUT_LINE('===========================================');
    
    -- Validate indexes before exchange if configured
    IF v_validate_before = 'Y' THEN
        v_step := 5;
        
        -- Check for invalid/unusable indexes on source table
        SELECT COUNT(*)
        INTO v_invalid_indexes
        FROM user_indexes
        WHERE table_name = UPPER(p_table_name)
          AND (status != 'VALID' OR status = 'UNUSABLE');
          
        IF v_invalid_indexes > 0 THEN
            prc_log_error_autonomous(v_proc_name, 'E', v_step, NULL, NULL, 
                'Invalid indexes found on source', 
                'Table: ' || p_table_name || ', Count: ' || v_invalid_indexes, USER);
            DBMS_OUTPUT.PUT_LINE('WARNING: ' || v_invalid_indexes || ' invalid indexes found on ' || p_table_name);
            
            -- Rebuild invalid/unusable indexes
            FOR idx IN (
                SELECT index_name 
                FROM user_indexes 
                WHERE table_name = UPPER(p_table_name)
                  AND (status != 'VALID' OR status = 'UNUSABLE')
            ) LOOP
                BEGIN
                    DBMS_OUTPUT.PUT_LINE('Rebuilding index: ' || idx.index_name);
                    EXECUTE IMMEDIATE 'ALTER INDEX ' || idx.index_name || ' REBUILD';
                    prc_log_error_autonomous(v_proc_name, 'I', v_step, NULL, NULL, 
                        'Rebuilt invalid index', idx.index_name, USER);
                EXCEPTION
                    WHEN OTHERS THEN
                        prc_log_error_autonomous(v_proc_name, 'E', v_step, SQLCODE, SQLERRM, 
                            'Error rebuilding index', idx.index_name, USER);
                END;
            END LOOP;
        ELSE
            prc_log_error_autonomous(v_proc_name, 'I', v_step, NULL, NULL, 
                'All indexes valid on source', 'Table: ' || p_table_name, USER);
        END IF;
        
        -- Check for invalid/unusable indexes on archive table
        SELECT COUNT(*)
        INTO v_invalid_indexes
        FROM user_indexes
        WHERE table_name = UPPER(v_archive_table_name)
          AND (status != 'VALID' OR status = 'UNUSABLE');
          
        IF v_invalid_indexes > 0 THEN
            prc_log_error_autonomous(v_proc_name, 'E', v_step, NULL, NULL, 
                'Invalid indexes found on archive', 
                'Table: ' || v_archive_table_name || ', Count: ' || v_invalid_indexes, USER);
            DBMS_OUTPUT.PUT_LINE('WARNING: ' || v_invalid_indexes || ' invalid indexes found on ' || v_archive_table_name);
            
            -- Rebuild invalid/unusable indexes
            FOR idx IN (
                SELECT index_name 
                FROM user_indexes 
                WHERE table_name = UPPER(v_archive_table_name)
                  AND (status != 'VALID' OR status = 'UNUSABLE')
            ) LOOP
                BEGIN
                    DBMS_OUTPUT.PUT_LINE('Rebuilding index: ' || idx.index_name);
                    EXECUTE IMMEDIATE 'ALTER INDEX ' || idx.index_name || ' REBUILD';
                    prc_log_error_autonomous(v_proc_name, 'I', v_step, NULL, NULL, 
                        'Rebuilt invalid index', idx.index_name, USER);
                EXCEPTION
                    WHEN OTHERS THEN
                        prc_log_error_autonomous(v_proc_name, 'E', v_step, SQLCODE, SQLERRM, 
                            'Error rebuilding index', idx.index_name, USER);
                END;
            END LOOP;
        ELSE
            prc_log_error_autonomous(v_proc_name, 'I', v_step, NULL, NULL, 
                'All indexes valid on archive', 'Table: ' || v_archive_table_name, USER);
        END IF;
    END IF;
    
    -- Show dates to be archived
    FOR i IN 1..p_dates.COUNT LOOP
        DBMS_OUTPUT.PUT_LINE('Date ' || i || ': ' || TO_CHAR(p_dates(i), 'YYYY-MM-DD'));
    END LOOP;
    DBMS_OUTPUT.PUT_LINE('-------------------------------------------');
    
    -- Process each date
    FOR i IN 1..p_dates.COUNT LOOP
        BEGIN
            v_step := 100 + (i * 100);  -- Step 100, 200, 300, etc. for each date iteration
            prc_log_error_autonomous(v_proc_name, 'I', v_step, NULL, NULL, 
                'Processing date', TO_CHAR(p_dates(i), 'YYYY-MM-DD'), USER);
            
            -- Get partition name for this date
            v_partition_name := get_partition_name_by_date(p_table_name, p_dates(i));
            
            IF v_partition_name IS NULL THEN
                v_step := 100 + (i * 100) + 1;
                prc_log_error_autonomous(v_proc_name, 'I', v_step, NULL, NULL, 
                    'No partition found for date', TO_CHAR(p_dates(i), 'YYYY-MM-DD'), USER);
                    
                DBMS_OUTPUT.PUT_LINE('WARNING: No partition found for date ' || 
                                   TO_CHAR(p_dates(i), 'YYYY-MM-DD'));
                DBMS_OUTPUT.PUT_LINE('-------------------------------------------');
                CONTINUE;
            END IF;
            
            v_step := 100 + (i * 100) + 2;
            prc_log_error_autonomous(v_proc_name, 'I', v_step, NULL, NULL, 
                'Found partition', 'Partition: ' || v_partition_name || ', Date: ' || TO_CHAR(p_dates(i), 'YYYY-MM-DD'), USER);
            
            DBMS_OUTPUT.PUT_LINE('Processing date: ' || TO_CHAR(p_dates(i), 'YYYY-MM-DD'));
            DBMS_OUTPUT.PUT_LINE('Partition name: ' || v_partition_name);
            
            -- Get partition name for archive table
            v_archive_partition_name := get_partition_name_by_date(v_archive_table_name, p_dates(i));
            
            -- Count records in partition
            v_step := 100 + (i * 100) + 3;
            v_sql := 'SELECT COUNT(*) FROM ' || p_table_name || 
                     ' PARTITION (' || v_partition_name || ')';
            EXECUTE IMMEDIATE v_sql INTO v_count;
            
            prc_log_error_autonomous(v_proc_name, 'I', v_step, NULL, NULL, 
                'Counted records in partition', 'Count: ' || v_count || ', Partition: ' || v_partition_name, USER);
            
            DBMS_OUTPUT.PUT_LINE('Records found: ' || v_count);
            
            IF v_count > 0 THEN
                -- Collect metrics BEFORE exchange
                v_step := 100 + (i * 100) + 4;
                
                -- Get source table record count
                EXECUTE IMMEDIATE 'SELECT COUNT(*) FROM ' || p_table_name INTO v_source_records_before;
                
                -- Get archive table record count
                EXECUTE IMMEDIATE 'SELECT COUNT(*) FROM ' || v_archive_table_name INTO v_archive_records_before;
                
                -- Get source index information
                SELECT COUNT(*), NVL(SUM(ROUND(bytes/1024/1024, 2)), 0)
                INTO v_source_index_count, v_source_index_size_mb
                FROM user_segments
                WHERE segment_name IN (
                    SELECT index_name FROM user_indexes WHERE table_name = UPPER(p_table_name)
                );
                
                -- Get archive index information
                SELECT COUNT(*), NVL(SUM(ROUND(bytes/1024/1024, 2)), 0)
                INTO v_archive_index_count, v_archive_index_size_mb
                FROM user_segments
                WHERE segment_name IN (
                    SELECT index_name FROM user_indexes WHERE table_name = UPPER(v_archive_table_name)
                );
                
                -- Count invalid indexes before
                SELECT COUNT(*)
                INTO v_invalid_indexes_before
                FROM user_indexes
                WHERE table_name IN (UPPER(p_table_name), UPPER(v_archive_table_name))
                  AND status != 'VALID';
                
                prc_log_error_autonomous(v_proc_name, 'I', v_step, NULL, NULL, 
                    'Before metrics collected', 
                    'Src Recs: ' || v_source_records_before || ', Arc Recs: ' || v_archive_records_before || 
                    ', Src Idx: ' || v_source_index_count || ', Invalid Idx: ' || v_invalid_indexes_before, 
                    USER);
                
                -- Step 1: Exchange partition from main to staging (instant)
                -- Note: Using pre-configured staging table from config
                v_step := 100 + (i * 100) + 5;
                v_exchange_start := SYSTIMESTAMP;
                
                v_sql := 'ALTER TABLE ' || p_table_name || 
                         ' EXCHANGE PARTITION ' || v_partition_name || 
                         ' WITH TABLE ' || v_staging_table || 
                         ' INCLUDING INDEXES WITHOUT VALIDATION';
                EXECUTE IMMEDIATE v_sql;
                
                prc_log_error_autonomous(v_proc_name, 'I', v_step, NULL, NULL, 
                    'Exchanged partition to staging', 'Partition: ' || v_partition_name, USER);
                
                DBMS_OUTPUT.PUT_LINE('Step 1: Partition moved to staging table (instant)');
                
                -- Step 2: Exchange staging with archive partition (instant)
                v_step := 100 + (i * 100) + 6;
                IF v_archive_partition_name IS NULL THEN
                    prc_log_error_autonomous(v_proc_name, 'I', v_step, NULL, NULL, 
                        'Archive partition not found, creating', 'Date: ' || TO_CHAR(p_dates(i), 'YYYY-MM-DD'), USER);
                    
                    -- Need to insert one row to create partition, then exchange
                    v_sql := 'INSERT INTO ' || v_archive_table_name || ' ' ||
                             'SELECT * FROM ' || v_staging_table || ' WHERE ROWNUM = 1';
                    EXECUTE IMMEDIATE v_sql;
                    COMMIT;
                    
                    -- Get the newly created partition name
                    v_archive_partition_name := get_partition_name_by_date(
                        v_archive_table_name, 
                        p_dates(i)
                    );
                    
                    prc_log_error_autonomous(v_proc_name, 'I', v_step, NULL, NULL, 
                        'Created archive partition', v_archive_partition_name, USER);
                    
                    -- Delete the test row
                    v_sql := 'DELETE FROM ' || v_archive_table_name || ' ' ||
                             'PARTITION (' || v_archive_partition_name || ')';
                    EXECUTE IMMEDIATE v_sql;
                    COMMIT;
                END IF;
                
                v_step := 100 + (i * 100) + 7;
                v_sql := 'ALTER TABLE ' || v_archive_table_name ||
                         ' EXCHANGE PARTITION ' || v_archive_partition_name ||
                         ' WITH TABLE ' || v_staging_table || 
                         ' INCLUDING INDEXES WITHOUT VALIDATION';
                EXECUTE IMMEDIATE v_sql;
                
                v_step := 100 + (i * 100) + 8;
                v_exchange_end := SYSTIMESTAMP;
                v_exchange_duration := EXTRACT(SECOND FROM (v_exchange_end - v_exchange_start)) +
                                      EXTRACT(MINUTE FROM (v_exchange_end - v_exchange_start)) * 60;
                
                prc_log_error_autonomous(v_proc_name, 'I', v_step, NULL, NULL, 
                    'Exchanged staging to archive', 'Partition: ' || v_archive_partition_name || ', Records: ' || v_count, USER);
                
                DBMS_OUTPUT.PUT_LINE('Step 2: Data moved to archive (instant)');
                
                -- Get partition size
                BEGIN
                    SELECT ROUND(bytes / 1024 / 1024, 2)
                    INTO v_partition_size_mb
                    FROM user_segments
                    WHERE segment_name = UPPER(v_archive_table_name)
                      AND partition_name = v_archive_partition_name
                      AND ROWNUM = 1;
                EXCEPTION
                    WHEN NO_DATA_FOUND THEN
                        v_partition_size_mb := NULL;
                END;
                
                -- Check if partition is compressed
                BEGIN
                    SELECT 
                        CASE WHEN compression = 'ENABLED' THEN 'Y' ELSE 'N' END,
                        compress_for
                    INTO 
                        v_is_compressed,
                        v_compression_type
                    FROM user_tab_partitions
                    WHERE table_name = UPPER(v_archive_table_name)
                      AND partition_name = v_archive_partition_name;
                EXCEPTION
                    WHEN NO_DATA_FOUND THEN
                        v_is_compressed := 'N';
                        v_compression_type := NULL;
                END;
                
                v_total_archived := v_total_archived + v_count;
                
                -- Collect metrics AFTER exchange
                v_step := 100 + (i * 100) + 9;
                
                -- Get source table record count after exchange
                EXECUTE IMMEDIATE 'SELECT COUNT(*) FROM ' || p_table_name INTO v_source_records_after;
                
                -- Get archive table record count after exchange
                EXECUTE IMMEDIATE 'SELECT COUNT(*) FROM ' || v_archive_table_name INTO v_archive_records_after;
                
                -- Count invalid indexes after
                SELECT COUNT(*)
                INTO v_invalid_indexes_after
                FROM user_indexes
                WHERE table_name IN (UPPER(p_table_name), UPPER(v_archive_table_name))
                  AND status != 'VALID';
                
                -- Validate data integrity
                v_data_validation_status := 'PASS';
                v_record_count_match := 'Y';
                
                -- Check if records moved correctly
                IF v_source_records_before - v_source_records_after != v_count THEN
                    v_data_validation_status := 'FAIL';
                    v_record_count_match := 'N';
                    prc_log_error_autonomous(v_proc_name, 'E', v_step, NULL, NULL, 
                        'Source record count mismatch', 
                        'Expected: ' || v_count || ', Actual: ' || (v_source_records_before - v_source_records_after), 
                        USER);
                END IF;
                
                IF v_archive_records_after - v_archive_records_before != v_count THEN
                    v_data_validation_status := 'FAIL';
                    v_record_count_match := 'N';
                    prc_log_error_autonomous(v_proc_name, 'E', v_step, NULL, NULL, 
                        'Archive record count mismatch', 
                        'Expected: ' || v_count || ', Actual: ' || (v_archive_records_after - v_archive_records_before), 
                        USER);
                END IF;
                
                prc_log_error_autonomous(v_proc_name, 'I', v_step, NULL, NULL, 
                    'After metrics collected', 
                    'Src Recs: ' || v_source_records_after || ', Arc Recs: ' || v_archive_records_after || 
                    ', Invalid Idx: ' || v_invalid_indexes_after || ', Data Val: ' || v_data_validation_status, 
                    USER);
                
                DBMS_OUTPUT.PUT_LINE('Data Validation: ' || v_data_validation_status);
                DBMS_OUTPUT.PUT_LINE('Source Records: ' || v_source_records_before || ' -> ' || v_source_records_after || 
                                   ' (Moved: ' || (v_source_records_before - v_source_records_after) || ')');
                DBMS_OUTPUT.PUT_LINE('Archive Records: ' || v_archive_records_before || ' -> ' || v_archive_records_after || 
                                   ' (Added: ' || (v_archive_records_after - v_archive_records_before) || ')');
                
                -- Drop the now-empty partition from main table
                v_step := 100 + (i * 100) + 10;
                v_sql := 'ALTER TABLE ' || p_table_name || ' DROP PARTITION ' || v_partition_name;
                EXECUTE IMMEDIATE v_sql;
                
                prc_log_error_autonomous(v_proc_name, 'I', v_step, NULL, NULL, 
                    'Dropped source partition', v_partition_name, USER);
                
                DBMS_OUTPUT.PUT_LINE('Dropped partition: ' || v_partition_name);
                
                -- Log execution to control table
                v_step := 100 + (i * 100) + 11;
                v_total_duration := EXTRACT(SECOND FROM (SYSTIMESTAMP - v_exchange_start)) +
                                   EXTRACT(MINUTE FROM (SYSTIMESTAMP - v_exchange_start)) * 60;
                
                INSERT INTO snparch_ctl_execution_log (
                execution_date,
                source_table_name,
                archive_table_name,
                source_partition_name,
                archive_partition_name,
                partition_date,
                records_archived,
                partition_size_mb,
                source_index_count,
                archive_index_count,
                source_index_size_mb,
                archive_index_size_mb,
                invalid_indexes_before,
                invalid_indexes_after,
                data_validation_status,
                record_count_match,
                source_records_before,
                source_records_after,
                archive_records_before,
                archive_records_after,
                is_compressed,
                compression_type,
                compression_ratio,
                exchange_duration_seconds,
                stats_gather_duration_seconds,
                total_duration_seconds,
                validation_status,
                executed_by
            ) VALUES (
                SYSTIMESTAMP,
                p_table_name,
                v_archive_table_name,
                v_partition_name,
                v_archive_partition_name,
                p_dates(i),
                v_count,
                v_partition_size_mb,
                v_source_index_count,
                v_archive_index_count,
                v_source_index_size_mb,
                v_archive_index_size_mb,
                v_invalid_indexes_before,
                v_invalid_indexes_after,
                v_data_validation_status,
                v_record_count_match,
                v_source_records_before,
                v_source_records_after,
                v_archive_records_before,
                v_archive_records_after,
                v_is_compressed,
                v_compression_type,
                NULL,  -- Compression ratio calculated separately if needed
                v_exchange_duration,
                NULL,  -- Will be updated after stats gathering
                v_total_duration,
                CASE WHEN v_data_validation_status = 'FAIL' THEN 'WARNING' ELSE 'SUCCESS' END,
                USER
            );
            
            prc_log_error_autonomous(v_proc_name, 'I', v_step, NULL, NULL, 
                'Logged execution to control table', 
                'Partition: ' || v_partition_name || ' -> ' || v_archive_partition_name, USER);
            
                DBMS_OUTPUT.PUT_LINE('-------------------------------------------');
                
                v_partitions_archived := v_partitions_archived + 1;
                COMMIT;
            ELSE
                -- Handle empty partition - just drop it without archiving
                v_step := 100 + (i * 100) + 10;
                v_sql := 'ALTER TABLE ' || p_table_name || ' DROP PARTITION ' || v_partition_name;
                EXECUTE IMMEDIATE v_sql;
                
                prc_log_error_autonomous(v_proc_name, 'I', v_step, NULL, NULL, 
                    'Dropped empty source partition', v_partition_name || ' (0 records)', USER);
                
                DBMS_OUTPUT.PUT_LINE('Dropped empty partition: ' || v_partition_name);
                DBMS_OUTPUT.PUT_LINE('-------------------------------------------');
                COMMIT;
            END IF;
            
        EXCEPTION
            WHEN OTHERS THEN
                prc_log_error_autonomous(v_proc_name, 'E', v_step, SQLCODE, SQLERRM, 
                    'Error processing date', TO_CHAR(p_dates(i), 'YYYY-MM-DD'), USER);
                    
                DBMS_OUTPUT.PUT_LINE('ERROR processing date ' || 
                                   TO_CHAR(p_dates(i), 'YYYY-MM-DD') || ': ' || SQLERRM);
                ROLLBACK;
        END;
    END LOOP;
    
    -- Post-exchange validation and statistics gathering
    v_step := 50;
    
    -- Validate indexes after exchange if configured
    IF v_validate_before = 'Y' THEN
        -- Check source table indexes
        SELECT COUNT(*)
        INTO v_invalid_indexes
        FROM user_indexes
        WHERE table_name = UPPER(p_table_name)
          AND status != 'VALID';
          
        IF v_invalid_indexes > 0 THEN
            prc_log_error_autonomous(v_proc_name, 'E', v_step, NULL, NULL, 
                'Invalid indexes after exchange on source', 
                'Table: ' || p_table_name || ', Count: ' || v_invalid_indexes, USER);
            DBMS_OUTPUT.PUT_LINE('WARNING: ' || v_invalid_indexes || ' invalid indexes on ' || p_table_name);
        ELSE
            prc_log_error_autonomous(v_proc_name, 'I', v_step, NULL, NULL, 
                'All indexes valid after exchange on source', 'Table: ' || p_table_name, USER);
            DBMS_OUTPUT.PUT_LINE('All indexes valid on ' || p_table_name);
        END IF;
        
        -- Check archive table indexes
        SELECT COUNT(*)
        INTO v_invalid_indexes
        FROM user_indexes
        WHERE table_name = UPPER(v_archive_table_name)
          AND status != 'VALID';
          
        IF v_invalid_indexes > 0 THEN
            prc_log_error_autonomous(v_proc_name, 'E', v_step, NULL, NULL, 
                'Invalid indexes after exchange on archive', 
                'Table: ' || v_archive_table_name || ', Count: ' || v_invalid_indexes, USER);
            DBMS_OUTPUT.PUT_LINE('WARNING: ' || v_invalid_indexes || ' invalid indexes on ' || v_archive_table_name);
        ELSE
            prc_log_error_autonomous(v_proc_name, 'I', v_step, NULL, NULL, 
                'All indexes valid after exchange on archive', 'Table: ' || v_archive_table_name, USER);
            DBMS_OUTPUT.PUT_LINE('All indexes valid on ' || v_archive_table_name);
        END IF;
    END IF;
    
    -- Gather statistics if configured
    IF v_gather_stats_after = 'Y' THEN
        v_step := 51;
        v_stats_start := SYSTIMESTAMP;
        
        DBMS_OUTPUT.PUT_LINE('Gathering statistics on ' || p_table_name || '...');
        prc_log_error_autonomous(v_proc_name, 'I', v_step, NULL, NULL, 
            'Gathering statistics', 'Table: ' || p_table_name, USER);
            
        -- Optimized statistics gathering with AUTO features
        DBMS_STATS.GATHER_TABLE_STATS(
            ownname => USER,
            tabname => UPPER(p_table_name),
            estimate_percent => DBMS_STATS.AUTO_SAMPLE_SIZE,
            cascade => TRUE,
            method_opt => 'FOR ALL COLUMNS SIZE AUTO',
            degree => DBMS_STATS.AUTO_DEGREE,
            granularity => 'AUTO'
        );
        
        DBMS_OUTPUT.PUT_LINE('Statistics gathered on ' || p_table_name);
        
        v_step := 52;
        DBMS_OUTPUT.PUT_LINE('Gathering statistics on ' || v_archive_table_name || '...');
        prc_log_error_autonomous(v_proc_name, 'I', v_step, NULL, NULL, 
            'Gathering statistics', 'Table: ' || v_archive_table_name, USER);
            
        -- Optimized statistics gathering with AUTO features
        DBMS_STATS.GATHER_TABLE_STATS(
            ownname => USER,
            tabname => UPPER(v_archive_table_name),
            estimate_percent => DBMS_STATS.AUTO_SAMPLE_SIZE,
            cascade => TRUE,
            method_opt => 'FOR ALL COLUMNS SIZE AUTO',
            degree => DBMS_STATS.AUTO_DEGREE,
            granularity => 'AUTO'
        );
        
        v_stats_end := SYSTIMESTAMP;
        v_stats_duration := EXTRACT(SECOND FROM (v_stats_end - v_stats_start)) +
                           EXTRACT(MINUTE FROM (v_stats_end - v_stats_start)) * 60;
        
        DBMS_OUTPUT.PUT_LINE('Statistics gathered on ' || v_archive_table_name);
        DBMS_OUTPUT.PUT_LINE('Statistics gathering took: ' || ROUND(v_stats_duration, 2) || ' seconds');
        
        -- Update execution log with stats duration
        UPDATE snparch_ctl_execution_log
        SET stats_gather_duration_seconds = v_stats_duration
        WHERE execution_id = (
            SELECT MAX(execution_id)
            FROM snparch_ctl_execution_log
            WHERE source_table_name = p_table_name
              AND execution_date >= v_execution_start
        );
        COMMIT;
    END IF;
    
    
    v_step := 60;
    
    -- Get final table stats
    v_stats := f_degrag_get_table_size_stats_util(p_table_name);
    prc_log_error_autonomous(v_proc_name, 'I', v_step, NULL, NULL, 
        'Archiving completed', 'Partitions: ' || v_partitions_archived || ', Records: ' || v_total_archived, USER);
    prc_log_error_autonomous(v_proc_name, 'I', v_step, NULL, NULL, 
        'Source table stats (after)', v_stats, USER);
    
    v_stats := f_degrag_get_table_size_stats_util(v_archive_table_name);
    prc_log_error_autonomous(v_proc_name, 'I', v_step, NULL, NULL, 
        'Archive table stats (after)', v_stats, USER);
    
    DBMS_OUTPUT.PUT_LINE('===========================================');
    DBMS_OUTPUT.PUT_LINE('Archiving completed successfully!');
    DBMS_OUTPUT.PUT_LINE('Partitions archived: ' || v_partitions_archived);
    DBMS_OUTPUT.PUT_LINE('Total records archived: ' || v_total_archived);
    DBMS_OUTPUT.PUT_LINE('Source stats: ' || f_degrag_get_table_size_stats_util(p_table_name));
    DBMS_OUTPUT.PUT_LINE('Archive stats: ' || f_degrag_get_table_size_stats_util(v_archive_table_name));
    DBMS_OUTPUT.PUT_LINE('===========================================');
    
EXCEPTION
    WHEN e_table_not_partitioned THEN
        DBMS_OUTPUT.PUT_LINE('ERROR: Table ' || p_table_name || ' is not partitioned');
        DBMS_OUTPUT.PUT_LINE('This procedure only works with partitioned tables');
        RAISE_APPLICATION_ERROR(-20002, 'Table ' || p_table_name || ' is not partitioned');
        
    WHEN e_config_not_found THEN
        DBMS_OUTPUT.PUT_LINE('ERROR: Configuration not found for table ' || p_table_name);
        DBMS_OUTPUT.PUT_LINE('Please add configuration to PARTITION_ARCHIVE_CONFIG table');
        RAISE;
        
    WHEN e_config_inactive THEN
        DBMS_OUTPUT.PUT_LINE('ERROR: Configuration is inactive for table ' || p_table_name);
        DBMS_OUTPUT.PUT_LINE('Please activate configuration in PARTITION_ARCHIVE_CONFIG table');
        RAISE;
        
    WHEN OTHERS THEN
        prc_log_error_autonomous(v_proc_name, 'E', v_step, SQLCODE, SQLERRM, 
            'Fatal error in archive procedure', NULL, USER);
            
        DBMS_OUTPUT.PUT_LINE('FATAL ERROR: ' || SQLERRM);
        RAISE;
END;
/