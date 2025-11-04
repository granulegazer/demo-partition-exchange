-- ========================================
-- HELPER FUNCTION: Get Partition Name by Date
-- ========================================

CREATE OR REPLACE FUNCTION get_partition_name_by_date(
    p_table_name IN VARCHAR2,
    p_date IN DATE
) RETURN VARCHAR2
IS
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
-- Oracle 19.26 Optimized
-- ========================================

CREATE OR REPLACE PROCEDURE archive_partitions_by_dates (
    p_table_name IN VARCHAR2,
    p_dates IN date_array_type
) AS
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
    
    -- Execution logging variables (Oracle 19c)
    v_execution_start TIMESTAMP(6);
    v_execution_end TIMESTAMP(6);
    v_exchange_start TIMESTAMP(6);
    v_exchange_end TIMESTAMP(6);
    v_stats_start TIMESTAMP(6);
    v_stats_end TIMESTAMP(6);
    v_exchange_duration NUMBER;
    v_stats_duration NUMBER;
    v_partition_size_mb NUMBER(12,2);
    v_is_compressed VARCHAR2(1);
    v_compression_ratio NUMBER(5,2);
    
    -- Validation variables
    v_invalid_indexes NUMBER;
    v_stale_stats NUMBER;
    
    -- Exception for configuration not found
    e_config_not_found EXCEPTION;
    e_config_inactive EXCEPTION;
    e_invalid_indexes_found EXCEPTION;
    
BEGIN
    v_execution_start := SYSTIMESTAMP;
    v_step := 1;
    
    -- Get configuration for this table
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
          AND archive_table_name IS NOT NULL;
          
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
    
    -- Get initial table stats
    v_step := v_step + 1;
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
        v_step := v_step + 1;
        
        -- Check for invalid indexes on source table
        SELECT COUNT(*)
        INTO v_invalid_indexes
        FROM user_indexes
        WHERE table_name = UPPER(p_table_name)
          AND status != 'VALID';
          
        IF v_invalid_indexes > 0 THEN
            prc_log_error_autonomous(v_proc_name, 'W', v_step, NULL, NULL, 
                'Invalid indexes found on source', 
                'Table: ' || p_table_name || ', Count: ' || v_invalid_indexes, USER);
            DBMS_OUTPUT.PUT_LINE('WARNING: ' || v_invalid_indexes || ' invalid indexes found on ' || p_table_name);
            
            -- Rebuild invalid indexes
            FOR idx IN (
                SELECT index_name 
                FROM user_indexes 
                WHERE table_name = UPPER(p_table_name)
                  AND status != 'VALID'
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
        
        -- Check for invalid indexes on archive table
        SELECT COUNT(*)
        INTO v_invalid_indexes
        FROM user_indexes
        WHERE table_name = UPPER(v_archive_table_name)
          AND status != 'VALID';
          
        IF v_invalid_indexes > 0 THEN
            prc_log_error_autonomous(v_proc_name, 'W', v_step, NULL, NULL, 
                'Invalid indexes found on archive', 
                'Table: ' || v_archive_table_name || ', Count: ' || v_invalid_indexes, USER);
            DBMS_OUTPUT.PUT_LINE('WARNING: ' || v_invalid_indexes || ' invalid indexes found on ' || v_archive_table_name);
            
            -- Rebuild invalid indexes
            FOR idx IN (
                SELECT index_name 
                FROM user_indexes 
                WHERE table_name = UPPER(v_archive_table_name)
                  AND status != 'VALID'
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
            v_step := v_step + 1;
            prc_log_error_autonomous(v_proc_name, 'I', v_step, NULL, NULL, 
                'Processing date', TO_CHAR(p_dates(i), 'YYYY-MM-DD'), USER);
            
            -- Get partition name for this date
            v_partition_name := get_partition_name_by_date(p_table_name, p_dates(i));
            
            IF v_partition_name IS NULL THEN
                v_step := v_step + 1;
                prc_log_error_autonomous(v_proc_name, 'I', v_step, NULL, NULL, 
                    'No partition found for date', TO_CHAR(p_dates(i), 'YYYY-MM-DD'), USER);
                    
                DBMS_OUTPUT.PUT_LINE('WARNING: No partition found for date ' || 
                                   TO_CHAR(p_dates(i), 'YYYY-MM-DD'));
                DBMS_OUTPUT.PUT_LINE('-------------------------------------------');
                CONTINUE;
            END IF;
            
            v_step := v_step + 1;
            prc_log_error_autonomous(v_proc_name, 'I', v_step, NULL, NULL, 
                'Found partition', 'Partition: ' || v_partition_name || ', Date: ' || TO_CHAR(p_dates(i), 'YYYY-MM-DD'), USER);
            
            DBMS_OUTPUT.PUT_LINE('Processing date: ' || TO_CHAR(p_dates(i), 'YYYY-MM-DD'));
            DBMS_OUTPUT.PUT_LINE('Partition name: ' || v_partition_name);
            
            -- Get partition name for archive table
            v_archive_partition_name := get_partition_name_by_date(v_archive_table_name, p_dates(i));
            
            -- Count records in partition
            v_step := v_step + 1;
            v_sql := 'SELECT COUNT(*) FROM ' || p_table_name || 
                     ' PARTITION (' || v_partition_name || ')';
            EXECUTE IMMEDIATE v_sql INTO v_count;
            
            prc_log_error_autonomous(v_proc_name, 'I', v_step, NULL, NULL, 
                'Counted records in partition', 'Count: ' || v_count || ', Partition: ' || v_partition_name, USER);
            
            DBMS_OUTPUT.PUT_LINE('Records found: ' || v_count);
            
            IF v_count > 0 THEN
                -- Create temporary staging table from template
                v_step := v_step + 1;
                v_sql := 'CREATE TABLE ' || v_staging_table || 
                         ' AS SELECT * FROM ' || p_table_name || ' WHERE 1=0';
                EXECUTE IMMEDIATE v_sql;
                
                prc_log_error_autonomous(v_proc_name, 'I', v_step, NULL, NULL, 
                    'Created staging table', v_staging_table, USER);
                
                -- Add primary key constraint to match source table
                v_step := v_step + 1;
                EXECUTE IMMEDIATE 'ALTER TABLE ' || v_staging_table || 
                                ' ADD CONSTRAINT pk_staging_temp PRIMARY KEY (sale_id, sale_date)';
                
                prc_log_error_autonomous(v_proc_name, 'I', v_step, NULL, NULL, 
                    'Added primary key to staging table', v_staging_table, USER);
                
                -- Step 1: Exchange partition from main to staging (instant)
                -- Note: No indexes on staging - they will be exchanged automatically
                v_step := v_step + 1;
                v_exchange_start := SYSTIMESTAMP;
                
                v_sql := 'ALTER TABLE ' || p_table_name || 
                         ' EXCHANGE PARTITION ' || v_partition_name || 
                         ' WITH TABLE ' || v_staging_table || 
                         ' WITHOUT VALIDATION';
                EXECUTE IMMEDIATE v_sql;
                
                prc_log_error_autonomous(v_proc_name, 'I', v_step, NULL, NULL, 
                    'Exchanged partition to staging', 'Partition: ' || v_partition_name, USER);
                
                DBMS_OUTPUT.PUT_LINE('Step 1: Partition moved to staging table (instant)');
                
                -- Step 2: Exchange staging with archive partition (instant)
                v_step := v_step + 1;
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
                
                v_step := v_step + 1;
                v_sql := 'ALTER TABLE ' || v_archive_table_name ||
                         ' EXCHANGE PARTITION ' || v_archive_partition_name ||
                         ' WITH TABLE ' || v_staging_table || 
                         ' WITHOUT VALIDATION';
                EXECUTE IMMEDIATE v_sql;
                
                v_exchange_end := SYSTIMESTAMP;
                v_exchange_duration := EXTRACT(SECOND FROM (v_exchange_end - v_exchange_start)) +
                                      EXTRACT(MINUTE FROM (v_exchange_end - v_exchange_start)) * 60;
                
                prc_log_error_autonomous(v_proc_name, 'I', v_step, NULL, NULL, 
                    'Exchanged staging to archive', 'Partition: ' || v_archive_partition_name || ', Records: ' || v_count, USER);
                
                DBMS_OUTPUT.PUT_LINE('Step 2: Data moved to archive (instant)');
                
                -- Get partition size (Oracle 19c optimized query)
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
                
                -- Check if partition is compressed (Oracle 19c)
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
                
                -- Drop staging table
                v_step := v_step + 1;
                EXECUTE IMMEDIATE 'DROP TABLE ' || v_staging_table;
                
                prc_log_error_autonomous(v_proc_name, 'I', v_step, NULL, NULL, 
                    'Dropped staging table', v_staging_table, USER);
            END IF;
            
            -- Drop the now-empty partition from main table
            v_step := v_step + 1;
            v_sql := 'ALTER TABLE ' || p_table_name || ' DROP PARTITION ' || v_partition_name;
            EXECUTE IMMEDIATE v_sql;
            
            prc_log_error_autonomous(v_proc_name, 'I', v_step, NULL, NULL, 
                'Dropped source partition', v_partition_name, USER);
            
            DBMS_OUTPUT.PUT_LINE('Dropped partition: ' || v_partition_name);
            
            -- Log execution to control table (Oracle 19c with identity column)
            v_step := v_step + 1;
            INSERT INTO snparch_ctl_execution_log (
                execution_date,
                source_table_name,
                archive_table_name,
                source_partition_name,
                archive_partition_name,
                partition_date,
                records_archived,
                partition_size_mb,
                is_compressed,
                compression_type,
                compression_ratio,
                exchange_duration_seconds,
                stats_gather_duration_seconds,
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
                v_is_compressed,
                v_compression_type,
                NULL,  -- Compression ratio calculated separately if needed
                v_exchange_duration,
                NULL,  -- Will be updated after stats gathering
                'SUCCESS',
                USER
            );
            
            prc_log_error_autonomous(v_proc_name, 'I', v_step, NULL, NULL, 
                'Logged execution to control table', 
                'Partition: ' || v_partition_name || ' -> ' || v_archive_partition_name, USER);
            
            DBMS_OUTPUT.PUT_LINE('-------------------------------------------');
            
            v_partitions_archived := v_partitions_archived + 1;
            COMMIT;
            
        EXCEPTION
            WHEN OTHERS THEN
                prc_log_error_autonomous(v_proc_name, 'E', v_step, SQLCODE, SQLERRM, 
                    'Error processing date', TO_CHAR(p_dates(i), 'YYYY-MM-DD'), USER);
                    
                DBMS_OUTPUT.PUT_LINE('ERROR processing date ' || 
                                   TO_CHAR(p_dates(i), 'YYYY-MM-DD') || ': ' || SQLERRM);
                -- Clean up if staging exists
                BEGIN
                    EXECUTE IMMEDIATE 'DROP TABLE ' || v_staging_table;
                EXCEPTION WHEN OTHERS THEN NULL;
                END;
                ROLLBACK;
        END;
    END LOOP;
    
    -- Post-exchange validation and statistics gathering
    v_step := v_step + 1;
    
    -- Validate indexes after exchange if configured
    IF v_validate_before = 'Y' THEN
        -- Check source table indexes
        SELECT COUNT(*)
        INTO v_invalid_indexes
        FROM user_indexes
        WHERE table_name = UPPER(p_table_name)
          AND status != 'VALID';
          
        IF v_invalid_indexes > 0 THEN
            prc_log_error_autonomous(v_proc_name, 'W', v_step, NULL, NULL, 
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
            prc_log_error_autonomous(v_proc_name, 'W', v_step, NULL, NULL, 
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
        v_step := v_step + 1;
        v_stats_start := SYSTIMESTAMP;
        
        DBMS_OUTPUT.PUT_LINE('Gathering statistics on ' || p_table_name || '...');
        prc_log_error_autonomous(v_proc_name, 'I', v_step, NULL, NULL, 
            'Gathering statistics', 'Table: ' || p_table_name, USER);
            
        -- Oracle 19c optimized statistics gathering
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
        
        v_step := v_step + 1;
        DBMS_OUTPUT.PUT_LINE('Gathering statistics on ' || v_archive_table_name || '...');
        prc_log_error_autonomous(v_proc_name, 'I', v_step, NULL, NULL, 
            'Gathering statistics', 'Table: ' || v_archive_table_name, USER);
            
        -- Oracle 19c optimized statistics gathering
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
    
    
    v_step := v_step + 1;
    
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