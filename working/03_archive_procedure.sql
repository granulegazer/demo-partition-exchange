-- ========================================
-- HELPER FUNCTION: Get Partition Name by Date
-- ========================================

CREATE OR REPLACE FUNCTION get_partition_name_by_date(
    p_table_name IN VARCHAR2,
    p_date IN DATE
) RETURN VARCHAR2
IS
    v_partition_name VARCHAR2(128);
    v_high_value DATE;
BEGIN
    -- Find partition where the high_value matches the date + 1
    -- (since partition high_value is exclusive boundary)
    SELECT partition_name
    INTO v_partition_name
    FROM user_tab_partitions
    WHERE table_name = UPPER(p_table_name)
      AND partition_name != 'SALES_OLD'
      AND TO_DATE(
            TRIM(BOTH '''' FROM REGEXP_SUBSTR(high_value, '''[^'']+''')),
            'YYYY-MM-DD'
          ) = p_date + 1;
    
    RETURN v_partition_name;
EXCEPTION
    WHEN NO_DATA_FOUND THEN
        RETURN NULL;
    WHEN TOO_MANY_ROWS THEN
        -- Should not happen with proper partitioning
        RETURN NULL;
    WHEN OTHERS THEN
        DBMS_OUTPUT.PUT_LINE('ERROR in get_partition_name_by_date: ' || SQLERRM);
        RETURN NULL;
END;
/

-- ========================================
-- ARCHIVE PROCEDURE: Multiple Dates Support
-- ========================================

CREATE OR REPLACE PROCEDURE archive_partitions_by_dates (
    p_table_name IN VARCHAR2,
    p_dates IN date_array_type
) AS
    v_partition_name VARCHAR2(128);
    v_archive_partition_name VARCHAR2(128);
    v_sql VARCHAR2(4000);
    v_count NUMBER;
    v_total_archived NUMBER := 0;
    v_partitions_archived NUMBER := 0;
    v_staging_table VARCHAR2(128) := 'sales_staging_temp';
    v_step NUMBER := 0;
    v_proc_name VARCHAR2(30) := 'ARCHIVE_PARTITIONS';
BEGIN
    v_step := 1;
    prc_log_error_autonomous(v_proc_name, 'I', v_step, NULL, NULL, 
        'Starting partition archiving', 'Table: ' || p_table_name || ', Dates: ' || p_dates.COUNT, USER);
    
    DBMS_OUTPUT.PUT_LINE('===========================================');
    DBMS_OUTPUT.PUT_LINE('Starting partition archiving');
    DBMS_OUTPUT.PUT_LINE('Table: ' || p_table_name);
    DBMS_OUTPUT.PUT_LINE('Dates to archive: ' || p_dates.COUNT);
    DBMS_OUTPUT.PUT_LINE('===========================================');
    
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
            v_archive_partition_name := get_partition_name_by_date(p_table_name || '_ARCHIVE', p_dates(i));
            
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
                
                -- Create matching indexes for exchange
                v_step := v_step + 1;
                EXECUTE IMMEDIATE 'CREATE INDEX idx_staging_date ON ' || v_staging_table || '(sale_date)';
                EXECUTE IMMEDIATE 'CREATE INDEX idx_staging_customer ON ' || v_staging_table || '(customer_id)';
                EXECUTE IMMEDIATE 'CREATE INDEX idx_staging_region ON ' || v_staging_table || '(region)';
                
                prc_log_error_autonomous(v_proc_name, 'I', v_step, NULL, NULL, 
                    'Created indexes on staging table', v_staging_table, USER);
                
                -- Step 1: Exchange partition from main to staging (instant)
                v_step := v_step + 1;
                v_sql := 'ALTER TABLE ' || p_table_name || 
                         ' EXCHANGE PARTITION ' || v_partition_name || 
                         ' WITH TABLE ' || v_staging_table || 
                         ' INCLUDING INDEXES WITHOUT VALIDATION';
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
                    v_sql := 'INSERT INTO ' || p_table_name || '_ARCHIVE ' ||
                             'SELECT * FROM ' || v_staging_table || ' WHERE ROWNUM = 1';
                    EXECUTE IMMEDIATE v_sql;
                    COMMIT;
                    
                    -- Get the newly created partition name
                    v_archive_partition_name := get_partition_name_by_date(
                        p_table_name || '_ARCHIVE', 
                        p_dates(i)
                    );
                    
                    prc_log_error_autonomous(v_proc_name, 'I', v_step, NULL, NULL, 
                        'Created archive partition', v_archive_partition_name, USER);
                    
                    -- Delete the test row
                    v_sql := 'DELETE FROM ' || p_table_name || '_ARCHIVE ' ||
                             'PARTITION (' || v_archive_partition_name || ')';
                    EXECUTE IMMEDIATE v_sql;
                    COMMIT;
                END IF;
                
                v_step := v_step + 1;
                v_sql := 'ALTER TABLE ' || p_table_name || '_ARCHIVE ' ||
                         'EXCHANGE PARTITION ' || v_archive_partition_name ||
                         ' WITH TABLE ' || v_staging_table || 
                         ' INCLUDING INDEXES WITHOUT VALIDATION';
                EXECUTE IMMEDIATE v_sql;
                
                prc_log_error_autonomous(v_proc_name, 'I', v_step, NULL, NULL, 
                    'Exchanged staging to archive', 'Partition: ' || v_archive_partition_name || ', Records: ' || v_count, USER);
                
                DBMS_OUTPUT.PUT_LINE('Step 2: Data moved to archive (instant)');
                
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
    
    v_step := v_step + 1;
    prc_log_error_autonomous(v_proc_name, 'I', v_step, NULL, NULL, 
        'Archiving completed', 'Partitions: ' || v_partitions_archived || ', Records: ' || v_total_archived, USER);
    
    DBMS_OUTPUT.PUT_LINE('===========================================');
    DBMS_OUTPUT.PUT_LINE('Archiving completed successfully!');
    DBMS_OUTPUT.PUT_LINE('Partitions archived: ' || v_partitions_archived);
    DBMS_OUTPUT.PUT_LINE('Total records archived: ' || v_total_archived);
    DBMS_OUTPUT.PUT_LINE('===========================================');
    
EXCEPTION
    WHEN OTHERS THEN
        prc_log_error_autonomous(v_proc_name, 'E', v_step, SQLCODE, SQLERRM, 
            'Fatal error in archive procedure', NULL, USER);
            
        DBMS_OUTPUT.PUT_LINE('FATAL ERROR: ' || SQLERRM);
        RAISE;
END;
/