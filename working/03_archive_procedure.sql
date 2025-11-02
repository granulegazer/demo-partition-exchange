-- ========================================
-- ARCHIVE PROCEDURE: Multiple Dates Support
-- ========================================

CREATE OR REPLACE PROCEDURE archive_partitions_by_dates (
    p_dates IN date_array_type  -- Custom type for compatibility
) AS
    v_partition_name VARCHAR2(128);
    v_partition_date DATE;
    v_sql VARCHAR2(4000);
    v_count NUMBER;
    v_total_archived NUMBER := 0;
    v_partitions_archived NUMBER := 0;
    
    -- Find partitions matching the supplied dates
    CURSOR c_partitions IS
        SELECT DISTINCT
            p.partition_name,
            TO_DATE(
                TRIM(BOTH '''' FROM REGEXP_SUBSTR(p.high_value, '''[^'']+''')),
                'YYYY-MM-DD'
            ) AS partition_date
        FROM user_tab_partitions p,
             TABLE(p_dates) d
        WHERE p.table_name = 'SALES'
          AND p.partition_name != 'P_INITIAL'
          AND TO_DATE(
                TRIM(BOTH '''' FROM REGEXP_SUBSTR(p.high_value, '''[^'']+''')),
                'YYYY-MM-DD'
              ) = d.COLUMN_VALUE + 1  -- Partition high_value is next day
        ORDER BY partition_date;
BEGIN
    DBMS_OUTPUT.PUT_LINE('===========================================');
    DBMS_OUTPUT.PUT_LINE('Starting partition archiving for ' || p_dates.COUNT || ' dates');
    DBMS_OUTPUT.PUT_LINE('===========================================');
    
    -- Show dates to be archived
    FOR i IN 1..p_dates.COUNT LOOP
        DBMS_OUTPUT.PUT_LINE('Date ' || i || ': ' || TO_CHAR(p_dates(i), 'YYYY-MM-DD'));
    END LOOP;
    DBMS_OUTPUT.PUT_LINE('-------------------------------------------');
    
    -- Process each matching partition
    FOR rec IN c_partitions LOOP
        BEGIN
            DBMS_OUTPUT.PUT_LINE('Processing partition: ' || rec.partition_name);
            DBMS_OUTPUT.PUT_LINE('Partition date: ' || TO_CHAR(rec.partition_date - 1, 'YYYY-MM-DD'));
            
            -- Count records in partition
            v_sql := 'SELECT COUNT(*) FROM sales PARTITION (' || rec.partition_name || ')';
            EXECUTE IMMEDIATE v_sql INTO v_count;
            DBMS_OUTPUT.PUT_LINE('Records found: ' || v_count);
            
            IF v_count > 0 THEN
                -- Create temporary staging table
                EXECUTE IMMEDIATE 'CREATE TABLE sales_staging_temp AS SELECT * FROM sales WHERE 1=0';
                
                -- Create matching indexes for exchange
                EXECUTE IMMEDIATE 'CREATE INDEX idx_staging_date ON sales_staging_temp(sale_date)';
                EXECUTE IMMEDIATE 'CREATE INDEX idx_staging_customer ON sales_staging_temp(customer_id)';
                EXECUTE IMMEDIATE 'CREATE INDEX idx_staging_region ON sales_staging_temp(region)';
                
                -- Exchange partition with staging table
                v_sql := 'ALTER TABLE sales EXCHANGE PARTITION ' || rec.partition_name || 
                         ' WITH TABLE sales_staging_temp INCLUDING INDEXES WITHOUT VALIDATION';
                EXECUTE IMMEDIATE v_sql;
                
                -- Exchange staging with archive partition
                -- The partition will be auto-created in archive table
                v_sql := 'ALTER TABLE sales_archive EXCHANGE PARTITION ' || rec.partition_name ||
                         ' WITH TABLE sales_staging_temp INCLUDING INDEXES WITHOUT VALIDATION';
                EXECUTE IMMEDIATE v_sql;
                
                v_total_archived := v_total_archived + v_count;
                COMMIT;
                
                DBMS_OUTPUT.PUT_LINE('Archived ' || v_count || ' records');
                
                -- Drop staging table
                EXECUTE IMMEDIATE 'DROP TABLE sales_staging_temp';
            END IF;
            
            -- Drop the now-empty partition from main table
            v_sql := 'ALTER TABLE sales DROP PARTITION ' || rec.partition_name;
            EXECUTE IMMEDIATE v_sql;
            DBMS_OUTPUT.PUT_LINE('Dropped partition: ' || rec.partition_name);
            DBMS_OUTPUT.PUT_LINE('-------------------------------------------');
            
            v_partitions_archived := v_partitions_archived + 1;
            
        EXCEPTION
            WHEN OTHERS THEN
                DBMS_OUTPUT.PUT_LINE('ERROR processing partition ' || rec.partition_name || ': ' || SQLERRM);
                -- Clean up if staging exists
                BEGIN
                    EXECUTE IMMEDIATE 'DROP TABLE sales_staging_temp';
                EXCEPTION WHEN OTHERS THEN NULL;
                END;
                ROLLBACK;
        END;
    END LOOP;
    
    DBMS_OUTPUT.PUT_LINE('===========================================');
    DBMS_OUTPUT.PUT_LINE('Archiving completed successfully!');
    DBMS_OUTPUT.PUT_LINE('Partitions archived: ' || v_partitions_archived);
    DBMS_OUTPUT.PUT_LINE('Total records archived: ' || v_total_archived);
    DBMS_OUTPUT.PUT_LINE('===========================================');
    
EXCEPTION
    WHEN OTHERS THEN
        DBMS_OUTPUT.PUT_LINE('FATAL ERROR: ' || SQLERRM);
        RAISE;
END;
/