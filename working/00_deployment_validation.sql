-- ========================================
-- DEPLOYMENT VALIDATION SCRIPT
-- Oracle 19.26
-- ========================================
-- Purpose: Validates all objects are deployed correctly
-- Run this after deployment to verify installation
-- ========================================

SET SERVEROUTPUT ON SIZE UNLIMITED
SET LINESIZE 200
SET PAGESIZE 50000
SET VERIFY OFF
SET FEEDBACK OFF

PROMPT
PROMPT ========================================
PROMPT DEPLOYMENT VALIDATION SCRIPT
PROMPT Oracle 19.26 Partition Exchange Demo
PROMPT ========================================
PROMPT Started: 
SELECT TO_CHAR(SYSDATE, 'YYYY-MM-DD HH24:MI:SS') AS validation_start FROM dual;
PROMPT ========================================

-- Variables for tracking
DECLARE
    v_error_count NUMBER := 0;
    v_warning_count NUMBER := 0;
    v_check_count NUMBER := 0;
    v_count NUMBER;
    v_status VARCHAR2(10);
    v_message VARCHAR2(4000);
BEGIN
    DBMS_OUTPUT.PUT_LINE('');
    DBMS_OUTPUT.PUT_LINE('========================================');
    DBMS_OUTPUT.PUT_LINE('1. CHECKING TABLES');
    DBMS_OUTPUT.PUT_LINE('========================================');
    
    -- Check SALES table
    v_check_count := v_check_count + 1;
    BEGIN
        SELECT COUNT(*) INTO v_count 
        FROM user_tables 
        WHERE table_name = 'SALES';
        
        IF v_count = 1 THEN
            -- Check if partitioned
            SELECT partitioned INTO v_status
            FROM user_tables
            WHERE table_name = 'SALES';
            
            IF v_status = 'YES' THEN
                DBMS_OUTPUT.PUT_LINE('[PASS] SALES table exists and is partitioned');
            ELSE
                DBMS_OUTPUT.PUT_LINE('[FAIL] SALES table exists but is NOT partitioned');
                v_error_count := v_error_count + 1;
            END IF;
        ELSE
            DBMS_OUTPUT.PUT_LINE('[FAIL] SALES table does not exist');
            v_error_count := v_error_count + 1;
        END IF;
    EXCEPTION
        WHEN OTHERS THEN
            DBMS_OUTPUT.PUT_LINE('[ERROR] Error checking SALES table: ' || SQLERRM);
            v_error_count := v_error_count + 1;
    END;
    
    -- Check SALES_ARCHIVE table
    v_check_count := v_check_count + 1;
    BEGIN
        SELECT COUNT(*) INTO v_count 
        FROM user_tables 
        WHERE table_name = 'SALES_ARCHIVE';
        
        IF v_count = 1 THEN
            SELECT partitioned INTO v_status
            FROM user_tables
            WHERE table_name = 'SALES_ARCHIVE';
            
            IF v_status = 'YES' THEN
                DBMS_OUTPUT.PUT_LINE('[PASS] SALES_ARCHIVE table exists and is partitioned');
            ELSE
                DBMS_OUTPUT.PUT_LINE('[FAIL] SALES_ARCHIVE table exists but is NOT partitioned');
                v_error_count := v_error_count + 1;
            END IF;
        ELSE
            DBMS_OUTPUT.PUT_LINE('[FAIL] SALES_ARCHIVE table does not exist');
            v_error_count := v_error_count + 1;
        END IF;
    EXCEPTION
        WHEN OTHERS THEN
            DBMS_OUTPUT.PUT_LINE('[ERROR] Error checking SALES_ARCHIVE table: ' || SQLERRM);
            v_error_count := v_error_count + 1;
    END;
    
    -- Check SNPARCH_CNF_PARTITION_ARCHIVE table
    v_check_count := v_check_count + 1;
    BEGIN
        SELECT COUNT(*) INTO v_count 
        FROM user_tables 
        WHERE table_name = 'SNPARCH_CNF_PARTITION_ARCHIVE';
        
        IF v_count = 1 THEN
            DBMS_OUTPUT.PUT_LINE('[PASS] SNPARCH_CNF_PARTITION_ARCHIVE table exists');
        ELSE
            DBMS_OUTPUT.PUT_LINE('[FAIL] SNPARCH_CNF_PARTITION_ARCHIVE table does not exist');
            v_error_count := v_error_count + 1;
        END IF;
    EXCEPTION
        WHEN OTHERS THEN
            DBMS_OUTPUT.PUT_LINE('[ERROR] Error checking SNPARCH_CNF_PARTITION_ARCHIVE table: ' || SQLERRM);
            v_error_count := v_error_count + 1;
    END;
    
    -- Check SNPARCH_CTL_EXECUTION_LOG table
    v_check_count := v_check_count + 1;
    BEGIN
        SELECT COUNT(*) INTO v_count 
        FROM user_tables 
        WHERE table_name = 'SNPARCH_CTL_EXECUTION_LOG';
        
        IF v_count = 1 THEN
            DBMS_OUTPUT.PUT_LINE('[PASS] SNPARCH_CTL_EXECUTION_LOG table exists');
            
            -- Check for IDENTITY column
            SELECT COUNT(*) INTO v_count
            FROM user_tab_columns
            WHERE table_name = 'SNPARCH_CTL_EXECUTION_LOG'
              AND column_name = 'EXECUTION_ID'
              AND identity_column = 'YES';
            
            IF v_count = 1 THEN
                DBMS_OUTPUT.PUT_LINE('[PASS] EXECUTION_ID column has IDENTITY property');
            ELSE
                DBMS_OUTPUT.PUT_LINE('[WARN] EXECUTION_ID column does not have IDENTITY property');
                v_warning_count := v_warning_count + 1;
            END IF;
        ELSE
            DBMS_OUTPUT.PUT_LINE('[FAIL] SNPARCH_CTL_EXECUTION_LOG table does not exist');
            v_error_count := v_error_count + 1;
        END IF;
    EXCEPTION
        WHEN OTHERS THEN
            DBMS_OUTPUT.PUT_LINE('[ERROR] Error checking SNPARCH_CTL_EXECUTION_LOG table: ' || SQLERRM);
            v_error_count := v_error_count + 1;
    END;
    
    DBMS_OUTPUT.PUT_LINE('');
    DBMS_OUTPUT.PUT_LINE('========================================');
    DBMS_OUTPUT.PUT_LINE('2. CHECKING TABLE STRUCTURES');
    DBMS_OUTPUT.PUT_LINE('========================================');
    
    -- Check SALES column count
    v_check_count := v_check_count + 1;
    BEGIN
        SELECT COUNT(*) INTO v_count
        FROM user_tab_columns
        WHERE table_name = 'SALES';
        
        IF v_count >= 8 THEN
            DBMS_OUTPUT.PUT_LINE('[PASS] SALES table has ' || v_count || ' columns');
        ELSE
            DBMS_OUTPUT.PUT_LINE('[FAIL] SALES table has only ' || v_count || ' columns (expected 8+)');
            v_error_count := v_error_count + 1;
        END IF;
    EXCEPTION
        WHEN OTHERS THEN
            DBMS_OUTPUT.PUT_LINE('[ERROR] Error checking SALES columns: ' || SQLERRM);
            v_error_count := v_error_count + 1;
    END;
    
    -- Check SALES_ARCHIVE column count
    v_check_count := v_check_count + 1;
    BEGIN
        SELECT COUNT(*) INTO v_count
        FROM user_tab_columns
        WHERE table_name = 'SALES_ARCHIVE';
        
        IF v_count >= 8 THEN
            DBMS_OUTPUT.PUT_LINE('[PASS] SALES_ARCHIVE table has ' || v_count || ' columns');
        ELSE
            DBMS_OUTPUT.PUT_LINE('[FAIL] SALES_ARCHIVE table has only ' || v_count || ' columns (expected 8+)');
            v_error_count := v_error_count + 1;
        END IF;
    EXCEPTION
        WHEN OTHERS THEN
            DBMS_OUTPUT.PUT_LINE('[ERROR] Error checking SALES_ARCHIVE columns: ' || SQLERRM);
            v_error_count := v_error_count + 1;
    END;
    
    -- Check SNPARCH_CTL_EXECUTION_LOG column count (should have 38 columns)
    v_check_count := v_check_count + 1;
    BEGIN
        SELECT COUNT(*) INTO v_count
        FROM user_tab_columns
        WHERE table_name = 'SNPARCH_CTL_EXECUTION_LOG';
        
        IF v_count = 38 THEN
            DBMS_OUTPUT.PUT_LINE('[PASS] SNPARCH_CTL_EXECUTION_LOG has 38 columns (enhanced version)');
        ELSIF v_count >= 19 THEN
            DBMS_OUTPUT.PUT_LINE('[WARN] SNPARCH_CTL_EXECUTION_LOG has ' || v_count || ' columns (expected 38)');
            v_warning_count := v_warning_count + 1;
        ELSE
            DBMS_OUTPUT.PUT_LINE('[FAIL] SNPARCH_CTL_EXECUTION_LOG has only ' || v_count || ' columns (expected 38)');
            v_error_count := v_error_count + 1;
        END IF;
    EXCEPTION
        WHEN OTHERS THEN
            DBMS_OUTPUT.PUT_LINE('[ERROR] Error checking SNPARCH_CTL_EXECUTION_LOG columns: ' || SQLERRM);
            v_error_count := v_error_count + 1;
    END;
    
    -- Validate SALES and SALES_ARCHIVE have matching structures
    v_check_count := v_check_count + 1;
    BEGIN
        SELECT COUNT(*) INTO v_count
        FROM (
            SELECT column_name, data_type, data_length, data_precision, data_scale, nullable
            FROM user_tab_columns WHERE table_name = 'SALES'
            MINUS
            SELECT column_name, data_type, data_length, data_precision, data_scale, nullable
            FROM user_tab_columns WHERE table_name = 'SALES_ARCHIVE'
            UNION ALL
            SELECT column_name, data_type, data_length, data_precision, data_scale, nullable
            FROM user_tab_columns WHERE table_name = 'SALES_ARCHIVE'
            MINUS
            SELECT column_name, data_type, data_length, data_precision, data_scale, nullable
            FROM user_tab_columns WHERE table_name = 'SALES'
        );
        
        IF v_count = 0 THEN
            DBMS_OUTPUT.PUT_LINE('[PASS] SALES and SALES_ARCHIVE structures match');
        ELSE
            DBMS_OUTPUT.PUT_LINE('[FAIL] SALES and SALES_ARCHIVE structures differ (' || v_count || ' differences)');
            v_error_count := v_error_count + 1;
        END IF;
    EXCEPTION
        WHEN OTHERS THEN
            DBMS_OUTPUT.PUT_LINE('[ERROR] Error comparing table structures: ' || SQLERRM);
            v_error_count := v_error_count + 1;
    END;
    
    DBMS_OUTPUT.PUT_LINE('');
    DBMS_OUTPUT.PUT_LINE('========================================');
    DBMS_OUTPUT.PUT_LINE('3. CHECKING TYPES');
    DBMS_OUTPUT.PUT_LINE('========================================');
    
    -- Check DATE_ARRAY_TYPE
    v_check_count := v_check_count + 1;
    BEGIN
        SELECT COUNT(*) INTO v_count
        FROM user_types
        WHERE type_name = 'DATE_ARRAY_TYPE';
        
        IF v_count = 1 THEN
            DBMS_OUTPUT.PUT_LINE('[PASS] DATE_ARRAY_TYPE type exists');
        ELSE
            DBMS_OUTPUT.PUT_LINE('[FAIL] DATE_ARRAY_TYPE type does not exist');
            v_error_count := v_error_count + 1;
        END IF;
    EXCEPTION
        WHEN OTHERS THEN
            DBMS_OUTPUT.PUT_LINE('[ERROR] Error checking DATE_ARRAY_TYPE: ' || SQLERRM);
            v_error_count := v_error_count + 1;
    END;
    
    DBMS_OUTPUT.PUT_LINE('');
    DBMS_OUTPUT.PUT_LINE('========================================');
    DBMS_OUTPUT.PUT_LINE('4. CHECKING FUNCTIONS');
    DBMS_OUTPUT.PUT_LINE('========================================');
    
    -- Check get_partition_name_by_date function
    v_check_count := v_check_count + 1;
    BEGIN
        SELECT COUNT(*) INTO v_count
        FROM user_objects
        WHERE object_name = 'GET_PARTITION_NAME_BY_DATE'
          AND object_type = 'FUNCTION'
          AND status = 'VALID';
        
        IF v_count = 1 THEN
            DBMS_OUTPUT.PUT_LINE('[PASS] GET_PARTITION_NAME_BY_DATE function exists and is VALID');
        ELSE
            SELECT COUNT(*) INTO v_count
            FROM user_objects
            WHERE object_name = 'GET_PARTITION_NAME_BY_DATE'
              AND object_type = 'FUNCTION';
            
            IF v_count = 1 THEN
                DBMS_OUTPUT.PUT_LINE('[FAIL] GET_PARTITION_NAME_BY_DATE function exists but is INVALID');
                v_error_count := v_error_count + 1;
            ELSE
                DBMS_OUTPUT.PUT_LINE('[FAIL] GET_PARTITION_NAME_BY_DATE function does not exist');
                v_error_count := v_error_count + 1;
            END IF;
        END IF;
    EXCEPTION
        WHEN OTHERS THEN
            DBMS_OUTPUT.PUT_LINE('[ERROR] Error checking GET_PARTITION_NAME_BY_DATE: ' || SQLERRM);
            v_error_count := v_error_count + 1;
    END;
    
    DBMS_OUTPUT.PUT_LINE('');
    DBMS_OUTPUT.PUT_LINE('========================================');
    DBMS_OUTPUT.PUT_LINE('5. CHECKING PROCEDURES');
    DBMS_OUTPUT.PUT_LINE('========================================');
    
    -- Check archive_partitions_by_dates procedure
    v_check_count := v_check_count + 1;
    BEGIN
        SELECT COUNT(*) INTO v_count
        FROM user_objects
        WHERE object_name = 'ARCHIVE_PARTITIONS_BY_DATES'
          AND object_type = 'PROCEDURE'
          AND status = 'VALID';
        
        IF v_count = 1 THEN
            DBMS_OUTPUT.PUT_LINE('[PASS] ARCHIVE_PARTITIONS_BY_DATES procedure exists and is VALID');
        ELSE
            SELECT COUNT(*) INTO v_count
            FROM user_objects
            WHERE object_name = 'ARCHIVE_PARTITIONS_BY_DATES'
              AND object_type = 'PROCEDURE';
            
            IF v_count = 1 THEN
                DBMS_OUTPUT.PUT_LINE('[FAIL] ARCHIVE_PARTITIONS_BY_DATES procedure exists but is INVALID');
                v_error_count := v_error_count + 1;
            ELSE
                DBMS_OUTPUT.PUT_LINE('[FAIL] ARCHIVE_PARTITIONS_BY_DATES procedure does not exist');
                v_error_count := v_error_count + 1;
            END IF;
        END IF;
    EXCEPTION
        WHEN OTHERS THEN
            DBMS_OUTPUT.PUT_LINE('[ERROR] Error checking ARCHIVE_PARTITIONS_BY_DATES: ' || SQLERRM);
            v_error_count := v_error_count + 1;
    END;
    
    -- Check prc_log_error_autonomous procedure
    v_check_count := v_check_count + 1;
    BEGIN
        SELECT COUNT(*) INTO v_count
        FROM user_objects
        WHERE object_name = 'PRC_LOG_ERROR_AUTONOMOUS'
          AND object_type = 'PROCEDURE'
          AND status = 'VALID';
        
        IF v_count = 1 THEN
            DBMS_OUTPUT.PUT_LINE('[PASS] PRC_LOG_ERROR_AUTONOMOUS procedure exists and is VALID');
        ELSE
            SELECT COUNT(*) INTO v_count
            FROM user_objects
            WHERE object_name = 'PRC_LOG_ERROR_AUTONOMOUS'
              AND object_type = 'PROCEDURE';
            
            IF v_count = 1 THEN
                DBMS_OUTPUT.PUT_LINE('[FAIL] PRC_LOG_ERROR_AUTONOMOUS procedure exists but is INVALID');
                v_error_count := v_error_count + 1;
            ELSE
                DBMS_OUTPUT.PUT_LINE('[FAIL] PRC_LOG_ERROR_AUTONOMOUS procedure does not exist');
                v_error_count := v_error_count + 1;
            END IF;
        END IF;
    EXCEPTION
        WHEN OTHERS THEN
            DBMS_OUTPUT.PUT_LINE('[ERROR] Error checking PRC_LOG_ERROR_AUTONOMOUS: ' || SQLERRM);
            v_error_count := v_error_count + 1;
    END;
    
    -- Check f_degrag_get_table_size_stats_util function
    v_check_count := v_check_count + 1;
    BEGIN
        SELECT COUNT(*) INTO v_count
        FROM user_objects
        WHERE object_name = 'F_DEGRAG_GET_TABLE_SIZE_STATS_UTIL'
          AND object_type = 'FUNCTION'
          AND status = 'VALID';
        
        IF v_count = 1 THEN
            DBMS_OUTPUT.PUT_LINE('[PASS] F_DEGRAG_GET_TABLE_SIZE_STATS_UTIL function exists and is VALID');
        ELSE
            SELECT COUNT(*) INTO v_count
            FROM user_objects
            WHERE object_name = 'F_DEGRAG_GET_TABLE_SIZE_STATS_UTIL'
              AND object_type = 'FUNCTION';
            
            IF v_count = 1 THEN
                DBMS_OUTPUT.PUT_LINE('[FAIL] F_DEGRAG_GET_TABLE_SIZE_STATS_UTIL function exists but is INVALID');
                v_error_count := v_error_count + 1;
            ELSE
                DBMS_OUTPUT.PUT_LINE('[FAIL] F_DEGRAG_GET_TABLE_SIZE_STATS_UTIL function does not exist');
                v_error_count := v_error_count + 1;
            END IF;
        END IF;
    EXCEPTION
        WHEN OTHERS THEN
            DBMS_OUTPUT.PUT_LINE('[ERROR] Error checking F_DEGRAG_GET_TABLE_SIZE_STATS_UTIL: ' || SQLERRM);
            v_error_count := v_error_count + 1;
    END;
    
    DBMS_OUTPUT.PUT_LINE('');
    DBMS_OUTPUT.PUT_LINE('========================================');
    DBMS_OUTPUT.PUT_LINE('6. CHECKING VIEWS');
    DBMS_OUTPUT.PUT_LINE('========================================');
    
    -- Check SALES_COMPLETE view
    v_check_count := v_check_count + 1;
    BEGIN
        SELECT COUNT(*) INTO v_count
        FROM user_views
        WHERE view_name = 'SALES_COMPLETE';
        
        IF v_count = 1 THEN
            DBMS_OUTPUT.PUT_LINE('[PASS] SALES_COMPLETE view exists');
        ELSE
            DBMS_OUTPUT.PUT_LINE('[WARN] SALES_COMPLETE view does not exist (optional)');
            v_warning_count := v_warning_count + 1;
        END IF;
    EXCEPTION
        WHEN OTHERS THEN
            DBMS_OUTPUT.PUT_LINE('[ERROR] Error checking SALES_COMPLETE view: ' || SQLERRM);
            v_warning_count := v_warning_count + 1;
    END;
    
    DBMS_OUTPUT.PUT_LINE('');
    DBMS_OUTPUT.PUT_LINE('========================================');
    DBMS_OUTPUT.PUT_LINE('7. CHECKING INDEXES');
    DBMS_OUTPUT.PUT_LINE('========================================');
    
    -- Check SALES indexes
    v_check_count := v_check_count + 1;
    BEGIN
        SELECT COUNT(*) INTO v_count
        FROM user_indexes
        WHERE table_name = 'SALES'
          AND status = 'VALID';
        
        IF v_count >= 2 THEN
            DBMS_OUTPUT.PUT_LINE('[PASS] SALES table has ' || v_count || ' valid indexes');
        ELSIF v_count > 0 THEN
            DBMS_OUTPUT.PUT_LINE('[WARN] SALES table has only ' || v_count || ' valid indexes');
            v_warning_count := v_warning_count + 1;
        ELSE
            DBMS_OUTPUT.PUT_LINE('[WARN] SALES table has no valid indexes');
            v_warning_count := v_warning_count + 1;
        END IF;
        
        -- Check for invalid indexes
        SELECT COUNT(*) INTO v_count
        FROM user_indexes
        WHERE table_name = 'SALES'
          AND status != 'VALID';
        
        IF v_count > 0 THEN
            DBMS_OUTPUT.PUT_LINE('[WARN] SALES table has ' || v_count || ' INVALID indexes');
            v_warning_count := v_warning_count + 1;
        END IF;
    EXCEPTION
        WHEN OTHERS THEN
            DBMS_OUTPUT.PUT_LINE('[ERROR] Error checking SALES indexes: ' || SQLERRM);
            v_error_count := v_error_count + 1;
    END;
    
    -- Check SALES_ARCHIVE indexes
    v_check_count := v_check_count + 1;
    BEGIN
        SELECT COUNT(*) INTO v_count
        FROM user_indexes
        WHERE table_name = 'SALES_ARCHIVE'
          AND status = 'VALID';
        
        IF v_count >= 2 THEN
            DBMS_OUTPUT.PUT_LINE('[PASS] SALES_ARCHIVE table has ' || v_count || ' valid indexes');
        ELSIF v_count > 0 THEN
            DBMS_OUTPUT.PUT_LINE('[WARN] SALES_ARCHIVE table has only ' || v_count || ' valid indexes');
            v_warning_count := v_warning_count + 1;
        ELSE
            DBMS_OUTPUT.PUT_LINE('[WARN] SALES_ARCHIVE table has no valid indexes');
            v_warning_count := v_warning_count + 1;
        END IF;
        
        -- Check for invalid indexes
        SELECT COUNT(*) INTO v_count
        FROM user_indexes
        WHERE table_name = 'SALES_ARCHIVE'
          AND status != 'VALID';
        
        IF v_count > 0 THEN
            DBMS_OUTPUT.PUT_LINE('[WARN] SALES_ARCHIVE table has ' || v_count || ' INVALID indexes');
            v_warning_count := v_warning_count + 1;
        END IF;
    EXCEPTION
        WHEN OTHERS THEN
            DBMS_OUTPUT.PUT_LINE('[ERROR] Error checking SALES_ARCHIVE indexes: ' || SQLERRM);
            v_error_count := v_error_count + 1;
    END;
    
    DBMS_OUTPUT.PUT_LINE('');
    DBMS_OUTPUT.PUT_LINE('========================================');
    DBMS_OUTPUT.PUT_LINE('8. CHECKING CONSTRAINTS');
    DBMS_OUTPUT.PUT_LINE('========================================');
    
    -- Check SALES primary key
    v_check_count := v_check_count + 1;
    BEGIN
        SELECT COUNT(*) INTO v_count
        FROM user_constraints
        WHERE table_name = 'SALES'
          AND constraint_type = 'P'
          AND status = 'ENABLED';
        
        IF v_count >= 1 THEN
            DBMS_OUTPUT.PUT_LINE('[PASS] SALES table has primary key');
        ELSE
            DBMS_OUTPUT.PUT_LINE('[WARN] SALES table has no enabled primary key');
            v_warning_count := v_warning_count + 1;
        END IF;
    EXCEPTION
        WHEN OTHERS THEN
            DBMS_OUTPUT.PUT_LINE('[ERROR] Error checking SALES constraints: ' || SQLERRM);
            v_error_count := v_error_count + 1;
    END;
    
    -- Check SALES_ARCHIVE primary key
    v_check_count := v_check_count + 1;
    BEGIN
        SELECT COUNT(*) INTO v_count
        FROM user_constraints
        WHERE table_name = 'SALES_ARCHIVE'
          AND constraint_type = 'P'
          AND status = 'ENABLED';
        
        IF v_count >= 1 THEN
            DBMS_OUTPUT.PUT_LINE('[PASS] SALES_ARCHIVE table has primary key');
        ELSE
            DBMS_OUTPUT.PUT_LINE('[WARN] SALES_ARCHIVE table has no enabled primary key');
            v_warning_count := v_warning_count + 1;
        END IF;
    EXCEPTION
        WHEN OTHERS THEN
            DBMS_OUTPUT.PUT_LINE('[ERROR] Error checking SALES_ARCHIVE constraints: ' || SQLERRM);
            v_error_count := v_error_count + 1;
    END;
    
    DBMS_OUTPUT.PUT_LINE('');
    DBMS_OUTPUT.PUT_LINE('========================================');
    DBMS_OUTPUT.PUT_LINE('9. CHECKING PARTITION CONFIGURATION');
    DBMS_OUTPUT.PUT_LINE('========================================');
    
    -- Check SALES partitions
    v_check_count := v_check_count + 1;
    BEGIN
        SELECT COUNT(*) INTO v_count
        FROM user_tab_partitions
        WHERE table_name = 'SALES';
        
        IF v_count >= 1 THEN
            DBMS_OUTPUT.PUT_LINE('[PASS] SALES table has ' || v_count || ' partitions');
        ELSE
            DBMS_OUTPUT.PUT_LINE('[FAIL] SALES table has no partitions (but marked as partitioned)');
            v_error_count := v_error_count + 1;
        END IF;
    EXCEPTION
        WHEN OTHERS THEN
            DBMS_OUTPUT.PUT_LINE('[ERROR] Error checking SALES partitions: ' || SQLERRM);
            v_error_count := v_error_count + 1;
    END;
    
    -- Check SALES_ARCHIVE partitions
    v_check_count := v_check_count + 1;
    BEGIN
        SELECT COUNT(*) INTO v_count
        FROM user_tab_partitions
        WHERE table_name = 'SALES_ARCHIVE';
        
        IF v_count >= 1 THEN
            DBMS_OUTPUT.PUT_LINE('[PASS] SALES_ARCHIVE table has ' || v_count || ' partitions');
        ELSE
            DBMS_OUTPUT.PUT_LINE('[FAIL] SALES_ARCHIVE table has no partitions (but marked as partitioned)');
            v_error_count := v_error_count + 1;
        END IF;
    EXCEPTION
        WHEN OTHERS THEN
            DBMS_OUTPUT.PUT_LINE('[ERROR] Error checking SALES_ARCHIVE partitions: ' || SQLERRM);
            v_error_count := v_error_count + 1;
    END;
    
    -- Check partition key compatibility
    v_check_count := v_check_count + 1;
    BEGIN
        DECLARE
            v_sales_key VARCHAR2(4000);
            v_archive_key VARCHAR2(4000);
        BEGIN
            SELECT LISTAGG(column_name, ',') WITHIN GROUP (ORDER BY column_position)
            INTO v_sales_key
            FROM user_part_key_columns
            WHERE name = 'SALES' AND object_type = 'TABLE';
            
            SELECT LISTAGG(column_name, ',') WITHIN GROUP (ORDER BY column_position)
            INTO v_archive_key
            FROM user_part_key_columns
            WHERE name = 'SALES_ARCHIVE' AND object_type = 'TABLE';
            
            IF v_sales_key = v_archive_key THEN
                DBMS_OUTPUT.PUT_LINE('[PASS] Partition keys match: ' || v_sales_key);
            ELSE
                DBMS_OUTPUT.PUT_LINE('[FAIL] Partition keys differ - SALES: ' || v_sales_key || 
                                   ', ARCHIVE: ' || v_archive_key);
                v_error_count := v_error_count + 1;
            END IF;
        END;
    EXCEPTION
        WHEN OTHERS THEN
            DBMS_OUTPUT.PUT_LINE('[ERROR] Error checking partition keys: ' || SQLERRM);
            v_error_count := v_error_count + 1;
    END;
    
    DBMS_OUTPUT.PUT_LINE('');
    DBMS_OUTPUT.PUT_LINE('========================================');
    DBMS_OUTPUT.PUT_LINE('10. CHECKING CONFIGURATION DATA');
    DBMS_OUTPUT.PUT_LINE('========================================');
    
    -- Check configuration records
    v_check_count := v_check_count + 1;
    BEGIN
        SELECT COUNT(*) INTO v_count
        FROM snparch_cnf_partition_archive;
        
        IF v_count >= 1 THEN
            DBMS_OUTPUT.PUT_LINE('[PASS] SNPARCH_CNF_PARTITION_ARCHIVE has ' || v_count || ' configuration record(s)');
            
            -- Check if SALES is configured
            SELECT COUNT(*) INTO v_count
            FROM snparch_cnf_partition_archive
            WHERE source_table_name = 'SALES';
            
            IF v_count = 1 THEN
                DBMS_OUTPUT.PUT_LINE('[PASS] SALES table is configured for archival');
                
                -- Check if active
                SELECT is_active INTO v_status
                FROM snparch_cnf_partition_archive
                WHERE source_table_name = 'SALES';
                
                IF v_status = 'Y' THEN
                    DBMS_OUTPUT.PUT_LINE('[PASS] SALES archival configuration is ACTIVE');
                ELSE
                    DBMS_OUTPUT.PUT_LINE('[WARN] SALES archival configuration is INACTIVE');
                    v_warning_count := v_warning_count + 1;
                END IF;
            ELSE
                DBMS_OUTPUT.PUT_LINE('[WARN] SALES table is not configured for archival');
                v_warning_count := v_warning_count + 1;
            END IF;
        ELSE
            DBMS_OUTPUT.PUT_LINE('[WARN] SNPARCH_CNF_PARTITION_ARCHIVE has no configuration records');
            v_warning_count := v_warning_count + 1;
        END IF;
    EXCEPTION
        WHEN OTHERS THEN
            DBMS_OUTPUT.PUT_LINE('[ERROR] Error checking configuration data: ' || SQLERRM);
            v_error_count := v_error_count + 1;
    END;
    
    DBMS_OUTPUT.PUT_LINE('');
    DBMS_OUTPUT.PUT_LINE('========================================');
    DBMS_OUTPUT.PUT_LINE('11. CHECKING OBJECT DEPENDENCIES');
    DBMS_OUTPUT.PUT_LINE('========================================');
    
    -- Check if archive procedure depends on required objects
    v_check_count := v_check_count + 1;
    BEGIN
        SELECT COUNT(*) INTO v_count
        FROM user_dependencies
        WHERE name = 'ARCHIVE_PARTITIONS_BY_DATES'
          AND referenced_name IN ('GET_PARTITION_NAME_BY_DATE', 'DATE_ARRAY_TYPE');
        
        IF v_count >= 2 THEN
            DBMS_OUTPUT.PUT_LINE('[PASS] ARCHIVE_PARTITIONS_BY_DATES has required dependencies');
        ELSE
            DBMS_OUTPUT.PUT_LINE('[WARN] ARCHIVE_PARTITIONS_BY_DATES missing some dependencies (' || v_count || '/2)');
            v_warning_count := v_warning_count + 1;
        END IF;
    EXCEPTION
        WHEN OTHERS THEN
            DBMS_OUTPUT.PUT_LINE('[ERROR] Error checking dependencies: ' || SQLERRM);
            v_error_count := v_error_count + 1;
    END;
    
    DBMS_OUTPUT.PUT_LINE('');
    DBMS_OUTPUT.PUT_LINE('========================================');
    DBMS_OUTPUT.PUT_LINE('12. CHECKING ALL INVALID OBJECTS');
    DBMS_OUTPUT.PUT_LINE('========================================');
    
    -- Check for any invalid objects
    v_check_count := v_check_count + 1;
    BEGIN
        SELECT COUNT(*) INTO v_count
        FROM user_objects
        WHERE status = 'INVALID'
          AND object_name IN (
              'SALES', 'SALES_ARCHIVE', 
              'SNPARCH_CNF_PARTITION_ARCHIVE', 'SNPARCH_CTL_EXECUTION_LOG',
              'DATE_ARRAY_TYPE', 'GET_PARTITION_NAME_BY_DATE',
              'ARCHIVE_PARTITIONS_BY_DATES', 'PRC_LOG_ERROR_AUTONOMOUS',
              'F_DEGRAG_GET_TABLE_SIZE_STATS_UTIL', 'SALES_COMPLETE'
          );
        
        IF v_count = 0 THEN
            DBMS_OUTPUT.PUT_LINE('[PASS] All deployment objects are VALID');
        ELSE
            DBMS_OUTPUT.PUT_LINE('[FAIL] Found ' || v_count || ' INVALID objects:');
            v_error_count := v_error_count + 1;
            
            FOR rec IN (
                SELECT object_name, object_type
                FROM user_objects
                WHERE status = 'INVALID'
                  AND object_name IN (
                      'SALES', 'SALES_ARCHIVE', 
                      'SNPARCH_CNF_PARTITION_ARCHIVE', 'SNPARCH_CTL_EXECUTION_LOG',
                      'DATE_ARRAY_TYPE', 'GET_PARTITION_NAME_BY_DATE',
                      'ARCHIVE_PARTITIONS_BY_DATES', 'PRC_LOG_ERROR_AUTONOMOUS',
                      'F_DEGRAG_GET_TABLE_SIZE_STATS_UTIL', 'SALES_COMPLETE'
                  )
                ORDER BY object_type, object_name
            ) LOOP
                DBMS_OUTPUT.PUT_LINE('      - ' || rec.object_type || ': ' || rec.object_name);
            END LOOP;
        END IF;
    EXCEPTION
        WHEN OTHERS THEN
            DBMS_OUTPUT.PUT_LINE('[ERROR] Error checking invalid objects: ' || SQLERRM);
            v_error_count := v_error_count + 1;
    END;
    
    DBMS_OUTPUT.PUT_LINE('');
    DBMS_OUTPUT.PUT_LINE('========================================');
    DBMS_OUTPUT.PUT_LINE('VALIDATION SUMMARY');
    DBMS_OUTPUT.PUT_LINE('========================================');
    DBMS_OUTPUT.PUT_LINE('Total Checks:   ' || v_check_count);
    DBMS_OUTPUT.PUT_LINE('Errors:         ' || v_error_count);
    DBMS_OUTPUT.PUT_LINE('Warnings:       ' || v_warning_count);
    DBMS_OUTPUT.PUT_LINE('Passed:         ' || (v_check_count - v_error_count - v_warning_count));
    DBMS_OUTPUT.PUT_LINE('');
    
    IF v_error_count = 0 AND v_warning_count = 0 THEN
        DBMS_OUTPUT.PUT_LINE('*** DEPLOYMENT VALIDATION: SUCCESS ***');
        DBMS_OUTPUT.PUT_LINE('All checks passed. System is ready for use.');
    ELSIF v_error_count = 0 THEN
        DBMS_OUTPUT.PUT_LINE('*** DEPLOYMENT VALIDATION: SUCCESS WITH WARNINGS ***');
        DBMS_OUTPUT.PUT_LINE('No critical errors found, but ' || v_warning_count || ' warning(s) detected.');
        DBMS_OUTPUT.PUT_LINE('Review warnings above and address if necessary.');
    ELSE
        DBMS_OUTPUT.PUT_LINE('*** DEPLOYMENT VALIDATION: FAILED ***');
        DBMS_OUTPUT.PUT_LINE('Found ' || v_error_count || ' error(s) and ' || v_warning_count || ' warning(s).');
        DBMS_OUTPUT.PUT_LINE('Please review and fix errors before using the system.');
    END IF;
    
    DBMS_OUTPUT.PUT_LINE('========================================');
END;
/

PROMPT
PROMPT ========================================
PROMPT DETAILED OBJECT LISTING
PROMPT ========================================

-- List all deployed objects
SELECT 
    object_type,
    object_name,
    status,
    TO_CHAR(created, 'YYYY-MM-DD HH24:MI:SS') AS created,
    TO_CHAR(last_ddl_time, 'YYYY-MM-DD HH24:MI:SS') AS last_modified
FROM user_objects
WHERE object_name IN (
    'SALES', 'SALES_ARCHIVE', 
    'SNPARCH_CNF_PARTITION_ARCHIVE', 'SNPARCH_CTL_EXECUTION_LOG',
    'DATE_ARRAY_TYPE', 'GET_PARTITION_NAME_BY_DATE',
    'ARCHIVE_PARTITIONS_BY_DATES', 'PRC_LOG_ERROR_AUTONOMOUS',
    'F_DEGRAG_GET_TABLE_SIZE_STATS_UTIL', 'SALES_COMPLETE'
)
ORDER BY object_type, object_name;

PROMPT
PROMPT ========================================
PROMPT Validation Completed
SELECT TO_CHAR(SYSDATE, 'YYYY-MM-DD HH24:MI:SS') AS validation_end FROM dual;
PROMPT ========================================

SET FEEDBACK ON
SET VERIFY ON
