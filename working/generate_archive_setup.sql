-- ========================================
-- ARCHIVE SETUP GENERATOR
-- ========================================
-- Generates DDL for archive table, staging table, and configuration
-- Uses DBMS_METADATA for accurate DDL extraction
-- Usage: @generate_archive_setup.sql
-- Example: @generate_archive_setup.sql

SET SERVEROUTPUT ON SIZE UNLIMITED
SET LINESIZE 200
SET PAGESIZE 50000
SET VERIFY OFF
SET FEEDBACK OFF
SET TRIMSPOOL ON
SET LONG 100000
SET LONGCHUNKSIZE 100000

DECLARE
    v_source_table VARCHAR2(128) := 'SALES';  -- Change this to your table name
    v_archive_table VARCHAR2(128);
    v_staging_table VARCHAR2(128);
    v_table_ddl CLOB;
    v_index_ddl CLOB;
    v_constraint_ddl CLOB;
    v_partitioned VARCHAR2(3);
    v_tablespace VARCHAR2(128);
    v_num_indexes NUMBER;
    v_num_constraints NUMBER;
    v_col_list VARCHAR2(4000);
    
BEGIN
    -- Set archive and staging table names with SNPARCH_ prefix
    v_archive_table := 'SNPARCH_' || v_source_table;
    v_staging_table := 'SNPARCH_' || v_source_table || '_STAGING_TEMP';
    
    DBMS_OUTPUT.PUT_LINE('================================================================================');
    DBMS_OUTPUT.PUT_LINE('ARCHIVE SETUP GENERATOR FOR TABLE: ' || v_source_table);
    DBMS_OUTPUT.PUT_LINE('================================================================================');
    DBMS_OUTPUT.PUT_LINE('');
    
    -- Check if table exists
    BEGIN
        SELECT partitioned, tablespace_name
        INTO v_partitioned, v_tablespace
        FROM user_tables
        WHERE table_name = v_source_table;
    EXCEPTION
        WHEN NO_DATA_FOUND THEN
            DBMS_OUTPUT.PUT_LINE('ERROR: Table ' || v_source_table || ' does not exist!');
            RETURN;
    END;
    
    -- Check if table is partitioned
    IF v_partitioned != 'YES' THEN
        DBMS_OUTPUT.PUT_LINE('WARNING: Table ' || v_source_table || ' is not partitioned!');
        DBMS_OUTPUT.PUT_LINE('This script is designed for partitioned tables only.');
        DBMS_OUTPUT.PUT_LINE('');
    END IF;
    
    DBMS_OUTPUT.PUT_LINE('-- Source Table    : ' || v_source_table);
    DBMS_OUTPUT.PUT_LINE('-- Archive Table   : ' || v_archive_table);
    DBMS_OUTPUT.PUT_LINE('-- Staging Table   : ' || v_staging_table);
    DBMS_OUTPUT.PUT_LINE('-- Partitioned     : ' || v_partitioned);
    DBMS_OUTPUT.PUT_LINE('-- Tablespace      : ' || NVL(v_tablespace, 'DEFAULT'));
    DBMS_OUTPUT.PUT_LINE('');
    
    -- Configure DBMS_METADATA for clean output
    DBMS_METADATA.SET_TRANSFORM_PARAM(DBMS_METADATA.SESSION_TRANSFORM, 'SQLTERMINATOR', TRUE);
    DBMS_METADATA.SET_TRANSFORM_PARAM(DBMS_METADATA.SESSION_TRANSFORM, 'PRETTY', TRUE);
    DBMS_METADATA.SET_TRANSFORM_PARAM(DBMS_METADATA.SESSION_TRANSFORM, 'SEGMENT_ATTRIBUTES', FALSE);
    DBMS_METADATA.SET_TRANSFORM_PARAM(DBMS_METADATA.SESSION_TRANSFORM, 'STORAGE', FALSE);
    DBMS_METADATA.SET_TRANSFORM_PARAM(DBMS_METADATA.SESSION_TRANSFORM, 'TABLESPACE', TRUE);
    DBMS_METADATA.SET_TRANSFORM_PARAM(DBMS_METADATA.SESSION_TRANSFORM, 'CONSTRAINTS', FALSE);
    DBMS_METADATA.SET_TRANSFORM_PARAM(DBMS_METADATA.SESSION_TRANSFORM, 'REF_CONSTRAINTS', FALSE);
    DBMS_METADATA.SET_TRANSFORM_PARAM(DBMS_METADATA.SESSION_TRANSFORM, 'CONSTRAINTS_AS_ALTER', TRUE);
    DBMS_METADATA.SET_TRANSFORM_PARAM(DBMS_METADATA.SESSION_TRANSFORM, 'EMIT_SCHEMA', FALSE);
    
    -- ========================================
    -- GENERATE ARCHIVE TABLE DDL
    -- ========================================
    DBMS_OUTPUT.PUT_LINE('-- ========================================');
    DBMS_OUTPUT.PUT_LINE('-- ARCHIVE TABLE DDL');
    DBMS_OUTPUT.PUT_LINE('-- ========================================');
    DBMS_OUTPUT.PUT_LINE('');
    
    -- Drop statement
    DBMS_OUTPUT.PUT_LINE('-- Drop if exists');
    DBMS_OUTPUT.PUT_LINE('BEGIN');
    DBMS_OUTPUT.PUT_LINE('    EXECUTE IMMEDIATE ''DROP TABLE ' || v_archive_table || ' PURGE'';');
    DBMS_OUTPUT.PUT_LINE('EXCEPTION');
    DBMS_OUTPUT.PUT_LINE('    WHEN OTHERS THEN NULL;');
    DBMS_OUTPUT.PUT_LINE('END;');
    DBMS_OUTPUT.PUT_LINE('/');
    DBMS_OUTPUT.PUT_LINE('');
    
    -- Get table DDL
    v_table_ddl := DBMS_METADATA.GET_DDL('TABLE', v_source_table, USER);
    
    -- Replace table name with archive table name
    v_table_ddl := REPLACE(v_table_ddl, '"' || v_source_table || '"', '"' || v_archive_table || '"');
    v_table_ddl := REPLACE(v_table_ddl, ' ' || v_source_table || ' ', ' ' || v_archive_table || ' ');
    
    -- Replace old partition name (e.g., SALES_OLD -> SALES_ARCHIVE_OLD)
    v_table_ddl := REPLACE(v_table_ddl, '"' || v_source_table || '_OLD"', '"' || v_archive_table || '_OLD"');
    v_table_ddl := REPLACE(v_table_ddl, ' PARTITION ' || v_source_table || '_OLD ', ' PARTITION ' || v_archive_table || '_OLD ');
    
    DBMS_OUTPUT.PUT_LINE(v_table_ddl);
    DBMS_OUTPUT.PUT_LINE('');
    
    -- ========================================
    -- CONSTRAINTS
    -- ========================================
    SELECT COUNT(*)
    INTO v_num_constraints
    FROM user_constraints
    WHERE table_name = v_source_table
      AND constraint_type IN ('P', 'U', 'C');
      
    IF v_num_constraints > 0 THEN
        DBMS_OUTPUT.PUT_LINE('-- Constraints');
        
        FOR con IN (
            SELECT constraint_name
            FROM user_constraints
            WHERE table_name = v_source_table
              AND constraint_type IN ('P', 'U', 'C')
            ORDER BY DECODE(constraint_type, 'P', 1, 'U', 2, 'C', 3)
        ) LOOP
            BEGIN
                v_constraint_ddl := DBMS_METADATA.GET_DDL('CONSTRAINT', con.constraint_name, USER);
                v_constraint_ddl := REPLACE(v_constraint_ddl, '"' || v_source_table || '"', '"' || v_archive_table || '"');
                v_constraint_ddl := REPLACE(v_constraint_ddl, ' ' || v_source_table || ' ', ' ' || v_archive_table || ' ');
                v_constraint_ddl := REPLACE(v_constraint_ddl, '"' || con.constraint_name || '"', 
                                           '"' || REPLACE(con.constraint_name, v_source_table, v_archive_table) || '"');
                DBMS_OUTPUT.PUT_LINE(v_constraint_ddl);
            EXCEPTION
                WHEN OTHERS THEN
                    DBMS_OUTPUT.PUT_LINE('-- Note: Could not extract constraint ' || con.constraint_name);
            END;
        END LOOP;
        DBMS_OUTPUT.PUT_LINE('');
    END IF;
    
    -- ========================================
    -- INDEXES
    -- ========================================
    SELECT COUNT(*)
    INTO v_num_indexes
    FROM user_indexes
    WHERE table_name = v_source_table;
      
    IF v_num_indexes > 0 THEN
        DBMS_OUTPUT.PUT_LINE('-- Indexes');
        
        FOR idx IN (
            SELECT index_name
            FROM user_indexes
            WHERE table_name = v_source_table
            ORDER BY index_name
        ) LOOP
            BEGIN
                v_index_ddl := DBMS_METADATA.GET_DDL('INDEX', idx.index_name, USER);
                v_index_ddl := REPLACE(v_index_ddl, '"' || v_source_table || '"', '"' || v_archive_table || '"');
                v_index_ddl := REPLACE(v_index_ddl, ' ' || v_source_table || ' ', ' ' || v_archive_table || ' ');
                v_index_ddl := REPLACE(v_index_ddl, '"' || idx.index_name || '"', 
                                      '"' || REPLACE(idx.index_name, v_source_table, v_archive_table) || '"');
                DBMS_OUTPUT.PUT_LINE(v_index_ddl);
            EXCEPTION
                WHEN OTHERS THEN
                    -- Index might be system-generated for constraint, skip it
                    NULL;
            END;
        END LOOP;
        DBMS_OUTPUT.PUT_LINE('');
    END IF;
    
    -- ========================================
    -- STAGING TABLE DDL
    -- ========================================
    DBMS_OUTPUT.PUT_LINE('-- ========================================');
    DBMS_OUTPUT.PUT_LINE('-- STAGING TABLE DDL');
    DBMS_OUTPUT.PUT_LINE('-- ========================================');
    DBMS_OUTPUT.PUT_LINE('');
    DBMS_OUTPUT.PUT_LINE('-- Drop if exists');
    DBMS_OUTPUT.PUT_LINE('BEGIN');
    DBMS_OUTPUT.PUT_LINE('    EXECUTE IMMEDIATE ''DROP TABLE ' || v_staging_table || ' PURGE'';');
    DBMS_OUTPUT.PUT_LINE('EXCEPTION');
    DBMS_OUTPUT.PUT_LINE('    WHEN OTHERS THEN NULL;');
    DBMS_OUTPUT.PUT_LINE('END;');
    DBMS_OUTPUT.PUT_LINE('/');
    DBMS_OUTPUT.PUT_LINE('');
    DBMS_OUTPUT.PUT_LINE('-- Create staging table (empty structure from source)');
    DBMS_OUTPUT.PUT_LINE('CREATE TABLE ' || v_staging_table || ' AS');
    DBMS_OUTPUT.PUT_LINE('SELECT * FROM ' || v_source_table || ' WHERE 1=0;');
    DBMS_OUTPUT.PUT_LINE('');
    DBMS_OUTPUT.PUT_LINE('-- Add primary key constraint to match source table');
    
    -- Get primary key columns
    FOR pk IN (
        SELECT constraint_name
        FROM user_constraints
        WHERE table_name = v_source_table
          AND constraint_type = 'P'
    ) LOOP
        v_col_list := '';
        FOR col IN (
            SELECT column_name
            FROM user_cons_columns
            WHERE constraint_name = pk.constraint_name
              AND table_name = v_source_table
            ORDER BY position
        ) LOOP
            IF v_col_list IS NOT NULL THEN
                v_col_list := v_col_list || ', ';
            END IF;
            v_col_list := v_col_list || col.column_name;
        END LOOP;
        
        IF v_col_list IS NOT NULL THEN
            DBMS_OUTPUT.PUT_LINE('ALTER TABLE ' || v_staging_table);
            DBMS_OUTPUT.PUT_LINE('ADD CONSTRAINT pk_staging_temp PRIMARY KEY (' || v_col_list || ');');
        END IF;
    END LOOP;
    DBMS_OUTPUT.PUT_LINE('');
    
    -- ========================================
    -- CONFIGURATION INSERT
    -- ========================================
    DBMS_OUTPUT.PUT_LINE('-- ========================================');
    DBMS_OUTPUT.PUT_LINE('-- CONFIGURATION INSERT');
    DBMS_OUTPUT.PUT_LINE('-- ========================================');
    DBMS_OUTPUT.PUT_LINE('');
    DBMS_OUTPUT.PUT_LINE('INSERT INTO snparch_cnf_partition_archive (');
    DBMS_OUTPUT.PUT_LINE('    source_table_name,');
    DBMS_OUTPUT.PUT_LINE('    archive_table_name,');
    DBMS_OUTPUT.PUT_LINE('    staging_table_name,');
    DBMS_OUTPUT.PUT_LINE('    is_active,');
    DBMS_OUTPUT.PUT_LINE('    validate_before_exchange,');
    DBMS_OUTPUT.PUT_LINE('    gather_stats_after_exchange,');
    DBMS_OUTPUT.PUT_LINE('    enable_compression,');
    DBMS_OUTPUT.PUT_LINE('    compression_type');
    DBMS_OUTPUT.PUT_LINE(') VALUES (');
    DBMS_OUTPUT.PUT_LINE('    ''' || v_source_table || ''',');
    DBMS_OUTPUT.PUT_LINE('    ''' || v_archive_table || ''',');
    DBMS_OUTPUT.PUT_LINE('    ''' || v_staging_table || ''',');
    DBMS_OUTPUT.PUT_LINE('    ''Y'',  -- is_active');
    DBMS_OUTPUT.PUT_LINE('    ''Y'',  -- validate_before_exchange');
    DBMS_OUTPUT.PUT_LINE('    ''Y'',  -- gather_stats_after_exchange');
    DBMS_OUTPUT.PUT_LINE('    ''N'',  -- enable_compression (change to Y if needed)');
    DBMS_OUTPUT.PUT_LINE('    NULL   -- compression_type (BASIC, OLTP, QUERY LOW/HIGH, ARCHIVE LOW/HIGH)');
    DBMS_OUTPUT.PUT_LINE(');');
    DBMS_OUTPUT.PUT_LINE('');
    DBMS_OUTPUT.PUT_LINE('COMMIT;');
    DBMS_OUTPUT.PUT_LINE('');
    
    -- ========================================
    -- SUMMARY
    -- ========================================
    DBMS_OUTPUT.PUT_LINE('');
    DBMS_OUTPUT.PUT_LINE('================================================================================');
    DBMS_OUTPUT.PUT_LINE('SUMMARY');
    DBMS_OUTPUT.PUT_LINE('================================================================================');
    DBMS_OUTPUT.PUT_LINE('The following DDL has been generated using DBMS_METADATA:');
    DBMS_OUTPUT.PUT_LINE('  1. Archive table: ' || v_archive_table);
    DBMS_OUTPUT.PUT_LINE('     - Complete table structure from ' || v_source_table);
    DBMS_OUTPUT.PUT_LINE('     - ' || v_num_constraints || ' constraint(s)');
    DBMS_OUTPUT.PUT_LINE('     - ' || v_num_indexes || ' index(es)');
    DBMS_OUTPUT.PUT_LINE('  2. Staging table: ' || v_staging_table);
    DBMS_OUTPUT.PUT_LINE('  3. Configuration insert for SNPARCH_CNF_PARTITION_ARCHIVE');
    DBMS_OUTPUT.PUT_LINE('');
    DBMS_OUTPUT.PUT_LINE('NOTES:');
    DBMS_OUTPUT.PUT_LINE('  - DDL extracted using DBMS_METADATA.GET_DDL for accuracy');
    DBMS_OUTPUT.PUT_LINE('  - Review and adjust compression settings if needed');
    DBMS_OUTPUT.PUT_LINE('  - Review and adjust tablespace assignments if needed');
    DBMS_OUTPUT.PUT_LINE('  - Test the DDL in a development environment first');
    DBMS_OUTPUT.PUT_LINE('================================================================================');
    
EXCEPTION
    WHEN OTHERS THEN
        DBMS_OUTPUT.PUT_LINE('ERROR: ' || SQLERRM);
        RAISE;
END;
/

SET VERIFY ON
SET FEEDBACK ON
