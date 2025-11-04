-- ========================================
-- ARCHIVE SETUP GENERATOR
-- ========================================
-- Generates DDL for archive table, staging table, and configuration
-- Usage: @generate_archive_setup.sql <source_table_name>
-- Example: @generate_archive_setup.sql SALES

SET SERVEROUTPUT ON SIZE UNLIMITED
SET LINESIZE 200
SET PAGESIZE 50000
SET VERIFY OFF
SET FEEDBACK OFF
SET TRIMSPOOL ON
SET LONG 100000
SET LONGCHUNKSIZE 100000

ACCEPT p_source_table PROMPT 'Enter source table name: '

DECLARE
    v_source_table VARCHAR2(128) := UPPER('&p_source_table');
    v_archive_table VARCHAR2(128);
    v_staging_table VARCHAR2(128);
    v_ddl CLOB;
    v_partitioned VARCHAR2(3);
    v_partition_type VARCHAR2(30);
    v_partition_key VARCHAR2(4000);
    v_interval_clause VARCHAR2(4000);
    v_tablespace VARCHAR2(128);
    v_compression VARCHAR2(30);
    v_compress_for VARCHAR2(30);
    
    -- Cursor for columns
    CURSOR c_columns IS
        SELECT column_name, data_type, data_length, data_precision, data_scale,
               nullable, data_default
        FROM user_tab_columns
        WHERE table_name = v_source_table
        ORDER BY column_id;
        
    -- Cursor for constraints
    CURSOR c_constraints IS
        SELECT constraint_name, constraint_type, search_condition, r_constraint_name
        FROM user_constraints
        WHERE table_name = v_source_table
          AND constraint_type IN ('P', 'U', 'C')
        ORDER BY DECODE(constraint_type, 'P', 1, 'U', 2, 'C', 3);
        
    -- Cursor for constraint columns
    CURSOR c_cons_cols(p_constraint_name VARCHAR2) IS
        SELECT column_name
        FROM user_cons_columns
        WHERE constraint_name = p_constraint_name
          AND table_name = v_source_table
        ORDER BY position;
        
    -- Cursor for indexes
    CURSOR c_indexes IS
        SELECT index_name, uniqueness, tablespace_name, index_type, locality
        FROM user_indexes
        WHERE table_name = v_source_table
          AND index_name NOT IN (
              SELECT constraint_name 
              FROM user_constraints 
              WHERE table_name = v_source_table
                AND constraint_type IN ('P', 'U')
          )
        ORDER BY index_name;
        
    -- Cursor for index columns
    CURSOR c_index_cols(p_index_name VARCHAR2) IS
        SELECT column_name, descend
        FROM user_ind_columns
        WHERE index_name = p_index_name
          AND table_name = v_source_table
        ORDER BY column_position;
        
    v_col_list VARCHAR2(4000);
    v_first BOOLEAN;
    
BEGIN
    -- Set archive and staging table names
    v_archive_table := v_source_table || '_ARCHIVE';
    v_staging_table := v_source_table || '_STAGING_TEMP';
    
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
    
    -- Get partition information
    IF v_partitioned = 'YES' THEN
        SELECT partitioning_type, 
               LISTAGG(column_name, ', ') WITHIN GROUP (ORDER BY column_position)
        INTO v_partition_type, v_partition_key
        FROM user_part_key_columns
        WHERE name = v_source_table
        GROUP BY partitioning_type;
        
        -- Get interval clause if applicable
        BEGIN
            SELECT interval
            INTO v_interval_clause
            FROM user_part_tables
            WHERE table_name = v_source_table
              AND interval IS NOT NULL;
        EXCEPTION
            WHEN NO_DATA_FOUND THEN
                v_interval_clause := NULL;
        END;
    END IF;
    
    -- Get compression info
    BEGIN
        SELECT compression, compress_for
        INTO v_compression, v_compress_for
        FROM user_tables
        WHERE table_name = v_source_table;
    EXCEPTION
        WHEN OTHERS THEN
            v_compression := 'DISABLED';
            v_compress_for := NULL;
    END;
    
    DBMS_OUTPUT.PUT_LINE('-- ========================================');
    DBMS_OUTPUT.PUT_LINE('-- SOURCE TABLE INFORMATION');
    DBMS_OUTPUT.PUT_LINE('-- ========================================');
    DBMS_OUTPUT.PUT_LINE('-- Source Table    : ' || v_source_table);
    DBMS_OUTPUT.PUT_LINE('-- Archive Table   : ' || v_archive_table);
    DBMS_OUTPUT.PUT_LINE('-- Staging Table   : ' || v_staging_table);
    DBMS_OUTPUT.PUT_LINE('-- Partitioned     : ' || v_partitioned);
    IF v_partitioned = 'YES' THEN
        DBMS_OUTPUT.PUT_LINE('-- Partition Type  : ' || v_partition_type);
        DBMS_OUTPUT.PUT_LINE('-- Partition Key   : ' || v_partition_key);
        IF v_interval_clause IS NOT NULL THEN
            DBMS_OUTPUT.PUT_LINE('-- Interval        : ' || v_interval_clause);
        END IF;
    END IF;
    DBMS_OUTPUT.PUT_LINE('-- Tablespace      : ' || NVL(v_tablespace, 'DEFAULT'));
    DBMS_OUTPUT.PUT_LINE('-- Compression     : ' || v_compression || 
                          CASE WHEN v_compress_for IS NOT NULL 
                               THEN ' (' || v_compress_for || ')' 
                               ELSE '' END);
    DBMS_OUTPUT.PUT_LINE('');
    DBMS_OUTPUT.PUT_LINE('');
    
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
    
    -- Create table
    v_ddl := 'CREATE TABLE ' || v_archive_table || ' (';
    DBMS_OUTPUT.PUT_LINE(v_ddl);
    
    v_first := TRUE;
    FOR rec IN c_columns LOOP
        IF NOT v_first THEN
            DBMS_OUTPUT.PUT_LINE(',');
        END IF;
        v_first := FALSE;
        
        v_ddl := '    ' || RPAD(rec.column_name, 30) || ' ';
        
        -- Data type
        IF rec.data_type IN ('VARCHAR2', 'CHAR', 'NVARCHAR2', 'NCHAR') THEN
            v_ddl := v_ddl || rec.data_type || '(' || rec.data_length || ')';
        ELSIF rec.data_type = 'NUMBER' THEN
            IF rec.data_precision IS NOT NULL THEN
                v_ddl := v_ddl || 'NUMBER(' || rec.data_precision;
                IF rec.data_scale IS NOT NULL AND rec.data_scale > 0 THEN
                    v_ddl := v_ddl || ',' || rec.data_scale;
                END IF;
                v_ddl := v_ddl || ')';
            ELSE
                v_ddl := v_ddl || 'NUMBER';
            END IF;
        ELSE
            v_ddl := v_ddl || rec.data_type;
        END IF;
        
        -- Nullable
        IF rec.nullable = 'N' THEN
            v_ddl := v_ddl || ' NOT NULL';
        END IF;
        
        DBMS_OUTPUT.PUT(v_ddl);
    END LOOP;
    
    DBMS_OUTPUT.PUT_LINE('');
    DBMS_OUTPUT.PUT_LINE(')');
    
    -- Partition clause
    IF v_partitioned = 'YES' THEN
        DBMS_OUTPUT.PUT_LINE('PARTITION BY RANGE (' || v_partition_key || ')');
        IF v_interval_clause IS NOT NULL THEN
            DBMS_OUTPUT.PUT_LINE('INTERVAL (' || v_interval_clause || ')');
        END IF;
        DBMS_OUTPUT.PUT_LINE('(');
        DBMS_OUTPUT.PUT_LINE('    PARTITION ' || LOWER(v_source_table) || '_old VALUES LESS THAN (DATE ''2000-01-01'')');
        DBMS_OUTPUT.PUT_LINE(')');
    END IF;
    
    -- Tablespace
    IF v_tablespace IS NOT NULL THEN
        DBMS_OUTPUT.PUT_LINE('TABLESPACE ' || v_tablespace);
    END IF;
    
    DBMS_OUTPUT.PUT_LINE(';');
    DBMS_OUTPUT.PUT_LINE('');
    
    -- ========================================
    -- CONSTRAINTS
    -- ========================================
    DBMS_OUTPUT.PUT_LINE('-- Constraints');
    FOR con_rec IN c_constraints LOOP
        IF con_rec.constraint_type = 'P' THEN
            -- Primary Key
            v_col_list := '';
            FOR col_rec IN c_cons_cols(con_rec.constraint_name) LOOP
                IF v_col_list IS NOT NULL THEN
                    v_col_list := v_col_list || ', ';
                END IF;
                v_col_list := v_col_list || col_rec.column_name;
            END LOOP;
            DBMS_OUTPUT.PUT_LINE('ALTER TABLE ' || v_archive_table);
            DBMS_OUTPUT.PUT_LINE('    ADD CONSTRAINT pk_' || LOWER(v_archive_table));
            DBMS_OUTPUT.PUT_LINE('    PRIMARY KEY (' || v_col_list || ');');
            DBMS_OUTPUT.PUT_LINE('');
            
        ELSIF con_rec.constraint_type = 'U' THEN
            -- Unique
            v_col_list := '';
            FOR col_rec IN c_cons_cols(con_rec.constraint_name) LOOP
                IF v_col_list IS NOT NULL THEN
                    v_col_list := v_col_list || ', ';
                END IF;
                v_col_list := v_col_list || col_rec.column_name;
            END LOOP;
            DBMS_OUTPUT.PUT_LINE('ALTER TABLE ' || v_archive_table);
            DBMS_OUTPUT.PUT_LINE('    ADD CONSTRAINT ' || REPLACE(con_rec.constraint_name, v_source_table, v_archive_table));
            DBMS_OUTPUT.PUT_LINE('    UNIQUE (' || v_col_list || ');');
            DBMS_OUTPUT.PUT_LINE('');
            
        ELSIF con_rec.constraint_type = 'C' AND con_rec.constraint_name NOT LIKE 'SYS_%' THEN
            -- Check constraint (skip system-generated NOT NULL constraints)
            DBMS_OUTPUT.PUT_LINE('ALTER TABLE ' || v_archive_table);
            DBMS_OUTPUT.PUT_LINE('    ADD CONSTRAINT ' || REPLACE(con_rec.constraint_name, v_source_table, v_archive_table));
            DBMS_OUTPUT.PUT_LINE('    CHECK (' || con_rec.search_condition || ');');
            DBMS_OUTPUT.PUT_LINE('');
        END IF;
    END LOOP;
    
    -- ========================================
    -- INDEXES
    -- ========================================
    DBMS_OUTPUT.PUT_LINE('-- Indexes');
    FOR idx_rec IN c_indexes LOOP
        v_col_list := '';
        FOR col_rec IN c_index_cols(idx_rec.index_name) LOOP
            IF v_col_list IS NOT NULL THEN
                v_col_list := v_col_list || ', ';
            END IF;
            v_col_list := v_col_list || col_rec.column_name;
            IF col_rec.descend = 'DESC' THEN
                v_col_list := v_col_list || ' DESC';
            END IF;
        END LOOP;
        
        v_ddl := 'CREATE ';
        IF idx_rec.uniqueness = 'UNIQUE' THEN
            v_ddl := v_ddl || 'UNIQUE ';
        END IF;
        v_ddl := v_ddl || 'INDEX ' || REPLACE(idx_rec.index_name, v_source_table, v_archive_table);
        DBMS_OUTPUT.PUT_LINE(v_ddl);
        DBMS_OUTPUT.PUT_LINE('    ON ' || v_archive_table || '(' || v_col_list || ')');
        
        IF idx_rec.locality = 'LOCAL' THEN
            DBMS_OUTPUT.PUT_LINE('    LOCAL');
        END IF;
        
        IF idx_rec.tablespace_name IS NOT NULL THEN
            DBMS_OUTPUT.PUT_LINE('    TABLESPACE ' || idx_rec.tablespace_name);
        END IF;
        
        DBMS_OUTPUT.PUT_LINE(';');
        DBMS_OUTPUT.PUT_LINE('');
    END LOOP;
    
    DBMS_OUTPUT.PUT_LINE('');
    
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
    DBMS_OUTPUT.PUT_LINE('-- Create staging template for exchange operations');
    DBMS_OUTPUT.PUT_LINE('CREATE TABLE ' || v_staging_table);
    DBMS_OUTPUT.PUT_LINE('FOR EXCHANGE WITH TABLE ' || v_source_table || ';');
    DBMS_OUTPUT.PUT_LINE('');
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
    DBMS_OUTPUT.PUT_LINE('The following DDL has been generated:');
    DBMS_OUTPUT.PUT_LINE('  1. Archive table: ' || v_archive_table);
    DBMS_OUTPUT.PUT_LINE('     - Table structure matching ' || v_source_table);
    FOR con_rec IN (SELECT COUNT(*) cnt FROM user_constraints WHERE table_name = v_source_table AND constraint_type IN ('P','U','C')) LOOP
        DBMS_OUTPUT.PUT_LINE('     - ' || con_rec.cnt || ' constraint(s)');
    END LOOP;
    FOR idx_rec IN (SELECT COUNT(*) cnt FROM user_indexes WHERE table_name = v_source_table) LOOP
        DBMS_OUTPUT.PUT_LINE('     - ' || idx_rec.cnt || ' index(es)');
    END LOOP;
    DBMS_OUTPUT.PUT_LINE('  2. Staging table: ' || v_staging_table);
    DBMS_OUTPUT.PUT_LINE('  3. Configuration insert for SNPARCH_CNF_PARTITION_ARCHIVE');
    DBMS_OUTPUT.PUT_LINE('');
    DBMS_OUTPUT.PUT_LINE('NOTES:');
    DBMS_OUTPUT.PUT_LINE('  - Review and adjust compression settings if needed');
    DBMS_OUTPUT.PUT_LINE('  - Review and adjust tablespace assignments if needed');
    DBMS_OUTPUT.PUT_LINE('  - Test the DDL in a development environment first');
    DBMS_OUTPUT.PUT_LINE('  - Ensure you have sufficient privileges to create these objects');
    DBMS_OUTPUT.PUT_LINE('================================================================================');
    
EXCEPTION
    WHEN OTHERS THEN
        DBMS_OUTPUT.PUT_LINE('ERROR: ' || SQLERRM);
        RAISE;
END;
/

SET VERIFY ON
SET FEEDBACK ON
