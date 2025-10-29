CREATE OR REPLACE FUNCTION get_partition_high_value_date(
    p_table_owner VARCHAR2,
    p_table_name VARCHAR2,
    p_partition_name VARCHAR2
) RETURN DATE
IS
    v_high_value LONG;
    v_high_value_str VARCHAR2(32767);
    v_date_value DATE;
    v_cursor INTEGER;
    v_result INTEGER;
BEGIN
    -- Open a cursor to fetch the LONG value
    v_cursor := DBMS_SQL.OPEN_CURSOR;
    
    -- Parse the query
    DBMS_SQL.PARSE(v_cursor,
        'SELECT high_value FROM all_tab_partitions ' ||
        'WHERE table_owner = :owner ' ||
        'AND table_name = :tname ' ||
        'AND partition_name = :pname',
        DBMS_SQL.NATIVE);
    
    -- Bind variables
    DBMS_SQL.BIND_VARIABLE(v_cursor, ':owner', p_table_owner);
    DBMS_SQL.BIND_VARIABLE(v_cursor, ':tname', p_table_name);
    DBMS_SQL.BIND_VARIABLE(v_cursor, ':pname', p_partition_name);
    
    -- Define column to fetch LONG as VARCHAR2
    DBMS_SQL.DEFINE_COLUMN_LONG(v_cursor, 1);
    
    -- Execute the query
    v_result := DBMS_SQL.EXECUTE(v_cursor);
    
    -- Fetch the result
    IF DBMS_SQL.FETCH_ROWS(v_cursor) > 0 THEN
        DBMS_SQL.COLUMN_VALUE_LONG(v_cursor, 1, 32767, 0, v_high_value_str, v_result);
        
        -- Close cursor
        DBMS_SQL.CLOSE_CURSOR(v_cursor);
        
        -- Remove TO_DATE wrapper if present and extract the date literal
        -- HIGH_VALUE typically looks like: TO_DATE(' 2024-01-01 00:00:00', 'SYYYY-MM-DD HH24:MI:SS', 'NLS_CALENDAR=GREGORIAN')
        v_high_value_str := TRIM(v_high_value_str);
        
        -- Execute the high_value expression to get the actual date
        BEGIN
            EXECUTE IMMEDIATE 'SELECT ' || v_high_value_str || ' FROM DUAL' INTO v_date_value;
            RETURN v_date_value;
        EXCEPTION
            WHEN OTHERS THEN
                -- If execution fails, return NULL
                RETURN NULL;
        END;
    ELSE
        DBMS_SQL.CLOSE_CURSOR(v_cursor);
        RETURN NULL;
    END IF;
    
EXCEPTION
    WHEN OTHERS THEN
        IF DBMS_SQL.IS_OPEN(v_cursor) THEN
            DBMS_SQL.CLOSE_CURSOR(v_cursor);
        END IF;
        RETURN NULL;
END get_partition_high_value_date;
/

-- Example usage:
-- SELECT 
--     partition_name,
--     get_partition_high_value_date(table_owner, table_name, partition_name) as high_value_date
-- FROM user_tab_partitions
-- WHERE table_name = 'YOUR_TABLE_NAME';