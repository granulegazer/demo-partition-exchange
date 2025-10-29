CREATE OR REPLACE FUNCTION convert_high_value_to_date(
    p_high_value_str VARCHAR2
) RETURN DATE
IS
    v_date_value DATE;
    v_clean_str VARCHAR2(32767);
BEGIN
    -- Clean up the input string
    v_clean_str := TRIM(p_high_value_str);
    
    -- Check if the string is not empty
    IF v_clean_str IS NULL OR LENGTH(v_clean_str) = 0 THEN
        RETURN NULL;
    END IF;
    
    -- Execute the high_value expression to get the actual date
    -- HIGH_VALUE typically looks like: TO_DATE(' 2024-01-01 00:00:00', 'SYYYY-MM-DD HH24:MI:SS', 'NLS_CALENDAR=GREGORIAN')
    BEGIN
        EXECUTE IMMEDIATE 'SELECT ' || v_clean_str || ' FROM DUAL' INTO v_date_value;
        RETURN v_date_value;
    EXCEPTION
        WHEN OTHERS THEN
            -- If execution fails, return NULL
            RETURN NULL;
    END;
    
EXCEPTION
    WHEN OTHERS THEN
        RETURN NULL;
END convert_high_value_to_date;
/

-- Example usage with a helper procedure to read LONG values:
-- Since you can't pass LONG directly, you need to convert it to VARCHAR2 first
-- Here's a procedure that demonstrates the full solution:

CREATE OR REPLACE PROCEDURE show_partition_dates(
    p_table_name VARCHAR2
) IS
    v_cursor INTEGER;
    v_result INTEGER;
    v_high_value_str VARCHAR2(32767);
    v_length INTEGER;
    v_partition_name VARCHAR2(128);
    v_date_value DATE;
BEGIN
    v_cursor := DBMS_SQL.OPEN_CURSOR;
    
    DBMS_SQL.PARSE(v_cursor,
        'SELECT partition_name, high_value FROM user_tab_partitions ' ||
        'WHERE table_name = :tname ORDER BY partition_position',
        DBMS_SQL.NATIVE);
    
    DBMS_SQL.BIND_VARIABLE(v_cursor, ':tname', UPPER(p_table_name));
    DBMS_SQL.DEFINE_COLUMN(v_cursor, 1, v_partition_name, 128);
    DBMS_SQL.DEFINE_COLUMN_LONG(v_cursor, 2);
    
    v_result := DBMS_SQL.EXECUTE(v_cursor);
    
    DBMS_OUTPUT.PUT_LINE('Partition Name' || CHR(9) || 'High Value Date');
    DBMS_OUTPUT.PUT_LINE('----------------------------------------');
    
    WHILE DBMS_SQL.FETCH_ROWS(v_cursor) > 0 LOOP
        DBMS_SQL.COLUMN_VALUE(v_cursor, 1, v_partition_name);
        DBMS_SQL.COLUMN_VALUE_LONG(v_cursor, 2, 32767, 0, v_high_value_str, v_length);
        
        v_date_value := convert_high_value_to_date(v_high_value_str);
        
        DBMS_OUTPUT.PUT_LINE(v_partition_name || CHR(9) || TO_CHAR(v_date_value, 'YYYY-MM-DD HH24:MI:SS'));
    END LOOP;
    
    DBMS_SQL.CLOSE_CURSOR(v_cursor);
EXCEPTION
    WHEN OTHERS THEN
        IF DBMS_SQL.IS_OPEN(v_cursor) THEN
            DBMS_SQL.CLOSE_CURSOR(v_cursor);
        END IF;
        RAISE;
END show_partition_dates;
/

-- Usage:
-- SET SERVEROUTPUT ON
-- EXEC show_partition_dates('YOUR_TABLE_NAME');

-- Or in a query (you still need DBMS_SQL to extract LONG first):
-- SELECT 
--     partition_name,
--     convert_high_value_to_date(high_value_varchar) as partition_date
-- FROM (
--     -- You'd need to use a pipelined function or similar to convert LONG to VARCHAR2
-- );