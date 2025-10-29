CREATE OR REPLACE FUNCTION get_partition_date(p_owner IN VARCHAR2,
                                              p_table IN VARCHAR2,
                                              p_partition IN VARCHAR2)
    RETURN DATE
IS
    v_str VARCHAR2(32767);
    v_date DATE;
BEGIN
    SELECT   high_value
      INTO   v_str
      FROM   dba_tab_partitions
     WHERE   table_owner = p_owner AND table_name = p_table AND partition_name = p_partition;
 
    IF UPPER(v_str) = 'MAXVALUE'
    THEN
        v_date := TO_DATE('9999-12-31 23:59:59', 'yyyy-mm-dd hh24:mi:ss');
    ELSE
        EXECUTE IMMEDIATE 'select ' || v_str || ' from dual' INTO   v_date;
    END IF;
    RETURN v_date;
EXCEPTION
    WHEN OTHERS
    THEN
        DBMS_OUTPUT.put_line(v_str || ' ' || SQLERRM);
        RETURN NULL;
END;
/