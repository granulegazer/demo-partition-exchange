-- ========================================
-- PARTITION DETAILS QUERY
-- ========================================
-- Shows detailed information for each partition including:
-- - Partition creation date
-- - High value (partition boundary)
-- - Record count
-- - Size in MB
-- - Distinct ETL dates (concatenated if multiple)
-- ========================================
-- Uses DBA_ tables for cross-schema access
-- Requires: SELECT_CATALOG_ROLE or SELECT ANY DICTIONARY privilege
-- ========================================

SET LINESIZE 300
SET PAGESIZE 1000
SET LONG 4000
COLUMN table_owner FORMAT A15
COLUMN table_name FORMAT A20
COLUMN partition_name FORMAT A30
COLUMN partition_position FORMAT 999 HEADING 'POS'
COLUMN created FORMAT A19
COLUMN high_value_display FORMAT A25 HEADING 'HIGH_VALUE'
COLUMN num_records FORMAT 999,999,999 HEADING 'RECORDS'
COLUMN size_mb FORMAT 999,999.99 HEADING 'SIZE_MB'
COLUMN distinct_etl_dates FORMAT A50 HEADING 'DISTINCT_ETL_DATES'

-- Set your schema name and table name here
DEFINE v_schema_name = 'YOUR_SCHEMA'
DEFINE v_table_name = 'SALES'
-- Replace 'd_etl' with your ETL date column in the query below

WITH partition_stats AS (
    SELECT 
        p.table_owner,
        p.table_name,
        p.partition_name,
        p.partition_position,
        o.created,
        -- Extract high value (for date partitions)
        SUBSTR(p.high_value, 1, 4000) AS high_value_text,
        -- Get partition statistics
        NVL(ps.num_rows, 0) AS num_records,
        -- Calculate size in MB
        ROUND(NVL(s.bytes, 0) / 1024 / 1024, 2) AS size_mb
    FROM 
        dba_tab_partitions p
        LEFT JOIN dba_objects o 
            ON o.owner = p.table_owner
            AND o.object_name = p.table_name 
            AND o.subobject_name = p.partition_name
            AND o.object_type = 'TABLE PARTITION'
        LEFT JOIN (
            SELECT table_owner, partition_name, table_name, num_rows
            FROM dba_tab_partitions
        ) ps ON ps.table_owner = p.table_owner
            AND ps.table_name = p.table_name 
            AND ps.partition_name = p.partition_name
        LEFT JOIN (
            SELECT owner, segment_name, partition_name, SUM(bytes) AS bytes
            FROM dba_segments
            WHERE segment_type = 'TABLE PARTITION'
            GROUP BY owner, segment_name, partition_name
        ) s ON s.owner = p.table_owner
            AND s.segment_name = p.table_name 
            AND s.partition_name = p.partition_name
    WHERE p.table_owner = '&v_schema_name'
      AND p.table_name = '&v_table_name'
),
etl_dates AS (
    -- This part retrieves distinct ETL dates from each partition
    -- You'll need to uncomment and adjust based on your table structure
    -- NOTE: This requires direct access to the table data
    SELECT 
        '&v_schema_name' AS table_owner,
        '&v_table_name' AS table_name,
        DBMS_ROWID.ROWID_OBJECT(ROWID) AS data_object_id,
        LISTAGG(DISTINCT TO_CHAR(TRUNC(sale_date), 'YYYY-MM-DD'), ', ') 
            WITHIN GROUP (ORDER BY TO_CHAR(TRUNC(sale_date), 'YYYY-MM-DD')) AS etl_dates
    -- Change schema.table_name and column name below
    FROM &v_schema_name..&v_table_name
    GROUP BY DBMS_ROWID.ROWID_OBJECT(ROWID)
)
SELECT 
    ps.table_owner,
    ps.table_name,
    ps.partition_name,
    ps.partition_position,
    TO_CHAR(ps.created, 'YYYY-MM-DD HH24:MI:SS') AS created,
    -- Format high value for display
    CASE 
        WHEN ps.high_value_text LIKE 'TO_DATE%' THEN
            REGEXP_REPLACE(ps.high_value_text, 
                '.*''([0-9]{4}-[0-9]{2}-[0-9]{2}).*', '\1')
        WHEN ps.high_value_text LIKE 'TIMESTAMP%' THEN
            REGEXP_REPLACE(ps.high_value_text, 
                '.*''([0-9]{4}-[0-9]{2}-[0-9]{2}).*', '\1')
        ELSE SUBSTR(ps.high_value_text, 1, 25)
    END AS high_value_display,
    ps.num_records,
    ps.size_mb,
    NVL(ed.etl_dates, 'N/A') AS distinct_etl_dates
FROM 
    partition_stats ps
    LEFT JOIN (
        SELECT 
            ed.table_owner,
            ed.table_name,
            ed.data_object_id,
            ed.etl_dates,
            o.subobject_name AS partition_name
        FROM etl_dates ed
        JOIN dba_objects o 
            ON o.owner = ed.table_owner
            AND o.data_object_id = ed.data_object_id
            AND o.object_type = 'TABLE PARTITION'
    ) ed ON ed.table_owner = ps.table_owner
        AND ed.table_name = ps.table_name 
        AND ed.partition_name = ps.partition_name
ORDER BY 
    ps.table_owner,
    ps.table_name,
    ps.partition_position;

-- ========================================
-- ALTERNATIVE: Simpler version without ETL dates
-- Use this if you want to add ETL dates separately
-- ========================================
/*
SELECT 
    p.table_owner,
    p.table_name,
    p.partition_name,
    p.partition_position,
    TO_CHAR(o.created, 'YYYY-MM-DD HH24:MI:SS') AS created,
    -- Extract and format high value
    CASE 
        WHEN p.high_value LIKE 'TO_DATE%' THEN
            REGEXP_REPLACE(p.high_value, '.*''([0-9]{4}-[0-9]{2}-[0-9]{2}).*', '\1')
        WHEN p.high_value LIKE 'TIMESTAMP%' THEN
            REGEXP_REPLACE(p.high_value, '.*''([0-9]{4}-[0-9]{2}-[0-9]{2}).*', '\1')
        ELSE SUBSTR(p.high_value, 1, 25)
    END AS high_value_display,
    NVL(p.num_rows, 0) AS num_records,
    ROUND(NVL(s.bytes, 0) / 1024 / 1024, 2) AS size_mb
FROM 
    dba_tab_partitions p
    LEFT JOIN dba_objects o 
        ON o.owner = p.table_owner
        AND o.object_name = p.table_name 
        AND o.subobject_name = p.partition_name
        AND o.object_type = 'TABLE PARTITION'
    LEFT JOIN (
        SELECT owner, segment_name, partition_name, SUM(bytes) AS bytes
        FROM dba_segments
        WHERE segment_type = 'TABLE PARTITION'
        GROUP BY owner, segment_name, partition_name
    ) s ON s.owner = p.table_owner
        AND s.segment_name = p.table_name 
        AND s.partition_name = p.partition_name
WHERE p.table_owner = '&v_schema_name'
  AND p.table_name = '&v_table_name'
ORDER BY p.table_owner, p.table_name, p.partition_position;
*/

-- ========================================
-- USAGE NOTES:
-- ========================================
-- 1. Set DEFINE variables at the top:
--    DEFINE v_schema_name = 'YOUR_SCHEMA'
--    DEFINE v_table_name = 'YOUR_TABLE'
-- 2. Replace 'sale_date' with your actual ETL date column (d_etl in your case)
--    in the etl_dates CTE
-- 3. Requires SELECT privilege on DBA_ views:
--    - DBA_TAB_PARTITIONS
--    - DBA_OBJECTS
--    - DBA_SEGMENTS
-- 4. Requires SELECT privilege on the target table for ETL dates aggregation
-- 5. If the table is very large, the query may take time due to full table scan
--    for ETL dates aggregation
-- 6. Consider gathering statistics if num_rows shows 0:
--    EXEC DBMS_STATS.GATHER_TABLE_STATS('SCHEMA', 'TABLE', GRANULARITY=>'PARTITION');
-- ========================================
