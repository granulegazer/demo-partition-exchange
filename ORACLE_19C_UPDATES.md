# Oracle 19.26 Updates and Execution Logging

## Overview
Updated partition archival system to use Oracle 19.26 syntax, renamed tables with proper prefixes (SNPARCH_CNF_* and SNPARCH_CTL_*), and added comprehensive execution logging.

## Table Name Changes

### Configuration Table
- **Old**: `partition_archive_config`
- **New**: `SNPARCH_CNF_PARTITION_ARCHIVE`
- **Prefix**: `SNPARCH_CNF_*` (Snap Archive Configuration)

### Execution Log Table (NEW)
- **Name**: `SNPARCH_CTL_EXECUTION_LOG`
- **Prefix**: `SNPARCH_CTL_*` (Snap Archive Control)
- **Purpose**: Tracks every partition exchange execution with detailed metadata

## New Features

### 1. Execution Logging (`SNPARCH_CTL_EXECUTION_LOG`)

Every partition exchange is now logged with:

#### Core Information
- `execution_id` - Auto-generated ID using Oracle 19c IDENTITY column
- `execution_date` - TIMESTAMP(6) when exchange was performed
- `source_table_name` - Source table
- `archive_table_name` - Archive table
- `source_partition_name` - Partition name in source (e.g., SYS_P21)
- `archive_partition_name` - Partition name in archive (e.g., SYS_P495298)
- `partition_date` - Business date of the data
- `records_archived` - Number of rows moved

#### Size and Compression
- `partition_size_mb` - Size of partition in megabytes
- `is_compressed` - Y/N flag indicating if partition is compressed
- `compression_type` - Type: BASIC, OLTP, QUERY LOW/HIGH, ARCHIVE LOW/HIGH
- `compression_ratio` - Compression ratio achieved (if applicable)

#### Performance Metrics
- `exchange_duration_seconds` - How long the exchange took
- `stats_gather_duration_seconds` - How long statistics gathering took

#### Status and Audit
- `validation_status` - SUCCESS, WARNING, or ERROR
- `error_code` - Oracle error code if failed
- `error_message` - Error message if failed
- `executed_by` - User who ran the procedure
- `session_id` - Oracle session ID

### 2. Configuration Enhancements

New columns in `SNPARCH_CNF_PARTITION_ARCHIVE`:

```sql
enable_compression VARCHAR2(1) DEFAULT 'N'
compression_type VARCHAR2(30) DEFAULT NULL
```

Supports Oracle 19c compression types:
- `BASIC` - Basic table compression
- `OLTP` - OLTP compression (good balance)
- `QUERY LOW` - Query-optimized low compression
- `QUERY HIGH` - Query-optimized high compression
- `ARCHIVE LOW` - Archive low compression
- `ARCHIVE HIGH` - Archive high compression (maximum)

### 3. Oracle 19c Syntax Improvements

#### IDENTITY Columns
```sql
execution_id NUMBER GENERATED ALWAYS AS IDENTITY (START WITH 1 INCREMENT BY 1)
```
No more manual sequence management!

#### TIMESTAMP(6) with SYSTIMESTAMP
```sql
execution_date TIMESTAMP(6) DEFAULT SYSTIMESTAMP
created_date TIMESTAMP(6) DEFAULT SYSTIMESTAMP
```
Microsecond precision for accurate timing.

#### FETCH FIRST (Row Limiting)
```sql
SELECT * FROM snparch_ctl_execution_log
ORDER BY execution_id DESC
FETCH FIRST 20 ROWS ONLY;  -- Oracle 19c syntax
```

#### INTERVAL Literals
```sql
WHERE updated_date > SYSTIMESTAMP - INTERVAL '30' DAY
```

#### Enhanced DBMS_STATS Options
```sql
DBMS_STATS.GATHER_TABLE_STATS(
    ownname => USER,
    tabname => 'SALES',
    estimate_percent => DBMS_STATS.AUTO_SAMPLE_SIZE,
    cascade => TRUE,
    method_opt => 'FOR ALL COLUMNS SIZE AUTO',
    degree => DBMS_STATS.AUTO_DEGREE,      -- NEW: Auto parallelism
    granularity => 'AUTO'                   -- NEW: Auto granularity
);
```

### 4. Compression Detection

The procedure now automatically detects compression status:

```sql
SELECT 
    CASE WHEN compression = 'ENABLED' THEN 'Y' ELSE 'N' END,
    compress_for
INTO 
    v_is_compressed,
    v_compression_type
FROM user_tab_partitions
WHERE table_name = 'SALES_ARCHIVE'
  AND partition_name = v_archive_partition_name;
```

## Usage Examples

### Query Execution History

```sql
-- Recent executions
SELECT 
    execution_id,
    TO_CHAR(execution_date, 'YYYY-MM-DD HH24:MI:SS') AS executed_at,
    source_table_name,
    source_partition_name,
    archive_partition_name,
    records_archived,
    partition_size_mb,
    is_compressed,
    compression_type,
    ROUND(exchange_duration_seconds, 3) AS exchange_sec,
    validation_status
FROM snparch_ctl_execution_log
ORDER BY execution_id DESC
FETCH FIRST 10 ROWS ONLY;
```

### Summary by Table

```sql
SELECT 
    source_table_name,
    COUNT(*) AS total_exchanges,
    SUM(records_archived) AS total_records,
    ROUND(SUM(partition_size_mb), 2) AS total_size_mb,
    SUM(CASE WHEN is_compressed = 'Y' THEN 1 ELSE 0 END) AS compressed_partitions,
    ROUND(AVG(exchange_duration_seconds), 3) AS avg_exchange_sec,
    ROUND(AVG(stats_gather_duration_seconds), 2) AS avg_stats_sec
FROM snparch_ctl_execution_log
GROUP BY source_table_name;
```

### Enable Compression

```sql
-- Configure OLTP compression for archive
UPDATE snparch_cnf_partition_archive
SET enable_compression = 'Y',
    compression_type = 'OLTP',
    updated_date = SYSTIMESTAMP,
    updated_by = USER
WHERE source_table_name = 'SALES';
COMMIT;
```

### Performance Analysis

```sql
-- Find slow executions
SELECT 
    execution_id,
    source_table_name,
    partition_date,
    records_archived,
    ROUND(exchange_duration_seconds, 3) AS exchange_sec,
    ROUND(stats_gather_duration_seconds, 2) AS stats_sec,
    ROUND(exchange_duration_seconds + NVL(stats_gather_duration_seconds, 0), 2) AS total_sec
FROM snparch_ctl_execution_log
WHERE exchange_duration_seconds + NVL(stats_gather_duration_seconds, 0) > 10  -- Slower than 10 seconds
ORDER BY total_sec DESC;
```

## Oracle 19c Compatibility Checklist

✅ **IDENTITY columns** - Auto-incrementing primary keys
✅ **TIMESTAMP(6)** - Microsecond precision timestamps
✅ **SYSTIMESTAMP** - Current timestamp with timezone
✅ **FETCH FIRST** - Row limiting clause (replaces ROWNUM)
✅ **INTERVAL literals** - Date arithmetic
✅ **SEGMENT CREATION IMMEDIATE** - Explicit segment creation
✅ **Enhanced DBMS_STATS** - AUTO_DEGREE and granularity options
✅ **Compression detection** - Query user_tab_partitions.compression
✅ **SYS_CONTEXT** - Session information (session_id)

## Performance Improvements

### Before (Old Approach)
- Manual sequence management for IDs
- No execution history tracking
- No compression tracking
- No performance metrics
- DATE datatype (day precision)
- Manual ROWNUM filtering

### After (Oracle 19c)
- Automatic ID generation
- Complete execution audit trail
- Automatic compression detection
- Detailed timing metrics
- TIMESTAMP(6) (microsecond precision)
- FETCH FIRST (optimizer-friendly)

## Migration from Old Version

### Automatic Migration
The `01_setup.sql` script automatically handles cleanup:

```sql
BEGIN
    EXECUTE IMMEDIATE 'DROP TABLE partition_archive_config PURGE';
EXCEPTION
    WHEN OTHERS THEN NULL;
END;
/
```

### Manual Migration (if needed)

```sql
-- Export old configuration
CREATE TABLE partition_archive_config_backup AS
SELECT * FROM partition_archive_config;

-- Insert into new table
INSERT INTO snparch_cnf_partition_archive (
    source_table_name,
    archive_table_name,
    staging_table_name,
    is_active,
    validate_before_exchange,
    gather_stats_after_exchange,
    enable_compression,
    compression_type
)
SELECT 
    source_table_name,
    archive_table_name,
    staging_table_name,
    is_active,
    validate_before_exchange,
    gather_stats_after_exchange,
    'N',  -- Default: No compression
    NULL
FROM partition_archive_config_backup;
COMMIT;
```

## Monitoring Queries

### Check Execution Log Growth

```sql
SELECT 
    TO_CHAR(TRUNC(execution_date), 'YYYY-MM-DD') AS execution_day,
    COUNT(*) AS executions,
    SUM(records_archived) AS total_records,
    ROUND(SUM(partition_size_mb), 2) AS total_mb
FROM snparch_ctl_execution_log
WHERE execution_date >= SYSTIMESTAMP - INTERVAL '7' DAY
GROUP BY TRUNC(execution_date)
ORDER BY execution_day DESC;
```

### Compression Effectiveness

```sql
SELECT 
    source_table_name,
    compression_type,
    COUNT(*) AS partitions,
    ROUND(SUM(partition_size_mb), 2) AS total_size_mb,
    ROUND(AVG(compression_ratio), 2) AS avg_compression_ratio
FROM snparch_ctl_execution_log
WHERE is_compressed = 'Y'
GROUP BY source_table_name, compression_type
ORDER BY source_table_name, compression_type;
```

### Error Analysis

```sql
SELECT 
    execution_id,
    TO_CHAR(execution_date, 'YYYY-MM-DD HH24:MI:SS') AS when_failed,
    source_table_name,
    partition_date,
    validation_status,
    error_code,
    error_message
FROM snparch_ctl_execution_log
WHERE validation_status != 'SUCCESS'
ORDER BY execution_date DESC;
```

## Files Updated

1. **01_setup.sql**
   - Renamed table to `SNPARCH_CNF_PARTITION_ARCHIVE`
   - Added `SNPARCH_CTL_EXECUTION_LOG` table
   - Added compression columns
   - Used Oracle 19c syntax (IDENTITY, TIMESTAMP(6))

2. **03_archive_procedure.sql**
   - Updated to read from `SNPARCH_CNF_PARTITION_ARCHIVE`
   - Added execution logging to `SNPARCH_CTL_EXECUTION_LOG`
   - Added compression detection
   - Added timing metrics
   - Enhanced DBMS_STATS calls with Oracle 19c options

3. **05_monitoring_queries.sql**
   - Updated table references
   - Added execution log queries
   - Added compression status queries
   - Used Oracle 19c FETCH FIRST syntax

4. **config_management.sql**
   - Updated table name throughout
   - Added compression configuration examples
   - Used SYSTIMESTAMP instead of SYSDATE
   - Added Oracle 19c INTERVAL syntax

5. **99_cleanup.sql**
   - Added drop for `SNPARCH_CTL_EXECUTION_LOG`
   - Updated table name references

## Best Practices

1. **Regular Log Cleanup**
   ```sql
   -- Archive old execution logs (keep last 90 days)
   DELETE FROM snparch_ctl_execution_log
   WHERE execution_date < SYSTIMESTAMP - INTERVAL '90' DAY
     AND validation_status = 'SUCCESS';
   COMMIT;
   ```

2. **Monitor Execution Performance**
   - Review `exchange_duration_seconds` regularly
   - Identify slow statistics gathering operations
   - Optimize compression settings based on results

3. **Compression Strategy**
   - Use `OLTP` for recent archives (good balance)
   - Use `ARCHIVE HIGH` for old data (maximum compression)
   - Monitor `partition_size_mb` to track space savings

4. **Validation Monitoring**
   - Always check `validation_status` column
   - Investigate any `WARNING` or `ERROR` entries
   - Set up alerts for failed executions

## Backward Compatibility

The procedure call interface remains unchanged:

```sql
BEGIN
    archive_partitions_by_dates(
        p_table_name => 'SALES',
        p_dates => date_array_type(DATE '2024-01-15')
    );
END;
/
```

All changes are internal - existing code continues to work!
