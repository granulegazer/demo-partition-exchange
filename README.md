# Oracle Partition Exchange Demo

A comprehensive demonstration of Oracle Database 19.26 partition exchange methodology for efficient data archival using interval partitioning, compression, and comprehensive execution tracking with data validation.

## Features

- ✅ **Instant Data Archival** - Partition exchange (metadata-only operation)
- ✅ **Configuration-Driven** - Centralized configuration table with audit trails
- ✅ **Comprehensive Execution Logging** - Complete history with before/after metrics
- ✅ **Structure Validation** - Automatic table structure compatibility check before exchange
- ✅ **Data Validation** - Automatic record count verification after exchange
- ✅ **Index Tracking** - Monitor index count and size before/after exchange
- ✅ **Compression Support** - Multiple compression types (OLTP, QUERY, ARCHIVE)
- ✅ **Partition Validation** - Automatic check for partitioned tables
- ✅ **Performance Metrics** - Track exchange duration and statistics gathering time
- ✅ **DDL Generator** - Automated setup script generation for new tables
- ✅ **Modern Oracle Features** - Uses latest syntax and optimizations
- ✅ **Production-Ready** - Comprehensive inline documentation and error handling

## Quick Start

### Prerequisites

- Oracle Database 19c or higher
- SQL*Plus or any Oracle SQL client
- Database user with appropriate privileges:
  - CREATE TABLE
  - CREATE TYPE
  - CREATE PROCEDURE
  - CREATE FUNCTION
  - CREATE VIEW
  - CREATE INDEX
  - SELECT_CATALOG_ROLE (for DBMS_METADATA usage in DDL generator)

### Installation

```bash
# Clone the repository
git clone https://github.com/granulegazer/demo-partition-exchange.git
cd demo-partition-exchange/working

# Connect to your Oracle database
sqlplus username/password@database

# Run all scripts at once
SQL> @00_run_all.sql

# Validate deployment
SQL> @00_deployment_validation.sql

# Or run individually in sequence
SQL> @01_setup.sql                  -- Create tables, types, and objects
SQL> @02_config_data.sql            -- Insert configuration data
SQL> @03_data_generator.sql         -- Generate sample data
SQL> @04_archive_procedure.sql      -- Create archival procedure and function
SQL> @05_test_scenarios.sql         -- Run test scenarios
SQL> @06_monitoring_queries.sql     -- View monitoring queries
SQL> @07_unified_view.sql           -- Create unified data view
SQL> @08_helper_functions.sql       -- Create utility functions

# After individual installation, validate
SQL> @00_deployment_validation.sql
```

### Deployment Validation

The `00_deployment_validation.sql` script performs 12 comprehensive checks:

1. **Tables** - Verifies all tables exist and are properly partitioned
2. **Table Structures** - Validates column counts and structure compatibility
3. **Types** - Checks collection types exist
4. **Functions** - Verifies functions exist and are VALID
5. **Procedures** - Checks procedures exist and are VALID
6. **Views** - Validates unified view exists
7. **Indexes** - Confirms indexes exist and are VALID
8. **Constraints** - Verifies primary keys and constraints
9. **Partition Configuration** - Checks partition setup and compatibility
10. **Configuration Data** - Validates configuration records
11. **Object Dependencies** - Ensures proper dependency chain
12. **Invalid Objects** - Lists any invalid objects

**Expected Output:**
```
Total Checks:   20+
Errors:         0
Warnings:       0-2 (optional objects)
Status:         SUCCESS
```

## Architecture Overview

### Core Tables

1. **`SALES`** - Main transaction table with interval partitioning (daily)
2. **`SALES_ARCHIVE`** - Archive table with same structure
3. **`SNPARCH_CNF_PARTITION_ARCHIVE`** - Configuration table (controls archival behavior)
4. **`SNPARCH_CTL_EXECUTION_LOG`** - Execution log (tracks all partition exchanges)

### Configuration Table (`SNPARCH_CNF_PARTITION_ARCHIVE`)

Centralized configuration for partition archival with these controls:

| Column | Purpose |
|--------|---------|
| `source_table_name` | Source partitioned table |
| `archive_table_name` | Archive partitioned table |
| `staging_table_name` | Temporary staging table name |
| `is_active` | Enable/disable archival (Y/N) |
| `validate_before_exchange` | Validate indexes before exchange (Y/N) |
| `gather_stats_after_exchange` | Gather statistics after exchange (Y/N) |
| `enable_compression` | Enable compression on archive (Y/N) |
| `compression_type` | BASIC, OLTP, QUERY LOW/HIGH, ARCHIVE LOW/HIGH |

### Execution Log Table (`SNPARCH_CTL_EXECUTION_LOG`)

Tracks every partition exchange with comprehensive before/after metrics:

**Basic Execution Details:**
- Execution ID (IDENTITY column - auto-incrementing)
- Execution date and timestamp
- Source and archive table names
- Source and archive partition names
- Session ID and executed by user
- Partition date and validation status

**Data Metrics:**
- Partition size in MB
- Records archived count
- Source records before/after exchange
- Archive records before/after exchange
- Record count match status (Y/N)
- Data validation status (PASS/FAIL)

**Index Metrics:**
- Source table index count
- Archive table index count
- Source table index size (MB)
- Archive table index size (MB)
- Invalid indexes before exchange count
- Invalid indexes after exchange count

**Performance Metrics:**
- Exchange duration (seconds)
- Statistics gathering duration (seconds)
- Total duration (seconds)

**Compression Details:**
- Compression status (Y/N)
- Compression type (BASIC/OLTP/QUERY/ARCHIVE)

**Error Tracking:**
- Error message (if failed)
- Error stack (if failed)

### Partition Exchange Flow

```
┌─────────────┐         ┌─────────────┐         ┌─────────────┐
│   SALES     │         │   STAGING   │         │   ARCHIVE   │
│ (Partition) │────────>│   TABLE     │────────>│ (Partition) │
└─────────────┘         └─────────────┘         └─────────────┘
   INSTANT                  INSTANT
   < 1 sec                  < 1 sec
```

Both exchanges are metadata-only operations - no physical data movement!

### Procedure Execution Steps (v_step Trace Codes)

The archival procedure uses granular step numbering for precise debugging and tracing. Each step has a unique trace code that appears in the execution log.

#### Initial Validation Steps (1-5)

| Step | Description | Purpose |
|------|-------------|---------|
| 1 | Verify table is partitioned | Confirms source table has partitioning enabled |
| 2 | Load configuration | Retrieves settings from SNPARCH_CNF_PARTITION_ARCHIVE |
| 3 | Validate table structures | Comprehensive compatibility check (columns, types, partition keys) |
| 4 | Get initial table stats | Baseline metrics for source and archive tables |
| 5 | Validate indexes (optional) | Checks and rebuilds INVALID/UNUSABLE indexes if validate_before_exchange = 'Y' |

#### Per-Date Processing Steps (100 + i*100 + N)

For each date to archive, where `i` is the date iteration (1, 2, 3...), the procedure executes these steps:

| Step Offset | Example (1st date) | Description | Details |
|-------------|-------------------|-------------|---------|
| +0 | 100 | Start processing date | Begin processing specific partition date |
| +1 | 101 | No partition found | Warning logged if partition doesn't exist (CONTINUE to next date) |
| +2 | 102 | Found partition | Log partition name for the date |
| +3 | 103 | Count records in partition | Get record count before exchange |
| **Metrics Collection** ||||
| +4 | 104 | Source table count before | Count all records in source table |
| +4.1 | 104.1 | Archive table count before | Count all records in archive table |
| +4.2 | 104.2 | Source index metrics | Get count and size of source indexes |
| +4.3 | 104.3 | Archive index metrics | Get count and size of archive indexes |
| +4.4 | 104.4 | Invalid indexes count before | Count INVALID/UNUSABLE indexes |
| +4.5 | 104.5 | Truncate staging table | Empty staging table before exchange |
| **First Exchange (Source → Staging)** ||||
| +5 | 105 | Exchange source → staging | ALTER TABLE ... EXCHANGE PARTITION ... WITHOUT VALIDATION |
| +5.1 | 105.1 | Rebuild source indexes | Unconditional full rebuild of ALL source table indexes |
| **Archive Partition Setup** ||||
| +6 | 106 | Check/create archive partition | Verify archive partition exists |
| +6.1 | 106.1 | Insert test row (if needed) | Create partition if doesn't exist |
| +6.2 | 106.2 | Get new partition name (if needed) | Retrieve newly created partition name |
| +6.3 | 106.3 | Delete test row (if needed) | Clean up test data |
| **Second Exchange (Staging → Archive)** ||||
| +7 | 107 | Exchange staging → archive | ALTER TABLE ... EXCHANGE PARTITION ... WITHOUT VALIDATION |
| +7.1 | 107.1 | Rebuild archive indexes | Unconditional full rebuild of ALL archive table indexes |
| **Post-Exchange Validation** ||||
| +8 | 108 | Log completion | Log exchange completion with duration |
| +9 | 109 | Source table count after | Count all records in source table |
| +9.1 | 109.1 | Archive table count after | Count all records in archive table |
| +9.2 | 109.2 | Invalid indexes count after | Count INVALID/UNUSABLE indexes |
| +9.3 | 109.3 | Validate source record count | Verify source records decreased correctly |
| +9.4 | 109.4 | Validate archive record count | Verify archive records increased correctly |
| **Cleanup** ||||
| +10 | 110 | Drop source partition | Remove now-empty partition from source table |
| +11 | 111 | Insert to execution log | Record all metrics to SNPARCH_CTL_EXECUTION_LOG |

**Example for multiple dates:**
- **1st date**: Steps 100-111
- **2nd date**: Steps 200-211
- **3rd date**: Steps 300-311

**Note on Empty Partitions:**
- If partition has 0 records at step 103, the procedure skips steps 104-111
- Empty partitions are logged but NOT dropped (preserving partition structure)

#### Post-Processing Steps (50-60)

| Step | Description | Purpose |
|------|-------------|---------|
| 50 | Validate indexes after all exchanges | Final check for INVALID/UNUSABLE indexes (if validate_before_exchange = 'Y') |
| 51 | Gather statistics on source table | Update optimizer stats (if gather_stats_after_exchange = 'Y') |
| 52 | Gather statistics on archive table | Update optimizer stats (if gather_stats_after_exchange = 'Y') |
| 60 | Final stats and summary | Log completion with final table metrics |

**Debugging with v_step:**
- All steps are logged to the error log via `prc_log_error_autonomous`
- Query error log by step number to trace exact execution point
- Step numbers allow pinpointing failures in multi-date operations
- Example: ORA-01502 at step 209 means 2nd date, during source table count after exchange

## DDL Generator Tool

The `generate_archive_setup.sql` script automates the creation of archive infrastructure for any partitioned table.

### Features

- **DBMS_METADATA Integration** - Uses Oracle's built-in DDL extraction for accuracy
- **Cross-Schema Support** - Uses DBA_ views to access any schema (requires privileges)
- **Automatic Naming** - Applies SNPARCH_* prefix to archive and staging tables
- **Complete Structure** - Generates constraints, indexes, and partitions
- **Configuration Ready** - Creates INSERT statement for config table
- **Clean Output** - No schema names or storage parameters
- **Index Alignment** - Automatically renames indexes to match archive table

### Prerequisites

- **SELECT_CATALOG_ROLE** or **SELECT ANY DICTIONARY** privilege
- SELECT privilege on DBA_TABLES, DBA_CONSTRAINTS, DBA_INDEXES, DBA_CONS_COLUMNS

### Configuration

Edit the script before running to set:
```sql
v_source_table VARCHAR2(128) := 'SALES';  -- Change to your table name
v_schema_name VARCHAR2(128) := USER;      -- Change to target schema or leave as USER
```

### Usage

```sql
-- After editing the script with your table name and schema
SQL> @generate_archive_setup.sql
```

### Generated Output

1. **Archive Table DDL** - `SNPARCH_ORDERS`
   - All columns from source table
   - All constraints (renamed to match archive table)
   - All indexes (renamed to match archive table)
   - All partitions (renamed: source_old → archive_old)
   - Same partition scheme as source

2. **Staging Table DDL** - `ORDERS_STAGING`
   - CTAS structure (CREATE TABLE AS SELECT ... WHERE 1=0)
   - Unpartitioned (required for exchange)
   - No indexes (added by procedure during exchange)

3. **Configuration INSERT**
   - Pre-populated with recommended settings
   - Ready to execute after reviewing/customizing

### Transform Parameters

The generator uses these DBMS_METADATA settings:
- `EMIT_SCHEMA` = FALSE (no schema names)
- `SEGMENT_ATTRIBUTES` = FALSE (no tablespace clauses)
- `STORAGE` = FALSE (no storage parameters)

## Repository Structure

```
demo-partition-exchange/
├── README.md                           # This file
├── SEPARATION_SUMMARY.md               # Framework/demo separation details
├── .github/
│   └── copilot-instructions.md         # AI coding guidelines
└── working/
    ├── FRAMEWORK FILES (Required):
    │   ├── 00_quick_start.sql          # Deployment guide with 3 options
    │   ├── 00_run_all.sql              # Master script - runs all components
    │   ├── 01_setup.sql                # Creates framework objects (types, tables, indexes)
    │   ├── 02_config_data.sql          # Production config template (customize!)
    │   ├── 04_archive_procedure.sql    # Core archival procedure with validation
    │   ├── 08_helper_functions.sql     # Utility functions (date ranges, partition info)
    │   └── generate_archive_setup.sql  # DDL generator for new table pairs (cross-schema)
    │
    ├── DEMO FILES (Optional - for learning):
    │   ├── demo_tables_setup.sql       # Creates SALES/SALES_ARCHIVE demo tables
    │   ├── demo_config_data.sql        # Configuration for demo tables
    │   ├── 03_data_generator.sql       # Generates test data for SALES table
    │   └── 05_test_scenarios.sql       # Comprehensive test scenarios
    │
    ├── MONITORING FILES (Optional but recommended):
    │   ├── 06_monitoring_queries.sql   # 16 monitoring and analysis queries
    │   └── 07_unified_view.sql         # Combined execution view with metrics
    │
    └── UTILITY FILES:
        ├── 00_deployment_validation.sql # Validates installation (12 categories, 20+ checks)
        ├── config_management.sql       # Configuration management scripts
        └── 99_cleanup.sql              # Cleanup script (use with caution)
```

### File Purposes

**Framework Files (Required for all deployments):**
- `01_setup.sql` - Core objects: DATE_ARRAY_TYPE, config table, execution log (38 columns)
- `04_archive_procedure.sql` - Main procedure with 7 pre-exchange validations
- `08_helper_functions.sql` - Helper functions for date ranges and partition info

**Demo Files (Optional - skip for production):**
- `demo_tables_setup.sql` - SALES and SALES_ARCHIVE interval partitioned tables
- `demo_config_data.sql` - Configuration linking SALES → SALES_ARCHIVE
- `03_data_generator.sql` - Sample data for testing
- `05_test_scenarios.sql` - Test execution and validation

**Deployment Options:**
1. **Complete Demo** (learning): Run `@00_run_all.sql` - includes everything
2. **Production Only**: Run framework files only, skip demo files
3. **Quick Validation**: Framework + demo for testing, skip monitoring

See `SEPARATION_SUMMARY.md` for detailed deployment scenarios.

## Usage Examples

### Generate Archive Setup for New Table

```sql
-- Generate complete DDL setup for archiving a new table
-- Creates archive table, staging table, and configuration INSERT

-- IMPORTANT: Edit the script first to set:
--   v_source_table := 'YOUR_TABLE_NAME';  -- Change this
--   v_schema_name := 'YOUR_SCHEMA';       -- Change this or leave as USER

SQL> @generate_archive_setup.sql

-- The script generates:
--   1. SNPARCH_YOUR_TABLE_NAME (archive table) - with all constraints, indexes, partitions
--   2. SNPARCH_YOUR_TABLE_NAME_STAGING_TEMP (staging table) - unpartitioned CTAS structure
--   3. INSERT statement for SNPARCH_CNF_PARTITION_ARCHIVE configuration

-- Prerequisites:
--   - SELECT_CATALOG_ROLE or SELECT ANY DICTIONARY privilege (for DBA_ views)
--   - SELECT privilege on DBA_TABLES, DBA_CONSTRAINTS, DBA_INDEXES

-- Review and execute the generated DDL
```

### Basic Archival

```sql
-- Archive specific dates
BEGIN
    archive_partitions_by_dates(
        p_table_name => 'SALES',
        p_dates => date_array_type(
            DATE '2024-01-15',
            DATE '2024-01-16',
            DATE '2024-01-17'
        )
    );
END;
/
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

-- Now archive with compression
BEGIN
    archive_partitions_by_dates(
        p_table_name => 'SALES',
        p_dates => date_array_type(DATE '2024-02-01')
    );
END;
/
```

### Check Execution History with Detailed Metrics

```sql
-- Recent executions with comprehensive metrics
SELECT 
    execution_id,
    TO_CHAR(execution_date, 'YYYY-MM-DD HH24:MI:SS') AS executed_at,
    source_partition_name,
    archive_partition_name,
    records_archived,
    partition_size_mb,
    source_index_count,
    archive_index_count,
    ROUND(source_index_size_mb, 2) AS src_idx_mb,
    ROUND(archive_index_size_mb, 2) AS arch_idx_mb,
    data_validation_status,
    record_count_match,
    is_compressed,
    ROUND(exchange_duration_seconds, 3) AS exchange_sec,
    ROUND(stats_gather_duration_seconds, 2) AS stats_sec,
    ROUND(total_duration_seconds, 2) AS total_sec,
    validation_status
FROM snparch_ctl_execution_log
ORDER BY execution_id DESC
FETCH FIRST 10 ROWS ONLY;

-- Check for data validation failures
SELECT 
    execution_id,
    partition_date,
    source_partition_name,
    source_records_before,
    source_records_after,
    archive_records_before,
    archive_records_after,
    record_count_match,
    data_validation_status,
    validation_status
FROM snparch_ctl_execution_log
WHERE data_validation_status = 'FAIL'
   OR record_count_match = 'N'
ORDER BY execution_date DESC;
```

### Query Execution Summary

```sql
-- Summary by table with index metrics
SELECT 
    source_table_name,
    COUNT(*) AS total_exchanges,
    SUM(records_archived) AS total_records,
    ROUND(SUM(partition_size_mb), 2) AS total_size_mb,
    ROUND(AVG(source_index_count), 1) AS avg_source_indexes,
    ROUND(AVG(archive_index_count), 1) AS avg_archive_indexes,
    ROUND(SUM(source_index_size_mb), 2) AS total_src_idx_mb,
    ROUND(SUM(archive_index_size_mb), 2) AS total_arch_idx_mb,
    SUM(CASE WHEN is_compressed = 'Y' THEN 1 ELSE 0 END) AS compressed_partitions,
    SUM(CASE WHEN data_validation_status = 'PASS' THEN 1 ELSE 0 END) AS passed_validations,
    SUM(CASE WHEN data_validation_status = 'FAIL' THEN 1 ELSE 0 END) AS failed_validations,
    ROUND(AVG(exchange_duration_seconds), 3) AS avg_exchange_sec,
    ROUND(AVG(total_duration_seconds), 2) AS avg_total_sec
FROM snparch_ctl_execution_log
GROUP BY source_table_name;
```

### Configuration Management

```sql
-- Disable validation for faster execution (not recommended for production)
UPDATE snparch_cnf_partition_archive
SET validate_before_exchange = 'N',
    gather_stats_after_exchange = 'N',
    updated_date = SYSTIMESTAMP,
    updated_by = USER
WHERE source_table_name = 'SALES';
COMMIT;

-- Re-enable for production (recommended)
UPDATE snparch_cnf_partition_archive
SET validate_before_exchange = 'Y',
    gather_stats_after_exchange = 'Y',
    updated_date = SYSTIMESTAMP,
    updated_by = USER
WHERE source_table_name = 'SALES';
COMMIT;
```

## Key Features

### Modern Oracle Optimizations

- **IDENTITY Columns** - Auto-incrementing PKs without sequences
- **TIMESTAMP(6)** - Microsecond precision for accurate timing
- **FETCH FIRST** - Modern row limiting (replaces ROWNUM)
- **INTERVAL Literals** - Clean date arithmetic
- **Enhanced DBMS_STATS** - AUTO_DEGREE, AUTO_SAMPLE_SIZE, and granularity options
- **Compression Detection** - Automatic detection of partition compression status
- **DBMS_METADATA** - Automated DDL extraction for archive setup generation
- **Automatic Index Rebuild** - Unconditionally rebuilds all indexes after each partition exchange (full index rebuild, not partition-level)
- **UNUSABLE Index Detection** - Post-exchange validation checks for both INVALID and UNUSABLE indexes

### Data Validation & Integrity

- **Structure Validation** - Comprehensive table structure compatibility check before exchange:
  - Verifies all three tables exist (source, archive, staging)
  - Confirms archive table is partitioned and staging table is NOT partitioned
  - Validates column count matches across all tables
  - Checks column names, data types, sizes, and nullability match exactly
  - Verifies partition key columns match between source and archive tables
  - Raises specific error codes for each validation failure (ORA-20010 through ORA-20018)
- **Partition Validation** - Procedure checks if table is partitioned, throws exception if not
- **Record Count Validation** - Automatic before/after comparison to detect data loss
- **Index Health Tracking** - Monitor invalid and unusable indexes before and after exchange
- **Automatic Index Rebuild** - Unconditionally rebuilds all indexes after each exchange (steps 5.1 and 7.1) using full index rebuild to ensure all partitions are usable
- **Empty Partition Handling** - Partitions that are already empty (before exchange) are skipped; only partitions that become empty after successful exchange are dropped
- **Data Validation Status** - PASS/FAIL indicator for each exchange
- **Warning Status** - Validation failures change status to WARNING for manual review

### Comprehensive Logging

The archival procedure logs 38 columns per execution:
- 4 before-exchange record counts (source/archive, before/after)
- 6 index metrics (counts and sizes for source/archive)
- 2 invalid index counts (before/after exchange)
- Data validation results (PASS/FAIL, match Y/N)
- 3 performance timers (exchange, stats, total duration)
- Full error tracking and session details

### Performance Benefits

| Operation | Traditional INSERT/DELETE | Partition Exchange |
|-----------|--------------------------|-------------------|
| 1M rows   | ~30 seconds              | < 1 second        |
| 10M rows  | ~5 minutes               | < 1 second        |
| 100M rows | ~50 minutes              | < 1 second        |

*Partition exchange time is constant regardless of data volume*

### Compression Types Supported

| Type | Use Case | Compression Ratio |
|------|----------|------------------|
| BASIC | General purpose | ~2-3x |
| OLTP | Active archives | ~2-4x |
| QUERY LOW | Read-heavy | ~3-5x |
| QUERY HIGH | Read-heavy | ~4-8x |
| ARCHIVE LOW | Cold storage | ~5-10x |
| ARCHIVE HIGH | Cold storage | ~10-20x |

## Testing

Run the test scenarios:

```sql
SQL> @05_test_scenarios.sql
```

This includes:
1. Single and multiple date archival
2. Compression enabled tests
3. Fast mode tests (no validation)
4. Data validation verification
5. Index tracking verification
6. Execution history verification

View monitoring queries:

```sql
SQL> @06_monitoring_queries.sql
```

Displays:
- Configuration status
- Recent executions with all metrics
- Index health (before/after)
- Statistics status
- Compression effectiveness
- Data validation results
- Performance metrics

## Cleanup

To remove all demo objects:

```sql
SQL> @99_cleanup.sql
```

## How to Contribute

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/improvement`)
3. Commit your changes (`git commit -am 'Add new feature'`)
4. Push to the branch (`git push origin feature/improvement`)
5. Create a Pull Request

## Common Issues & Solutions

**Q: ORA-01502: index <index_name> or partition of such index is in unusable state**  
**A:** The procedure now handles this automatically:
1. **Pre-Exchange Validation**: Checks for and rebuilds both INVALID and UNUSABLE indexes before starting (if validate_before_exchange = 'Y')
2. **Automatic Rebuild After Exchange**: Unconditionally rebuilds ALL indexes on both source and archive tables after each partition exchange (steps 5.1 and 7.1), regardless of status
3. **Post-Exchange Validation**: Checks for any remaining invalid/unusable indexes after all exchanges complete (if validate_before_exchange = 'Y')

The full index rebuild (not partition-level) ensures all index partitions are rebuilt and usable. If you still encounter this error, it occurred outside the archival process. Manually rebuild: `ALTER INDEX index_name REBUILD;`

**Q: ORA-14097: column type or size mismatch in ALTER TABLE EXCHANGE PARTITION**  
**A:** The procedure now automatically validates table structure before attempting exchange. If you see this error, it means:
1. The structure validation was bypassed or disabled
2. The table structure changed after validation
Common causes and solutions:
- **Missing PRIMARY KEY on staging table**: Ensure the staging table has the same PRIMARY KEY as source table: `ALTER TABLE staging_table ADD CONSTRAINT pk_staging PRIMARY KEY (sale_id, sale_date);`
- **Column mismatch detected**: The procedure will raise ORA-20016 (source vs archive) or ORA-20017 (source vs staging) during validation phase
- **Data type differences**: Check the specific columns mentioned in the error and verify they match exactly across all three tables

**Q: ORA-14098: index mismatch in ALTER TABLE EXCHANGE PARTITION**  
**A:** Don't create indexes on the staging table manually. The procedure uses `WITHOUT VALIDATION` which exchanges index segments automatically from the partitioned tables.

**Q: ORA-14019: partition bound element must be one of: DATE, DATETIME, INTERVAL, NUMBER, or VARCHAR2**  
**A:** The table is not partitioned. The archival procedure now automatically validates this and throws a custom exception: `e_table_not_partitioned: The table is not partitioned. Partition exchange requires a partitioned table.`

**Q: ORA-20010 through ORA-20018 errors - what do they mean?**  
**A:** These are structure validation errors raised during the compatibility check:
- **ORA-20010**: Source table does not exist
- **ORA-20011**: Archive table does not exist  
- **ORA-20012**: Staging table does not exist
- **ORA-20013**: Archive table is not partitioned (must be partitioned)
- **ORA-20014**: Staging table is partitioned (must NOT be partitioned)
- **ORA-20015**: Column count mismatch between tables
- **ORA-20016**: Column structure mismatch between source and archive (names, types, or sizes differ)
- **ORA-20017**: Column structure mismatch between source and staging (names, types, or sizes differ)
- **ORA-20018**: Partition key columns don't match between source and archive tables

All these validations run BEFORE any exchange attempt, preventing runtime errors.

**Q: Why use a staging table instead of direct exchange?**  
**A:** Direct exchange would swap data between source and archive. The staging table acts as an intermediary to move data from source → archive while maintaining the source table structure.

**Q: What happens to empty partitions?**  
**A:** 
- **Partitions already empty (before exchange)**: Skipped entirely - no exchange, no drop, just logged
- **Partitions with data**: Exchanged to archive, then the now-empty source partition is dropped at step 10
This prevents unnecessary operations on partitions that were never populated.

**Q: How do I check if compression is working?**  
**A:** Query the execution log: `SELECT partition_date, is_compressed, compression_type, partition_size_mb FROM snparch_ctl_execution_log WHERE is_compressed = 'Y';`

**Q: Can I archive multiple tables?**  
**A:** Yes! Add each table to `SNPARCH_CNF_PARTITION_ARCHIVE` with its own configuration, then call the procedure with the appropriate `p_table_name`. Use `generate_archive_setup.sql` to create the archive table and configuration for each new table.

**Q: What happens if the procedure fails mid-execution?**  
**A:** The procedure logs errors to `SNPARCH_CTL_EXECUTION_LOG` with status='ERROR' and automatically cleans up the staging table. Check the `error_message` column for details.

**Q: Why is my execution showing WARNING status?**  
**A:** WARNING status indicates the partition exchange completed but data validation failed (record counts don't match before/after). Check the `data_validation_status`, `record_count_match`, and the before/after record count columns to investigate.

**Q: How do I monitor index health during archival?**  
**A:** The execution log tracks:
- `source_index_count` / `archive_index_count` - number of indexes
- `source_index_size_mb` / `archive_index_size_mb` - total index size
- `invalid_indexes_before` / `invalid_indexes_after` - count of invalid indexes

**Q: What if data validation keeps failing?**  
**A:** Check for:
1. Concurrent DML on source table during archival
2. Triggers that might be modifying data
3. Application code inserting into the partition being archived
Consider adding a table lock or running archival during maintenance windows.

## Best Practices

1. **Always enable validation in production**
   ```sql
   validate_before_exchange = 'Y'
   gather_stats_after_exchange = 'Y'
   ```

2. **Monitor data validation results**
   ```sql
   -- Check for validation failures
   SELECT execution_id, partition_date, data_validation_status, 
          record_count_match, validation_status
   FROM snparch_ctl_execution_log 
   WHERE data_validation_status = 'FAIL' OR validation_status = 'WARNING';
   ```

3. **Track index health**
   ```sql
   -- Monitor invalid indexes
   SELECT execution_id, partition_date, 
          invalid_indexes_before, invalid_indexes_after
   FROM snparch_ctl_execution_log
   WHERE invalid_indexes_after > 0;
   ```

4. **Use appropriate compression for your use case**
   - Recent archives (< 1 year): `OLTP`
   - Old archives (> 1 year): `ARCHIVE HIGH`

5. **Monitor execution log regularly**
   ```sql
   SELECT * FROM snparch_ctl_execution_log 
   WHERE validation_status NOT IN ('SUCCESS', 'WARNING');
   ```

6. **Archive execution log periodically**
   ```sql
   -- Keep last 90 days of successful executions
   DELETE FROM snparch_ctl_execution_log
   WHERE execution_date < SYSTIMESTAMP - INTERVAL '90' DAY
     AND validation_status = 'SUCCESS'
     AND data_validation_status = 'PASS';
   COMMIT;
   ```

7. **Test configuration changes in non-production first**

8. **Use the DDL generator for consistency**
   ```sql
   -- Always use generate_archive_setup.sql for new tables
   -- Ensures consistent naming conventions and structure
   SQL> @generate_archive_setup.sql
   ```

9. **Review WARNING status executions**
   ```sql
   -- Investigate executions that completed but failed validation
   SELECT * FROM snparch_ctl_execution_log
   WHERE validation_status = 'WARNING'
   ORDER BY execution_date DESC;
   ```

10. **Prevent concurrent access during archival**
    - Schedule archival during low-activity periods
    - Consider table locks for critical partitions
    - Monitor for concurrent DML that might affect validation

## Additional Resources

- [Oracle Partitioning Documentation](https://docs.oracle.com/en/database/oracle/oracle-database/19/vldbg/)
- [Partition Exchange Best Practices](https://docs.oracle.com/en/database/oracle/oracle-database/19/vldbg/partition-admin.html)

## License

This project is provided as-is for educational and demonstration purposes.

## Author

Created by [@granulegazer](https://github.com/granulegazer)

## Version History

**v3.0** - Oracle 19.26 production-ready release (Current)
- **Deployment Validation Script**: Created comprehensive validation script (12 check categories, 20+ individual checks)
  - Validates all tables, types, functions, procedures, views, indexes, and constraints
  - Checks object status (VALID/INVALID)
  - Verifies table structure compatibility
  - Validates partition configuration and key compatibility
  - Checks configuration data completeness
  - Reports detailed summary with error/warning counts
- **Structure Validation**: Added comprehensive table structure compatibility check before exchange
  - Validates all three tables exist (source, archive, staging)
  - Confirms archive table is partitioned and staging table is NOT partitioned
  - Verifies column count, names, data types, sizes, and nullability match exactly
  - Checks partition key columns match between source and archive
  - Raises specific error codes (ORA-20010 through ORA-20018) for each validation failure
- Enhanced execution logging with 38 tracked metrics
- Added comprehensive data validation (before/after record counts)
- Added index tracking (count and size for source/archive tables)
- Added invalid index monitoring (before/after exchange)
- Added partition validation (throws exception if table not partitioned)
- Added data validation status (PASS/FAIL)
- Added WARNING status for failed validations
- Added total_duration_seconds tracking
- Created DDL generator (`generate_archive_setup.sql`) using DBMS_METADATA
- Implemented SNPARCH_* naming convention for archive objects
- Separated configuration data into dedicated script (02_config_data.sql)
- Enhanced test scenarios with validation and index health checks
- Enhanced monitoring queries with 16 comprehensive queries including validation tracking
- Enhanced unified view with 6 example queries and performance tips
- Added comprehensive inline documentation (200+ lines)
- Updated all version references to Oracle 19.26
- Restricted logging to 'I' (Info) and 'E' (Error) types only

**v2.0** - Oracle 19c optimized release
- Added configuration table (`SNPARCH_CNF_PARTITION_ARCHIVE`)
- Added execution logging (`SNPARCH_CTL_EXECUTION_LOG`)
- Added compression support
- Added validation and statistics gathering
- Oracle 19c syntax (IDENTITY, TIMESTAMP, FETCH FIRST)
- Performance metrics tracking

**v1.0** - Initial release
- Basic partition exchange methodology

---

⭐ **Star this repo if you find it helpful!**

For questions or issues, please open a GitHub issue.
