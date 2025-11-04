# Oracle Partition Exchange Demo

A comprehensive demonstration of Oracle Database partition exchange methodology for efficient data archival using interval partitioning, compression, and execution tracking.

## Features

- ✅ **Instant Data Archival** - Partition exchange (metadata-only operation)
- ✅ **Configuration-Driven** - Centralized configuration table with audit trails
- ✅ **Execution Logging** - Complete history of all partition exchanges
- ✅ **Compression Support** - Multiple compression types (OLTP, QUERY, ARCHIVE)
- ✅ **Automatic Validation** - Index and statistics validation before/after exchange
- ✅ **Performance Metrics** - Track exchange duration and statistics gathering time
- ✅ **Oracle 19c Optimized** - Uses latest syntax and features

## Quick Start

### Prerequisites

- Oracle Database 19c or higher (tested on 19.26)
- SQL*Plus or any Oracle SQL client
- Database user with appropriate privileges:
  - CREATE TABLE
  - CREATE TYPE
  - CREATE PROCEDURE
  - CREATE VIEW
  - CREATE INDEX

### Installation

```bash
# Clone the repository
git clone https://github.com/granulegazer/demo-partition-exchange.git
cd demo-partition-exchange/working

# Connect to your Oracle database
sqlplus username/password@database

# Run all scripts at once
SQL> @00_run_all.sql

# Or run individually
SQL> @01_setup.sql
SQL> @02_data_generator.sql
SQL> @03_archive_procedure.sql
SQL> @04_test_scenarios.sql
SQL> @05_monitoring_queries.sql
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

Tracks every partition exchange with:

- Source and archive partition names
- Partition date and record count
- Partition size in MB
- Compression status and type
- Exchange duration (seconds)
- Statistics gathering duration (seconds)
- Validation status (SUCCESS/WARNING/ERROR)
- Error details (if failed)
- Session ID and executed by user

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

## Repository Structure

```
demo-partition-exchange/
├── README.md                       # This file
├── .github/
│   └── copilot-instructions.md     # AI coding guidelines
└── working/
    ├── 00_run_all.sql              # Master script - runs all components
    ├── 01_setup.sql                # Creates tables, indexes, config tables
    ├── 02_data_generator.sql       # Generates sample data for testing
    ├── 03_archive_procedure.sql    # Core archival procedure with logging
    ├── 04_test_scenarios.sql       # Test cases including compression tests
    ├── 05_monitoring_queries.sql   # Monitoring and execution history queries
    ├── 06_unified_view.sql         # View combining active + archived data
    ├── 07_helper_functions.sql     # Utility functions
    ├── config_management.sql       # Configuration management scripts
    └── 99_cleanup.sql              # Cleanup script
```

## Usage Examples

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

### Check Execution History

```sql
-- Recent executions with performance metrics
SELECT 
    execution_id,
    TO_CHAR(execution_date, 'YYYY-MM-DD HH24:MI:SS') AS executed_at,
    source_partition_name,
    archive_partition_name,
    records_archived,
    partition_size_mb,
    is_compressed,
    ROUND(exchange_duration_seconds, 3) AS exchange_sec,
    ROUND(stats_gather_duration_seconds, 2) AS stats_sec,
    validation_status
FROM snparch_ctl_execution_log
ORDER BY execution_id DESC
FETCH FIRST 10 ROWS ONLY;
```

### Query Execution Summary

```sql
-- Summary by table
SELECT 
    source_table_name,
    COUNT(*) AS total_exchanges,
    SUM(records_archived) AS total_records,
    ROUND(SUM(partition_size_mb), 2) AS total_size_mb,
    SUM(CASE WHEN is_compressed = 'Y' THEN 1 ELSE 0 END) AS compressed_partitions,
    ROUND(AVG(exchange_duration_seconds), 3) AS avg_exchange_sec
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

### Oracle 19c Optimizations

- **IDENTITY Columns** - Auto-incrementing PKs without sequences
- **TIMESTAMP(6)** - Microsecond precision for accurate timing
- **FETCH FIRST** - Modern row limiting (replaces ROWNUM)
- **INTERVAL Literals** - Clean date arithmetic
- **Enhanced DBMS_STATS** - AUTO_DEGREE and granularity options
- **Compression Detection** - Automatic detection of partition compression status

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
SQL> @04_test_scenarios.sql
```

This includes:
1. Single and multiple date archival
2. Compression enabled tests
3. Fast mode tests (no validation)
4. Execution history verification

View monitoring queries:

```sql
SQL> @05_monitoring_queries.sql
```

Displays:
- Configuration status
- Recent executions
- Index health
- Statistics status
- Compression effectiveness

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

**Q: ORA-14097: column type or size mismatch in ALTER TABLE EXCHANGE PARTITION**  
**A:** The staging table is missing the PRIMARY KEY constraint. The procedure automatically adds it, but if creating manually, ensure you add: `ALTER TABLE staging_table ADD CONSTRAINT pk_staging PRIMARY KEY (sale_id, sale_date);`

**Q: ORA-14098: index mismatch in ALTER TABLE EXCHANGE PARTITION**  
**A:** Don't create indexes on the staging table manually. The procedure uses `WITHOUT VALIDATION` which exchanges index segments automatically from the partitioned tables.

**Q: Why use a staging table instead of direct exchange?**  
**A:** Direct exchange would swap data between source and archive. The staging table acts as an intermediary to move data from source → archive while maintaining the source table structure.

**Q: How do I check if compression is working?**  
**A:** Query the execution log: `SELECT partition_date, is_compressed, compression_type, partition_size_mb FROM snparch_ctl_execution_log WHERE is_compressed = 'Y';`

**Q: Can I archive multiple tables?**  
**A:** Yes! Add each table to `SNPARCH_CNF_PARTITION_ARCHIVE` with its own configuration, then call the procedure with the appropriate `p_table_name`.

**Q: What happens if the procedure fails mid-execution?**  
**A:** The procedure logs errors to `SNPARCH_CTL_EXECUTION_LOG` with status='ERROR' and automatically cleans up the staging table. Check the `error_message` column for details.

## Best Practices

1. **Always enable validation in production**
   ```sql
   validate_before_exchange = 'Y'
   gather_stats_after_exchange = 'Y'
   ```

2. **Use appropriate compression for your use case**
   - Recent archives (< 1 year): `OLTP`
   - Old archives (> 1 year): `ARCHIVE HIGH`

3. **Monitor execution log regularly**
   ```sql
   SELECT * FROM snparch_ctl_execution_log 
   WHERE validation_status != 'SUCCESS';
   ```

4. **Archive execution log periodically**
   ```sql
   -- Keep last 90 days
   DELETE FROM snparch_ctl_execution_log
   WHERE execution_date < SYSTIMESTAMP - INTERVAL '90' DAY
     AND validation_status = 'SUCCESS';
   ```

5. **Test configuration changes in non-production first**

## Additional Resources

- [Oracle Partitioning Documentation](https://docs.oracle.com/en/database/oracle/oracle-database/19/vldbg/)
- [Partition Exchange Best Practices](https://docs.oracle.com/en/database/oracle/oracle-database/19/vldbg/partition-admin.html)

## License

This project is provided as-is for educational and demonstration purposes.

## Author

Created by [@granulegazer](https://github.com/granulegazer)

## Version History

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
