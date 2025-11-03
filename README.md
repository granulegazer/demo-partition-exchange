# Oracle Partition Exchange Demo

A comprehensive demonstration of Oracle Database partition exchange methodology for efficient data archival using interval partitioning and compression.

## Quick Start

### Clone the Repository

```bash
git clone https://github.com/granulegazer/demo-partition-exchange.git
cd demo-partition-exchange/working
```

### Prerequisites

- Oracle Database 19c or higher (tested on 19.26)
- SQL*Plus or any Oracle SQL client
- Database user with appropriate privileges:
  - CREATE TABLE
  - CREATE TYPE
  - CREATE PROCEDURE
  - CREATE VIEW
  - CREATE INDEX

### Run the Demo

```bash
# Connect to your Oracle database
sqlplus username/password@database

# Option 1: Run all scripts at once
SQL> @00_run_all.sql

# Option 2: Run scripts individually
SQL> @01_setup.sql
SQL> @02_data_generator.sql
SQL> @03_archive_procedure.sql
SQL> @04_test_scenarios.sql
SQL> @05_monitoring_queries.sql
SQL> @06_unified_view.sql
SQL> @07_helper_functions.sql
```

## What This Demo Does

This project demonstrates:

1. **Automatic Partition Management**: Uses Oracle interval partitioning to create daily partitions automatically
2. **Instant Data Archival**: Moves data using partition exchange (metadata operation) instead of INSERT/DELETE
3. **Storage Optimization**: Applies basic compression to archived data
4. **Unified Data Access**: Provides seamless querying across active and archived data

## Repository Structure

```
demo-partition-exchange/
├── README.md                       # This file
└── working/
    ├── 00_run_all.sql              # Master script - runs all components
    ├── 01_setup.sql                # Creates tables, indexes, and types
    ├── 02_data_generator.sql       # Generates sample data for testing
    ├── 03_archive_procedure.sql    # Core archival procedure
    ├── 04_test_scenarios.sql       # Test cases and examples
    ├── 05_monitoring_queries.sql   # Queries to monitor partitions
    ├── 06_unified_view.sql         # View combining active + archived data
    ├── 07_helper_functions.sql     # Utility functions
    └── 99_cleanup.sql              # Cleanup script
```

## Usage Examples

### Archive Specific Dates

```sql
DECLARE
    v_dates date_array_type;
BEGIN
    v_dates := date_array_type(
        DATE '2024-01-15',
        DATE '2024-01-16',
        DATE '2024-01-17'
    );
    
    archive_partitions_by_dates(v_dates);
END;
/
```

### Query All Data (Active + Archived)

```sql
SELECT * FROM sales_unified_view
WHERE sale_date BETWEEN DATE '2024-01-01' AND DATE '2024-01-31';
```

### Check Partition Status

```sql
SELECT table_name, partition_name, high_value
FROM user_tab_partitions
WHERE table_name IN ('SALES', 'SALES_ARCHIVE')
ORDER BY table_name, partition_position;
```

## Key Features

### Partition Exchange Flow

```
Sales Table (Partition) → Staging Table → Archive Table (Partition)
     [Instant]                 [Instant]
```

Both exchanges are metadata-only operations - no physical data movement occurs.

### Tables Created

- **`sales`**: Main transaction table with interval partitioning
- **`sales_archive`**: Archive table with compression enabled
- **`sales_staging_template`**: Template for exchange operations
- **`sales_unified_view`**: Combined view of sales + archive

### Performance Benefits

| Operation | Traditional INSERT | Partition Exchange |
|-----------|-------------------|-------------------|
| 1M rows   | ~30 seconds       | < 1 second        |
| 10M rows  | ~5 minutes        | < 1 second        |
| 100M rows | ~50 minutes       | < 1 second        |

*Time is constant regardless of partition size with exchange*

## Testing the Demo

Run `04_test_scenarios.sql` to see:

1. Single date archival
2. Multiple date batch archival
3. Unified view queries
4. Partition verification
5. Index validation

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

## Common Issues

**Q: ORA-14097: column type or size mismatch in ALTER TABLE EXCHANGE PARTITION**  
**A:** Ensure you're using `FOR EXCHANGE WITH TABLE` syntax when creating archive and staging tables.

**Q: Why use staging table instead of direct exchange?**  
**A:** Direct exchange would swap data between tables. We use staging as an intermediary to move data from source → archive while keeping source partition structure intact.

**Q: Can I use this with Oracle 12c or 18c?**  
**A:** Yes, partition exchange and interval partitioning are available in Oracle 11g and later.

## Additional Resources

- [Oracle Partitioning Documentation](https://docs.oracle.com/en/database/oracle/oracle-database/19/vldbg/)
- [Partition Exchange Best Practices](https://docs.oracle.com/en/database/oracle/oracle-database/19/vldbg/partition-admin.html)

## License

This project is provided as-is for educational and demonstration purposes.

## Author

Created by [@granulegazer](https://github.com/granulegazer)

## Version

**v1.0** - Initial release with partition exchange methodology

---

⭐ Star this repo if you find it helpful!
