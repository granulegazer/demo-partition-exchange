-- ========================================
-- QUICK START GUIDE
-- ========================================
-- Three deployment options based on your needs
-- ========================================

-- ========================================
-- OPTION 1: Complete Demo (Recommended for Learning)
-- ========================================
-- Run everything to see the framework in action
-- Includes demo tables, test data, and test scenarios
-- ========================================
@00_run_all.sql

-- OR manually run in this order:
/*
@01_setup.sql                    -- Framework objects
@demo_tables_setup.sql           -- Demo tables (SALES, SALES_ARCHIVE)
@demo_config_data.sql            -- Demo configuration
@03_data_generator.sql           -- Test data
@04_archive_procedure.sql        -- Archive procedure
@08_helper_functions.sql         -- Helper functions
@07_unified_view.sql             -- Monitoring view
@05_test_scenarios.sql           -- Run tests
@06_monitoring_queries.sql       -- Review results
@00_deployment_validation.sql    -- Validate installation
*/

-- ========================================
-- OPTION 2: Production Deployment
-- ========================================
-- Deploy framework without demo components
-- Use your own tables and configuration
-- ========================================
/*
@01_setup.sql                    -- Framework objects
@02_config_data.sql              -- Your configuration (modify first!)
@04_archive_procedure.sql        -- Archive procedure
@08_helper_functions.sql         -- Helper functions
@07_unified_view.sql             -- Monitoring view (optional)
@00_deployment_validation.sql    -- Validate installation

-- Then configure for your tables:
-- 1. Edit 02_config_data.sql with your table names
-- 2. Create staging table: CREATE TABLE your_staging FOR EXCHANGE WITH TABLE your_source;
-- 3. Run archive procedure with your parameters
*/

-- ========================================
-- OPTION 3: Framework Testing
-- ========================================
-- Test framework with demo but skip monitoring
-- Quick validation of core functionality
-- ========================================
/*
@01_setup.sql                    -- Framework objects
@demo_tables_setup.sql           -- Demo tables
@demo_config_data.sql            -- Demo configuration
@03_data_generator.sql           -- Test data
@04_archive_procedure.sql        -- Archive procedure
@08_helper_functions.sql         -- Helper functions
@05_test_scenarios.sql           -- Run tests
@00_deployment_validation.sql    -- Validate installation
*/

-- ========================================
-- FILE DESCRIPTIONS
-- ========================================
-- FRAMEWORK FILES (Required for all deployments):
--   01_setup.sql                 - Creates core objects (types, tables, indexes)
--   04_archive_procedure.sql     - Main archival procedure with validation
--   08_helper_functions.sql      - Utility functions (date range, partition info)
--
-- DEMO FILES (Optional - for learning/testing):
--   demo_tables_setup.sql        - Creates SALES and SALES_ARCHIVE tables
--   demo_config_data.sql         - Configuration for demo tables
--   03_data_generator.sql        - Generates test data for SALES table
--   05_test_scenarios.sql        - Runs comprehensive test scenarios
--
-- PRODUCTION FILES (Modify for your environment):
--   02_config_data.sql           - Template for production configuration
--
-- MONITORING FILES (Optional but recommended):
--   06_monitoring_queries.sql    - 16 monitoring and analysis queries
--   07_unified_view.sql          - Combined execution view with metrics
--
-- UTILITY FILES:
--   00_run_all.sql               - Automated complete installation
--   00_deployment_validation.sql - Validates all objects installed correctly
--   99_cleanup.sql               - Removes all objects (use with caution)
--   generate_archive_setup.sql   - Generates DDL for new table pairs
--
-- ========================================
-- PREREQUISITES
-- ========================================
-- 1. Oracle Database 19c or higher
-- 2. User privileges:
--    - CREATE TABLE, CREATE TYPE, CREATE PROCEDURE, CREATE VIEW
--    - CREATE INDEX, CREATE SEQUENCE
--    - For generate_archive_setup.sql: SELECT_CATALOG_ROLE or SELECT ANY DICTIONARY
-- 3. Tablespace with sufficient space
-- 4. For compression: Advanced Compression option license
--
-- ========================================
-- POST-INSTALLATION
-- ========================================
-- 1. Validate deployment:
--    @00_deployment_validation.sql
--
-- 2. Review configuration:
--    SELECT * FROM snparch_cnf_partition_archive;
--
-- 3. Generate DDL for your tables (if needed):
--    @generate_archive_setup.sql
--    -- Modify v_source_table and v_schema_name variables
--
-- 4. Execute archival:
--    EXEC snparch_archive_partition_by_date('YOUR_TABLE', 'YOUR_ARCHIVE', DATE_ARRAY_TYPE(DATE'2024-01-01'));
--
-- 5. Monitor execution:
--    @06_monitoring_queries.sql
--    SELECT * FROM snparch_vw_unified_execution_view;
--
-- ========================================
-- TROUBLESHOOTING
-- ========================================
-- If installation fails:
--   1. Check database version: SELECT * FROM v$version;
--   2. Check privileges: SELECT * FROM user_sys_privs;
--   3. Review errors in: @00_deployment_validation.sql
--   4. Check execution log: SELECT * FROM snparch_ctl_execution_log WHERE validation_status = 'ERROR';
--
-- To start over:
--   @99_cleanup.sql
--   @00_run_all.sql
--
-- For support:
--   Review README.md for detailed documentation
--   Check deployment validation output for specific issues
-- ========================================
