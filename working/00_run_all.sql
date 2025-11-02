-- ========================================
-- MAIN SCRIPT - Run files in this order
-- ========================================
@01_setup.sql
@02_data_generator.sql
@03_archive_procedure.sql
@04_test_scenarios.sql
@05_monitoring_queries.sql
@06_unified_view.sql
@07_helper_functions.sql

-- Optional: Run cleanup when needed
-- @99_cleanup.sql