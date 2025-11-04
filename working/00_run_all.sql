-- ========================================
-- MAIN SCRIPT - Run files in this order
-- ========================================
@01_setup.sql
@02_config_data.sql
@03_data_generator.sql
@04_archive_procedure.sql
@05_test_scenarios.sql
@06_monitoring_queries.sql
@07_unified_view.sql
@08_helper_functions.sql

-- Optional: Run cleanup when needed
-- @99_cleanup.sql