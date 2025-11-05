-- ========================================
-- MAIN SCRIPT - Run files in this order
-- ========================================
-- This script runs all components in sequence
-- For production use: Skip demo-specific scripts (marked as DEMO)
-- ========================================

PROMPT
PROMPT ========================================
PROMPT Starting Framework Installation
PROMPT ========================================
PROMPT

-- ========================================
-- STEP 1: Framework Setup (REQUIRED)
-- ========================================
PROMPT Step 1: Creating framework objects...
@01_setup.sql

-- ========================================
-- STEP 2: Demo Tables Setup (OPTIONAL - DEMO ONLY)
-- ========================================
-- Comment out these lines for production deployment
PROMPT Step 2: Creating demo tables...
@demo_tables_setup.sql

-- ========================================
-- STEP 3: Configuration Data
-- ========================================
-- For DEMO: Use demo_config_data.sql
-- For PRODUCTION: Use 02_config_data.sql (modify for your tables)
PROMPT Step 3: Loading configuration data...
@demo_config_data.sql
-- @02_config_data.sql  -- Use this for production

-- ========================================
-- STEP 4: Test Data Generation (OPTIONAL - DEMO ONLY)
-- ========================================
-- Comment out for production deployment
PROMPT Step 4: Generating test data...
@03_data_generator.sql

-- ========================================
-- STEP 5: Archive Procedure (REQUIRED)
-- ========================================
PROMPT Step 5: Creating archive procedure...
@04_archive_procedure.sql

-- ========================================
-- STEP 6: Helper Functions (REQUIRED)
-- ========================================
PROMPT Step 6: Creating helper functions...
@08_helper_functions.sql

-- ========================================
-- STEP 7: Unified Monitoring View (OPTIONAL)
-- ========================================
PROMPT Step 7: Creating unified monitoring view...
@07_unified_view.sql

-- ========================================
-- STEP 8: Test Scenarios (OPTIONAL - DEMO ONLY)
-- ========================================
-- Comment out for production deployment
PROMPT Step 8: Running test scenarios...
@05_test_scenarios.sql

PROMPT
PROMPT ========================================
PROMPT Installation Complete
PROMPT ========================================
PROMPT
PROMPT Framework Status:
PROMPT   - Core objects: Created
PROMPT   - Demo tables: Created
PROMPT   - Configuration: Loaded
PROMPT   - Test data: Generated
PROMPT   - Procedures: Created
PROMPT   - Views: Created
PROMPT
PROMPT Next Steps:
PROMPT   1. Review test scenario results above
PROMPT   2. Run monitoring queries: @06_monitoring_queries.sql
PROMPT   3. Optional: Run deployment validation: @00_deployment_validation.sql
PROMPT
PROMPT For production deployment:
PROMPT   - Comment out demo-specific steps (2, 4, 8)
PROMPT   - Modify 02_config_data.sql for your tables
PROMPT   - Skip test data generation
PROMPT ========================================

-- Optional: Run cleanup when needed
-- @99_cleanup.sql