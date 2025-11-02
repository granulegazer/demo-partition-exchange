-- ========================================
-- TEST SCENARIOS
-- ========================================

SET SERVEROUTPUT ON SIZE UNLIMITED

-- Scenario 1: Archive specific dates (3 dates)
DECLARE
    v_dates date_array_type := date_array_type(
        DATE '2024-01-15',
        DATE '2024-01-20',
        DATE '2024-01-25'
    );
BEGIN
    archive_partitions_by_dates(v_dates);
END;
/

-- Scenario 2: Archive an entire week
DECLARE
    v_dates date_array_type := date_array_type();
    v_start_date DATE := DATE '2024-02-01';
BEGIN
    -- Build list of 7 consecutive dates
    FOR i IN 0..6 LOOP
        v_dates.EXTEND;
        v_dates(v_dates.COUNT) := v_start_date + i;
    END LOOP;
    
    archive_partitions_by_dates(v_dates);
END;
/

-- Scenario 3: Archive first day of each month (Jan-Jun)
DECLARE
    v_dates date_array_type := date_array_type(
        DATE '2024-01-01',
        DATE '2024-02-01',
        DATE '2024-03-01',
        DATE '2024-04-01',
        DATE '2024-05-01',
        DATE '2024-06-01'
    );
BEGIN
    archive_partitions_by_dates(v_dates);
END;
/

-- Scenario 4: Archive random dates across multiple months
DECLARE
    v_dates date_array_type := date_array_type(
        DATE '2024-01-10',
        DATE '2024-02-14',
        DATE '2024-03-17',
        DATE '2024-04-22',
        DATE '2024-05-30'
    );
BEGIN
    archive_partitions_by_dates(v_dates);
END;
/