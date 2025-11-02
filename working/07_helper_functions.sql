-- ========================================
-- HELPER FUNCTIONS
-- ========================================

-- Helper to generate date list for a range
CREATE OR REPLACE FUNCTION get_date_list(
    p_start_date DATE,
    p_end_date DATE
) RETURN date_array_type IS
    v_dates date_array_type := date_array_type();
    v_current_date DATE := p_start_date;
BEGIN
    WHILE v_current_date <= p_end_date LOOP
        v_dates.EXTEND;
        v_dates(v_dates.COUNT) := v_current_date;
        v_current_date := v_current_date + 1;
    END LOOP;
    RETURN v_dates;
END;
/

-- Example usage: Archive entire January 2024
DECLARE
    v_dates date_array_type;
BEGIN
    v_dates := get_date_list(DATE '2024-01-01', DATE '2024-01-31');
    archive_partitions_by_dates(v_dates);
END;
/