-- ========================================
-- DATA GENERATOR: Create test data
-- ========================================

-- Generate data for 180 days with 10-15 records per day
DECLARE
    v_sale_id NUMBER := 1;
    v_records_per_day NUMBER;
    v_start_date DATE := DATE '2024-01-01';
    v_days NUMBER := 180;
    v_regions SYS.ODCIVARCHAR2LIST := SYS.ODCIVARCHAR2LIST('North', 'South', 'East', 'West', 'Central');
    v_statuses SYS.ODCIVARCHAR2LIST := SYS.ODCIVARCHAR2LIST('Completed', 'Pending', 'Shipped', 'Delivered');
BEGIN
    DBMS_OUTPUT.PUT_LINE('Starting data generation for ' || v_days || ' days...');
    
    -- Loop through each day
    FOR day_offset IN 0..v_days-1 LOOP
        -- Random 10-15 records per day
        v_records_per_day := TRUNC(DBMS_RANDOM.VALUE(10, 16));
        
        -- Insert records for this day
        FOR rec IN 1..v_records_per_day LOOP
            INSERT INTO sales VALUES (
                v_sale_id,
                v_start_date + day_offset,
                TRUNC(DBMS_RANDOM.VALUE(1000, 5000)),  -- customer_id
                TRUNC(DBMS_RANDOM.VALUE(100, 999)),    -- product_id
                ROUND(DBMS_RANDOM.VALUE(50, 5000), 2), -- amount
                TRUNC(DBMS_RANDOM.VALUE(1, 50)),       -- quantity
                v_regions(TRUNC(DBMS_RANDOM.VALUE(1, 6))), -- region
                v_statuses(TRUNC(DBMS_RANDOM.VALUE(1, 5))) -- status
            );
            
            v_sale_id := v_sale_id + 1;
        END LOOP;
        
        -- Commit every 10 days
        IF MOD(day_offset, 10) = 0 THEN
            COMMIT;
            DBMS_OUTPUT.PUT_LINE('Committed data for day ' || day_offset);
        END IF;
    END LOOP;
    
    COMMIT;
    DBMS_OUTPUT.PUT_LINE('Data generation completed!');
    DBMS_OUTPUT.PUT_LINE('Total records inserted: ' || (v_sale_id - 1));
END;
/

-- Gather statistics
EXEC DBMS_STATS.GATHER_TABLE_STATS(USER, 'SALES');