-- DDL Script: 04_ddl_indexing_and_views.sql

\echo '--- Starting Optimization and Views Creation ---'

-----------------------------------------------------------
-- PART 1: PERFORMANCE TUNING (INDEXING)
-- Goal: Ensure fast join execution on the large FACT table.
-----------------------------------------------------------

-- Indexing Strategy: We prioritize indexes on Foreign Keys (FKs) 
-- and columns used in WHERE or ORDER BY clauses.

-- 1. Index Foreign Keys on FACT_RIDES (Essential for joins)
-- Indexes speed up queries joining the FACT table to DIM tables.
CREATE INDEX idx_fact_user_key ON fact_rides (user_key);
CREATE INDEX idx_fact_driver_key ON fact_rides (driver_key);
CREATE INDEX idx_fact_start_date ON fact_rides (start_date_key);

-- 2. Create a Composite Index (Targeted for Sequential Analysis)
-- This index is designed to speed up the complex driver utilization query:
-- It allows the DB to quickly filter by driver (driver_key) and order by time (start_time).
CREATE INDEX idx_driver_time_sequence ON fact_rides (driver_key, start_time);


-----------------------------------------------------------
-- PART 2: REPORTING SIMPLIFICATION (VIEWS)
-- Goal: Provide analysts with pre-calculated, easy-to-query metrics.
-----------------------------------------------------------

-- 3. Create an Analytical View for Driver Daily Utilization
-- This View calculates the average waiting time for drivers, which is a key business metric.
-- 
CREATE OR REPLACE VIEW v_driver_daily_utilization AS
SELECT
    dd.driver_name,
    ddate.full_date,
    
    -- Aggregate the key derived metric
    AVG(fr.time_to_next_ride_minutes) AS avg_wait_time_minutes,
    -- Simple metrics
    COUNT(fr.ride_id) AS total_rides_that_day,
    SUM(fr.fare_amount) AS total_revenue_that_day
FROM
    fact_rides fr
JOIN
    dim_driver dd ON fr.driver_key = dd.driver_key
JOIN
    dim_date ddate ON fr.start_date_key = ddate.date_key
WHERE
    -- Filter out NULL values for the wait time (the first ride of the day for a driver)
    fr.time_to_next_ride_minutes IS NOT NULL
GROUP BY
    dd.driver_name,
    ddate.full_date
ORDER BY
    ddate.full_date,
    dd.driver_name;

\echo '--- Indexing and Views Creation Complete ---'