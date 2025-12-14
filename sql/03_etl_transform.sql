-- ============================================================================
-- ETL Script: 03_etl_transform.sql
-- ============================================================================
-- Purpose: Transform raw ride data and load into FACT_RIDES (The Core ETL Logic)
-- 
-- Architecture:
--   - Implements multi-stage ETL using Common Table Expressions (CTEs)
--   - Each CTE represents a logical transformation step
--   - Final INSERT statement loads transformed data into star schema
--
-- Data Flow:
--   1. base_rides_data: Join raw tables to dimensions, calculate simple metrics
--   2. sequential_analysis: Apply LAG() window function to detect previous rides
--   3. final_fact_data: Calculate temporal gaps between consecutive driver rides
--   4. INSERT: Load fully transformed data into fact_rides
--
-- Key Transformations:
--   - Surrogate Key Mapping: raw_id → dim_key (using INNER JOINs)
--   - Temporal Calculations: Duration (minutes), Peak Hour detection
--   - Window Functions: LAG() for sequential ride analysis
--   - Time Series Gap Detection: idle time between consecutive rides
--
-- Error Handling:
--   - COALESCE: Handles NULL previous_ride_end_time for first ride
--   - LEFT JOIN on ratings: Gracefully handles rides without ratings
--
-- ============================================================================

-- ============================================================================
-- STAGE 1: BASE DATA & DIMENSION JOINS
-- ============================================================================
-- Purpose: Aggregate raw data and join to dimension tables to get surrogate keys
-- 
-- Logic:
--   - INNER JOIN dim_user: Ensures all rides have valid user_key
--   - INNER JOIN dim_driver: Ensures all rides have valid driver_key
--   - INNER JOIN dim_date: Converts ride timestamp to date_key for time dimension
--   - LEFT JOIN raw_ratings: Optional ratings (not all rides are rated)
--
-- Calculated Metrics:
--   - ride_duration_minutes: EXTRACT EPOCH converts timestamp difference to seconds
--   - is_peak_hour: Binary flag for 7-8 AM and 4-5 PM traffic periods
--   - average_driver_rating: Rounded to 2 decimals from raw_ratings table
--
-- ============================================================================
-- This CTE calculates simple derived metrics and joins to the dimension keys.
WITH base_rides_data AS (
    SELECT
        rr.ride_id,
        du.user_key,                              -- Surrogate Key from DIM_USER
        dd.driver_key,                            -- Surrogate Key from DIM_DRIVER
        dd_date.date_key AS start_date_key,       -- Date Dimension Key from DIM_DATE
        
        rr.ride_start_time,
        rr.ride_end_time,
        rr.distance_km,
        rr.fare_amount,
        
        -- Calculate ride duration in minutes using SQL epoch conversion
        -- EXTRACT(EPOCH ...) converts interval to seconds, divide by 60 for minutes
        EXTRACT(EPOCH FROM (rr.ride_end_time - rr.ride_start_time)) / 60 AS ride_duration_minutes,
        
        -- Peak Hour Detection: Flag rides during 7-8 AM and 4-5 PM (typical traffic peaks)
        CASE
            WHEN EXTRACT(HOUR FROM rr.ride_start_time) IN (7, 8, 16, 17) THEN TRUE
            ELSE FALSE
        END AS is_peak_hour,
        
        -- Load driver ratings from raw_ratings; some rides may not have ratings yet
        ROUND(rrating.rating_value::numeric, 2) AS average_driver_rating
        
    FROM
        raw_rides rr
    -- INNER JOINs ensure data quality: only rides with valid users/drivers are included
    INNER JOIN dim_user du ON rr.user_id = du.user_id
    INNER JOIN dim_driver dd ON rr.driver_id = dd.driver_id
    INNER JOIN dim_date dd_date ON CAST(TO_CHAR(rr.ride_start_time, 'YYYYMMDD') AS INTEGER) = dd_date.date_key
    -- LEFT JOIN on ratings: gracefully handles rides without ratings (rating may come later)
    LEFT JOIN raw_ratings rrating ON rr.ride_id = rrating.ride_id
),

-- ============================================================================
-- STAGE 2: SEQUENTIAL ANALYSIS WITH WINDOW FUNCTIONS
-- ============================================================================
-- Purpose: Apply LAG() to identify previous ride for each driver
-- 
-- Window Function: LAG()
--   - PARTITION BY driver_key: Reset calculation for each new driver
--   - ORDER BY ride_start_time: Determines chronological ride sequence
--   - Result: Brings forward the END TIME of the previous ride
--
-- Use Case: Detects driver idle time (gap between consecutive rides)
--
-- ============================================================================
-- This CTE performs advanced Sequential Analysis using LAG() Window Function
sequential_analysis AS (
    SELECT
        *,
        -- LAG() returns the previous row's value within each driver partition
        -- For first ride per driver: LAG() returns NULL (handled in next CTE with COALESCE)
        LAG(ride_end_time) OVER (
            PARTITION BY driver_key      -- Reset for each driver
            ORDER BY ride_start_time      -- Chronological order determines previous ride
        ) AS previous_ride_end_time
    FROM
        base_rides_data
),

-- ============================================================================
-- STAGE 3: FINAL CALCULATIONS & NULL HANDLING
-- ============================================================================
-- Purpose: Calculate time gaps between consecutive rides for each driver
-- 
-- Logic:
--   - Time Gap = Current Ride Start - Previous Ride End
--   - COALESCE: Sets gap to NULL for first ride (no previous ride to measure from)
--
-- Business Meaning:
--   - Gap time represents driver turnaround/idle time between rides
--   - High gaps = longer wait time between jobs (efficiency metric)
--   - Used for driver utilization and queue analysis
--
-- ============================================================================
-- Final CTE to calculate the time between consecutive rides
final_fact_data AS (
    SELECT
        *,
        -- Calculate Time Between Consecutive Rides (in minutes)
        -- This represents idle time after the driver's previous ride
        -- COALESCE handles first ride of each driver where LAG() returns NULL
        COALESCE(
            EXTRACT(EPOCH FROM (ride_start_time - previous_ride_end_time)) / 60,
            NULL  -- First ride per driver has no previous ride to measure from
        ) AS time_to_next_ride_minutes
    FROM
        sequential_analysis
)

-- ============================================================================
-- FINAL: INSERT INTO FACT TABLE
-- ============================================================================
-- Purpose: Load fully transformed data into fact_rides star schema table
-- 
-- Column Mapping:
--   - Dimension keys (user_key, driver_key, start_date_key): From dimension tables
--   - Temporal columns (start_time, end_time, duration): Calculated from timestamps
--   - Distance & Fare: From raw_rides (business metrics)
--   - Time Gap: From sequential analysis (efficiency metric)
--   - Peak Hour & Rating: From calculations and joins above
--
-- Constraints:
--   - All rows must have valid user_key, driver_key, start_date_key (INNER JOINs enforce)
--   - average_driver_rating and time_to_next_ride_minutes may be NULL (handled in analytics)
--
-- ============================================================================
INSERT INTO fact_rides (
    ride_id,
    user_key,
    driver_key,
    start_date_key,
    start_time,
    end_time,
    ride_duration_minutes,
    distance_km,
    fare_amount,
    time_to_next_ride_minutes,
    is_peak_hour,
    average_driver_rating
)
SELECT
    ride_id,
    user_key,
    driver_key,
    start_date_key,
    ride_start_time,
    ride_end_time,
    ride_duration_minutes,
    distance_km,
    fare_amount,
    time_to_next_ride_minutes,
    is_peak_hour,
    average_driver_rating
FROM
    final_fact_data;

-- Verification Query: Check the results of the window function for one driver
-- SELECT * FROM fact_rides WHERE driver_key = 1 ORDER BY start_time;