-- ETL Script: 03_etl_transform.sql
-- Part 4: Transform and Load FACT_RIDES (The Core ETL)

-- This CTE calculates simple derived metrics and joins to the dimension keys.
WITH base_rides_data AS (
    SELECT
        rr.ride_id,
        du.user_key, -- Get Surrogate Key from DIM_USER
        dd.driver_key, -- Get Surrogate Key from DIM_DRIVER
        dd_date.date_key AS start_date_key, -- Get Date Key from DIM_DATE
        
        rr.ride_start_time,
        rr.ride_end_time,
        rr.distance_km,
        rr.fare_amount,
        
        -- Calculate the ride duration in minutes (Temporal Calculation)
        EXTRACT(EPOCH FROM (rr.ride_end_time - rr.ride_start_time)) / 60 AS ride_duration_minutes,
        
        -- Flag for Peak Hour (e.g., 7-9 AM and 4-6 PM)
        CASE
            WHEN EXTRACT(HOUR FROM rr.ride_start_time) IN (7, 8, 16, 17) THEN TRUE
            ELSE FALSE
        END AS is_peak_hour,
        
        -- Load driver ratings from raw_ratings table
        ROUND(rrating.rating_value::numeric, 2) AS average_driver_rating
        
    FROM
        raw_rides rr
    -- Join to dimensions to get the Surrogate Keys
    INNER JOIN dim_user du ON rr.user_id = du.user_id
    INNER JOIN dim_driver dd ON rr.driver_id = dd.driver_id
    INNER JOIN dim_date dd_date ON CAST(TO_CHAR(rr.ride_start_time, 'YYYYMMDD') AS INTEGER) = dd_date.date_key
    -- Left join to raw_ratings to get ride-level ratings (some rides may not have ratings)
    LEFT JOIN raw_ratings rrating ON rr.ride_id = rrating.ride_id
),

-- This CTE performs advanced Sequential Analysis using a Window Function
sequential_analysis AS (
    SELECT
        *,
        -- Use the LAG() Window Function:
        -- 1. PARTITION BY driver_key: The calculation resets for each new driver.
        -- 2. ORDER BY ride_start_time: Determines the sequence of rides.
        -- 3. The result is the previous ride's end time.
        LAG(ride_end_time) OVER (
            PARTITION BY driver_key
            ORDER BY ride_start_time
        ) AS previous_ride_end_time
    FROM
        base_rides_data
),

-- Final CTE to calculate the time between rides
final_fact_data AS (
    SELECT
        *,
        -- Calculate Time Between Rides: Current Ride Start Time - Previous Ride End Time
        -- This represents the gap/turnaround time after the driver's previous ride
        -- Use COALESCE to handle the first ride (where LAG() returns NULL)
        COALESCE(
            EXTRACT(EPOCH FROM (ride_start_time - previous_ride_end_time)) / 60,
            NULL -- The first ride has no previous time
        ) AS time_to_next_ride_minutes
    FROM
        sequential_analysis
)

-- Final INSERT statement into the Fact Table
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