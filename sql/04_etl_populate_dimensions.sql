-- Script: 02_populate_dimensions.sql
-- Purpose: Populate dimension tables from raw data

-- =====================================================
-- 1. Populate DIM_DATE
-- =====================================================

INSERT INTO dim_date (date_key, full_date, day_of_week, day_name, month_name, year, is_weekend)
WITH date_range AS (
    -- Get all unique dates from raw_rides to define the exact date dimension range
    SELECT DISTINCT DATE(ride_start_time) AS ride_date
    FROM raw_rides
    -- Note: Ensure raw_rides is loaded before running this!
)
SELECT
    CAST(TO_CHAR(ride_date, 'YYYYMMDD') AS INTEGER) AS date_key,
    ride_date AS full_date,
    EXTRACT(DOW FROM ride_date)::INTEGER AS day_of_week, 
    TRIM(TO_CHAR(ride_date, 'Day')) AS day_name,     -- TRIM to remove padding
    TRIM(TO_CHAR(ride_date, 'Month')) AS month_name, -- TRIM to remove padding
    EXTRACT(YEAR FROM ride_date)::INTEGER AS year,
    CASE 
        WHEN EXTRACT(DOW FROM ride_date) IN (0, 6) THEN TRUE
        ELSE FALSE
    END AS is_weekend
FROM date_range
ON CONFLICT (full_date) DO NOTHING; -- Ensure idempotency

\echo '--- DIM_DATE populated ---'

-- =====================================================
-- 2. Populate DIM_USER (with enhanced cleaning/masking)
-- =====================================================

INSERT INTO dim_user (
    user_id,
    masked_name,
    masked_email_hash,
    masked_phone_hash,
    registration_date,
    age,
    gender,
    location_city
)
SELECT
    ru.user_id,
    -- Enhanced Masking: Show first initial of the name
    LEFT(ru.name, 1) || '.' AS masked_name,
    MD5(ru.email)::VARCHAR(32) AS masked_email_hash,
    MD5(ru.phone_number)::VARCHAR(32) AS masked_phone_hash,
    ru.registration_date,
    ru.age,
    -- Normalize gender (already assumed clean)
    ru.gender,
    -- Location: Use as-is since it's already a single city name (no comma-separated format)
    TRIM(ru.location) AS location_city
    -- is_active_subscriber removed here, should be added based on a separate logic or default TRUE on load
FROM raw_users ru
ON CONFLICT (user_id) DO NOTHING;

\echo '--- DIM_USER populated ---'

-- =====================================================
-- 3. Populate DIM_DRIVER (using LEFT JOIN)
-- =====================================================

INSERT INTO dim_driver (
    driver_id,
    driver_name,
    vehicle_make,
    vehicle_model,
    vehicle_year,
    vehicle_capacity,
    is_available_flag
)
SELECT
    rd.driver_id,
    rd.name AS driver_name,
    rv.make AS vehicle_make,
    rv.model AS vehicle_model,
    rv.year AS vehicle_year,
    rv.capacity AS vehicle_capacity,
    rd.available AS is_available_flag
FROM raw_drivers rd
LEFT JOIN raw_vehicles rv ON rd.vehicle_id = rv.vehicle_id
ON CONFLICT (driver_id) DO NOTHING;

\echo '--- DIM_DRIVER populated ---'
\echo '--- All Dimension Tables Populated Successfully ---'
