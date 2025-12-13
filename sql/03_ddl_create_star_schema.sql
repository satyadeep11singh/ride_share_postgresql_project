-- DDL Script: 02_ddl_star_schema.sql
-- Purpose: Define star schema tables for the ride-sharing data warehouse.

-- Clean up tables before creating the Star Schema (useful for re-runs)
DROP TABLE IF EXISTS fact_rides;
DROP TABLE IF EXISTS dim_date;
DROP TABLE IF EXISTS dim_driver;
DROP TABLE IF EXISTS dim_user;

-- 1. DIM_USER (Dimension Table for Riders)
-- PII like email/phone is OMITTED or MASKED here.
CREATE TABLE dim_user (
    user_key SERIAL PRIMARY KEY, -- Surrogate Key
    user_id VARCHAR(50) UNIQUE NOT NULL, -- Natural Key from raw data
    masked_name VARCHAR(255), -- Name masked for privacy
    masked_email_hash VARCHAR(32), -- Hashed email (MD5 is 32 chars)
    masked_phone_hash VARCHAR(32), -- Hashed phone
    registration_date DATE,
    age INTEGER,
    gender VARCHAR(20),
    location_city VARCHAR(100), -- Cleaned/Normalized location
    is_active_subscriber BOOLEAN
);

-- 2. DIM_DRIVER (Dimension Table for Drivers)
-- Combines raw_drivers and raw_vehicles data.
CREATE TABLE dim_driver (
    driver_key SERIAL PRIMARY KEY, -- Surrogate Key
    driver_id VARCHAR(50) UNIQUE NOT NULL, -- Natural Key
    driver_name VARCHAR(255),
    -- Vehicle details will be joined and normalized
    vehicle_make VARCHAR(100),
    vehicle_model VARCHAR(100),
    vehicle_year INTEGER,
    vehicle_capacity INTEGER,
    is_available_flag BOOLEAN
);

-- 3. DIM_DATE (Time Dimension)
-- The foundation for all time-based analysis. We will populate this next.
CREATE TABLE dim_date (
    date_key INTEGER PRIMARY KEY, -- YYYYMMDD format for easy joining
    full_date DATE UNIQUE NOT NULL,
    day_of_week INTEGER,
    day_name VARCHAR(10),
    month_name VARCHAR(10),
    year INTEGER,
    is_weekend BOOLEAN
);

-- 4. FACT_RIDES (The central measurement table)
CREATE TABLE fact_rides (
    ride_id VARCHAR(50) PRIMARY KEY, -- Natural Key from raw data (for reference)
    
    -- Foreign Keys to Dimensions
    user_key INTEGER REFERENCES dim_user(user_key),
    driver_key INTEGER REFERENCES dim_driver(driver_key),
    start_date_key INTEGER REFERENCES dim_date(date_key),
    
    -- Time attributes for more precise analysis
    start_time TIMESTAMP WITHOUT TIME ZONE,
    end_time TIMESTAMP WITHOUT TIME ZONE,
    
    -- Facts (Metrics)
    ride_duration_minutes NUMERIC(10, 2), -- Calculated using ride_end_time - ride_start_time
    distance_km NUMERIC(10, 2),
    fare_amount NUMERIC(10, 2),
    
    -- Derived Metrics (Calculated using Window Functions later)
    time_to_next_ride_minutes NUMERIC(10, 2), -- Window function LAG() result - can be large if drivers have long gaps
    is_peak_hour BOOLEAN,
    average_driver_rating NUMERIC(3, 2) -- Driver's average rating at time of ride
);