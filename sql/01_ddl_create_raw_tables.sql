-- DDL Script: 01_ddl_create_raw_tables.sql (Finalized)
-- Purpose: Define raw tables for the ride-sharing data warehouse.
-- Assumptions: Input CSV files have headers and proper encoding (UTF-8)
-- Expected data volumes: ~10,000 users, ~300 drivers, ~300 vehicles, ~50,000 rides, ~15,000 ratings

-- 1. Table for raw_users
-- Includes PII (email, phone_number) and demographics for later masking/transformation.
CREATE TABLE raw_users (
    user_id VARCHAR(50) PRIMARY KEY,
    name VARCHAR(255),
    email VARCHAR(255),
    phone_number VARCHAR(50),
    registration_date DATE,
    age INTEGER,
    gender VARCHAR(20),
    location VARCHAR(255) -- Assuming a simple string format for raw location
);

-- 2. Table for raw_drivers
-- Includes information about driver status and performance (rating, total_rides).
CREATE TABLE raw_drivers (
    driver_id VARCHAR(50) PRIMARY KEY,
    name VARCHAR(255),
    vehicle_id VARCHAR(50), -- Links to raw_vehicles
    rating NUMERIC(3, 2), -- To store ratings like 4.50
    total_rides INTEGER,
    available BOOLEAN -- To store true/false status
);

-- 3. Table for raw_vehicles
-- Vehicle specifics, including capacity (useful for calculating potential utilization).
CREATE TABLE raw_vehicles (
    vehicle_id VARCHAR(50) PRIMARY KEY,
    make VARCHAR(100),
    model VARCHAR(100),
    year INTEGER,
    capacity INTEGER
);

-- 4. Table for raw_rides (The main transactional table)
-- Contains the core trip data, including detailed start/end locations and times.
CREATE TABLE raw_rides (
    ride_id VARCHAR(50) PRIMARY KEY,
    user_id VARCHAR(50),
    start_location VARCHAR(255), -- Raw, potentially messy location data
    end_location VARCHAR(255),   -- Raw, potentially messy location data
    ride_start_time TIMESTAMP WITHOUT TIME ZONE,
    ride_end_time TIMESTAMP WITHOUT TIME ZONE,
    distance_km NUMERIC(10, 2),
    fare_amount NUMERIC(10, 2),
    driver_id VARCHAR(50),
    -- Define Foreign Keys to enforce relationships
    FOREIGN KEY (user_id) REFERENCES raw_users(user_id),
    FOREIGN KEY (driver_id) REFERENCES raw_drivers(driver_id)
);

-- 5. Table for raw_ratings
-- Detailed feedback data.
CREATE TABLE raw_ratings (
    rating_id VARCHAR(50) PRIMARY KEY,
    ride_id VARCHAR(50),
    user_id VARCHAR(50),
    rating_value INTEGER, -- Typically 1-5
    comments TEXT,
    rating_date DATE,
    -- Define Foreign Keys
    FOREIGN KEY (ride_id) REFERENCES raw_rides(ride_id),
    FOREIGN KEY (user_id) REFERENCES raw_users(user_id)
);