-- Load Script: 02_load_raw_data_from_csv.sql
-- Purpose: Bulk load CSV files into raw staging tables
-- Expected results: 10,000 users, 300 drivers, 300 vehicles, 50,000 rides, 15,000 ratings

-- WARNING: The \COPY command is a psql meta-command and must be run 
-- directly from the psql terminal or via the -f flag, NOT inside a transaction block 
-- or a generic SQL client that doesn't support psql commands.

-- Ensure all CSV files are in the 'raw_data/' directory relative to execution location.

\echo '--- Starting Data Load ---'

-- 1. Load raw_users
\COPY raw_users FROM 'raw_data/users.csv' DELIMITER ',' CSV HEADER;

-- 2. Load raw_drivers
\COPY raw_drivers FROM 'raw_data/drivers.csv' DELIMITER ',' CSV HEADER;

-- 3. Load raw_vehicles
\COPY raw_vehicles FROM 'raw_data/vehicles.csv' DELIMITER ',' CSV HEADER;

-- 4. Load raw_rides (The largest file, ensure it runs smoothly)
\COPY raw_rides FROM 'raw_data/rides.csv' DELIMITER ',' CSV HEADER;

-- 5. Load raw_ratings
\COPY raw_ratings FROM 'raw_data/ratings.csv' DELIMITER ',' CSV HEADER;

\echo '--- Data Load Complete ---'