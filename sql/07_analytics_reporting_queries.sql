-- Reporting Queries: 05_reporting_queries.sql

\echo '--- Starting Analytical Reporting Queries ---'

-----------------------------------------------------------
-- Q1: Driver Utilization and Efficiency Analysis
-- Business Question: Which drivers are the most efficient (lowest time spent idle) 
-- and how does their efficiency relate to their total rides?
-----------------------------------------------------------

SELECT
    driver_name,
    -- Aggregate the mean wait time across all days
    AVG(avg_wait_time_minutes) AS overall_avg_wait_time_minutes,
    SUM(total_rides_that_day) AS total_rides
FROM
    v_driver_daily_utilization -- Querying the View makes the SQL much simpler!
GROUP BY
    driver_name
HAVING
    -- Only include drivers with a significant number of rides for meaningful average
    SUM(total_rides_that_day) > 100
ORDER BY
    overall_avg_wait_time_minutes ASC, -- Efficiency: Lower wait time is better
    total_rides DESC                     -- Tie-breaker: More rides is better
LIMIT 10;
-- 


-----------------------------------------------------------
-- Q2: Peak Hour Revenue Analysis for Surge Pricing Strategy
-- Business Question: What are the highest revenue-generating hours and days?
-----------------------------------------------------------

SELECT
    dd.day_name,
    EXTRACT(HOUR FROM fr.start_time) AS hour_of_day,
    -- Metrics
    COUNT(fr.ride_id) AS total_rides,
    SUM(fr.fare_amount) AS total_revenue
FROM
    fact_rides fr
JOIN
    dim_date dd ON fr.start_date_key = dd.date_key
GROUP BY
    dd.day_name,
    hour_of_day
ORDER BY
    total_revenue DESC
LIMIT 20;
-- 


-----------------------------------------------------------
-- Q3: Customer Segmentation for Loyalty Programs (Lifetime Value Proxy)
-- Business Question: Identify the top 5 most valuable users based on their spending and frequency.
-----------------------------------------------------------

SELECT
    du.masked_name, -- Using the masked name protects PII while allowing identification
    COUNT(fr.ride_id) AS total_rides,
    SUM(fr.fare_amount) AS total_fare_spent,
    AVG(fr.distance_km) AS avg_ride_distance_km
FROM
    fact_rides fr
JOIN
    dim_user du ON fr.user_key = du.user_key
GROUP BY
    du.masked_name
ORDER BY
    total_fare_spent DESC, -- Primary driver for value
    total_rides DESC
LIMIT 5;
--