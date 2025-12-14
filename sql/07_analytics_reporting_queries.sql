-- Reporting Queries: 07_analytics_reporting_queries.sql

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


===================================================================
-- ADVANCED WINDOW FUNCTION ANALYTICS (Q4-Q13)
-- These queries demonstrate modern SQL window functions for deeper insights
===================================================================


-----------------------------------------------------------
-- Q4: ROW_NUMBER() — Driver Performance Leaderboard (Unique Ranking with Tie-Breaking)
-- Business Question: How do drivers rank against each other with unique positions?
-- Value: ROW_NUMBER() assigns 1, 2, 3... (no gaps) creating a true leaderboard
-- Use Case: Monthly driver efficiency leaderboard for recognition/incentives
-----------------------------------------------------------

SELECT
    ROW_NUMBER() OVER (ORDER BY AVG(avg_wait_time_minutes) ASC) AS efficiency_rank,
    driver_name,
    ROUND(AVG(avg_wait_time_minutes)::numeric, 2) AS overall_avg_wait_time_minutes,
    SUM(total_rides_that_day) AS total_rides,
    ROUND(AVG(avg_wait_time_minutes) OVER ()::numeric, 2) AS fleet_avg_wait_time -- Fleet benchmark
FROM
    v_driver_daily_utilization
WHERE
    total_rides_that_day > 0
GROUP BY
    driver_name
ORDER BY
    efficiency_rank
LIMIT 15;
-- Business Insight: "Driver ranked #1 has 2 min avg wait vs fleet average of 5 min"
--


-----------------------------------------------------------
-- Q5: RANK() — Revenue-Based Driver Tier Classification (Shows Tied Positions)
-- Business Question: Which drivers are in the top revenue tier? Who's tied for position?
-- Value: RANK() leaves gaps when ties exist, showing explicit "cluster of equals"
-- Use Case: Commission tiers — "All rank 1 drivers get premium commission"
-- OPTIMIZATION: Use CTE to cache RANK() result (avoids 3x recalculation in CASE statement)
-----------------------------------------------------------

WITH driver_revenue_ranks AS (
    SELECT
        dd.driver_name,
        COUNT(fr.ride_id) AS total_rides,
        SUM(fr.fare_amount) AS total_revenue,
        RANK() OVER (ORDER BY SUM(fr.fare_amount) DESC) AS revenue_rank
    FROM fact_rides fr
    JOIN dim_driver dd ON fr.driver_key = dd.driver_key
    GROUP BY dd.driver_name
)
SELECT
    revenue_rank,
    driver_name,
    total_rides,
    total_revenue,
    ROUND((total_revenue / total_rides)::numeric, 2) AS avg_fare_per_ride,
    CASE
        WHEN revenue_rank <= 10 THEN 'Top Tier (Rank <=10)'
        WHEN revenue_rank <= 30 THEN 'Mid Tier (Rank 11-30)'
        ELSE 'Growth Tier (Rank >30)'
    END AS performance_tier
FROM driver_revenue_ranks
ORDER BY
    revenue_rank
LIMIT 30;
-- Business Insight: "If 3 drivers earn $50K, all get rank 1; next driver gets rank 4 (not 2)"
--


-----------------------------------------------------------
-- Q6: DENSE_RANK() — Performance Tiers with Compact Numbering (No Gaps)
-- Business Question: Create bonus tiers without numbering gaps for stakeholder clarity
-- Value: DENSE_RANK() assigns 1, 2, 3 even with ties (no rank 4 gaps)
-- Use Case: Transparent tier-based bonus structure marketing
-- OPTIMIZATION: Use CTE to cache DENSE_RANK() result (avoids 3x recalculation in CASE statement)
-- DATA SOURCE: Ratings loaded from raw_ratings during ETL transformation (03_etl_transform.sql)
-----------------------------------------------------------

WITH driver_ratings_with_rank AS (
    SELECT 
        dd.driver_name, 
        COUNT(fr.ride_id) AS rides_completed,
        ROUND(AVG(fr.average_driver_rating)::numeric, 2) AS avg_rating,
        DENSE_RANK() OVER (ORDER BY AVG(fr.average_driver_rating) DESC) AS quality_tier
    FROM fact_rides fr
    JOIN dim_driver dd ON fr.driver_key = dd.driver_key
    WHERE fr.average_driver_rating IS NOT NULL  -- Only include rides with ratings
    GROUP BY dd.driver_name
    HAVING COUNT(fr.ride_id) >= 10  -- Minimum ride threshold for meaningful rating
)
SELECT
    quality_tier,
    driver_name,
    rides_completed,
    avg_rating,
    CASE
        WHEN quality_tier = 1 THEN 'Platinum (4.8+)'
        WHEN quality_tier = 2 THEN 'Gold (4.5-4.7)'
        WHEN quality_tier = 3 THEN 'Silver (4.0-4.4)'
        ELSE 'Development (Below 4.0)'
    END AS tier_name
FROM driver_ratings_with_rank
ORDER BY
    quality_tier
LIMIT 20;
-- Business Insight: "5 drivers at Tier 1, next 8 at Tier 2 (clean tiers, no rank gaps)"
--


-----------------------------------------------------------
-- Q7: PERCENT_RANK() — Percentile-Based Commission Allocation (0.0 to 1.0)
-- Business Question: What percentage of peers does each driver outperform?
-- Value: Returns 0.0 (best) to 1.0 (worst) for dynamic incentive scaling
-- Use Case: "Top 20% by percentile gets 15% commission boost"
-- OPTIMIZATION: Use CTE to cache PERCENT_RANK() result (avoids 3x recalculation in CASE statement)
-----------------------------------------------------------

WITH driver_revenue_with_percentile AS (
    SELECT
        dd.driver_name,
        SUM(fr.fare_amount) AS total_revenue,
        COUNT(fr.ride_id) AS total_rides,
        ROUND((PERCENT_RANK() OVER (ORDER BY SUM(fr.fare_amount) DESC))::numeric, 3) AS revenue_percentile
    FROM fact_rides fr
    JOIN dim_driver dd ON fr.driver_key = dd.driver_key
    GROUP BY dd.driver_name
)
SELECT
    driver_name,
    total_revenue,
    total_rides,
    ROUND((total_revenue / NULLIF(total_rides, 0))::numeric, 2) AS revenue_per_ride,
    revenue_percentile,
    CASE
        WHEN revenue_percentile <= 0.20 THEN 'Top 20% - +15% Commission'
        WHEN revenue_percentile <= 0.50 THEN 'Top 50% - +10% Commission'
        ELSE 'Growth Pool - +5% Commission'
    END AS commission_tier
FROM driver_revenue_with_percentile
ORDER BY
    revenue_percentile ASC  -- Best performers (lowest percentile) first
LIMIT 25;
-- Business Insight: "Driver at 0.18 percentile = top 18% earners = qualifies for +15% bonus"
--


-----------------------------------------------------------
-- Q8: CUME_DIST() — Cumulative Distribution for Quartile Segmentation
-- Business Question: Create quartile segments for marketing campaigns
-- Value: Proportional cumulative ranking; handles ties realistically
-- Use Case: "VIP segment (Q1) eligible for premium vehicle program"
-- OPTIMIZATION: Use CTE to cache CUME_DIST() result (avoids 4x recalculation in CASE statement)
-----------------------------------------------------------

WITH driver_efficiency_ranks AS (
    SELECT
        driver_name,
        SUM(total_rides_that_day) AS total_rides,
        AVG(avg_wait_time_minutes) AS avg_wait_time,
        ROUND((CUME_DIST() OVER (ORDER BY AVG(avg_wait_time_minutes) ASC))::numeric, 3) AS efficiency_cume_dist
    FROM v_driver_daily_utilization
    WHERE total_rides_that_day > 0
    GROUP BY driver_name
)
SELECT
    driver_name,
    total_rides,
    avg_wait_time,
    efficiency_cume_dist,
    CASE
        WHEN efficiency_cume_dist <= 0.25 
            THEN 'Q1 (Best) - Fleet Stars'
        WHEN efficiency_cume_dist <= 0.50 
            THEN 'Q2 (Good) - Reliable'
        WHEN efficiency_cume_dist <= 0.75 
            THEN 'Q3 (Fair) - Training Eligible'
        ELSE 'Q4 (Development) - Mentoring Program'
    END AS efficiency_quartile
FROM driver_efficiency_ranks
ORDER BY
    efficiency_cume_dist ASC
LIMIT 30;
-- Business Insight: "Q1 drivers at 0.24 quartile = top 24% performers; all tied drivers grouped fairly"
--


-----------------------------------------------------------
-- Q9: NTILE(4) — Equal Bucketing into Customer Value Segments
-- Business Question: Segment 10K customers into 4 equal groups for campaign targeting
-- Value: NTILE divides rows into N equal-sized buckets
-- Use Case: "VIP segment (quartile 1) gets exclusive perks; Basic gets standard service"
-- OPTIMIZATION: Use CTE to cache NTILE(4) result (avoids 5x recalculation in CASE statements)
-----------------------------------------------------------

WITH customer_aggregates AS (
    SELECT
        du.masked_name,
        COUNT(fr.ride_id) AS total_rides,
        SUM(fr.fare_amount) AS total_fare_spent,
        AVG(fr.distance_km) AS avg_distance_km
    FROM fact_rides fr
    JOIN dim_user du ON fr.user_key = du.user_key
    GROUP BY du.masked_name
),
customer_value_segments AS (
    SELECT
        masked_name,
        total_rides,
        total_fare_spent,
        avg_distance_km,
        NTILE(4) OVER (ORDER BY total_fare_spent DESC) AS customer_value_quartile
    FROM customer_aggregates
)
SELECT
    customer_value_quartile,
    masked_name,
    total_rides,
    total_fare_spent,
    avg_distance_km,
    CASE
        WHEN customer_value_quartile = 1 
            THEN 'VIP (Top 25%)'
        WHEN customer_value_quartile = 2 
            THEN 'Premium (25-50%)'
        WHEN customer_value_quartile = 3 
            THEN 'Standard (50-75%)'
        ELSE 'Basic (Bottom 25%)'
    END AS segment_name,
    CASE
        WHEN customer_value_quartile = 1 
            THEN 'VIP: Free upgrades, priority support'
        WHEN customer_value_quartile = 2 
            THEN 'Premium: 5% off future rides'
        WHEN customer_value_quartile = 3 
            THEN 'Standard: Loyalty points'
        ELSE 'Basic: Standard service'
    END AS offer
FROM customer_value_segments
ORDER BY
    customer_value_quartile, total_fare_spent DESC
LIMIT 50;
-- Business Insight: "Segment VIP (top 25%) = ~2,500 users; Standard (50-75%) = ~2,500 users"
--


-----------------------------------------------------------
-- Q10: LEAD() — Forward-Looking Idle Time Detection (Driver Churn Risk)
-- Business Question: Which drivers have dangerously long gaps between rides?
-- Value: LEAD() looks to NEXT row (next ride); identifies churn risk
-- Use Case: "Drivers with 4+ hour idle periods → Send retention incentive"
-----------------------------------------------------------

SELECT
    driver_name,
    DATE(current_ride_start) AS ride_date,
    EXTRACT(HOUR FROM current_ride_start) AS start_hour,
    current_fare,
    next_ride_start,
    CASE
        WHEN next_ride_start IS NOT NULL
            THEN ROUND((EXTRACT(EPOCH FROM (next_ride_start - current_ride_end)) / 3600)::numeric, 1)
        ELSE NULL
    END AS idle_hours_until_next_ride,
    CASE
        WHEN next_ride_start IS NOT NULL AND EXTRACT(EPOCH FROM (next_ride_start - current_ride_end)) / 3600 > 4 
            THEN 'CHURN RISK: >4hr idle'
        WHEN next_ride_start IS NOT NULL AND EXTRACT(EPOCH FROM (next_ride_start - current_ride_end)) / 3600 > 2 
            THEN 'WARNING: 2-4hr idle'
        WHEN next_ride_start IS NOT NULL
            THEN 'Normal: <2hr idle'
        ELSE 'Last ride (no next ride data)'
    END AS retention_risk_flag
FROM (
    SELECT
        dd.driver_name,
        fr.start_time AS current_ride_start,
        fr.end_time AS current_ride_end,
        fr.fare_amount AS current_fare,
        LEAD(fr.start_time) OVER (
            PARTITION BY fr.driver_key 
            ORDER BY fr.start_time
        ) AS next_ride_start
    FROM fact_rides fr
    JOIN dim_driver dd ON fr.driver_key = dd.driver_key
    WHERE fr.driver_key IN (1, 5, 10, 15, 20)  -- Sample 5 drivers for demo
) driver_sequence
WHERE
    next_ride_start IS NOT NULL
ORDER BY
    driver_name, current_ride_start
LIMIT 30;
-- Business Insight: "Driver X has 6.2 hour idle after ride 47 → likely taking break or switching platforms"
--


-----------------------------------------------------------
-- Q11: FIRST_VALUE() — Baseline Comparison (Driver Progression from First Ride)
-- Business Question: How much has each driver improved since their first ride?
-- Value: Compares current metrics to baseline (first-ever ride)
-- Use Case: "New driver onboarding effectiveness: fares trending up from day 1?"
-----------------------------------------------------------

SELECT
    driver_name,
    ride_number,
    DATE(current_ride_start) AS ride_date,
    current_fare,
    first_ever_fare,
    ROUND((current_fare - first_ever_fare)::numeric, 2) AS fare_improvement,
    ROUND((((current_fare - first_ever_fare) / NULLIF(first_ever_fare, 0)) * 100)::numeric, 1) AS improvement_percent,
    CASE
        WHEN (current_fare - first_ever_fare) >= 20 THEN 'Strong Growth (>$20 gain)'
        WHEN (current_fare - first_ever_fare) >= 5 THEN 'Positive Trend ($5-$20 gain)'
        WHEN (current_fare - first_ever_fare) >= 0 THEN 'Stable ($0-$5)'
        ELSE 'Declining (<$0)'
    END AS performance_trend
FROM (
    SELECT
        dd.driver_name,
        fr.fare_amount AS current_fare,
        ROW_NUMBER() OVER (PARTITION BY fr.driver_key ORDER BY fr.start_time) AS ride_number,
        fr.start_time AS current_ride_start,
        FIRST_VALUE(fr.fare_amount) OVER (
            PARTITION BY fr.driver_key
            ORDER BY fr.start_time
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        ) AS first_ever_fare
    FROM fact_rides fr
    JOIN dim_driver dd ON fr.driver_key = dd.driver_key
) driver_progression
WHERE
    driver_name IN ('Joann Wolfe', 'Jeremy Bautista', 'Christina Chang', 'Mark Davis', 'Brooke Snyder')
ORDER BY
    driver_name, ride_number
LIMIT 50;
-- Business Insight: "Driver at ride #50 earning $32 vs $12 first ride = +167% growth → strong retention candidate"
--


-----------------------------------------------------------
-- Q12: LAST_VALUE() — Recent Trend Detection (Quality Degradation Alerts)
-- Business Question: Are drivers' recent performance metrics declining?
-- Value: Compares most recent metrics to historical average
-- Use Case: "Quality dropped 0.5+ stars recently → Send performance improvement message"
-- DATA SOURCE: Ratings loaded from raw_ratings during ETL transformation (03_etl_transform.sql)
-----------------------------------------------------------

WITH driver_quality_trends AS (
    SELECT
        fr.driver_key,
        dd.driver_name,
        COUNT(*) OVER (PARTITION BY fr.driver_key) AS total_rides,
        ROUND(AVG(fr.average_driver_rating) OVER (PARTITION BY fr.driver_key)::numeric, 2) AS lifetime_avg_rating,
        ROUND(LAST_VALUE(fr.average_driver_rating) OVER (
            PARTITION BY fr.driver_key
            ORDER BY fr.start_time
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        )::numeric, 2) AS most_recent_rating,
        ROW_NUMBER() OVER (PARTITION BY fr.driver_key ORDER BY fr.start_time DESC) AS rn
    FROM fact_rides fr
    JOIN dim_driver dd ON fr.driver_key = dd.driver_key
    WHERE fr.average_driver_rating IS NOT NULL  -- Only consider rides with ratings
)
SELECT
    driver_name,
    total_rides,
    lifetime_avg_rating,
    most_recent_rating,
    ROUND((most_recent_rating - lifetime_avg_rating)::numeric, 2) AS recent_trend,
    CASE
        WHEN (most_recent_rating - lifetime_avg_rating) < -0.5 
            THEN 'ALERT: Quality declining >0.5 stars'
        WHEN (most_recent_rating - lifetime_avg_rating) < 0 
            THEN 'WARNING: Slight quality decline'
        WHEN (most_recent_rating - lifetime_avg_rating) >= 0 
            THEN 'POSITIVE: Maintaining or improving'
    END AS quality_trend_flag
FROM driver_quality_trends
WHERE
    rn = 1  -- Get one row per driver (most recent)
ORDER BY
    recent_trend ASC  -- Show declining drivers first
LIMIT 20;
-- Business Insight: "Driver X lifetime 4.2★ → recent 3.5★ = 0.7★ decline → intervention needed"
--


-----------------------------------------------------------
-- Q13: NTH_VALUE() — Milestone Analysis (Driver Lifecycle Tracking)
-- Business Question: Are new drivers completing milestones? Where do they drop off?
-- Value: Compares metrics at milestones (1st, 10th, 50th, 100th ride)
-- Use Case: "Drivers stuck at ride #8-15 range → training intervention needed"
-----------------------------------------------------------

WITH driver_milestones AS (
    SELECT
        fr.driver_key,
        dd.driver_name,
        COUNT(*) OVER (PARTITION BY fr.driver_key) AS total_rides_to_date,
        ROUND(NTH_VALUE(fr.fare_amount, 1) OVER (
            PARTITION BY fr.driver_key ORDER BY fr.start_time 
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        )::numeric, 2) AS ride_1_fare,
        ROUND(NTH_VALUE(fr.fare_amount, 10) OVER (
            PARTITION BY fr.driver_key ORDER BY fr.start_time 
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        )::numeric, 2) AS ride_10_fare,
        ROUND(NTH_VALUE(fr.fare_amount, 50) OVER (
            PARTITION BY fr.driver_key ORDER BY fr.start_time 
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        )::numeric, 2) AS ride_50_fare,
        ROUND(NTH_VALUE(fr.fare_amount, 100) OVER (
            PARTITION BY fr.driver_key ORDER BY fr.start_time 
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        )::numeric, 2) AS ride_100_fare,
        ROW_NUMBER() OVER (PARTITION BY fr.driver_key ORDER BY fr.start_time DESC) AS rn
    FROM fact_rides fr
    JOIN dim_driver dd ON fr.driver_key = dd.driver_key
)
SELECT
    driver_name,
    total_rides_to_date,
    CASE
        WHEN total_rides_to_date >= 100 THEN 'Active (100+ rides)'
        WHEN total_rides_to_date >= 50 THEN 'Engaged (50+ rides)'
        WHEN total_rides_to_date >= 10 THEN 'Onboarded (10+ rides)'
        ELSE 'New (<10 rides)'
    END AS driver_lifecycle_stage,
    ride_1_fare,
    ride_10_fare,
    ride_50_fare,
    ride_100_fare,
    CASE
        WHEN total_rides_to_date >= 50 AND ride_10_fare > ride_1_fare
        THEN 'Strong onboarding (fares increasing)'
        ELSE 'Monitor growth pattern'
    END AS onboarding_quality
FROM driver_milestones
WHERE
    rn = 1 AND total_rides_to_date >= 10  -- Focus on drivers with meaningful sample size, get one row per driver
ORDER BY
    total_rides_to_date DESC
LIMIT 30;
-- Business Insight: "Driver A: R1=$15 → R10=$22 → R50=$28 = strong uptrend; Driver B: R1=$15 → R10=$14 = struggling with onboarding"
--