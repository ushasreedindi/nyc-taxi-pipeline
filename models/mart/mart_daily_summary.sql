-- models/mart/mart_daily_summary.sql
-- Final analytics table for Power BI dashboard

WITH staging AS (
    SELECT * FROM {{ ref('stg_taxi_trips') }}
)

SELECT
    pickup_date,
    pickup_day,
    pickup_hour,

    -- volume metrics
    COUNT(*)                                AS total_trips,
    SUM(PASSENGER_COUNT)                    AS total_passengers,

    -- revenue metrics
    ROUND(SUM(fare_amount), 2)              AS total_revenue,
    ROUND(AVG(fare_amount), 2)              AS avg_fare,
    ROUND(SUM(tip_amount), 2)               AS total_tips,
    ROUND(AVG(tip_amount), 2)               AS avg_tip,
    ROUND(SUM(total_amount), 2)             AS total_collected,

    -- trip metrics
    ROUND(AVG(trip_distance_miles), 2)      AS avg_distance_miles,
    ROUND(AVG(trip_duration_mins), 2)       AS avg_duration_mins,
    ROUND(SUM(trip_distance_miles), 2)      AS total_miles,

    -- payment breakdown
    SUM(CASE WHEN payment_type = 1 THEN 1 ELSE 0 END) AS credit_card_trips,
    SUM(CASE WHEN payment_type = 2 THEN 1 ELSE 0 END) AS cash_trips

FROM staging
GROUP BY 1, 2, 3
ORDER BY 1, 3