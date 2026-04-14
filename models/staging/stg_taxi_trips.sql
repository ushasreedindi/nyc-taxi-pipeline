-- models/staging/stg_taxi_trips.sql
-- This cleans our raw data and adds calculated columns


WITH source AS (
    SELECT * FROM {{ source('raw', 'taxi_trips') }}
),

cleaned AS (
    SELECT
        VENDORID,
        TRY_TO_TIMESTAMP(TPEP_PICKUP_DATETIME)  AS pickup_datetime,
        TRY_TO_TIMESTAMP(TPEP_DROPOFF_DATETIME) AS dropoff_datetime,
        PASSENGER_COUNT,
        ROUND(TRIP_DISTANCE, 2)                  AS trip_distance_miles,
        PAYMENT_TYPE,
        ROUND(FARE_AMOUNT, 2)                    AS fare_amount,
        ROUND(TIP_AMOUNT, 2)                     AS tip_amount,
        ROUND(TOLLS_AMOUNT, 2)                   AS tolls_amount,
        ROUND(TOTAL_AMOUNT, 2)                   AS total_amount,

        -- calculated columns
        DATEDIFF('minute',
            TRY_TO_TIMESTAMP(TPEP_PICKUP_DATETIME),
            TRY_TO_TIMESTAMP(TPEP_DROPOFF_DATETIME)
        )                                        AS trip_duration_mins,

        HOUR(TRY_TO_TIMESTAMP(TPEP_PICKUP_DATETIME)) AS pickup_hour,
        DAYNAME(TRY_TO_TIMESTAMP(TPEP_PICKUP_DATETIME)) AS pickup_day,
        DATE(TRY_TO_TIMESTAMP(TPEP_PICKUP_DATETIME)) AS pickup_date

    FROM source
    WHERE FARE_AMOUNT > 0
      AND TRIP_DISTANCE > 0
      AND PASSENGER_COUNT > 0
      AND TOTAL_AMOUNT > 0
      AND DATEDIFF('minute',
            TRY_TO_TIMESTAMP(TPEP_PICKUP_DATETIME),
            TRY_TO_TIMESTAMP(TPEP_DROPOFF_DATETIME)) > 0
)

SELECT * FROM cleaned