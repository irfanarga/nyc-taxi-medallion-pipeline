WITH staging AS (
    SELECT * FROM {{ ref('stg_silver_yellow_taxi') }}
)
SELECT
    DATE_TRUNC('month', pickup_datetime)::DATE AS trip_month,
    pickup_location_id,
    dropoff_location_id,
    payment_type_id,
    COUNT(*) AS total_trips,
    SUM(passenger_count) AS total_passengers,
    SUM(trip_distance_miles) AS total_distance_miles,
    SUM(fare_amount) AS total_fare_amount,
    SUM(tip_amount) AS total_tip_amount,
    SUM(total_amount) AS total_gross_revenue,
    ROUND(AVG(trip_duration_minutes), 2) AS avg_duration_minutes,
    ROUND(
        CASE 
            WHEN SUM(fare_amount) > 0 THEN (SUM(tip_amount) / SUM(fare_amount)) * 100 
            ELSE 0 
        END, 2
    ) AS avg_tip_percentage
FROM staging
GROUP BY 1, 2, 3, 4