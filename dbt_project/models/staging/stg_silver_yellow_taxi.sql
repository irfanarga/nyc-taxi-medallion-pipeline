WITH source AS (
    SELECT * FROM {{ source('silver_layer', 'silver_yellow_taxi') }}
)
SELECT
    COALESCE(CAST("VendorID" AS INT), 0) AS vendor_id,
    CAST(tpep_pickup_datetime AS TIMESTAMP) AS pickup_datetime,
    CAST(tpep_dropoff_datetime AS TIMESTAMP) AS dropoff_datetime,
    CAST("PULocationID" AS INT) AS pickup_location_id,
    CAST("DOLocationID" AS INT) AS dropoff_location_id,
    CAST(passenger_count AS INT) AS passenger_count,
    CAST(trip_distance AS NUMERIC(10,2)) AS trip_distance_miles,
    CAST("RatecodeID" AS INT) AS rate_code_id,
    CAST("payment_type" AS INT) AS payment_type_id,
    CAST(fare_amount AS NUMERIC(10,2)) AS fare_amount,
    CAST(tip_amount AS NUMERIC(10,2)) AS tip_amount,
    CAST(tolls_amount AS NUMERIC(10,2)) AS tolls_amount,
    CAST(total_amount AS NUMERIC(10,2)) AS total_amount,
    CAST(trip_duration_minutes AS NUMERIC(10,2)) AS trip_duration_minutes
FROM source