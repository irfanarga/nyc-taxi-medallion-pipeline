WITH source AS (
    SELECT * FROM {{ source('silver_source', 'silver_yellow_taxi') }}
)
SELECT
    COALESCE(CAST({% if target.name == 'gcp' %} VendorID {% else %} "VendorID" {% endif %} AS {% if target.name == 'gcp' %} INT64 {% else %} INT {% endif %}), 0) AS vendor_id,
    CAST(tpep_pickup_datetime AS TIMESTAMP) AS pickup_datetime,
    CAST(tpep_dropoff_datetime AS TIMESTAMP) AS dropoff_datetime,
    CAST({% if target.name == 'gcp' %} PULocationID {% else %} "PULocationID" {% endif %} AS {% if target.name == 'gcp' %} INT64 {% else %} INT {% endif %}) AS pickup_location_id,
    CAST({% if target.name == 'gcp' %} DOLocationID {% else %} "DOLocationID" {% endif %} AS {% if target.name == 'gcp' %} INT64 {% else %} INT {% endif %}) AS dropoff_location_id,
    CAST(passenger_count AS {% if target.name == 'gcp' %} INT64 {% else %} INT {% endif %}) AS passenger_count,
    CAST(trip_distance AS NUMERIC) AS trip_distance_miles,
    CAST({% if target.name == 'gcp' %} RatecodeID {% else %} "RatecodeID" {% endif %} AS {% if target.name == 'gcp' %} INT64 {% else %} INT {% endif %}) AS rate_code_id,
    CAST({% if target.name == 'gcp' %} payment_type {% else %} "payment_type" {% endif %} AS {% if target.name == 'gcp' %} INT64 {% else %} INT {% endif %}) AS payment_type_id,
    CAST(fare_amount AS NUMERIC) AS fare_amount,
    CAST(tip_amount AS NUMERIC) AS tip_amount,
    CAST(tolls_amount AS NUMERIC) AS tolls_amount,
    CAST(total_amount AS NUMERIC) AS total_amount,
    CAST(trip_duration_minutes AS NUMERIC) AS trip_duration_minutes
FROM source