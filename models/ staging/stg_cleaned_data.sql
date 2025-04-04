{{ config(materialized='view', schema='staging_DWH') }}

WITH cleaned_data AS (
    SELECT
        CAST("quarter" AS SMALLINT) AS quarter,  -- Small numbers (1-4)
        COALESCE("payment_channel", 'Unknown') AS payment_channel,
        COALESCE("merchant_location", 'Unknown') AS merchant_location,
        CAST("week_number" AS SMALLINT) AS week_number,  -- Small numbers (1-52)
        CAST("latitude" AS DOUBLE PRECISION) AS latitude,  -- Requires decimals
        COALESCE("gender", 'Unknown') AS gender,
        COALESCE("type", 'Unknown') AS type,
        CAST("account_creation_date" AS DATE) AS account_creation_date,  -- Date format
        CAST(COALESCE("newbalancedest", 0) AS DOUBLE PRECISION) AS newbalancedest,  -- Requires decimals
        CASE WHEN COALESCE("is_holiday", 0) = 1 THEN TRUE ELSE FALSE END AS is_holiday,  -- Convert DOUBLE PRECISION to BOOLEAN
        COALESCE("full_name", 'Unknown') AS full_name,
        CAST("Month" AS SMALLINT) AS month,  -- Small numbers (1-12)
        CASE WHEN COALESCE("isflaggedfraud", 0) = 1 THEN TRUE ELSE FALSE END AS isflaggedfraud,  -- Convert DOUBLE PRECISION to BOOLEAN
        CAST(COALESCE("merchant_rating", 0) AS SMALLINT) AS merchant_rating,  -- Small numbers (e.g., 1-5)
        COALESCE("manufacturer", 'Unknown') AS manufacturer,
        CAST(COALESCE("postal_code", '0') AS TEXT) AS postal_code,  -- Can be alphanumeric
        CAST(COALESCE("app_version", '0') AS TEXT) AS app_version,  -- Text (e.g., "1.2.3")
        CAST(COALESCE("contact_number", '0') AS TEXT) AS contact_number,  -- Text for country codes or separators
        CAST(COALESCE("age", 0) AS SMALLINT) AS age,  -- Small numbers (0-150)
        COALESCE("nameorig", 'Unknown') AS nameorig,
        COALESCE("namedest", 'Unknown') AS namedest,
        CASE WHEN COALESCE("isfraud", 0) = 1 THEN TRUE ELSE FALSE END AS isfraud,  -- Convert DOUBLE PRECISION to BOOLEAN
        CAST("dayofweek" AS SMALLINT) AS dayofweek,  -- Small numbers (1-7)
        CAST(COALESCE("amount", 0) AS DOUBLE PRECISION) AS amount,  -- Requires decimals
        COALESCE("merchant_category", 'Unknown') AS merchant_category,
        CAST(COALESCE("oldbalancedest", 0) AS DOUBLE PRECISION) AS oldbalancedest,  -- Requires decimals
        CAST("Hour" AS SMALLINT) AS hour,  -- Small numbers (0-23)
        COALESCE("device_type", 'Unknown') AS device_type,
        CAST(COALESCE("newbalanceorig", 0) AS DOUBLE PRECISION) AS newbalanceorig,  -- Requires decimals
        COALESCE("city", 'Unknown') AS city,
        CAST("longitude" AS DOUBLE PRECISION) AS longitude,  -- Requires decimals
        CAST("Date" AS DATE) AS date,  -- Date format
        COALESCE("customer_type", 'Unknown') AS customer_type,
        COALESCE("operating_system", 'Unknown') AS operating_system,
        CAST("Year" AS INTEGER) AS year,  -- Larger numbers (e.g., 2023)
        CAST("device_id" AS INTEGER) AS device_id,  -- Can be complex text
        COALESCE("region", 'Unknown') AS region,
        COALESCE("country", 'Unknown') AS country,
        COALESCE("merchant_name", 'Unknown') AS merchant_name,
        CAST("step" AS INTEGER) AS step,  -- Whole numbers, potentially larger
        CAST(COALESCE("oldbalanceorg", 0) AS DOUBLE PRECISION) AS oldbalanceorg,  -- Requires decimals
        COALESCE("_id", 'Unknown') AS transaction_id,
        CAST("Day" AS SMALLINT) AS day  -- Small numbers (1-31)
    FROM {{ source('raw_data', 'stgtransactions') }}
    WHERE "_id" IS NOT NULL AND "device_id" IS NOT NULL -- Remove rows where transaction_id is NULL
)
SELECT * FROM cleaned_data