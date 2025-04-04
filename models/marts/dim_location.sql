{{ config(materialized='table', schema='marts_DWH') }}

WITH deduplicated AS (
    SELECT
        city,
        region,
        country,
        latitude,
        longitude,
        postal_code,
        ROW_NUMBER() OVER (PARTITION BY city, region, country ORDER BY city) AS rn
    FROM {{ ref('stg_cleaned_data') }}
    WHERE city IS NOT NULL AND region IS NOT NULL AND country IS NOT NULL
)
SELECT
    {{ dbt_utils.generate_surrogate_key(['city', 'region', 'country']) }} AS location_sk,
    city AS city_bk,
    region AS region_bk,
    country AS country_bk,
    latitude,
    longitude,
    postal_code
FROM deduplicated
WHERE rn = 1