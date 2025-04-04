{{ config(materialized='table', schema='marts_DWH') }}

WITH deduplicated AS (
    SELECT
        namedest,
        merchant_name,
        merchant_category,
        merchant_location,
        merchant_rating,
        ROW_NUMBER() OVER (PARTITION BY namedest ORDER BY merchant_name) AS rn
    FROM {{ ref('stg_cleaned_data') }}
    WHERE namedest IS NOT NULL
)
SELECT
    {{ dbt_utils.generate_surrogate_key(['namedest']) }} AS merchant_sk,
    namedest AS merchant_bk,
    merchant_name,
    merchant_category,
    merchant_location,
    merchant_rating
FROM deduplicated
WHERE rn = 1