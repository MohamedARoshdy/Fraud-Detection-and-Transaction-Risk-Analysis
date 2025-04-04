{{ config(materialized='table', schema='marts_DWH') }}

WITH deduplicated AS (
    SELECT
        date,
        hour,
        year,
        quarter,
        month,
        day,
        dayofweek,
        ROW_NUMBER() OVER (PARTITION BY date, hour ORDER BY date) AS rn
    FROM {{ ref('stg_cleaned_data') }}
    WHERE date IS NOT NULL AND hour IS NOT NULL
)
SELECT
    {{ dbt_utils.generate_surrogate_key(['date', 'hour']) }} AS time_sk,
    date AS date_bk,
    hour AS hour_bk,
    year,
    quarter,
    month,
    day,
    dayofweek
FROM deduplicated
WHERE rn = 1