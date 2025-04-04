{{ config(materialized='table', schema='marts_DWH') }}

WITH deduplicated AS (
    SELECT
        device_id,
        device_type,
        manufacturer,
        operating_system,
        app_version,
        ROW_NUMBER() OVER (PARTITION BY device_id ORDER BY device_id) AS rn
    FROM {{ ref('stg_cleaned_data') }}
    WHERE device_id IS NOT NULL
)
SELECT
    {{ dbt_utils.generate_surrogate_key(['device_id']) }} AS device_sk,
    device_id AS device_bk,
    device_type,
    manufacturer,
    operating_system,
    app_version
FROM deduplicated
WHERE rn = 1