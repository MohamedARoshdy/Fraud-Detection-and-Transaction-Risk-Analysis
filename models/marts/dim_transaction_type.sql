{{ config(materialized='table', schema='marts_DWH') }}

WITH deduplicated AS (
    SELECT
        payment_channel,
        type,
        ROW_NUMBER() OVER (PARTITION BY payment_channel, type ORDER BY payment_channel) AS rn
    FROM {{ ref('stg_cleaned_data') }}
    WHERE payment_channel IS NOT NULL AND type IS NOT NULL
)
SELECT
    {{ dbt_utils.generate_surrogate_key(['payment_channel', 'type']) }} AS transaction_type_sk,
    payment_channel AS payment_channel_bk,
    type AS type_bk
FROM deduplicated
WHERE rn = 1