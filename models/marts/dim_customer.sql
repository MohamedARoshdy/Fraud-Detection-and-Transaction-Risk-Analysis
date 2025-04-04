{{ config(materialized='table', schema='marts_DWH') }}

WITH deduplicated AS (
    SELECT
        nameorig,
        full_name,
        contact_number,
        account_creation_date,
        customer_type,
        gender,
        age,
        city,
        region,
        country,
        ROW_NUMBER() OVER (PARTITION BY nameorig ORDER BY account_creation_date DESC) AS rn
    FROM {{ ref('stg_cleaned_data') }}
    WHERE nameorig IS NOT NULL
)
SELECT
    {{ dbt_utils.generate_surrogate_key(['nameorig']) }} AS customer_sk,
    nameorig AS customer_bk,
    full_name,
    contact_number,
    account_creation_date,
    customer_type,
    gender,
    age,
    city,
    region,
    country
FROM deduplicated
WHERE rn = 1