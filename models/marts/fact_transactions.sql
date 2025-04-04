{{ config(materialized='table', schema='marts_DWH') }}

SELECT
    {{ dbt_utils.generate_surrogate_key(['transaction_id']) }} AS transaction_sk,
    {{ dbt_utils.generate_surrogate_key(['nameorig']) }} AS customer_sk,
    {{ dbt_utils.generate_surrogate_key(['namedest']) }} AS merchant_sk,
    {{ dbt_utils.generate_surrogate_key(['date', 'hour']) }} AS time_sk,
    {{ dbt_utils.generate_surrogate_key(['device_id']) }} AS device_sk,
    {{ dbt_utils.generate_surrogate_key(['city', 'region', 'country']) }} AS location_sk,
    {{ dbt_utils.generate_surrogate_key(['payment_channel', 'type']) }} AS transaction_type_sk,
    st.amount AS transaction_amount,
    st.oldbalanceorg,
    st.newbalanceorig,
    st.oldbalancedest,
    st.newbalancedest,
    st.isfraud,
    st.isflaggedfraud,
    st.step,
    st.transaction_id AS transaction_bk
FROM {{ ref('stg_cleaned_data') }} st