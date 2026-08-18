{{ config(materialized='view') }}

-- Q2: countries where declined amount (USD) exceeded $25M
with declined_by_country as (
    select
            country_code,
            count(*) as declined_transaction_count,
            sum(amount_usd) as declined_amount_usd,
            max(dw_modified_at) as dw_modified_at
        from {{ ref('fct_globepay_payments') }}
        where is_declined
        group by 1
    )
    select *
        from declined_by_country
        where declined_amount_usd > 25000000
