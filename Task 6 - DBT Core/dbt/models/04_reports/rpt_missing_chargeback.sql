{{ config(materialized='view') }}

-- Q3: transactions missing chargeback data
select
        transaction_id,
        provider_event_id,
        transaction_at,
        transaction_state,
        country_code,
        currency_code,
        amount_usd,
        has_chargeback,
        is_missing_chargeback,
        dw_modified_at
    from {{ ref('fct_globepay_payments') }}
    where is_missing_chargeback
