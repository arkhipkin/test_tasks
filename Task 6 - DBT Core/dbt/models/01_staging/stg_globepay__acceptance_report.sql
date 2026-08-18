{#
    Model: stg_globepay__acceptance_report
    Author: Anton Arkhipkin

    Incremental MERGE on transaction_id. Seed is still a full replace;
    we detect new / changed rows via record_hash (not a source updated_at).

    - same ID + same hash → keep dw_created_at (merge_exclude_columns) / dw_modified_at
    - same ID + different hash → keep dw_created_at, bump dw_modified_at
    - new ID → set both timestamps
    - missing ID (in this table, gone from seed) → not handled (no deletes).
      Stale rows stay until `dbt run --full-refresh`.

    record_hash business-change key: report_status, transaction_state,
    provider_event_id. Other columns can change without bumping dw_modified_at.

    See README: `dw_modified_at` + incremental staging.
#}

{{
  config(
    materialized='incremental',
    unique_key='transaction_id',
    incremental_strategy='merge',
    merge_exclude_columns=['dw_created_at']
  )
}}

with source as (
        select * from {{ ref('globepay_acceptance_report') }}
    )

    , renamed as (
        select
                external_ref as transaction_id
                , {{ seed_value_to_boolean('status') }} as report_status
                , source as payment_source
                , ref as provider_event_id
                , date_time::timestamp_ntz as transaction_at
                , upper(trim(state)) as transaction_state
                , {{ seed_value_to_boolean('cvv_provided') }} as is_cvv_provided
                , try_to_double(amount) as amount
                , upper(trim(country)) as country_code
                , upper(trim(currency)) as currency_code
                , try_parse_json(rates) as rates
            from source
    )

    , enriched as (
        select
                transaction_id
                , report_status
                , payment_source
                , provider_event_id
                , transaction_at
                , cast(transaction_at as date) as transaction_date
                , transaction_state
                , is_cvv_provided
                , amount
                , country_code
                , currency_code
                , rates
                -- rates values are units of currency per 1 USD (USD=1) → settle amount in USD
                , case
                    when currency_code = 'USD' then amount
                    when rates is null or rates[currency_code] is null then null
                    when try_to_double(rates[currency_code]::varchar) = 0 then null
                    else amount / try_to_double(rates[currency_code]::varchar)
                end as amount_usd
                , hash(
                    coalesce(report_status::varchar, '__NULL__'),
                    coalesce(transaction_state, '__NULL__'),
                    coalesce(provider_event_id, '__NULL__')
                ) as record_hash
            from renamed
    )

{% if is_incremental() %}
    , target as (
        select
                transaction_id
                , dw_modified_at
                , record_hash
            from {{ this }}
    )
  {% endif %}

    select
            e.transaction_id
            , e.report_status
            , e.payment_source
            , e.provider_event_id
            , e.transaction_at
            , e.transaction_date
            , e.transaction_state
            , e.is_cvv_provided
            , e.amount
            , e.country_code
            , e.currency_code
            , e.rates
            , e.amount_usd
            , e.record_hash
            -- MERGE INSERT uses this; MERGE UPDATE skips it (merge_exclude_columns)
            , {{ dw_modified_at() }} as dw_created_at
{% if is_incremental() %}
            , case
                when t.transaction_id is null then {{ dw_modified_at() }}
                when e.record_hash <> t.record_hash then {{ dw_modified_at() }}
                else t.dw_modified_at
            end as dw_modified_at
  {% else %}
            , {{ dw_modified_at() }} as dw_modified_at
  {% endif %}
    from enriched as e
{% if is_incremental() %}
        left join target as t
            on e.transaction_id = t.transaction_id
  {% endif %}
