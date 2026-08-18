{#
    Model: stg_globepay__chargeback_report
    Author: Anton Arkhipkin

    Incremental MERGE on transaction_id. Seed is still a full replace;
    we detect new / changed rows via record_hash (not a source updated_at).

    - same ID + same hash → keep dw_created_at (merge_exclude_columns) / dw_modified_at
    - same ID + different hash → keep dw_created_at, bump dw_modified_at
    - new ID → set both timestamps
    - missing ID (in this table, gone from seed) → not handled (no deletes).
      Stale rows stay until `dbt run --full-refresh`.

    record_hash business-change key: report_status, has_chargeback.
    payment_source can change without bumping dw_modified_at.

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
    select * from {{ ref('globepay_chargeback_report') }}
    )

    , renamed as (
        select
                external_ref as transaction_id
                , {{ seed_value_to_boolean('status') }} as report_status
                , source as payment_source
                , {{ seed_value_to_boolean('chargeback') }} as has_chargeback
            from source
    )

    , hashed as (
        select
            transaction_id
            , report_status
            , payment_source
            , has_chargeback
            , hash(
                coalesce(report_status::varchar, '__NULL__'),
                coalesce(has_chargeback::varchar, '__NULL__')
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
            h.transaction_id
            , h.report_status
            , h.payment_source
            , h.has_chargeback
            , h.record_hash
            -- MERGE INSERT uses this; MERGE UPDATE skips it (merge_exclude_columns)
            , {{ dw_modified_at() }} as dw_created_at
{% if is_incremental() %}
            , case
                    when t.transaction_id is null then {{ dw_modified_at() }}
                    when h.record_hash <> t.record_hash then {{ dw_modified_at() }}
                    else t.dw_modified_at
                end as dw_modified_at
  {% else %}
            , {{ dw_modified_at() }} as dw_modified_at
  {% endif %}
        from hashed as h
{% if is_incremental() %}
            left join target as t
                on h.transaction_id = t.transaction_id
  {% endif %}
