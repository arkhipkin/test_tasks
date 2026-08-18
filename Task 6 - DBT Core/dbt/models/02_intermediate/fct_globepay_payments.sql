{#
    Model: fct_globepay_payments
    Author: Anton Arkhipkin

    Incremental MERGE on transaction_id.
    Watermark = max(this.source_dw_modified_at) vs staging dw_modified_at.
    Re-process IDs where either staging stamp is newer, then re-read the full
    current row from both staging tables (do not watermark-filter the join
    sources — that would drop the unchanged side and fake missing CB).

    - source_dw_modified_at = greatest(acceptance, chargeback) dw_modified_at
    (same clock domain as staging — incremental filter uses this).
    - dw_modified_at = now() (fact write audit).
    - dw_created_at: SELECT always now(); MERGE UPDATE skips it
    (merge_exclude_columns).

    - missing ID (gone from staging) → not handled (no deletes).
      Stale fact rows stay until `dbt run --full-refresh`.

    See README: `dw_modified_at` + incremental staging / `backfill_days`.

        Partial history replay without --full-refresh (project var `backfill_days`,
        default 0 = watermark only). Acceptance-driven: chargeback has no
        transaction_date; those IDs still pick up the full CB row in the join.
            dbt run --select fct_globepay_payments --vars 'backfill_days: 720'
            dbt build --select fct_globepay_payments+ --vars '{"backfill_days":365}'
#}

{{
    config(
        materialized='incremental',
        unique_key='transaction_id',
        incremental_strategy='merge',
        merge_exclude_columns=['dw_created_at']
    )
}}

  with
{% if is_incremental() %}
    changed as (
        select transaction_id
            from {{ ref('stg_globepay__acceptance_report') }}
            where
                -- technical watermark: process rows where staging dw_modified_at is newer than the watermark
                dw_modified_at > (
                    select coalesce(max(source_dw_modified_at), '1900-01-01'::timestamp_ntz)
                        from {{ this }}
                    )
                or 
                -- custom backfilling logic for partial historical processing (using backfill_days variable)
                -- business window (opt-in): last N calendar days by transaction_date
                -- cheaper than --full-refresh when only recent history must be replayed
                transaction_date >= (current_date - {{ var('backfill_days') }})
        union
        select transaction_id
            from {{ ref('stg_globepay__chargeback_report') }}
            where dw_modified_at > (
                select coalesce(max(source_dw_modified_at), '1900-01-01'::timestamp_ntz)
                    from {{ this }}
                )
        )
        , 
  {% endif %}
        acceptance as (
            select *
                from {{ ref('stg_globepay__acceptance_report') }}
{% if is_incremental() %}
                where transaction_id in (select transaction_id from changed)
  {% endif %}
        )

    , chargeback as (
        select *
            from {{ ref('stg_globepay__chargeback_report') }}
{% if is_incremental() %}
                where transaction_id in (select transaction_id from changed)
  {% endif %}
        )

    , joined as (
        select
                a.transaction_id
                , a.provider_event_id
                , a.payment_source
                , a.transaction_at
                , a.transaction_date
                , a.transaction_state
                , (a.transaction_state = 'ACCEPTED') as is_accepted
                , (a.transaction_state = 'DECLINED') as is_declined
                , a.country_code
                , a.currency_code
                , a.amount
                , a.amount_usd
                , a.is_cvv_provided
                , c.has_chargeback
                , (c.transaction_id is null or c.has_chargeback is null) as is_missing_chargeback
                , a.report_status as acceptance_report_status
                , c.report_status as chargeback_report_status
                , a.rates
                , iff(
                    c.dw_modified_at is null,
                    a.dw_modified_at,
                    greatest(a.dw_modified_at, c.dw_modified_at)
                ) as source_dw_modified_at
            from acceptance as a
                left join chargeback as c
                    on a.transaction_id = c.transaction_id
    )

    select
            j.*
            -- MERGE INSERT uses this; MERGE UPDATE skips it (merge_exclude_columns)
            , {{ dw_modified_at() }} as dw_created_at
            -- fact write time (not the incremental watermark)
            , {{ dw_modified_at() }} as dw_modified_at
        from joined as j
