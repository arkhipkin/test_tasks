{{ config(materialized='view') }}

-- Q1: acceptance rate over time (daily)
select
    transaction_date,
    count_if(is_accepted) as accepted_count,
    count(*) as total_count,
    count_if(is_accepted) / nullif(count(*), 0) as acceptance_rate,
    max(dw_modified_at) as dw_modified_at
from {{ ref('fct_globepay_payments') }}
group by 1
