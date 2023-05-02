/***** Task 2 - Feature Table Computation *****/
select 
        TRANSACTION_ID
        , USER_ID
        , DATE
        , (select count(*)
            from transactions t2
                where t2.USER_ID = t1.USER_ID
                    and t2.DATE between 
                            dateadd(day, -7, t1.DATE) 
                            and dateadd(day, -1, t1.DATE)
        ) trans_prev_7d
    from transactions t1
    ;
    
    
/***** Task 3 - Dimension Deduplication *****/
create or replace table dim_dep_agreement_compacted as
    with 
        all_data as (
            select *
                    , coalesce(client_id::varchar,'') 
                        || '/' || coalesce(product_id::varchar,'') 
                        || '/' || coalesce(interest_rate::varchar,'') 
                      as key_factors_combination
                from agreement
        ) 
        , with_previous_value_to_skip_the_same as (
            select *
                    , lag(key_factors_combination, 1, 'FIRST')
                        OVER (
                            Partition by AGRMNT_ID
                            ORDER BY ACTUAL_FROM_DT
                            )
                        as previous_key_factors
                from all_data
        )
        select 
                AGRMNT_ID
                , ACTUAL_FROM_DT
                , lead(DATEADD(day, -1, ACTUAL_FROM_DT), 1, '9999-12-31'::date) 
                        OVER (
                            Partition by AGRMNT_ID
                            ORDER BY ACTUAL_FROM_DT
                            )
                    as ACTUAL_TO_DT
                , CLIENT_ID
                , PRODUCT_ID
                , INTEREST_RATE
            from with_previous_value_to_skip_the_same
            where
                -- merge intervals where values are the same
                KEY_FACTORS_COMBINATION != PREVIOUS_KEY_FACTORS
    ;     