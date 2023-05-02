# Task 1 - Joining Data Sets (Python)

## Comments to the task definition
In the DDL of table `transactions` you mentioned that `transaction_amount` column has datatype `INTEGER`, when data in `transactions.csv` is in `DECIMAL` format (for example, `41.18`).

I assume that it is a type mistake in the definition of table DDL.

## Implementation

### Comment regarding additional libraries

> Your task is now to write a Python program using only the Python Standard Library1,2 (preferably not using external libraries at all)

The only additional library I decided to use - is decimal (even when it is still a standard library =) )

**The reason:** it is not the best thing to use for money amounts.

**Example:** `float('0.1') + float('0.2')`.

### Explanation of the decided approach
> Your task is now to write a Python program using only the Python Standard Library (preferably not using external libraries at all), which reads data from CSV files transactions.csv and users.csv, and computes the equivalent result of the SQL query in an efficient way that would be scalable for large data sets as well

Taking this requirement into account we are not able to read all the transactions in RAM at once. So, we will need to get the transactions one by one.

But keeping in mind performance we can't check for each transaction's user if he/she is active or not by running through `users.csv` everytime. Also, we need to calculate `COUNT(DISTINCT t.user_id)` for every `t.transaction_category_id`.

Therefore I assume that we are able to keep all the `user_id`s in the the memory. In this case the nice approach is to create a list of all active users and afterwards transform it to `set` datatype (to be able to perform `exists in` operation faster to reproduce SQL `join` operation).


Then we will need to iterate through all the transactions:
- check `t.is_blocked = False` condition
- check if transaction's `user_id` is in the `set` of active users
- for the selected transactions use the data to adjust accumulated running values

Since we need to reproduce the logic of SQL query with aggregated functions - we will need to store some running values in the list of dictionaries per each `transaction_category_id` to be able to perform the final aggregations afterwards.

The structure of the dictionary will be:
```
{
    'transaction_category_id': 'x',
    'total_amount': 0,
    'num_users': 0,
    'users_set': {}
}
```
As result, after running across all the transactions using this dictionary we will be able to calculate `SUM(t.transaction_amount) AS sum_amount` and `COUNT(DISTINCT t.user_id) AS num_users` values.

### Additional note regarding the CSV data extraction
We are working with the fixed CSV structure, so we could just refer the columns in the string by their number. But to make it more dynamic and support order/count of columns changes - we will do it a bit dynamic based on headers


## Test & run instructions
- open directory with unpacked files in Terminal
- run `docker build . -t docker_python_n26` to build image
- run `docker run docker_python_n26 "Anton_Arkhipkin.py"` to execute script





## Task 2 - Feature Table Computation
```sql
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
```

> Also elaborate (in a free text form) what happens in the database server under the hood when one executes a query. What would a database engine consider to execute your query effectively?

We are making the subquery to calculate the number of transactions the user had within the previous seven days. To perform this operation faster it would be nice to have an index on the table based on 2 columns: `USER_ID` and `DATE`. In this case database engine will not scan the whole heap of data each time, but make this faster using index's locator points.





## Task 3 - Dimension Deduplication
```sql
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
```

> Describe script logic in detail.

To avoid usage of intermediate/temporary tables, updates or deletes we will use CTEs (common table expression):
- in CTE `all_data` we just have all the raw data and generating a text column `key_factors_combination` with combination of 3 important business attributes (`client_id`, `product_id`, `interest_rate`)
- in the 2nd level CTE `with_previous_value_to_skip_the_same` - using window function `LAG` we are getting the previous (`ORDER BY ACTUAL_FROM_DT`) value of key attributes combination for the same `AGRMNT_ID`
- in the final statement we are:
    - filtering out all the redundant rows by condition `KEY_FACTORS_COMBINATION != PREVIOUS_KEY_FACTORS`
    - and also using the window function `LEAD` calculating the right value of `ACTUAL_TO_DT` to provide “smooth history” for every agreement (`agrmnt_id`) - without any gaps or intersections in row validity intervals (`actual_from_dt` - `actual_to_dt`)