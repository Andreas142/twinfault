{{ config(location=var('output_root') ~ '/fct_daily_payments',
          options={'partition_by': 'month', 'write_partition_columns': true, 'overwrite_or_ignore': true}) }}

-- The daily cube: one row per day, token, amount band and route.
-- Unique counts are approximate (HyperLogLog) and only valid within a row.
with transfers as (
    select * from {{ ref('stg_transfers') }}
),

transactions as (
    select hash, direct_token from {{ ref('stg_transactions') }}
),

labelled as (
    select
        t.date,
        t.token,
        case
            when t.amount < 10 then 'under 10'
            when t.amount < 1000 then '10 to 1k'
            when t.amount < 100000 then '1k to 100k'
            else '100k and over'
        end as amount_band,
        case
            when t.amount < 10 then 1
            when t.amount < 1000 then 2
            when t.amount < 100000 then 3
            else 4
        end as amount_band_order,
        case when x.direct_token = t.token then 'direct' else 'via contract' end as route,
        t.amount,
        t.from_address,
        t.to_address,
        t.transaction_hash
    from transfers as t
    left join transactions as x on x.hash = t.transaction_hash
)

select
    '{{ var("month") }}' as month,
    date,
    token,
    amount_band,
    amount_band_order,
    route,
    concat_ws('|', date, token, amount_band, route) as cube_key,
    count(*) as transfers,
    sum(amount) as volume,
    approx_count_distinct(transaction_hash) as transactions,
    approx_count_distinct(from_address) as senders,
    approx_count_distinct(to_address) as receivers
from labelled
group by all
order by date, token, amount_band_order, route
