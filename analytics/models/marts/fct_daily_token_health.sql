{{ config(location=var('output_root') ~ '/fct_daily_token_health',
          options={'partition_by': 'month', 'write_partition_columns': true, 'overwrite_or_ignore': true}) }}

-- One row per day and token, from transactions sent straight to the token contract.
-- Failed transactions emit no transfers, so this is the only place failures appear.
select
    '{{ var("month") }}' as month,
    date,
    direct_token as token,
    concat_ws('|', date, direct_token) as health_key,
    count(*) as direct_calls,
    count(*) filter (where not succeeded) as failed_calls,
    count(*) filter (where not succeeded) / count(*) as failure_rate,
    median(fee_eth) as median_fee_eth,
    sum(fee_eth) as total_fee_eth
from {{ ref('stg_transactions') }}
where direct_token is not null
group by all
order by date, token
