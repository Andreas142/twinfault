-- One row per Ethereum block.
select
    date,
    number as block_number,
    timestamp as block_timestamp,
    transaction_count,
    gas_used,
    gas_limit,
    base_fee_per_gas / 1e9 as base_fee_gwei
from {{ source('stablecoins', 'blocks') }}
where date >= cast('{{ var("month") }}-01' as date)
  and date < cast('{{ var("month") }}-01' as date) + interval 1 month
