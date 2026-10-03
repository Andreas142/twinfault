-- One row per stablecoin transfer in the month being built.
select
    date,
    block_timestamp,
    block_number,
    log_index,
    transaction_hash,
    token,
    from_address,
    to_address,
    amount
from {{ source('stablecoins', 'stablecoin_transfers') }}
where date >= cast('{{ var("month") }}-01' as date)
  and date < cast('{{ var("month") }}-01' as date) + interval 1 month
