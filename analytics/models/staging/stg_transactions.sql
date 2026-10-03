-- One row per transaction that moved a stablecoin or called a token contract.
select
    date,
    block_timestamp,
    block_number,
    hash,
    from_address,
    to_address,
    direct_token,
    receipt_status = 1 as succeeded,
    cast(receipt_gas_used as double) * receipt_effective_gas_price / 1e18 as fee_eth,
    transaction_type
from {{ source('stablecoins', 'stablecoin_transactions') }}
where date >= cast('{{ var("month") }}-01' as date)
  and date < cast('{{ var("month") }}-01' as date) + interval 1 month
