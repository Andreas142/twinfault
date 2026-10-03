{{ config(location=var('output_root') ~ '/fct_daily_pipeline',
          options={'partition_by': 'month', 'write_partition_columns': true, 'overwrite_or_ignore': true}) }}

-- One row per day: is the data complete and on time?
-- This is the first table an analyst should check before explaining a metric move.
with blocks as (
    select
        date,
        count(*) as blocks,
        max(block_number) - min(block_number) + 1 - count(distinct block_number) as missing_blocks,
        count(*) - count(distinct block_number) as duplicate_blocks,
        avg(base_fee_gwei) as avg_base_fee_gwei
    from {{ ref('stg_blocks') }}
    group by date
),

transfers as (
    select date, count(*) as stablecoin_transfers
    from {{ ref('stg_transfers') }}
    group by date
),

loads as (
    select
        date,
        sum(source_rows) filter (where source_table = 'token_transfers') as source_transfer_rows,
        sum(source_rows) filter (where source_table = 'transactions') as source_transaction_rows,
        sum(files) as source_files,
        max(max_lag_hours) as max_load_lag_hours,
        max(last_written) as last_written
    from {{ ref('stg_load_manifest') }}
    group by date
)

select
    '{{ var("month") }}' as month,
    b.date,
    b.blocks,
    b.missing_blocks,
    b.duplicate_blocks,
    b.avg_base_fee_gwei,
    t.stablecoin_transfers,
    l.source_transfer_rows,
    l.source_transaction_rows,
    l.source_files,
    l.max_load_lag_hours,
    l.last_written
from blocks as b
left join transfers as t using (date)
left join loads as l using (date)
order by b.date
