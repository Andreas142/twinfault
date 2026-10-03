-- One row per source table per day, as the load log recorded it.
select
    source_table,
    date,
    rows as source_rows,
    files,
    min_block,
    max_block,
    last_written,
    max_lag_s / 3600.0 as max_lag_hours
from {{ source('stablecoins', 'load_manifest') }}
where date >= cast('{{ var("month") }}-01' as date)
  and date < cast('{{ var("month") }}-01' as date) + interval 1 month
