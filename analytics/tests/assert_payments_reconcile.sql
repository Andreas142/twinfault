-- Fails if, on any day, the cube's transfer total differs from the raw transfer count.
with cube as (
    select date, sum(transfers) as transfers
    from {{ ref('fct_daily_payments') }}
    group by date
),

raw as (
    select date, count(*) as transfers
    from {{ ref('stg_transfers') }}
    group by date
)

select
    coalesce(c.date, r.date) as date,
    c.transfers as cube_transfers,
    r.transfers as raw_transfers
from cube as c
full outer join raw as r on c.date = r.date
where c.transfers is distinct from r.transfers
