"""The checks run on one episode, on the raw rows of its peak day and of the week before it.

Views expected on the connection:
  payments, health, pipeline   the daily marts
  tr, tx, bl                   raw transfers, transactions and blocks of the days downloaded

Every function returns a flat dict of facts (numbers and short strings), which the
verdict rules read and the report prints. Pipeline checks come first: no behavioural
explanation is attempted on rows that are themselves in doubt.
"""
from __future__ import annotations

import math

import duckdb

LATE_HOURS = 48          # a row is late if written over 48 hours after its block
ZERO40 = "0" * 40        # the zero address: mints come from it, burns go to it
# Transfer addresses are 32-byte words, transaction addresses 20 bytes: compare the last 40 characters.


def one(con: duckdb.DuckDBPyConnection, sql: str) -> tuple:
    return con.execute(sql).fetchone()


def num(x) -> float | None:
    return None if x is None else float(x)


def in_days(days: list[str]) -> str:
    return "(" + ", ".join(f"date '{d}'" for d in days) + ")"


# ------------------------------------------------------------------ pipeline first

def pipeline_facts(con, metric: str, token: str, peak: str, baseline: list[str]) -> dict:
    """Is the peak day's data itself in doubt?"""
    f: dict = {}
    row = one(con, f"""
        select missing_blocks, duplicate_blocks,
               max_load_lag_hours / (select median(max_load_lag_hours) from pipeline)
        from pipeline where date = date '{peak}'""")
    f["missing_blocks"], f["duplicate_blocks"], f["load_lag_vs_usual"] = (int(row[0]), int(row[1]), num(row[2])) \
        if row else (None, None, None)

    last, blocks = one(con, f"select strftime(max(timestamp), '%H:%M'), count(*) from bl where date = date '{peak}'")
    f["last_block_time"], f["blocks"] = last, int(blocks or 0)
    f["day_complete"] = bool(last and last >= "23:50")

    raw, key, where = ("tx", "hash", f"direct_token = '{token}'") if metric == "failure_rate" \
        else ("tr", "transaction_hash || '-' || log_index", f"token = '{token}'")
    late = f"date_diff('second', block_timestamp::timestamp, last_modified::timestamp) > {LATE_HOURS} * 3600"
    f["pct_rows_late"], f["duplicate_rows"] = map(num, one(con, f"""
        select 100.0 * count(*) filter (where {late}) / nullif(count(*), 0), count(*) - count(distinct {key})
        from {raw} where date = date '{peak}' and {where}"""))
    f["pct_rows_late_baseline"] = num(one(con, f"""
        select median(p) from (select date, 100.0 * count(*) filter (where {late}) / count(*) as p
                               from {raw} where date in {in_days(baseline)} and {where} group by date)""")[0])
    if metric == "failure_rate":
        f["pct_failed_rows_late"] = num(one(con, f"""
            select 100.0 * count(*) filter (where {late}) / nullif(count(*), 0)
            from tx where date = date '{peak}' and {where} and receipt_status = 0""")[0])

    # Does the mart agree with the raw rows? A mismatch is a modelling fault.
    if metric == "transfers":
        raw_value = num(one(con, f"select count(*) from tr where date = date '{peak}' and token = '{token}'")[0])
        mart_value = num(one(con, f"select sum(transfers) from payments where date = date '{peak}' and token = '{token}'")[0])
    elif metric == "volume":
        raw_value = num(one(con, f"select sum(amount) from tr where date = date '{peak}' and token = '{token}'")[0])
        mart_value = num(one(con, f"select sum(volume) from payments where date = date '{peak}' and token = '{token}'")[0])
    else:
        raw_value = num(one(con, f"""select count(*) filter (where receipt_status = 0) / count(*)
                                     from tx where date = date '{peak}' and direct_token = '{token}'""")[0])
        mart_value = num(one(con, f"select failure_rate from health where date = date '{peak}' and token = '{token}'")[0])
    f["raw_value"], f["mart_value"] = raw_value, mart_value
    f["mart_matches_raw"] = (raw_value is not None and mart_value is not None
                             and abs(raw_value - mart_value) <= 1e-6 * max(abs(mart_value), 1e-9))
    return f


# ------------------------------------------------------------------ failure rate

def failure_facts(con, token: str, peak: str, baseline: list[str]) -> dict:
    """Who failed on the peak day: regular senders or new ones, one program or many."""
    con.execute(f"""
        create or replace temp table calls as
        select date, from_address as sender, receipt_status = 0 as failed, receipt_gas_used as gas
        from tx where direct_token = '{token}' and date in {in_days(baseline + [peak])}
    """)
    con.execute(f"create or replace temp table regular as select distinct sender from calls where date <> date '{peak}'")
    con.execute(f"""
        create or replace temp table peak_calls as
        select c.*, r.sender is not null as is_regular from calls c left join regular r using (sender)
        where c.date = date '{peak}'
    """)
    f: dict = {}
    (f["calls"], f["failed"], f["calls_regular"], f["failed_regular"], f["calls_new"], f["failed_new"]) = map(
        lambda x: int(x or 0), one(con, """
        select count(*), count(*) filter (where failed),
               count(*) filter (where is_regular), count(*) filter (where is_regular and failed),
               count(*) filter (where not is_regular), count(*) filter (where not is_regular and failed)
        from peak_calls"""))
    f["usual_rate"] = num(one(con, "select count(*) filter (where failed) / count(*) from calls "
                                   f"where date <> date '{peak}'")[0])
    f["rate"] = f["failed"] / f["calls"] if f["calls"] else None
    f["rate_regular"] = f["failed_regular"] / f["calls_regular"] if f["calls_regular"] else None
    f["rate_new"] = f["failed_new"] / f["calls_new"] if f["calls_new"] else None
    f["share_calls_new"] = f["calls_new"] / f["calls"] if f["calls"] else None
    f["share_failures_new"] = f["failed_new"] / f["failed"] if f["failed"] else None

    # A fingerprint: one exact gas value shared by most failed calls means one program sent them.
    gas, n = one(con, "select gas, count(*) from peak_calls where failed group by gas order by 2 desc, 1 limit 1") or (None, 0)
    f["fingerprint_gas"] = int(gas) if gas is not None else None
    f["fingerprint_share_of_failures"] = n / f["failed"] if f["failed"] else None
    if gas is not None:
        calls_fp, senders_fp, rate_without = one(con, f"""
            select count(*) filter (where gas = {gas}), count(distinct sender) filter (where gas = {gas}),
                   count(*) filter (where failed and gas <> {gas}) / nullif(count(*) filter (where gas <> {gas}), 0)
            from peak_calls""")
        f["fingerprint_calls"], f["fingerprint_senders"] = int(calls_fp), int(senders_fp)
        f["rate_without_fingerprint"] = num(rate_without)
        f["fingerprint_share_baseline"] = num(one(con, f"""
            select count(*) filter (where gas = {gas}) / nullif(count(*), 0)
            from calls where failed and date <> date '{peak}'""")[0])

    # Concentration: the senders that fail the most.
    top, rate_without_top = one(con, """
        with s as (select sender, count(*) as calls, count(*) filter (where failed) as failed
                   from peak_calls group by 1),
        r as (select *, row_number() over (order by failed desc, calls desc, sender) as rk from s)
        select sum(failed) filter (where rk <= 10) / nullif(sum(failed), 0),
               sum(failed) filter (where rk > 10) / nullif(sum(calls) filter (where rk > 10), 0)
        from r""")
    f["top10_share_of_failures"], f["rate_without_top10"] = num(top), num(rate_without_top)
    n_always, share_always, rate_without_always = one(con, """
        with s as (select sender, count(*) as calls, count(*) filter (where failed) as failed
                   from peak_calls group by 1)
        select count(*) filter (where calls >= 5 and failed >= 0.9 * calls),
               sum(failed) filter (where calls >= 5 and failed >= 0.9 * calls) / nullif(sum(failed), 0),
               sum(failed) filter (where not (calls >= 5 and failed >= 0.9 * calls))
                   / nullif(sum(calls) filter (where not (calls >= 5 and failed >= 0.9 * calls)), 0)
        from s""")
    f["always_failing_senders"], f["always_failing_share"] = int(n_always or 0), num(share_always)
    f["rate_without_always_failing"] = num(rate_without_always)

    # For a fall: did a group that used to fail stop sending?
    f["baseline_failures_from_absent_senders"] = num(one(con, f"""
        select count(*) filter (where failed and sender not in (select sender from peak_calls))
               / nullif(count(*) filter (where failed), 0)
        from calls where date <> date '{peak}'""")[0])
    return f


# ------------------------------------------------------------------ transfers

def transfer_facts(con, token: str, peak: str, baseline: list[str]) -> dict:
    """What the extra (or missing) transfers are made of."""
    con.execute(f"""
        create or replace temp table t as
        select date, from_address as sender, amount, hour(block_timestamp) as hour
        from tr where token = '{token}' and date in {in_days(baseline + [peak])}
    """)
    f: dict = {}
    nb = len(baseline)
    f["transfers"] = int(one(con, f"select count(*) from t where date = date '{peak}'")[0])
    f["baseline_avg"] = num(one(con, f"select count(*) / {nb} from t where date <> date '{peak}'")[0])

    # Dust: transfers under 1 token, the signature of address poisoning and spam.
    f["dust"], f["dust_baseline_avg"] = map(num, one(con, f"""
        select count(*) filter (where date = date '{peak}' and amount < 1),
               count(*) filter (where date <> date '{peak}' and amount < 1) / {nb} from t"""))

    # New senders: not seen in the week before. Their usual level is measured the same way
    # for the last day of that week, against the six days before it.
    last = max(baseline)
    f["new_sender_transfers"] = num(one(con, f"""
        select count(*) from t where date = date '{peak}'
          and sender not in (select sender from t where date <> date '{peak}')""")[0])
    f["new_sender_transfers_baseline"] = num(one(con, f"""
        select count(*) from t where date = date '{last}'
          and sender not in (select sender from t where date <> date '{peak}' and date <> date '{last}')""")[0]) * 7 / 6

    # Concentration: the busiest senders of the day.
    f["top10_sender_transfers"] = num(one(con, f"""
        select sum(n) from (select count(*) as n from t where date = date '{peak}' group by sender
                            order by n desc limit 10)""")[0])
    f["top10_sender_transfers_baseline"] = num(one(con, f"""
        select avg(n) from (select date, sum(n) as n from (
            select date, sender, count(*) as n, row_number() over (partition by date order by count(*) desc) as rk
            from t where date <> date '{peak}' group by date, sender) where rk <= 10 group by date)""")[0])
    # For a fall: how many transfers did the week's 50 busiest senders lose?
    f["lost_from_top50"] = num(one(con, f"""
        with b as (select sender, count(*) / {nb} as usual from t where date <> date '{peak}'
                   group by sender order by usual desc limit 50),
        p as (select sender, count(*) as n from t where date = date '{peak}' group by sender)
        select sum(b.usual - coalesce(p.n, 0)) from b left join p using (sender)""")[0])

    # Hours with no transfers at all, where the week before always had some.
    f["empty_hours"] = int(one(con, f"""
        with h as (select hour, count(*) filter (where date = date '{peak}') as peak,
                          count(*) filter (where date <> date '{peak}') / {nb} as usual
                   from t group by hour)
        select count(*) from h where peak = 0 and usual >= 10""")[0] or 0)
    return f


# ------------------------------------------------------------------ value moved

def volume_facts(con, token: str, peak: str, baseline: list[str]) -> dict:
    """What the extra (or missing) value is made of, and whether the units changed."""
    con.execute(f"""
        create or replace temp table v as
        select date, amount, hour(block_timestamp) as hour,
               right(from_address, 40) = '{ZERO40}' as mint, right(to_address, 40) = '{ZERO40}' as burn
        from tr where token = '{token}' and date in {in_days(baseline + [peak])}
    """)
    f: dict = {}
    nb = len(baseline)
    (f["volume"], f["minted"], f["burned"], f["between_holders"], f["transfers"]) = map(num, one(con, f"""
        select sum(amount), sum(amount) filter (where mint), sum(amount) filter (where burn),
               sum(amount) filter (where not mint and not burn), count(*)
        from v where date = date '{peak}'"""))
    (f["baseline_avg"], f["supply_baseline_avg"], f["between_holders_baseline_avg"],
     f["transfers_baseline_avg"]) = map(num, one(con, f"""
        select sum(amount) / {nb}, coalesce(sum(amount) filter (where mint or burn), 0) / {nb},
               sum(amount) filter (where not mint and not burn) / {nb}, count(*) / {nb}
        from v where date <> date '{peak}'"""))
    f["minted"] = f["minted"] or 0.0
    f["burned"] = f["burned"] or 0.0

    f["top10_transfer_volume"] = num(one(con, f"""
        select sum(amount) from (select amount from v where date = date '{peak}' order by amount desc limit 10)""")[0])
    f["top10_transfer_volume_baseline"] = num(one(con, f"""
        select avg(s) from (select date, sum(amount) as s from (
            select date, amount, row_number() over (partition by date order by amount desc) as rk
            from v where date <> date '{peak}') where rk <= 10 group by date)""")[0])

    # Units: if amounts are off by a power of ten, the typical transfer moves by exactly that
    # factor, from some hour on, while the number of transfers stays normal.
    med_peak, med_base = one(con, f"""
        select median(amount) filter (where date = date '{peak}'), median(amount) filter (where date <> date '{peak}')
        from v""")
    f["median_amount"], f["median_amount_baseline"] = num(med_peak), num(med_base)
    f["units_shift_power"], f["units_shift_from_hour"] = units_shift(con, peak, baseline)
    return f


def units_shift(con, peak: str, baseline: list[str]) -> tuple[int | None, int | None]:
    """Did amounts change units at some hour of the peak day?

    A units change moves every part of the distribution by the same power of ten. For each
    possible cut-over hour, the 25th, 50th and 75th percentiles after it must all be exactly
    10^k times the week before (k not 0), and before it unchanged. A real change in the mix
    of transfers moves the percentiles by different amounts, so it does not pass.
    """
    qs = "[0.25, 0.5, 0.75]"
    base = one(con, f"select quantile_cont(amount, {qs}) from v where date in {in_days(baseline)}")[0]
    if not base or min(base) <= 0:
        return None, None
    cols = ", ".join(f"quantile_cont(amount, {qs}) filter (where hour < {h}), "
                     f"quantile_cont(amount, {qs}) filter (where hour >= {h}), count(*) filter (where hour >= {h})"
                     for h in range(24))
    row = one(con, f"select count(*), {cols} from v where date = date '{peak}'")
    total, best = row[0], None

    def shifts(q):
        return [math.log10(x / b) if x and x > 0 else float("nan") for x, b in zip(q, base)]

    for h in range(24):
        head, tail, n_tail = row[1 + 3 * h: 4 + 3 * h]
        if not tail or n_tail < max(30, 0.03 * total):
            continue
        t = shifts(tail)
        k = round(sorted(t)[1])
        if k == 0 or any(not abs(x - k) < 0.2 for x in t):
            continue
        hd = shifts(head) if h else [0.0]
        if any(not abs(x) < 0.2 for x in hd):
            continue
        fit = max(abs(x - k) for x in t) + max(abs(x) for x in hd)
        if best is None or fit < best[0]:
            best = (fit, k, h)
    return (best[1], best[2]) if best else (None, None)
