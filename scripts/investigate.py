"""Investigate the unusual days the website flags: rule out the pipeline, then explain the business.

Three questions, each answered from the raw rows of only the days involved:

  failures   Why did the failure rate of direct calls jump? One sender, or everyone?
             And could the pipeline have caused it?
  outliers   Is a day with an absurd value moved a data bug, or did it really happen?
  rewrites   When the source rewrote a day later, what changed, and how much would a
             dashboard built on the first version have missed?

The script downloads what it needs from the Hugging Face dataset, runs the checks in
DuckDB and writes to --out:
  report.md     every table, to read
  *.parquet     the same tables, for the website

Usage:
  python scripts/investigate.py --out investigations [--upload]
  python scripts/investigate.py --out investigations --data ./local_copy   (no download)
"""
from __future__ import annotations

import argparse
import datetime as dt
import os
import sys
import time
from pathlib import Path

import duckdb
import pandas as pd

HF_REPO = "andrew142/stablecoin-payments-eth"
ZERO = "0x0000000000000000000000000000000000000000"

# Windows to search for failure spikes. Inside each, a spike day is one where the
# token's failure rate is over 3 times its median of the previous 28 days.
FAILURE_CASES = [
    {"id": "failures_nov_2025", "title": "USDT and USDC failure spike, late November 2025",
     "start": "2025-11-20", "end": "2025-12-05", "tokens": ["USDT", "USDC"]},
    {"id": "failures_pyusd_2024", "title": "PYUSD failure spike, August to September 2024",
     "start": "2024-08-15", "end": "2024-09-30", "tokens": ["PYUSD"]},
]
SPIKE_FACTOR = 3        # a spike day: failure rate over 3 times the usual
BASELINE_DAYS = 14      # normal days compared against: the 14 before the first spike day
OUTLIER_FACTOR = 1000   # an outlier day: value moved over 1,000 times the token's median day
LATE_HOURS = 48         # a row is late if written over 48 hours after its block
REWRITE_SAMPLE = 6      # rewritten days examined row by row


# ------------------------------------------------------------------ helpers

def fetch(data: Path, patterns: list[str], offline: bool) -> None:
    """Download only the files matching the patterns (skipped with --data)."""
    if offline or not patterns:
        return
    from huggingface_hub import snapshot_download

    for attempt in range(1, 6):
        try:
            snapshot_download(HF_REPO, repo_type="dataset", local_dir=data, allow_patterns=patterns,
                              token=os.environ.get("HF_TOKEN") or None)
            return
        except Exception as exc:  # rate limits: wait and retry
            print(f"Download attempt {attempt} failed: {exc}", file=sys.stderr)
            if attempt == 5:
                raise
            time.sleep(60 * attempt)


def day_patterns(table: str, days) -> list[str]:
    return [f"{table}/date={d}/*" for d in sorted({str(d) for d in days})]


def view(con, name: str, data: Path, table: str, days=None) -> None:
    """A view over the downloaded files of one source table, limited to some days."""
    files = sorted((data / table).glob("date=*/*.parquet"))
    if days is not None:
        keep = {f"date={d}" for d in days}
        files = [f for f in files if f.parent.name in keep]
    if not files:
        con.execute(f"create or replace view {name} as select null::date as date where false")
        return
    listing = ", ".join(f"'{f.as_posix()}'" for f in files)
    con.execute(f"create or replace view {name} as select * from read_parquet([{listing}], hive_partitioning = false)")


def md_table(df: pd.DataFrame, limit: int = 40) -> str:
    """A small Markdown table, numbers formatted for reading."""
    if df.empty:
        return "_No rows._\n"

    def fmt(v):
        if isinstance(v, float):
            if pd.isna(v):
                return ""
            if abs(v) >= 1000:
                return f"{v:,.0f}"
            return f"{v:.4g}"
        if isinstance(v, (pd.Timestamp, dt.datetime)):
            return v.strftime("%Y-%m-%d" if (v.hour, v.minute) == (0, 0) else "%Y-%m-%d %H:%M")
        return str(v)

    rows = df.head(limit)
    out = "| " + " | ".join(rows.columns) + " |\n|" + "---|" * len(rows.columns) + "\n"
    for _, r in rows.iterrows():
        out += "| " + " | ".join(fmt(v) for v in r.values) + " |\n"
    if len(df) > limit:
        out += f"\n_{len(df) - limit} more rows not shown._\n"
    return out


class Report:
    """The whole report in report.md, and each section in report_<section>.md (short enough to read online)."""

    def __init__(self, out: Path, con):
        self.out = out
        self.con = con
        self.parts = ["# Investigations\n",
                      f"Generated {dt.datetime.now(dt.timezone.utc):%Y-%m-%d %H:%M} UTC "
                      f"from the Hugging Face dataset `{HF_REPO}`.\n"]
        self.sections: dict[str, list[str]] = {}
        self.current: str | None = None

    def section(self, key: str, title: str) -> None:
        self.current = key
        self.sections[key] = [f"## {title}\n"]
        self.parts.append(f"## {title}\n")

    def text(self, s: str) -> None:
        self.parts.append(s.strip() + "\n")
        if self.current:
            self.sections[self.current].append(s.strip() + "\n")

    def table(self, name: str, title: str, df: pd.DataFrame, limit: int = 40) -> pd.DataFrame:
        self.con.register("_table", df)
        self.con.execute(f"copy _table to '{(self.out / f'{name}.parquet').as_posix()}' (format parquet)")
        self.con.unregister("_table")
        body = md_table(df, limit) if limit else f"_{len(df):,} rows, saved for the website._\n"
        block = f"### {title}\n\n`{name}`\n\n{body}"
        self.parts.append(block)
        if self.current:
            self.sections[self.current].append(block)
        return df

    def save(self) -> None:
        (self.out / "report.md").write_text("\n".join(self.parts))
        for key, parts in self.sections.items():
            (self.out / f"report_{key}.md").write_text("\n".join(parts))


# ------------------------------------------------------------------ 1. failure spikes

def spike_days(con, case) -> pd.DataFrame:
    tokens = ", ".join(f"'{t}'" for t in case["tokens"])
    return con.execute(f"""
        with h as (
            select date, token, direct_calls, failed_calls, failure_rate,
                   median(failure_rate) over (partition by token order by date
                       rows between 28 preceding and 1 preceding) as usual_rate
            from health where token in ({tokens})
        )
        select date, token, direct_calls, failed_calls, failure_rate, usual_rate,
               failure_rate / nullif(usual_rate, 0) as times_usual
        from h
        where date between date '{case["start"]}' and date '{case["end"]}'
          and failure_rate > {SPIKE_FACTOR} * usual_rate and direct_calls >= 500
        order by date, token
    """).df()


def investigate_failures(con, report: Report, data: Path, offline: bool, case) -> None:
    cid = case["id"]
    report.section(cid, case["title"])
    spikes = report.table(f"{cid}_spike_days", "Spike days (failure rate over 3 times its 28-day median)",
                          spike_days(con, case))
    if spikes.empty:
        report.text("No spike day in this window.")
        return

    spike_dates = sorted(spikes.date.dt.date.unique())
    first = spike_dates[0]
    baseline = [first - dt.timedelta(days=i) for i in range(BASELINE_DAYS, 0, -1)]
    days = baseline + spike_dates
    tokens = ", ".join(f"'{t}'" for t in case["tokens"])

    report.table(f"{cid}_context", "Daily context from the marts: failures, small transfers and the network fee", con.execute(f"""
        with p as (
            select date, token, sum(transfers) as transfers,
                   sum(transfers) filter (where amount_band = 'under 10') as transfers_under_10
            from payments group by all
        )
        select h.date, h.token, h.direct_calls, h.failed_calls,
               round(100 * h.failure_rate, 3) as pct_failed,
               p.transfers, p.transfers_under_10,
               round(100.0 * p.transfers_under_10 / p.transfers, 1) as pct_transfers_under_10,
               round(pl.avg_base_fee_gwei, 3) as avg_base_fee_gwei
        from health as h
        left join p using (date, token)
        left join pipeline as pl using (date)
        where h.token in ({tokens})
          and h.date between date '{first}' - interval {BASELINE_DAYS} day and date '{case["end"]}'
        order by h.token, h.date
    """).df(), limit=80)
    fetch(data, day_patterns("stablecoin_transactions", days), offline)
    view(con, "tx_all", data, "stablecoin_transactions", days)

    spike_list = ", ".join(f"date '{d}'" for d in spike_dates)
    con.execute(f"""
        create or replace temp table tx as
        select date, block_timestamp, block_number, hash, from_address as sender, direct_token as token,
               receipt_status = 1 as succeeded, receipt_gas_used as gas_used,
               receipt_effective_gas_price / 1e9 as gas_price_gwei,
               date_diff('second', block_timestamp::timestamp, last_modified::timestamp) > {LATE_HOURS} * 3600 as late,
               date in ({spike_list}) as spike_day
        from tx_all where direct_token in ({tokens})
    """)

    # Pipeline first: is anything wrong with these rows themselves?
    report.table(f"{cid}_pipeline", "Pipeline checks (spike days and baseline)", con.execute("""
        with rows as (
            select date, spike_day,
                   count(*) as direct_calls,
                   count(*) - count(distinct hash) as duplicate_hashes,
                   round(100.0 * count(*) filter (where late) / count(*), 2) as pct_rows_late,
                   round(100.0 * count(*) filter (where late and not succeeded)
                         / nullif(count(*) filter (where not succeeded), 0), 2) as pct_failed_rows_late
            from tx group by all
        )
        select r.*, p.blocks, p.missing_blocks, p.duplicate_blocks,
               round(p.max_load_lag_hours, 1) as max_load_lag_hours
        from rows as r left join pipeline as p using (date)
        order by date
    """).df())

    # Who failed? Each sender's calls and failures per day.
    con.execute("""
        create or replace temp table senders as
        select date, token, spike_day, sender, count(*) as calls,
               count(*) filter (where not succeeded) as failed
        from tx group by all
    """)
    con.execute("""
        create or replace temp table ranked as
        select *, row_number() over (partition by date, token order by failed desc, calls desc) as rank
        from senders
    """)
    report.table(f"{cid}_concentration", "Failure rate with and without the top failing senders", con.execute("""
        select date, token, spike_day,
               sum(calls) as calls, sum(failed) as failed,
               round(100.0 * sum(failed) / sum(calls), 3) as pct_failed,
               round(100.0 * max(failed) filter (where rank = 1) / nullif(sum(failed), 0), 1) as pct_failures_from_top_sender,
               round(100.0 * sum(failed) filter (where rank <= 10) / nullif(sum(failed), 0), 1) as pct_failures_from_top_10,
               round(100.0 * sum(failed) filter (where rank > 1) / sum(calls) filter (where rank > 1), 3) as pct_failed_without_top_sender,
               round(100.0 * sum(failed) filter (where rank > 10) / sum(calls) filter (where rank > 10), 3) as pct_failed_without_top_10,
               count(*) filter (where failed > 0) as failing_senders
        from ranked group by all order by token, date
    """).df(), limit=60)

    report.table(f"{cid}_always_failing", "Senders that almost always fail (5+ calls, 90%+ failed) vs everyone else", con.execute("""
        with s as (select *, calls >= 5 and failed >= 0.9 * calls as always_fails from senders)
        select date, token, spike_day,
               count(*) filter (where always_fails) as always_failing_senders,
               coalesce(sum(failed) filter (where always_fails), 0) as their_failed_calls,
               round(100.0 * coalesce(sum(failed) filter (where always_fails), 0) / nullif(sum(failed), 0), 1) as pct_failures_from_them,
               round(100.0 * sum(failed) filter (where not always_fails) / sum(calls) filter (where not always_fails), 3) as pct_failed_everyone_else
        from s group by all order by token, date
    """).df(), limit=80)

    report.table(f"{cid}_sender_profile", "The failing senders: new or known, one call or many, did they also succeed", con.execute("""
        with known as (select distinct token, sender from senders where not spike_day)
        select s.date, s.token, s.spike_day,
               count(*) filter (where s.failed > 0) as failing_senders,
               round(100.0 * count(*) filter (where s.failed > 0 and k.sender is null)
                     / nullif(count(*) filter (where s.failed > 0), 0), 1) as pct_not_seen_on_baseline_days,
               round(100.0 * count(*) filter (where s.failed > 0 and s.calls = 1)
                     / nullif(count(*) filter (where s.failed > 0), 0), 1) as pct_with_a_single_call,
               round(100.0 * count(*) filter (where s.failed > 0 and s.failed < s.calls)
                     / nullif(count(*) filter (where s.failed > 0), 0), 1) as pct_that_also_succeeded,
               median(s.calls) filter (where s.failed > 0) as median_calls_per_failing_sender
        from senders as s left join known as k on k.token = s.token and k.sender = s.sender
        group by all order by s.token, s.date
    """).df(), limit=80)

    report.table(f"{cid}_failed_gas_modes", "Most common gas used by failed calls (a fixed value suggests a fixed gas limit)", con.execute("""
        select * from (
            select token, spike_day, gas_used, count(*) as failed_calls,
                   round(100.0 * count(*) / sum(count(*)) over (partition by token, spike_day), 1) as pct_of_failed,
                   row_number() over (partition by token, spike_day order by count(*) desc) as rank
            from tx where not succeeded group by token, spike_day, gas_used
        ) where rank <= 5
        order by token, spike_day, failed_calls desc
    """).df())

    report.table(f"{cid}_top_senders", "Top 5 failing senders on each spike day", con.execute("""
        select date, token, rank, sender, calls, failed,
               round(100.0 * failed / calls, 1) as pct_of_own_calls_failed
        from ranked where spike_day and rank <= 5 and failed > 0
        order by date, token, rank
    """).df())

    report.table(f"{cid}_repeat_senders", "Senders in the top 5 on more than one spike day", con.execute("""
        select sender, count(distinct date) as spike_days_in_top_5, sum(failed) as failed, sum(calls) as calls
        from ranked where spike_day and rank <= 5 and failed > 0
        group by sender having count(distinct date) > 1
        order by failed desc
    """).df())

    report.table(f"{cid}_top_sender_history", "The biggest failing sender's calls on every day examined", con.execute("""
        with top as (
            select sender from ranked where spike_day and rank = 1
            group by sender order by sum(failed) desc limit 1
        )
        select s.date, s.token, s.spike_day, s.calls, s.failed
        from senders as s join top using (sender)
        order by s.date, s.token
    """).df(), limit=60)

    report.table(f"{cid}_hourly", "Failed and total direct calls by hour, spike days", con.execute("""
        select date, token, hour(block_timestamp) as hour_utc, count(*) as calls,
               count(*) filter (where not succeeded) as failed
        from tx where spike_day group by all order by date, token, hour_utc
    """).df(), limit=48)

    report.table(f"{cid}_gas", "Gas used and price, failed vs succeeded", con.execute("""
        select token, spike_day, succeeded, count(*) as calls,
               median(gas_used) as median_gas_used,
               round(median(gas_price_gwei), 3) as median_gas_price_gwei,
               round(quantile_cont(gas_price_gwei, 0.9), 3) as p90_gas_price_gwei
        from tx group by all order by token, spike_day, succeeded
    """).df())


# ------------------------------------------------------------------ 2. value outliers

def investigate_outliers(con, report: Report, data: Path, offline: bool) -> None:
    report.section("outliers", "Days with an absurd value moved")
    outliers = report.table("outlier_days", f"Token-days with value moved over {OUTLIER_FACTOR:,} times the token's median day",
                            con.execute(f"""
        with daily as (select date, token, sum(volume) as volume from payments group by all)
        select date, token, volume, volume / median(volume) over (partition by token) as times_median
        from daily
        qualify volume > {OUTLIER_FACTOR} * median(volume) over (partition by token)
        order by date
    """).df())
    if outliers.empty:
        return

    days = sorted(outliers.date.dt.date.unique())
    fetch(data, day_patterns("stablecoin_transfers", days), offline)
    view(con, "transfers_out", data, "stablecoin_transfers", days)
    pairs = " or ".join(f"(date = date '{d.date()}' and token = '{t}')" for d, t in zip(outliers.date, outliers.token))

    report.table("outlier_mint_burn", "Mints (from the zero address) and burns (to it) over 1 million tokens", con.execute(f"""
        select block_timestamp, block_number, token,
               case when from_address = '{ZERO}' then 'mint' else 'burn' end as kind,
               amount, transaction_hash,
               case when from_address = '{ZERO}' then to_address else from_address end as counterparty
        from transfers_out
        where ({pairs}) and (from_address = '{ZERO}' or to_address = '{ZERO}') and amount >= 1e6
        order by block_timestamp, block_number
    """).df())

    report.table("outlier_summary", "What the day's value moved is made of", con.execute(f"""
        select date, token, count(*) as transfers, sum(amount) as value_moved,
               sum(amount) filter (where from_address = '{ZERO}') as minted,
               sum(amount) filter (where to_address = '{ZERO}') as burned,
               sum(amount) filter (where from_address <> '{ZERO}' and to_address <> '{ZERO}') as moved_between_holders,
               max(amount) as largest_transfer
        from transfers_out where {pairs}
        group by all order by date
    """).df())

    report.table("outlier_largest", "The 10 largest transfers on each outlier day", con.execute(f"""
        select * from (
            select date, token, block_timestamp, amount, from_address, to_address, transaction_hash,
                   row_number() over (partition by date, token order by amount desc) as rank
            from transfers_out where {pairs}
        ) where rank <= 10 order by date, token, rank
    """).df())


# ------------------------------------------------------------------ 3. rewritten days

def investigate_rewrites(con, report: Report, data: Path, offline: bool) -> None:
    report.section("rewrites", "Days the source rewrote later")
    days = report.table("rewrite_days", "Each source table, each day: when it was first and last written", con.execute("""
        with m as (
            select source_table, date, rows, files,
                   date_diff('minute', date::timestamp + interval 1 day, first_written::timestamp) / 60.0 as first_written_hours_after_day,
                   date_diff('minute', date::timestamp + interval 1 day, last_written::timestamp) / 60.0 as last_written_hours_after_day,
                   max_lag_s / 3600.0 as max_lag_hours
            from manifest
        ),
        usual as (select source_table, median(max_lag_hours) as usual_max_lag from m group by all)
        select m.*, case
                   when max_lag_hours <= 2 * usual_max_lag then 'normal'
                   when first_written_hours_after_day > 2 * usual_max_lag then 'whole day written late'
                   else 'written on time, then patched'
               end as pattern
        from m join usual using (source_table)
        order by source_table, date
    """).df(), limit=0)

    con.register("days_df", days)
    report.table("rewrite_patterns", "How many days follow each pattern", con.execute("""
        select source_table, pattern, count(*) as days,
               round(median(last_written_hours_after_day), 1) as median_hours_until_last_write,
               round(max(last_written_hours_after_day), 1) as max_hours_until_last_write
        from days_df group by all order by source_table, pattern
    """).df())

    flagged = days[(days.source_table == "token_transfers") & (days.pattern != "normal")]
    report.table("rewrite_flagged", "Token-transfer days that were late or patched", flagged.drop(columns=["source_table"]), limit=70)
    if flagged.empty:
        return

    # Row by row on the worst days: what share of rows arrived late, and where were they?
    sample = flagged.sort_values("max_lag_hours", ascending=False).head(REWRITE_SAMPLE)
    sample_days = sorted(sample.date.dt.date.unique())
    fetch(data, day_patterns("stablecoin_transfers", sample_days) + day_patterns("stablecoin_transactions", sample_days), offline)
    view(con, "transfers_rw", data, "stablecoin_transfers", sample_days)
    view(con, "tx_rw", data, "stablecoin_transactions", sample_days)

    report.table("rewrite_rows", "Stablecoin transfers on the worst days: how many arrived late", con.execute(f"""
        with t as (
            select date, token, amount, block_number,
                   date_diff('second', block_timestamp::timestamp, last_modified::timestamp) > {LATE_HOURS} * 3600 as late
            from transfers_rw
        )
        select date, token, count(*) as transfers,
               count(*) filter (where late) as late_transfers,
               round(100.0 * count(*) filter (where late) / count(*), 2) as pct_late,
               sum(amount) filter (where late) as late_value,
               round(100.0 * sum(amount) filter (where late) / sum(amount), 2) as pct_value_late
        from t group by all order by date, token
    """).df())

    report.table("rewrite_blocks", "Where the late rows are: block range and how many blocks they touch", con.execute(f"""
        with t as (
            select date, block_number,
                   date_diff('second', block_timestamp::timestamp, last_modified::timestamp) > {LATE_HOURS} * 3600 as late
            from transfers_rw
        ),
        b as (
            select date, block_number, bool_and(late) as all_late, bool_or(late) as any_late from t group by all
        )
        select date,
               count(*) as blocks_with_transfers,
               count(*) filter (where any_late) as blocks_with_late_rows,
               count(*) filter (where all_late) as blocks_entirely_late,
               min(block_number) filter (where any_late) as first_late_block,
               max(block_number) filter (where any_late) as last_late_block
        from b group by all order by date
    """).df())

    report.table("rewrite_write_times", "When the rows of the worst days were written", con.execute("""
        select date, date_trunc('hour', last_modified) as written_hour, count(*) as transfers
        from transfers_rw group by all order by date, written_hour
    """).df(), limit=120)

    report.table("rewrite_tx_rows", "Their transactions: share late, failed and succeeded", con.execute(f"""
        select date, receipt_status = 1 as succeeded, count(*) as transactions,
               round(100.0 * count(*) filter (where date_diff('second', block_timestamp::timestamp, last_modified::timestamp) > {LATE_HOURS} * 3600)
                     / count(*), 2) as pct_late
        from tx_rw group by all order by date, succeeded
    """).df())


# ------------------------------------------------------------------ main

def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--out", default="investigations")
    parser.add_argument("--data", help="use a local copy of the dataset instead of downloading")
    parser.add_argument("--upload", action="store_true", help="upload --out to investigations/ on Hugging Face")
    args = parser.parse_args()

    out = Path(args.out)
    out.mkdir(parents=True, exist_ok=True)
    offline = bool(args.data)
    data = Path(args.data or "hf_data")

    # The marts and load logs are small: take them whole.
    fetch(data, ["marts/fct_daily_token_health/*", "marts/fct_daily_payments/*", "marts/fct_daily_pipeline/*",
                 "load_manifest/*"], offline)

    con = duckdb.connect()
    con.execute("set memory_limit = '12GB'")
    con.execute("set TimeZone = 'UTC'")
    con.execute(f"create view health as select * from read_parquet('{(data / 'marts/fct_daily_token_health').as_posix()}/*/*.parquet', hive_partitioning = false)")
    con.execute(f"create view payments as select * from read_parquet('{(data / 'marts/fct_daily_payments').as_posix()}/*/*.parquet', hive_partitioning = false)")
    con.execute(f"create view pipeline as select * from read_parquet('{(data / 'marts/fct_daily_pipeline').as_posix()}/*/*.parquet', hive_partitioning = false)")
    con.execute(f"create view manifest as select * from read_parquet('{(data / 'load_manifest').as_posix()}/*/*.parquet', hive_partitioning = false)")

    report = Report(out, con)
    for case in FAILURE_CASES:
        investigate_failures(con, report, data, offline, case)
    investigate_outliers(con, report, data, offline)
    investigate_rewrites(con, report, data, offline)
    report.save()
    print(f"Wrote {out / 'report.md'} and {len(list(out.glob('*.parquet')))} tables")

    if args.upload:
        from huggingface_hub import HfApi

        api = HfApi(token=os.environ["HF_TOKEN"])
        for attempt in range(1, 6):
            try:
                api.upload_folder(repo_id=HF_REPO, repo_type="dataset", folder_path=out,
                                  path_in_repo="investigations", delete_patterns="*",
                                  commit_message="Investigations")
                print("Uploaded to investigations/")
                break
            except Exception as exc:
                print(f"Upload attempt {attempt} failed: {exc}", file=sys.stderr)
                if attempt == 5:
                    return 1
                time.sleep(30 * attempt)
    return 0


if __name__ == "__main__":
    sys.exit(main())
