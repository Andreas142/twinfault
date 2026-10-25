"""Look for rows that cannot exist on Ethereum, day by day, in one month of the dataset.

Ethereum's rules make some rows impossible: a failed transaction moves no tokens, a
transfer happens in the same block as its transaction, a block's parent is the block
before it, every transaction pays at least the block's base fee. Any such row in the
data is a bug in the source (or in our extract), never real behaviour.

The checks the extract already runs before upload (block gaps, duplicate blocks or
transfers, missing amounts or addresses, transfers without their transaction or block)
are not repeated here.

Usage:
  python scripts/impossible.py --month 2025-06 --data hf_data --out impossible
  python scripts/impossible.py --combine impossible --out checks      (after all months)

Per month it writes <out>/<month>.parquet: one row per day and check, with the rows
checked, the rows that broke the rule, and one example. --combine merges the months
into impossible_days.parquet, a summary table and report.md.
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

import duckdb
import pandas as pd

CONTRACTS = {"USDT": "0xdac17f958d2ee523a2206206994597c13d831ec7",
             "USDC": "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48",
             "PYUSD": "0x6c3ea9036406852006290770bedfcaba0e23a0e8"}

# check: (table, who would be at fault, what the rule is)
CHECKS = {
    "transfer in a failed transaction":
        ("transfers", "source", "A failed (reverted) transaction emits no events, so it cannot move tokens."),
    "transfer and its transaction in different blocks":
        ("transfers", "source", "A transfer event is emitted by its transaction, in the same block at the same time."),
    "transfer time differs from its block":
        ("transfers", "source", "Every row of a block carries that block's timestamp."),
    "negative amount":
        ("transfers", "source", "Token amounts are unsigned integers."),
    "transfer stored under the wrong day":
        ("transfers", "source", "A row's day folder must match its block's date (UTC)."),
    "repeated log index in a block":
        ("transfers", "source", "Each event in a block has its own log index."),
    "fee below the block's base fee":
        ("transactions", "source", "Since August 2021 every transaction pays at least the block's base fee."),
    "transaction time differs from its block":
        ("transactions", "source", "Every row of a block carries that block's timestamp."),
    "transaction index beyond the block's size":
        ("transactions", "source", "A transaction's position must be smaller than the block's transaction count."),
    "repeated transaction index in a block":
        ("transactions", "source", "Each transaction in a block has its own position."),
    "transaction stored under the wrong day":
        ("transactions", "source", "A row's day folder must match its block's date (UTC)."),
    "direct call labelled with the wrong token":
        ("transactions", "our extract", "direct_token must match the contract the transaction called."),
    "parent hash does not match the previous block":
        ("blocks", "source", "Each block names the hash of the block before it."),
    "block time not after the previous block":
        ("blocks", "source", "Block times strictly increase."),
    "block over its gas limit":
        ("blocks", "source", "A block cannot use more gas than its limit."),
    "block stored under the wrong day":
        ("blocks", "source", "A row's day folder must match its block's date (UTC)."),
}


def views(con: duckdb.DuckDBPyConnection, data: Path, month: str) -> None:
    for name, folder in (("tr", "stablecoin_transfers"), ("tx", "stablecoin_transactions"), ("bl", "blocks")):
        con.execute(f"create or replace view {name} as select * from "
                    f"read_parquet('{data.as_posix()}/{folder}/date={month}-*/*.parquet', hive_partitioning = false)")


def run_month(data: Path, month: str) -> pd.DataFrame:
    con = duckdb.connect()
    con.execute("set preserve_insertion_order = false")
    con.execute("set memory_limit = '10GB'")
    con.execute("set temp_directory = '.duckdb_tmp'")
    views(con, data, month)
    token_case = " ".join(f"when '{t}' then '{c}'" for t, c in CONTRACTS.items())
    parts = []

    # One pass over transfers, joined to their transaction and block.
    parts.append(con.execute("""
        select t.date, 'transfers' as tbl, count(*) as checked,
            count(*) filter (where x.receipt_status = 0) as "transfer in a failed transaction",
            min(t.transaction_hash) filter (where x.receipt_status = 0) as "ex:transfer in a failed transaction",
            count(*) filter (where x.block_number <> t.block_number or x.block_timestamp <> t.block_timestamp)
                as "transfer and its transaction in different blocks",
            min(t.transaction_hash) filter (where x.block_number <> t.block_number or x.block_timestamp <> t.block_timestamp)
                as "ex:transfer and its transaction in different blocks",
            count(*) filter (where b.timestamp is distinct from t.block_timestamp) as "transfer time differs from its block",
            min(t.transaction_hash) filter (where b.timestamp is distinct from t.block_timestamp)
                as "ex:transfer time differs from its block",
            count(*) filter (where t.amount < 0) as "negative amount",
            min(t.transaction_hash) filter (where t.amount < 0) as "ex:negative amount",
            count(*) filter (where t.block_timestamp::date <> t.date) as "transfer stored under the wrong day",
            min(t.transaction_hash) filter (where t.block_timestamp::date <> t.date) as "ex:transfer stored under the wrong day"
        from tr t
        left join tx x on x.hash = t.transaction_hash
        left join bl b on b.number = t.block_number
        group by t.date
    """).df())
    parts.append(con.execute("""
        select date, 'transfers' as tbl, sum(n - 1) as "repeated log index in a block",
               min(block_number::bigint || ':' || log_index) as "ex:repeated log index in a block"
        from (select date, block_number, log_index, count(*) as n from tr group by all having count(*) > 1)
        group by date
    """).df())

    # One pass over transactions, joined to their block.
    parts.append(con.execute(f"""
        select x.date, 'transactions' as tbl, count(*) as checked,
            count(*) filter (where x.receipt_effective_gas_price < b.base_fee_per_gas) as "fee below the block's base fee",
            min(x.hash) filter (where x.receipt_effective_gas_price < b.base_fee_per_gas) as "ex:fee below the block's base fee",
            count(*) filter (where b.timestamp is distinct from x.block_timestamp) as "transaction time differs from its block",
            min(x.hash) filter (where b.timestamp is distinct from x.block_timestamp) as "ex:transaction time differs from its block",
            count(*) filter (where x.transaction_index >= b.transaction_count) as "transaction index beyond the block's size",
            min(x.hash) filter (where x.transaction_index >= b.transaction_count) as "ex:transaction index beyond the block's size",
            count(*) filter (where x.block_timestamp::date <> x.date) as "transaction stored under the wrong day",
            min(x.hash) filter (where x.block_timestamp::date <> x.date) as "ex:transaction stored under the wrong day",
            count(*) filter (where x.direct_token is not null
                             and lower(x.to_address) <> case x.direct_token {token_case} end)
                as "direct call labelled with the wrong token",
            min(x.hash) filter (where x.direct_token is not null
                                and lower(x.to_address) <> case x.direct_token {token_case} end)
                as "ex:direct call labelled with the wrong token"
        from tx x
        left join bl b on b.number = x.block_number
        group by x.date
    """).df())
    parts.append(con.execute("""
        select date, 'transactions' as tbl, sum(n - 1) as "repeated transaction index in a block",
               min(block_number::bigint || ':' || transaction_index) as "ex:repeated transaction index in a block"
        from (select date, block_number, transaction_index, count(*) as n from tx group by all having count(*) > 1)
        group by date
    """).df())

    # One pass over blocks, in order.
    parts.append(con.execute("""
        with b as (
            select *, lag(number) over w as prev_number, lag(hash) over w as prev_hash,
                   lag(timestamp) over w as prev_time
            from bl window w as (order by number)
        )
        select date, 'blocks' as tbl, count(*) as checked,
            count(*) filter (where prev_number = number - 1 and parent_hash <> prev_hash)
                as "parent hash does not match the previous block",
            min(number::bigint::varchar) filter (where prev_number = number - 1 and parent_hash <> prev_hash)
                as "ex:parent hash does not match the previous block",
            count(*) filter (where timestamp <= prev_time) as "block time not after the previous block",
            min(number::bigint::varchar) filter (where timestamp <= prev_time) as "ex:block time not after the previous block",
            count(*) filter (where gas_used > gas_limit) as "block over its gas limit",
            min(number::bigint::varchar) filter (where gas_used > gas_limit) as "ex:block over its gas limit",
            count(*) filter (where timestamp::date <> date) as "block stored under the wrong day",
            min(number::bigint::varchar) filter (where timestamp::date <> date) as "ex:block stored under the wrong day"
        from b group by date
    """).df())

    # Rows checked per day and table, and each check's result per day (repeat checks only
    # return days that have repeats: every other day was checked and found clean).
    checked, found = {}, {}
    for part in parts:
        for rec in part.to_dict("records"):
            if "checked" in rec:
                checked[(rec["date"], rec["tbl"])] = int(rec["checked"])
            for check in CHECKS:
                if check in rec:
                    found[(rec["date"], check)] = (int(rec[check] or 0), rec.get(f"ex:{check}"))
    days = sorted({day for day, _ in checked})
    rows = []
    for check, (table, fault, _) in CHECKS.items():
        for day in days:
            violations, example = found.get((day, check), (0, None))
            rows.append({"month": month, "date": day, "check": check, "table": table, "fault": fault,
                         "checked": checked.get((day, table), 0), "violations": violations,
                         "example": example if violations else None})
    return pd.DataFrame(rows)


def save(df: pd.DataFrame, path: Path) -> None:
    con = duckdb.connect()
    con.register("df", df)
    con.execute(f"copy df to '{path.as_posix()}' (format parquet)")


def combine(folder: Path, out: Path) -> str:
    con = duckdb.connect()
    days = con.execute(f"select * from read_parquet('{folder.as_posix()}/*.parquet') order by date, \"check\"").df()
    save(days, out / "impossible_days.parquet")
    summary = con.execute("""
        select "check", any_value("table") as tbl, any_value(fault) as fault, sum(checked) as rows_checked,
               sum(violations) as violations, count(*) filter (where violations > 0) as days_affected,
               min(date) filter (where violations > 0) as first_day, max(date) filter (where violations > 0) as last_day,
               min(example) filter (where violations > 0) as example
        from days group by "check"
    """).df()
    order = {c: i for i, c in enumerate(CHECKS)}
    summary["rule"] = summary.check.map(lambda c: CHECKS[c][2])
    summary = summary.sort_values("check", key=lambda s: s.map(order)).reset_index(drop=True)
    save(summary, out / "impossible_summary.parquet")

    n_days = days.date.nunique()
    lines = ["# Rows that cannot exist on Ethereum", "",
             f"{len(CHECKS)} rules checked on every row of {n_days:,} days "
             f"({days.month.min()} to {days.month.max()}).", "",
             "| Rule | Table | Rows checked | Rows breaking it | Days affected | First | Last | Example |",
             "| --- | --- | --- | --- | --- | --- | --- | --- |"]
    for r in summary.itertuples():
        first = "" if pd.isna(r.first_day) else f"{pd.Timestamp(r.first_day):%Y-%m-%d}"
        last = "" if pd.isna(r.last_day) else f"{pd.Timestamp(r.last_day):%Y-%m-%d}"
        lines.append(f"| {r.check} | {r.tbl} | {int(r.rows_checked):,} | {int(r.violations):,} | {int(r.days_affected):,} "
                     f"| {first} | {last} | {'' if pd.isna(r.example) else r.example} |")
    broken = summary[summary.violations > 0]
    lines += ["", f"**{len(broken)} of {len(CHECKS)} rules broken.**" if len(broken)
              else "**No row breaks any rule.**", "",
              "Rules:", ""] + [f"- **{c}** ({t[1]}): {t[2]}" for c, t in CHECKS.items()]
    report = "\n".join(lines) + "\n"
    (out / "report.md").write_text(report)
    return report


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--month")
    ap.add_argument("--data", default="hf_data")
    ap.add_argument("--combine", help="Folder of per-month results to merge")
    ap.add_argument("--out", default="impossible")
    args = ap.parse_args()
    out = Path(args.out)
    out.mkdir(parents=True, exist_ok=True)
    if args.combine:
        print(combine(Path(args.combine), out))
        return 0
    if not args.month:
        ap.error("--month or --combine is required")
    df = run_month(Path(args.data), args.month)
    save(df, out / f"{args.month}.parquet")
    broken = df[df.violations > 0].groupby("check").violations.sum()
    print(f"{args.month}: {df.date.nunique()} days checked; "
          + ("no rule broken" if broken.empty else "; ".join(f"{c}: {n:,}" for c, n in broken.items())))
    return 0


if __name__ == "__main__":
    sys.exit(main())
