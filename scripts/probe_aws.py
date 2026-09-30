"""Phase 1 probe: find out what the AWS public Ethereum data really contains.

Reads straight from the public S3 bucket (no AWS account needed) and writes a
Markdown report with:
  - row counts for the chosen days
  - stablecoin transfer counts (USDT, USDC, PYUSD)
  - whether a failure-rate metric is possible (receipt status column)
  - block continuity: gaps and duplicates, checked from the data itself
  - the schema of every table
  - how long each query took

Usage:
  python scripts/probe_aws.py --dates 2025-06-01 --out probe_results.md
  python scripts/probe_aws.py --dates "2025-06-*" --out probe_results.md
"""

import argparse
import re
import sys
import time

import duckdb

DEFAULT_BASE = "s3://aws-public-blockchain/v1.0/eth"

# Ethereum contract addresses of the stablecoins we study (lower case).
STABLECOINS = {
    "USDT": "0xdac17f958d2ee523a2206206994597c13d831ec7",
    "USDC": "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48",
    "PYUSD": "0x6c3ea9036406852006290770bedfcaba0e23a0e8",
}

# One day (2025-06-01) or a whole month (2025-06-*). Nothing else is accepted,
# so the value can never inject anything into a path or a query.
DATES_PATTERN = re.compile(r"^\d{4}-\d{2}-(\d{2}|\*)$")

TABLES = ["blocks", "transactions", "token_transfers"]


def connect(base: str) -> duckdb.DuckDBPyConnection:
    con = duckdb.connect()
    # Reading from S3 is network-bound, so more threads than cores helps.
    con.execute("SET threads = 16")
    if base.startswith("s3://"):
        con.execute("INSTALL httpfs")
        con.execute("LOAD httpfs")
        con.execute("SET s3_region = 'us-east-2'")
    return con


def source(base: str, table: str, dates: str) -> str:
    return f"read_parquet('{base}/{table}/date={dates}/*.parquet', hive_partitioning = true)"


def timed(con, sql: str, timings: list, label: str):
    start = time.perf_counter()
    rows = con.execute(sql).fetchall()
    timings.append((label, time.perf_counter() - start))
    return rows


def fmt(n) -> str:
    return f"{n:,}" if isinstance(n, int) else str(n)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--dates", default="2025-06-01", help="One day (2025-06-01) or a month (2025-06-*)")
    parser.add_argument("--base", default=DEFAULT_BASE, help="Where the data lives (default: the public AWS bucket)")
    parser.add_argument("--out", default="probe_results.md", help="Markdown report to write")
    args = parser.parse_args()

    if not DATES_PATTERN.match(args.dates):
        print(f"--dates must look like 2025-06-01 or 2025-06-*, got: {args.dates!r}", file=sys.stderr)
        return 2

    con = connect(args.base)
    timings: list = []
    errors: list = []
    report = [f"# Phase 1 probe results\n", f"Days read: `{args.dates}`  \nSource: `{args.base}`\n"]

    # 1. Schemas and row counts ------------------------------------------------
    schemas: dict = {}
    counts: dict = {}
    for table in TABLES:
        src = source(args.base, table, args.dates)
        try:
            schemas[table] = timed(con, f"DESCRIBE SELECT * FROM {src}", timings, f"{table}: schema")
            counts[table] = timed(con, f"SELECT count(*) FROM {src}", timings, f"{table}: row count")[0][0]
        except Exception as exc:  # report and carry on with the other tables
            errors.append(f"{table}: {exc}")

    report.append("## Row counts\n")
    report.append("| Table | Rows |\n| --- | --- |")
    for table in TABLES:
        report.append(f"| {table} | {fmt(counts.get(table, 'error'))} |")
    report.append("")

    def columns(table: str) -> set:
        return {row[0] for row in schemas.get(table, [])}

    addresses = ", ".join(f"'{a}'" for a in STABLECOINS.values())
    by_address = {a: name for name, a in STABLECOINS.items()}

    # 2. Stablecoin transfers --------------------------------------------------
    report.append("## Stablecoin transfers\n")
    if "token_address" in columns("token_transfers"):
        try:
            rows = timed(
                con,
                f"""
                SELECT lower(token_address) AS token, count(*) AS transfers
                FROM {source(args.base, 'token_transfers', args.dates)}
                WHERE lower(token_address) IN ({addresses})
                GROUP BY 1
                """,
                timings,
                "token_transfers: stablecoin counts",
            )
            found = {by_address[token]: n for token, n in rows}
            total = counts.get("token_transfers") or 0
            report.append("| Token | Transfers | Share of all token transfers |\n| --- | --- | --- |")
            for name in STABLECOINS:
                n = found.get(name, 0)
                share = f"{n / total:.1%}" if total else "n/a"
                report.append(f"| {name} | {fmt(n)} | {share} |")
            stable_total = sum(found.values())
            share = f"{stable_total / total:.1%}" if total else "n/a"
            report.append(f"| **All three** | **{fmt(stable_total)}** | **{share}** |\n")
        except Exception as exc:
            errors.append(f"stablecoin counts: {exc}")
            report.append("Failed; see Errors below.\n")
    else:
        report.append("No `token_address` column found; see the schema below.\n")

    # 3. Failure rate is only possible with a receipt status column ------------
    report.append("## Failure rate on stablecoin contracts\n")
    tx_cols = columns("transactions")
    if {"receipt_status", "to_address"} <= tx_cols:
        try:
            rows = timed(
                con,
                f"""
                SELECT lower(to_address) AS contract,
                       count(*) AS txs,
                       count(*) FILTER (WHERE receipt_status = 0) AS failed
                FROM {source(args.base, 'transactions', args.dates)}
                WHERE lower(to_address) IN ({addresses})
                GROUP BY 1
                """,
                timings,
                "transactions: failure rate",
            )
            report.append("`receipt_status` exists, so a failed-transaction rate is possible.\n")
            report.append("| Token contract | Transactions | Failed | Failure rate |\n| --- | --- | --- | --- |")
            for contract, txs, failed in sorted(rows, key=lambda r: by_address[r[0]]):
                rate = f"{failed / txs:.2%}" if txs else "n/a"
                report.append(f"| {by_address[contract]} | {fmt(txs)} | {fmt(failed)} | {rate} |")
            report.append("")
        except Exception as exc:
            errors.append(f"failure rate: {exc}")
            report.append("Failed; see Errors below.\n")
    else:
        missing = {"receipt_status", "to_address"} - tx_cols
        report.append(f"Not possible yet: missing column(s) {sorted(missing)} in `transactions`.\n")

    # 4. Block continuity: completeness and duplicates from the data itself ----
    report.append("## Block continuity\n")
    if "number" in columns("blocks"):
        try:
            lo, hi, n, distinct = timed(
                con,
                f"SELECT min(number), max(number), count(*), count(DISTINCT number) "
                f"FROM {source(args.base, 'blocks', args.dates)}",
                timings,
                "blocks: continuity",
            )[0]
            expected = hi - lo + 1
            report.append("| Check | Result |\n| --- | --- |")
            report.append(f"| Block range | {fmt(lo)} to {fmt(hi)} |")
            report.append(f"| Missing blocks in range | {fmt(expected - distinct)} |")
            report.append(f"| Duplicate block rows | {fmt(n - distinct)} |\n")
        except Exception as exc:
            errors.append(f"block continuity: {exc}")
            report.append("Failed; see Errors below.\n")
    else:
        report.append("No `number` column found in `blocks`.\n")

    # 5. Schemas, timings, errors ----------------------------------------------
    report.append("## Schemas\n")
    for table in TABLES:
        report.append(f"<details><summary>{table}</summary>\n")
        report.append("| Column | Type |\n| --- | --- |")
        for row in schemas.get(table, []):
            report.append(f"| {row[0]} | {row[1]} |")
        report.append("\n</details>\n")

    report.append("## Query timings\n")
    report.append("| Query | Seconds |\n| --- | --- |")
    for label, seconds in timings:
        report.append(f"| {label} | {seconds:.1f} |")
    report.append("")

    if errors:
        report.append("## Errors\n")
        report.extend(f"- {e}" for e in errors)
        report.append("")

    text = "\n".join(report)
    with open(args.out, "w", encoding="utf-8") as fh:
        fh.write(text)
    print(text)
    return 1 if errors else 0


if __name__ == "__main__":
    sys.exit(main())
