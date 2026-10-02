"""Phase 1 extract: pull one month of stablecoin payment data into compact Parquet.

Reads the AWS public Ethereum data (no AWS account needed) and writes four outputs:

  stablecoin_transfers/     every USDT, USDC and PYUSD transfer, one folder per day
  stablecoin_transactions/  the transactions behind those transfers, plus every
                            transaction sent straight to a stablecoin contract.
                            Reverted transactions emit no transfer events, so the
                            failure rate can only come from this table.
  blocks/                   every block in the month, for completeness checks
  load_manifest.parquet     what a warehouse load log would show: rows, files,
                            block range and write-time lag per source table and day

It then runs data-quality checks, measures the size of every output, and writes a
Markdown report with a size projection for the full history.

Usage:
  python scripts/extract_month.py --month 2025-06 --out-dir data --report extract_results.md
"""

import argparse
import calendar
import os
import re
import shutil
import sys
import time

import duckdb

DEFAULT_BASE = "s3://aws-public-blockchain/v1.0/eth"

# Ethereum contract addresses (lower case). All three tokens use 6 decimals.
STABLECOINS = {
    "USDT": "0xdac17f958d2ee523a2206206994597c13d831ec7",
    "USDC": "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48",
    "PYUSD": "0x6c3ea9036406852006290770bedfcaba0e23a0e8",
}
DECIMALS = 6

# The planned history: January 2024 to September 2026.
HISTORY_MONTHS = 33

MONTH_PATTERN = re.compile(r"^\d{4}-(0[1-9]|1[0-2])$")


def fmt(n) -> str:
    return f"{n:,}" if isinstance(n, int) else str(n)


def folder_bytes(path: str) -> int:
    if os.path.isfile(path):
        return os.path.getsize(path)
    total = 0
    for root, _, files in os.walk(path):
        total += sum(os.path.getsize(os.path.join(root, f)) for f in files)
    return total


def mb(n_bytes: int) -> str:
    return f"{n_bytes / 1e6:,.1f} MB"


class Run:
    """Holds the connection, timings and errors for one extract."""

    def __init__(self, base: str, month: str, out_dir: str):
        self.base, self.month, self.out = base, month, out_dir
        self.timings: list = []
        self.errors: list = []
        os.makedirs(out_dir, exist_ok=True)
        self.con = duckdb.connect()
        self.con.execute("SET threads = 16")  # reading from S3 is network-bound
        self.con.execute("SET memory_limit = '12GB'")
        tmp = os.path.join(out_dir, ".tmp").replace("'", "")
        self.con.execute(f"SET temp_directory = '{tmp}'")
        if base.startswith("s3://"):
            self.con.execute("INSTALL httpfs")
            self.con.execute("LOAD httpfs")
            self.con.execute("SET s3_region = 'us-east-2'")

    def src(self, table: str, filename: bool = False) -> str:
        extra = ", filename = true" if filename else ""
        return f"read_parquet('{self.base}/{table}/date={self.month}-*/*.parquet', hive_partitioning = true{extra})"

    def local(self, name: str) -> str:
        return f"read_parquet('{self.out}/{name}/*/*.parquet', hive_partitioning = true)"

    def sql(self, label: str, query: str):
        start = time.perf_counter()
        rows = self.con.execute(query).fetchall()
        self.timings.append((label, time.perf_counter() - start))
        return rows

    def columns(self, table: str) -> set:
        return {r[0] for r in self.con.execute(f"DESCRIBE SELECT * FROM {self.src(table)}").fetchall()}


def token_case(column: str) -> str:
    whens = " ".join(f"WHEN '{a}' THEN '{name}'" for name, a in STABLECOINS.items())
    return f"CASE lower({column}) {whens} END"


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--month", default="2025-06", help="Month to extract, as YYYY-MM")
    parser.add_argument("--base", default=DEFAULT_BASE, help="Where the data lives (default: the public AWS bucket)")
    parser.add_argument("--out-dir", default="data", help="Folder for the Parquet outputs")
    parser.add_argument("--report", default="extract_results.md", help="Markdown report to write")
    args = parser.parse_args()

    if not MONTH_PATTERN.match(args.month):
        print(f"--month must look like 2025-06, got: {args.month!r}", file=sys.stderr)
        return 2
    if "'" in args.out_dir or "'" in args.base:
        print("Paths must not contain quotes.", file=sys.stderr)
        return 2

    year, mon = map(int, args.month.split("-"))
    days_in_month = calendar.monthrange(year, mon)[1]
    shutil.rmtree(args.out_dir, ignore_errors=True)
    run = Run(args.base, args.month, args.out_dir)
    free_before = shutil.disk_usage(args.out_dir).free
    addresses = ", ".join(f"'{a}'" for a in STABLECOINS.values())
    copy_opts = "FORMAT parquet, COMPRESSION zstd, PARTITION_BY (date), OVERWRITE_OR_IGNORE"

    try:
        tt_cols = run.columns("token_transfers")
        tx_cols = run.columns("transactions")
        bl_cols = run.columns("blocks")
        lm = lambda cols: ", last_modified" if "last_modified" in cols else ""

        # 1. Stablecoin transfers --------------------------------------------
        run.sql("write stablecoin_transfers", f"""
            COPY (
                SELECT date, block_timestamp, block_number, log_index, transaction_hash,
                       {token_case('token_address')} AS token,
                       lower(from_address) AS from_address,
                       lower(to_address) AS to_address,
                       value / 1e{DECIMALS} AS amount{lm(tt_cols)}
                FROM {run.src('token_transfers')}
                WHERE lower(token_address) IN ({addresses})
                ORDER BY block_number, log_index
            ) TO '{run.out}/stablecoin_transfers' ({copy_opts})
        """)

        # 2. Transactions behind the transfers, plus direct calls (incl. failed)
        run.sql("write stablecoin_transactions", f"""
            COPY (
                SELECT t.date, t.block_timestamp, t.block_number, t.transaction_index, t.hash,
                       lower(t.from_address) AS from_address,
                       lower(t.to_address) AS to_address,
                       {token_case('t.to_address')} AS direct_token,
                       t.receipt_status, t.receipt_gas_used, t.receipt_effective_gas_price,
                       t.transaction_type{lm(tx_cols).replace(', ', ', t.')}
                FROM {run.src('transactions')} AS t
                WHERE lower(t.to_address) IN ({addresses})
                   OR t.hash IN (SELECT transaction_hash FROM {run.local('stablecoin_transfers')})
                ORDER BY t.block_number, t.transaction_index
            ) TO '{run.out}/stablecoin_transactions' ({copy_opts})
        """)

        # 3. Blocks -------------------------------------------------------------
        run.sql("write blocks", f"""
            COPY (
                SELECT date, number, hash, parent_hash, timestamp, transaction_count,
                       gas_used, gas_limit, base_fee_per_gas{lm(bl_cols)}
                FROM {run.src('blocks')}
                ORDER BY number
            ) TO '{run.out}/blocks' ({copy_opts})
        """)

        # 4. Load manifest: what the warehouse load log would show --------------
        def manifest_part(table: str, block_col: str, time_col: str, cols: set) -> str:
            if "last_modified" in cols:
                lag = f"date_diff('second', {time_col}, last_modified)"
                written = f"min(last_modified) AS first_written, max(last_modified) AS last_written, " \
                          f"quantile_cont({lag}, 0.5) AS median_lag_s, max({lag}) AS max_lag_s"
            else:
                written = "NULL AS first_written, NULL AS last_written, NULL AS median_lag_s, NULL AS max_lag_s"
            return f"""
                SELECT '{table}' AS source_table, date, count(*) AS rows,
                       count(DISTINCT filename) AS files,
                       min({block_col}) AS min_block, max({block_col}) AS max_block, {written}
                FROM {run.src(table, filename=True)}
                GROUP BY date
            """
        manifest_sql = " UNION ALL ".join([
            manifest_part("token_transfers", "block_number", "block_timestamp", tt_cols),
            manifest_part("transactions", "block_number", "block_timestamp", tx_cols),
            manifest_part("blocks", "number", "timestamp", bl_cols),
        ])
        run.sql("write load_manifest", f"COPY (({manifest_sql}) ORDER BY source_table, date) "
                                       f"TO '{run.out}/load_manifest.parquet' (FORMAT parquet)")
    except Exception as exc:
        run.errors.append(f"extract: {exc}")

    report = [f"# Phase 1 extract results\n", f"Month: `{args.month}`  \nSource: `{args.base}`\n"]

    # 5. Sizes and row counts ---------------------------------------------------
    outputs = ["stablecoin_transfers", "stablecoin_transactions", "blocks", "load_manifest.parquet"]
    sizes, counts = {}, {}
    for name in outputs:
        path = os.path.join(run.out, name)
        if not os.path.exists(path):
            continue
        sizes[name] = folder_bytes(path)
        reader = f"read_parquet('{path}')" if name.endswith(".parquet") else run.local(name)
        counts[name] = run.con.execute(f"SELECT count(*) FROM {reader}").fetchone()[0]

    report.append("## Outputs\n")
    report.append("| Output | Rows | Size on disk | Bytes per row |\n| --- | --- | --- | --- |")
    for name in outputs:
        if name in sizes:
            per_row = f"{sizes[name] / counts[name]:.0f}" if counts[name] else "n/a"
            report.append(f"| {name} | {fmt(counts[name])} | {mb(sizes[name])} | {per_row} |")
    month_total = sum(sizes.values())
    report.append(f"| **Total** | | **{mb(month_total)}** | |\n")

    report.append("## Projection for the full history\n")
    report.append(f"{HISTORY_MONTHS} months (January 2024 to September 2026) at this month's size: "
                  f"**about {month_total * HISTORY_MONTHS / 1e9:,.1f} GB**. "
                  f"Rough: activity changes month to month.\n")

    # 6. Data-quality checks on the outputs -------------------------------------
    report.append("## Data-quality checks\n")
    if {"stablecoin_transfers", "stablecoin_transactions", "blocks"} <= set(counts):
        tr, tx, bl = (run.local(n) for n in ("stablecoin_transfers", "stablecoin_transactions", "blocks"))
        checks = [
            ("Days present", f"SELECT count(DISTINCT date) FROM {tr}", days_in_month),
            ("Duplicate transfers (same hash and log index)",
             f"SELECT count(*) - count(DISTINCT (transaction_hash, log_index)) FROM {tr}", 0),
            ("Transfers with a missing amount or address",
             f"SELECT count(*) FROM {tr} WHERE amount IS NULL OR from_address IS NULL OR to_address IS NULL", 0),
            ("Transfers whose transaction is missing",
             f"SELECT count(DISTINCT transaction_hash) FROM {tr} "
             f"WHERE transaction_hash NOT IN (SELECT hash FROM {tx})", 0),
            ("Transfers whose block is missing",
             f"SELECT count(*) FROM {tr} WHERE block_number NOT IN (SELECT number FROM {bl})", 0),
            ("Missing blocks in the month's range",
             f"SELECT max(number) - min(number) + 1 - count(DISTINCT number) FROM {bl}", 0),
            ("Duplicate block rows", f"SELECT count(*) - count(DISTINCT number) FROM {bl}", 0),
        ]
        report.append("| Check | Result | Expected | Pass |\n| --- | --- | --- | --- |")
        for label, query, expected in checks:
            try:
                value = run.sql(f"check: {label}", query)[0][0]
                report.append(f"| {label} | {fmt(value)} | {fmt(expected)} | {'yes' if value == expected else '**no**'} |")
            except Exception as exc:
                run.errors.append(f"check '{label}': {exc}")
        report.append("")

        # Failure rate, now from the curated table
        try:
            rows = run.sql("failure rate", f"""
                SELECT direct_token, count(*), count(*) FILTER (WHERE receipt_status = 0)
                FROM {tx} WHERE direct_token IS NOT NULL GROUP BY 1 ORDER BY 1
            """)
            report.append("## Failure rate on direct calls to the token contracts\n")
            report.append("| Token | Transactions | Failed | Failure rate |\n| --- | --- | --- | --- |")
            for token, n, failed in rows:
                report.append(f"| {token} | {fmt(n)} | {fmt(failed)} | {failed / n:.2%} |")
            via = run.sql("indirect share", f"SELECT count(*) FILTER (WHERE direct_token IS NULL), count(*) FROM {tx}")[0]
            report.append(f"\n{fmt(via[0])} of {fmt(via[1])} transactions ({via[0] / via[1]:.0%}) moved stablecoins "
                          f"through another contract, such as an exchange.\n")
        except Exception as exc:
            run.errors.append(f"failure rate: {exc}")
    else:
        report.append("Skipped: one or more outputs are missing. See Errors.\n")

    # 7. Load manifest summary --------------------------------------------------
    if "load_manifest.parquet" in counts:
        rows = run.con.execute(f"""
            SELECT source_table, sum(files), sum(rows),
                   median(median_lag_s), max(max_lag_s), max(last_written)
            FROM read_parquet('{run.out}/load_manifest.parquet') GROUP BY 1 ORDER BY 1
        """).fetchall()
        report.append("## Load manifest (source tables)\n")
        report.append("Lag is the time between a block and when its row was written to the bucket.\n")
        report.append("| Source table | Files | Rows | Median lag | Max lag | Last written |\n"
                      "| --- | --- | --- | --- | --- | --- |")
        to_h = lambda s: "n/a" if s is None else f"{s / 3600:,.1f} h"
        for table, files, n, med, mx, last in rows:
            report.append(f"| {table} | {fmt(int(files))} | {fmt(int(n))} | {to_h(med)} | {to_h(mx)} | {last} |")
        report.append("")

    # 8. Storage experiment: hex text vs raw bytes, on the first day ------------
    first_day = f"{args.month}-01"
    day_dir = os.path.join(run.out, "stablecoin_transfers", f"date={first_day}")
    if os.path.isdir(day_dir):
        exp = os.path.join(run.out, ".experiment.parquet")
        to_bytes = lambda col: f"try(unhex(substr({col}, 3))) AS {col}"  # NULL if a value is not hex
        try:
            run.sql("experiment: binary encoding", f"""
                COPY (
                    SELECT date, block_timestamp, block_number, log_index,
                           {to_bytes('transaction_hash')}, token,
                           {to_bytes('from_address')}, {to_bytes('to_address')},
                           amount{lm(tt_cols)}
                    FROM read_parquet('{day_dir}/*.parquet')
                ) TO '{exp}' (FORMAT parquet, COMPRESSION zstd)
            """)
            text_size, bin_size = folder_bytes(day_dir), os.path.getsize(exp)
            report.append("## Storage experiment\n")
            report.append(f"Transfers on {first_day}: hashes and addresses as hex text take {mb(text_size)}; "
                          f"as raw bytes, {mb(bin_size)} ({1 - bin_size / text_size:.0%} smaller).\n")
        except Exception as exc:
            run.errors.append(f"storage experiment: {exc}")
        finally:
            if os.path.exists(exp):
                os.remove(exp)

    # 9. Disk, timings, errors --------------------------------------------------
    shutil.rmtree(os.path.join(run.out, ".tmp"), ignore_errors=True)
    free_after = shutil.disk_usage(args.out_dir).free
    report.append("## Runner disk\n")
    report.append(f"Free before: {free_before / 1e9:,.1f} GB. Free after: {free_after / 1e9:,.1f} GB.\n")

    report.append("## Timings\n")
    report.append("| Step | Seconds |\n| --- | --- |")
    for label, seconds in run.timings:
        report.append(f"| {label} | {seconds:.1f} |")
    report.append("")

    if run.errors:
        report.append("## Errors\n")
        report.extend(f"- {e}" for e in run.errors)
        report.append("")

    text = "\n".join(report)
    with open(args.report, "w", encoding="utf-8") as fh:
        fh.write(text)
    print(text)
    return 1 if run.errors else 0


if __name__ == "__main__":
    sys.exit(main())
