"""Helpers for the backfill workflow.

  months   START END            print the months from START to END as a JSON list
  upload   --month M ...         upload one extracted month to the Hugging Face dataset
  summary  STATS_DIR MONTHS_JSON print a Markdown table of every month's results

Dataset layout on Hugging Face:

  stablecoin_transfers/date=YYYY-MM-DD/*.parquet
  stablecoin_transactions/date=YYYY-MM-DD/*.parquet
  blocks/date=YYYY-MM-DD/*.parquet
  load_manifest/month=YYYY-MM/load_manifest.parquet
  reports/YYYY-MM.md and reports/YYYY-MM.json

Usage:
  python scripts/backfill_tools.py months 2024-01 2026-09
  python scripts/backfill_tools.py upload --month 2025-06 --data-dir data --report report.md \\
      --stats stats.json --repo andrew142/stablecoin-payments-eth
  python scripts/backfill_tools.py summary stats '["2025-06"]'
"""

import argparse
import glob
import json
import os
import re
import shutil
import sys
import time

MONTH = re.compile(r"^\d{4}-(0[1-9]|1[0-2])$")
REPO = re.compile(r"^[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+$")
MAX_MONTHS = 60
TABLES = ["stablecoin_transfers", "stablecoin_transactions", "blocks"]


def month_range(start: str, end: str) -> list:
    for value in (start, end):
        if not MONTH.match(value):
            raise ValueError(f"Months must look like 2024-01, got {value!r}")
    year, mon = map(int, start.split("-"))
    months = []
    while f"{year:04d}-{mon:02d}" <= end:
        months.append(f"{year:04d}-{mon:02d}")
        year, mon = (year + 1, 1) if mon == 12 else (year, mon + 1)
        if len(months) > MAX_MONTHS:
            raise ValueError(f"More than {MAX_MONTHS} months requested")
    if not months:
        raise ValueError(f"Start {start} is after end {end}")
    return months


def cmd_months(args) -> int:
    print(json.dumps(month_range(args.start, args.end)))
    return 0


def stage(month: str, data_dir: str, report: str, stats: str) -> list:
    """Put the manifest and reports where they belong in the dataset layout."""
    shutil.rmtree(os.path.join(data_dir, ".tmp"), ignore_errors=True)
    manifest = os.path.join(data_dir, "load_manifest.parquet")
    if os.path.exists(manifest):
        target = os.path.join(data_dir, "load_manifest", f"month={month}")
        os.makedirs(target, exist_ok=True)
        shutil.move(manifest, os.path.join(target, "load_manifest.parquet"))
    reports = os.path.join(data_dir, "reports")
    os.makedirs(reports, exist_ok=True)
    shutil.copy(report, os.path.join(reports, f"{month}.md"))
    shutil.copy(stats, os.path.join(reports, f"{month}.json"))

    staged = []
    for root, _, files in os.walk(data_dir):
        staged += [os.path.relpath(os.path.join(root, f), data_dir) for f in files]
    return sorted(staged)


def cmd_upload(args) -> int:
    if not MONTH.match(args.month) or not REPO.match(args.repo):
        print("Bad --month or --repo value.", file=sys.stderr)
        return 2

    staged = stage(args.month, args.data_dir, args.report, args.stats)
    folders = {path.split("/")[0] for path in staged}
    print(f"Staged {len(staged)} files for {args.month} in: {', '.join(sorted(folders))}")
    missing = [t for t in TABLES if t not in folders]
    if missing:
        print(f"Refusing to upload: no output for {missing}.", file=sys.stderr)
        return 1
    if args.dry_run:
        print("Dry run: nothing uploaded.")
        return 0

    token = os.environ.get("HF_TOKEN")
    if not token:
        print("HF_TOKEN is not set. Add it as a GitHub repository secret.", file=sys.stderr)
        return 1

    from huggingface_hub import HfApi

    api = HfApi(token=token)
    # Remove this month's old files first, so a re-run never leaves stale files behind.
    delete = [f"{t}/date={args.month}-*/*" for t in TABLES] + [f"load_manifest/month={args.month}/*"]
    allow = [f"{t}/*" for t in TABLES] + ["load_manifest/*", "reports/*"]
    for attempt in range(1, args.attempts + 1):
        try:
            info = api.upload_folder(
                repo_id=args.repo,
                repo_type="dataset",
                folder_path=args.data_dir,
                commit_message=f"Add {args.month}",
                allow_patterns=allow,
                delete_patterns=delete,
            )
            print(f"Uploaded {args.month}: {getattr(info, 'commit_url', info)}")
            return 0
        except Exception as exc:  # other months commit at the same time; wait and retry
            wait = 30 * attempt
            print(f"Attempt {attempt} failed: {exc}", file=sys.stderr)
            if attempt < args.attempts:
                print(f"Retrying in {wait} s", file=sys.stderr)
                time.sleep(wait)
    return 1


def cmd_summary(args) -> int:
    expected = json.loads(args.months_json)
    found = {}
    for path in glob.glob(os.path.join(args.stats_dir, "**", "*.json"), recursive=True):
        with open(path, encoding="utf-8") as fh:
            stats = json.load(fh)
        found[stats["month"]] = stats

    lines = ["# Backfill results\n",
             "| Month | Transfers | Transactions | Blocks | Size | Checks passed | Errors |",
             "| --- | --- | --- | --- | --- | --- | --- |"]
    totals = {"transfers": 0, "transactions": 0, "blocks": 0, "bytes": 0}
    failed_checks = []
    for month in expected:
        if month not in found:
            lines.append(f"| {month} | | | | | | **job failed: no results** |")
            continue
        s = found[month]
        rows = lambda name: s["outputs"].get(name, {}).get("rows", 0)
        transfers, txs, blocks = rows("stablecoin_transfers"), rows("stablecoin_transactions"), rows("blocks")
        passed = sum(c["passed"] for c in s["checks"])
        failed_checks += [f"{month}: {c['check']} = {c['result']} (expected {c['expected']})"
                          for c in s["checks"] if not c["passed"]]
        totals["transfers"] += transfers
        totals["transactions"] += txs
        totals["blocks"] += blocks
        totals["bytes"] += s["total_bytes"]
        errors = len(s["errors"]) or ""
        lines.append(f"| {month} | {transfers:,} | {txs:,} | {blocks:,} | {s['total_bytes'] / 1e9:,.2f} GB "
                     f"| {passed} of {len(s['checks'])} | {errors} |")
    lines.append(f"| **Total** | **{totals['transfers']:,}** | **{totals['transactions']:,}** "
                 f"| **{totals['blocks']:,}** | **{totals['bytes'] / 1e9:,.1f} GB** | | |")
    lines.append(f"\n{len(found)} of {len(expected)} months have results.\n")
    if failed_checks:
        lines.append("## Failed checks\n")
        lines += [f"- {item}" for item in failed_checks]
        lines.append("")
    print("\n".join(lines))
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = parser.add_subparsers(dest="command", required=True)

    p = sub.add_parser("months", help="Print a JSON list of months")
    p.add_argument("start")
    p.add_argument("end")
    p.set_defaults(func=cmd_months)

    p = sub.add_parser("upload", help="Upload one extracted month to Hugging Face")
    p.add_argument("--month", required=True)
    p.add_argument("--data-dir", default="data")
    p.add_argument("--report", required=True)
    p.add_argument("--stats", required=True)
    p.add_argument("--repo", required=True, help="Dataset id, e.g. andrew142/stablecoin-payments-eth")
    p.add_argument("--attempts", type=int, default=5)
    p.add_argument("--dry-run", action="store_true", help="Stage the files but do not upload")
    p.set_defaults(func=cmd_upload)

    p = sub.add_parser("summary", help="Print a Markdown table of all months")
    p.add_argument("stats_dir")
    p.add_argument("months_json")
    p.set_defaults(func=cmd_summary)

    args = parser.parse_args()
    try:
        return args.func(args)
    except ValueError as exc:
        print(exc, file=sys.stderr)
        return 2


if __name__ == "__main__":
    sys.exit(main())
