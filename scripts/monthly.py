"""Phase 7 helpers: load each new month on its own, then report what it found.

  plan    decide whether to load a month now, and which one
  report  turn the run's results into one Markdown report (also opened as a GitHub issue)

A month is loaded only when:
  - it is not in the dataset yet (its load log is missing), unless --force;
  - it ended at least 48 hours ago, the time the source normally needs; and
  - every day of it is in all three source tables.
If it is still incomplete on the 10th of the next month, plan says it is overdue.

Usage:
  python scripts/monthly.py plan --month auto --repo andrew142/stablecoin-payments-eth \\
      --output "$GITHUB_OUTPUT" --summary "$GITHUB_STEP_SUMMARY"
  python scripts/monthly.py report --month 2026-10 --artifacts artifacts \\
      --results "load=success,build=success,rules=success,watch=success" --out report.md --title title.txt
"""
from __future__ import annotations

import argparse
import calendar
import datetime as dt
import json
import os
import re
import sys
from pathlib import Path

import duckdb
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent))

from snapshot_source import DEFAULT_BASE, DUE_HOURS, TABLES, connect  # noqa: E402

MONTH = re.compile(r"^\d{4}-(0[1-9]|1[0-2])$")
OVERDUE_DAY = 10  # the last day the schedule tries; still incomplete then means overdue
SITE = "https://andreas142.github.io/twinfault"


# ------------------------------------------------------------------ plan

def last_month(today: dt.date) -> str:
    first = today.replace(day=1)
    prev = first - dt.timedelta(days=1)
    return f"{prev:%Y-%m}"


def month_days(month: str) -> list[str]:
    year, mon = map(int, month.split("-"))
    return [f"{month}-{d:02d}" for d in range(1, calendar.monthrange(year, mon)[1] + 1)]


def month_end(month: str) -> dt.datetime:
    year, mon = map(int, month.split("-"))
    return dt.datetime(year + (mon == 12), mon % 12 + 1, 1)


def missing_days(base: str, month: str) -> dict[str, list[str]]:
    """Days of the month that have no file in each source table. One listing per table."""
    con = connect(base)
    days = set(month_days(month))
    out = {}
    for table in TABLES:
        files = [r[0] for r in con.execute(
            f"select file from glob('{base}/{table}/date={month}-*/*.parquet')").fetchall()]
        present = {m.group(1) for f in files if (m := re.search(r"date=(\d{4}-\d{2}-\d{2})", f))}
        out[table] = sorted(days - present)
    return out


def is_loaded(repo: str, month: str) -> bool:
    from huggingface_hub import HfApi

    api = HfApi(token=os.environ.get("HF_TOKEN"))
    return api.file_exists(repo, f"load_manifest/month={month}/load_manifest.parquet", repo_type="dataset")


def decide(month: str, now: dt.datetime, loaded: bool, missing: dict[str, list[str]] | None,
           force: bool = False) -> dict:
    """Pure decision, so it can be tested without the network. missing is None if not checked."""
    due = month_end(month) + dt.timedelta(hours=DUE_HOURS)
    overdue_at = month_end(month).replace(day=OVERDUE_DAY)
    if loaded and not force:
        return {"load": False, "overdue": False, "reason": f"{month} is already loaded."}
    if now < due:
        return {"load": False, "overdue": False,
                "reason": f"{month} is not due yet: the source needs until {due:%Y-%m-%d %H:%M} UTC."}
    gaps = {t: d for t, d in (missing or {}).items() if d}
    if gaps:
        listed = "; ".join(f"{t}: {', '.join(d[:5])}{' and more' if len(d) > 5 else ''}" for t, d in gaps.items())
        return {"load": False, "overdue": now >= overdue_at,
                "reason": f"{month} is not complete in the source yet ({listed})."}
    again = " again" if loaded else ""
    return {"load": True, "overdue": False,
            "reason": f"{month} is complete in the source: loading it{again}."}


def cmd_plan(args) -> int:
    now = dt.datetime.now(dt.timezone.utc).replace(tzinfo=None)
    month = last_month(now.date()) if args.month in ("", "auto") else args.month
    if not MONTH.match(month):
        print(f"--month must look like 2026-10, got {month!r}", file=sys.stderr)
        return 2
    loaded = is_loaded(args.repo, month)
    missing = None
    if (not loaded or args.force) and now >= month_end(month) + dt.timedelta(hours=DUE_HOURS):
        missing = missing_days(args.base, month)
    d = decide(month, now, loaded, missing, args.force)
    print(d["reason"])
    if args.output:
        with open(args.output, "a", encoding="utf-8") as fh:
            fh.write(f"month={month}\nload={str(d['load']).lower()}\noverdue={str(d['overdue']).lower()}\n"
                     f"reason={d['reason']}\n")
    if args.summary:
        with open(args.summary, "a", encoding="utf-8") as fh:
            fh.write(f"# Monthly load: {month}\n\n{d['reason']}\n")
    return 0


# ------------------------------------------------------------------ report

STEPS = [("load", "Load from AWS, with the checks before upload"),
         ("build", "dbt build and tests"),
         ("rules", "Rows that cannot exist on Ethereum"),
         ("watch", "Unusual days, found and investigated")]


def label(month: str) -> str:
    return dt.datetime.strptime(month, "%Y-%m").strftime("%B %Y")


def read_json(path: Path) -> dict | None:
    return json.loads(path.read_text()) if path.exists() else None


def load_line(stats: dict | None) -> str:
    if not stats:
        return "no results"
    rows = lambda name: stats["outputs"].get(name, {}).get("rows", 0)
    checks = stats.get("checks", [])
    passed = sum(c["passed"] for c in checks)
    return (f"{passed} of {len(checks)} checks passed; {rows('stablecoin_transfers'):,} transfers, "
            f"{rows('stablecoin_transactions'):,} transactions, {rows('blocks'):,} blocks")


def build_line(results: dict | None) -> str:
    if not results:
        return "no results"
    tests = [r for r in results.get("results", []) if r.get("unique_id", "").startswith("test.")]
    models = [r for r in results.get("results", []) if r.get("unique_id", "").startswith("model.")]
    ok = sum(r.get("status") == "pass" for r in tests)
    built = sum(r.get("status") == "success" for r in models)
    return f"{built} of {len(models)} models built; {ok} of {len(tests)} tests passed"


def rules_line(days: pd.DataFrame | None, month: str) -> str:
    if days is None:
        return "no results"
    m = days[days.month == month]
    if m.empty:
        return "month not in the results"
    broken = m[m.violations > 0]
    checked = m.groupby("check").checked.sum()
    return (f"{len(checked)} rules on every row of the month: "
            + ("no row breaks any rule" if broken.empty else
               f"{int(broken.violations.sum()):,} rows break {broken.check.nunique()} of them"))


def in_month(eps: pd.DataFrame, month: str) -> pd.DataFrame:
    first = pd.Timestamp(f"{month}-01")
    last = pd.Timestamp(month_end(month)) - pd.Timedelta(days=1)
    return eps[(eps.end >= first) & (eps.start <= last)].sort_values("peak_date")


def read_parquet(path: Path) -> pd.DataFrame | None:
    if not path.exists():
        return None
    return duckdb.connect().execute(f"select * from read_parquet('{path.as_posix()}')").df()


def cmd_report(args) -> int:
    import watch  # the same wording as the website's "Every unusual day" page

    art = Path(args.artifacts)
    results = dict(part.split("=", 1) for part in args.results.split(",") if "=" in part)
    stats = read_json(art / f"stats-{args.month}" / "stats.json")
    run = read_json(art / f"dbt-{args.month}" / "run_results.json")
    days = read_parquet(art / "checks" / "impossible_days.parquet")
    eps = read_parquet(art / "watch" / "episodes.parquet")
    flagged = read_parquet(art / "watch" / "pipeline_days.parquet")

    details = {"load": load_line(stats), "build": build_line(run), "rules": rules_line(days, args.month)}
    new = in_month(eps, args.month) if eps is not None else None
    if new is not None:
        details["watch"] = (f"{len(new)} unusual {'episode' if len(new) == 1 else 'episodes'} touch the month; "
                            f"{int(new.investigated.sum())} investigated")
    else:
        details["watch"] = "no results"
    failed = [name for name, _ in STEPS if results.get(name, "skipped") != "success"]

    name = label(args.month)
    lines = [f"# {name}: " + (f"failed at: {dict(STEPS)[failed[0]].lower()}" if failed else "loaded and checked"), "",
             "| Step | Result | What it found |", "| --- | --- | --- |"]
    for key, title in STEPS:
        lines.append(f"| {title} | {results.get(key, 'skipped')} | {details.get(key, '')} |")

    if new is not None:
        lines += ["", f"## Unusual days in {name}", ""]
        if new.empty:
            lines.append("None: every token's transfers, value moved and failure rate stayed in their usual range.")
        else:
            lines += ["| Peak day | Token | What moved | Days | Peak vs usual | Verdict | What the checks found |",
                      "| --- | --- | --- | --- | --- | --- | --- |"]
            for _, r in new.iterrows():
                verdict = r.label if r.investigated else "not investigated"
                lines.append(f"| {r.peak_date:%Y-%m-%d} | {r.token} | {watch.METRIC_NAMES[r.metric]} {r.direction} "
                             f"| {r.days} | {watch.move(r)} | {verdict} | {r.title if r.investigated else ''} |")
    if flagged is not None:
        f = flagged[flagged.date.dt.strftime("%Y-%m") == args.month]
        lines += ["", "## Pipeline", ""]
        lines.append("No day of the month has missing or duplicate blocks or rows written late." if f.empty else
                     "Days the pipeline flags: " + "; ".join(f"{d:%Y-%m-%d} ({i})" for d, i in zip(f.date, f.issue)) + ".")
    if not failed:
        lines += ["", f"Website (rebuilt after this run): {SITE}/ · every unusual day: {SITE}/watch · "
                      f"data health: {SITE}/data-health"]
    text = "\n".join(lines) + "\n"
    Path(args.out).write_text(text)

    if failed:
        title = f"{name}: monthly load failed at {dict(STEPS)[failed[0]].lower()}"
    else:
        n = len(new) if new is not None else 0
        title = f"{name} loaded: {n} unusual {'episode' if n == 1 else 'episodes'}, " + (
            "no rule broken" if "no row breaks" in details["rules"] else "rows break a rule, see the report")
    if args.title:
        Path(args.title).write_text(title)
    print(title)
    print(text)
    return 0


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = ap.add_subparsers(dest="cmd", required=True)
    p = sub.add_parser("plan", help="Decide whether to load a month now")
    p.add_argument("--month", default="auto", help="YYYY-MM, or auto for last month")
    p.add_argument("--repo", required=True)
    p.add_argument("--base", default=DEFAULT_BASE, help="Where the source lives")
    p.add_argument("--force", action="store_true", help="Load it again even if it is already loaded")
    p.add_argument("--output", help="File to append name=value lines to (GITHUB_OUTPUT)")
    p.add_argument("--summary", help="File to append a Markdown note to (GITHUB_STEP_SUMMARY)")
    p.set_defaults(func=cmd_plan)
    r = sub.add_parser("report", help="Write the monthly report from the run's artifacts")
    r.add_argument("--month", required=True)
    r.add_argument("--artifacts", required=True, help="Folder with one sub-folder per artifact")
    r.add_argument("--results", required=True, help="step=result pairs, comma separated")
    r.add_argument("--out", required=True)
    r.add_argument("--title", help="File to write the issue title to")
    r.set_defaults(func=cmd_report)
    args = ap.parse_args()
    return args.func(args)


if __name__ == "__main__":
    sys.exit(main())
