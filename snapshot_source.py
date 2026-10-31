"""Take a snapshot of the AWS source every day, and record every day that arrives late or changes.

A warehouse loads a day once and assumes it never changes. The AWS public Ethereum data
sometimes arrives late, and sometimes rewrites days it has already published. This script
measures each source day the same way the extract's load log did (rows, files, block
range, first and last write time) plus the stablecoin rows inside it, and keeps every
measurement. Comparing snapshots over time shows, as it happens:

  arrived      a day first appeared in the source (and how long after the day ended)
  late         a day that should exist by now is still missing
  changed      a day already seen has different rows, files, blocks or write times
  republished  a day differs from what this project loaded and published

Usage:
  python scripts/snapshot_source.py snapshot --days 21 --out loads/new           # recent days
  python scripts/snapshot_source.py snapshot --month 2025-06 --out loads/new     # one past month
  python scripts/snapshot_source.py events --snapshots loads --manifest hf_data/load_manifest --out loads

Snapshots are small (one row per source table and day), so every one is kept.
"""
from __future__ import annotations

import argparse
import datetime as dt
import sys
from pathlib import Path

import duckdb
import pandas as pd

DEFAULT_BASE = "s3://aws-public-blockchain/v1.0/eth"
TOKENS = {"USDT": "0xdac17f958d2ee523a2206206994597c13d831ec7",
          "USDC": "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48",
          "PYUSD": "0x6c3ea9036406852006290770bedfcaba0e23a0e8"}
ADDRESSES = ", ".join(f"'{a}'" for a in TOKENS.values())
# source table: (block column, extra stablecoin measures)
TABLES = {
    "token_transfers": ("block_number", f"""
        count(*) filter (where lower(token_address) in ({ADDRESSES})) as stable_rows,
        sum(value) filter (where lower(token_address) in ({ADDRESSES})) / 1e6 as stable_amount,
        null::bigint as stable_failed"""),
    "transactions": ("block_number", f"""
        count(*) filter (where lower(to_address) in ({ADDRESSES})) as stable_rows,
        null::double as stable_amount,
        count(*) filter (where lower(to_address) in ({ADDRESSES}) and receipt_status = 0) as stable_failed"""),
    "blocks": ("number", "null::bigint as stable_rows, null::double as stable_amount, null::bigint as stable_failed"),
}
# What counts as a change between two snapshots of the same day.
MEASURES = ["rows", "files", "file_names", "min_block", "max_block", "first_written", "last_written",
            "stable_rows", "stable_amount", "stable_failed"]
DUE_HOURS = 48       # a day should be in the source within 48 hours of its end
LATE_ROW_HOURS = 48  # a row written over 48 hours after its day ended arrived late


# ------------------------------------------------------------------ snapshot

def connect(base: str) -> duckdb.DuckDBPyConnection:
    con = duckdb.connect()
    con.execute("set threads = 16")  # reading from S3 is network-bound
    if base.startswith("s3://"):
        con.execute("install httpfs")
        con.execute("load httpfs")
        con.execute("set s3_region = 'us-east-2'")
    return con


LIGHT = "null::bigint as stable_rows, null::double as stable_amount, null::bigint as stable_failed"


def snapshot(con: duckdb.DuckDBPyConnection, base: str, days: list[str], taken_at: str,
             light: bool = False) -> pd.DataFrame:
    """Measure every source table on every listed day. Days not in the source get rows = 0.

    light skips the stablecoin measures, so only block numbers and write times are read:
    enough to compare with the published load log, and much faster over many months.
    """
    out = []
    for table, (block_col, stable) in TABLES.items():
        stable = LIGHT if light else stable
        files = []
        for day in days:
            files += [r[0] for r in con.execute(
                f"select file from glob('{base}/{table}/date={day}/*.parquet')").fetchall()]
        present = pd.DataFrame()
        if files:
            listing = ", ".join(f"'{f}'" for f in files)
            source = f"read_parquet([{listing}], hive_partitioning = true, filename = true)"
            cols = {r[0] for r in con.execute(f"describe select * from {source}").fetchall()}
            written = ("min(last_modified) as first_written, max(last_modified) as last_written"
                       if "last_modified" in cols else "null::timestamp as first_written, null::timestamp as last_written")
            present = con.execute(f"""
                select date::varchar as date, count(*) as rows, count(distinct filename) as files,
                       md5(string_agg(distinct regexp_extract(filename, '[^/]+$'), ',' order by
                                      regexp_extract(filename, '[^/]+$'))) as file_names,
                       min({block_col}) as min_block, max({block_col}) as max_block, {written}, {stable}
                from {source} group by date
            """).df()
        for day in days:
            row = present[present.date == day]
            base_row = {"taken_at": taken_at, "source_table": table, "date": day}
            if row.empty:
                out.append({**base_row, "rows": 0, "files": 0})
            else:
                out.append({**base_row, **row.iloc[0].drop("date").to_dict()})
    df = pd.DataFrame(out)
    for col in MEASURES:
        if col not in df:
            df[col] = None
    df["taken_at"] = pd.to_datetime(df.taken_at)
    df["date"] = pd.to_datetime(df.date)
    return df[["taken_at", "source_table", "date"] + MEASURES]


# ------------------------------------------------------------------ events

def same(a, b) -> bool:
    if pd.isna(a) and pd.isna(b):
        return True
    if isinstance(a, float) or isinstance(b, float):
        return a is not None and b is not None and not pd.isna(a) and not pd.isna(b) \
            and abs(float(a) - float(b)) <= 1e-6 * max(1.0, abs(float(a)))
    return a == b


def describe(col: str, a, b) -> str:
    if col in ("first_written", "last_written"):
        fa = "–" if pd.isna(a) else pd.Timestamp(a).strftime("%Y-%m-%d %H:%M")
        fb = "–" if pd.isna(b) else pd.Timestamp(b).strftime("%Y-%m-%d %H:%M")
        return f"{col.replace('_', ' ')} {fa} → {fb}"
    if col == "file_names":
        return "different file names"
    fmt = (lambda x: "–" if pd.isna(x) else f"{float(x):,.2f}") if col == "stable_amount" \
        else (lambda x: "–" if pd.isna(x) else f"{int(x):,}")
    return f"{col.replace('_', ' ')} {fmt(a)} → {fmt(b)}"


def events(snaps: pd.DataFrame, manifest: pd.DataFrame | None, now: pd.Timestamp) -> pd.DataFrame:
    """Every arrival, delay and change visible in the snapshots, oldest first."""
    rows = []
    snaps = snaps.sort_values(["source_table", "date", "taken_at"])
    for (table, day), g in snaps.groupby(["source_table", "date"]):
        day_end = pd.Timestamp(day) + pd.Timedelta(days=1)
        seen = g[g.rows > 0]
        if seen.empty:
            if (now - day_end).total_seconds() / 3600 > DUE_HOURS:
                rows.append({"kind": "late", "source_table": table, "date": day, "seen_at": g.taken_at.max(),
                             "detail": f"not in the source more than {DUE_HOURS} hours after the day ended"})
            continue
        first = seen.iloc[0]
        if pd.notna(first.first_written):
            hours = (pd.Timestamp(first.first_written) - day_end).total_seconds() / 3600
            rows.append({"kind": "arrived", "source_table": table, "date": day, "seen_at": first.taken_at,
                         "detail": f"first written {hours:.1f} hours after the day ended"
                                   + (" (late)" if hours > LATE_ROW_HOURS else ""),
                         "hours_after_day": hours})
        prev = None
        for _, cur in g.iterrows():
            if prev is not None and prev.rows > 0:
                diffs = [describe(c, prev[c], cur[c]) for c in MEASURES
                         if not (c.startswith("stable_") and (pd.isna(prev[c]) or pd.isna(cur[c])))
                         and not same(prev[c], cur[c])]
                if diffs:
                    rows.append({"kind": "changed", "source_table": table, "date": day, "seen_at": cur.taken_at,
                                 "detail": "; ".join(diffs)})
            prev = cur

    if manifest is not None and not manifest.empty:
        latest = snaps.sort_values("taken_at").groupby(["source_table", "date"]).tail(1)
        m = manifest.merge(latest, on=["source_table", "date"], suffixes=("_published", ""))
        for _, r in m[m.rows > 0].iterrows():
            diffs = [describe(c, r[f"{c}_published"], r[c]) for c in
                     ("rows", "files", "min_block", "max_block", "first_written", "last_written")
                     if not same(r[f"{c}_published"], r[c])]
            if diffs:
                rows.append({"kind": "republished", "source_table": r.source_table, "date": r.date,
                             "seen_at": r.taken_at, "detail": "since this project loaded it: " + "; ".join(diffs)})
    cols = ["kind", "source_table", "date", "seen_at", "detail", "hours_after_day"]
    out = pd.DataFrame(rows, columns=cols)
    out["event_id"] = (out.kind + "|" + out.source_table + "|" + pd.to_datetime(out.date).dt.strftime("%Y-%m-%d")
                       + "|" + out.detail)
    return out.sort_values(["seen_at", "date", "source_table"]).reset_index(drop=True)


def report(ev: pd.DataFrame, new: pd.DataFrame, n_snapshots: int, last_taken: pd.Timestamp) -> str:
    lines = ["# Source snapshots", "",
             f"{n_snapshots:,} snapshots, the latest taken {last_taken:%Y-%m-%d %H:%M} UTC.", ""]
    counts = ev.kind.value_counts()
    lines += ["| Event | Count |", "| --- | --- |"]
    for kind in ("arrived", "late", "changed", "republished"):
        lines.append(f"| {kind} | {int(counts.get(kind, 0)):,} |")
    if not ev.empty and (ev.kind == "arrived").any():
        arr = ev[ev.kind == "arrived"]
        lines += ["", f"Days arrive a median of {arr.hours_after_day.median():.1f} hours after they end; "
                      f"{int((arr.hours_after_day > LATE_ROW_HOURS).sum())} arrived over {LATE_ROW_HOURS} hours late."]
    lines += ["", f"## New in this run ({len(new)})", ""]
    if new.empty:
        lines.append("Nothing new: every day looks as it did in the previous snapshot.")
    else:
        lines += ["| Event | Table | Day | Detail |", "| --- | --- | --- | --- |"]
        lines += [f"| {r.kind} | {r.source_table} | {pd.Timestamp(r.date):%Y-%m-%d} | {r.detail} |"
                  for r in new.itertuples()]
    return "\n".join(lines) + "\n"


# ------------------------------------------------------------------ main

def read_parquet_dir(con: duckdb.DuckDBPyConnection, pattern: str) -> pd.DataFrame:
    try:
        return con.execute(f"select * from read_parquet('{pattern}', union_by_name = true)").df()
    except duckdb.IOException:
        return pd.DataFrame()


def save(df: pd.DataFrame, path: Path) -> None:
    con = duckdb.connect()
    con.register("df", df)
    con.execute(f"copy df to '{path.as_posix()}' (format parquet)")


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = ap.add_subparsers(dest="cmd", required=True)
    s = sub.add_parser("snapshot", help="Measure source days now")
    s.add_argument("--days", type=int, help="The last N days, ending yesterday (UTC)")
    s.add_argument("--month", help="Every day of one month, as YYYY-MM")
    s.add_argument("--base", default=DEFAULT_BASE)
    s.add_argument("--out", default="loads/new")
    s.add_argument("--now", help="Pretend the time is this (for tests), as YYYY-MM-DD HH:MM")
    s.add_argument("--light", action="store_true", help="Skip the stablecoin measures (faster)")
    e = sub.add_parser("events", help="Turn all snapshots into events")
    e.add_argument("--snapshots", default="loads", help="Folder with snapshots/*.parquet (and events.parquet)")
    e.add_argument("--manifest", help="The published load_manifest folder, to compare with")
    e.add_argument("--out", default="loads")
    e.add_argument("--now")
    args = ap.parse_args()

    now = pd.Timestamp(args.now) if args.now else pd.Timestamp(dt.datetime.now(dt.timezone.utc).replace(tzinfo=None))
    if args.cmd == "snapshot":
        if args.month:
            start = pd.Timestamp(args.month + "-01")
            days = pd.date_range(start, start + pd.offsets.MonthEnd(0))
        else:
            end = now.normalize() - pd.Timedelta(days=1)
            days = pd.date_range(end - pd.Timedelta(days=(args.days or 21) - 1), end)
        df = snapshot(connect(args.base), args.base, [d.strftime("%Y-%m-%d") for d in days], now.isoformat(),
                      light=args.light)
        out = Path(args.out)
        out.mkdir(parents=True, exist_ok=True)
        name = f"{now:%Y-%m-%dT%H%M}" + (f"_{args.month}" if args.month else "")
        save(df, out / f"{name}.parquet")
        present = df[df.rows > 0]
        print(f"Snapshot {name}: {present.date.nunique()} of {len(days)} days present; "
              f"{len(df) - len(present)} table-days not in the source yet")
        return 0

    con = duckdb.connect()
    folder = Path(args.snapshots)
    snaps = read_parquet_dir(con, f"{folder.as_posix()}/snapshots/*.parquet")
    if snaps.empty:
        print("No snapshots yet.", file=sys.stderr)
        return 1
    manifest = None
    if args.manifest:
        manifest = read_parquet_dir(con, f"{args.manifest}/*/*.parquet")
        if not manifest.empty:
            manifest = manifest[["source_table", "date", "rows", "files", "min_block", "max_block",
                                 "first_written", "last_written"]]
            manifest["date"] = pd.to_datetime(manifest.date)
    previous = read_parquet_dir(con, f"{folder.as_posix()}/events.parquet")
    ev = events(snaps, manifest, now)
    known = set(previous.event_id) if not previous.empty else set()
    new = ev[~ev.event_id.isin(known) & ev.kind.isin(["late", "changed", "republished"])]
    out = Path(args.out)
    out.mkdir(parents=True, exist_ok=True)
    save(ev, out / "events.parquet")
    save(new, out / "new_events.parquet")
    text = report(ev, new, snaps.taken_at.nunique(), pd.Timestamp(snaps.taken_at.max()))
    (out / "report.md").write_text(text)
    (out / "new_events.md").write_text(text.split("## New in this run")[1] if not new.empty else "")
    print(text)
    return 0


if __name__ == "__main__":
    sys.exit(main())
