"""Find and investigate every unusual day in the data, automatically.

1. Scores every token and day of the daily marts (transfers, value moved, failure rate)
   and groups unusual days into episodes. No date is given to it.
2. For the most severe episodes, downloads the raw rows of the peak day and the week
   before, checks the pipeline first, then looks for a group that explains the move,
   and records a verdict with its evidence (twinfault.verdicts).
3. Compares its verdicts with the four cases investigated by hand on the website, which
   it never sees while deciding.

Writes to --out:
  episodes.parquet       every episode, with the verdict where investigated
  facts.jsonl            every fact behind every verdict
  pipeline_days.parquet  days the pipeline mart flags on its own
  report.md              the whole report; report_summary.md and report_agreement.md are short parts

Usage:
  python scripts/watch.py --out watch --upload
  python scripts/watch.py --out watch --data ./local_copy      (no download)
"""
from __future__ import annotations

import argparse
import datetime as dt
import json
import os
import shutil
import sys
import time
from pathlib import Path

import duckdb
import pandas as pd

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from twinfault import checks, episodes, verdicts  # noqa: E402

HF_REPO = "andrew142/stablecoin-payments-eth"
BASELINE_DAYS = 7
RAW = {"failure_rate": "stablecoin_transactions", "transfers": "stablecoin_transfers", "volume": "stablecoin_transfers"}
EMPTY = {
    "tr": "null::date as date, null::timestamp as block_timestamp, null::bigint as block_number, "
          "null::bigint as log_index, null::varchar as transaction_hash, null::varchar as token, "
          "null::varchar as from_address, null::varchar as to_address, null::double as amount, "
          "null::timestamp as last_modified",
    "tx": "null::date as date, null::timestamp as block_timestamp, null::bigint as block_number, "
          "null::bigint as transaction_index, null::varchar as hash, null::varchar as from_address, "
          "null::varchar as to_address, null::varchar as direct_token, null::bigint as receipt_status, "
          "null::bigint as receipt_gas_used, null::bigint as receipt_effective_gas_price, "
          "null::bigint as transaction_type, null::timestamp as last_modified",
    "bl": "null::date as date, null::bigint as number, null::timestamp as timestamp",
}
FOLDER = {"tr": "stablecoin_transfers", "tx": "stablecoin_transactions", "bl": "blocks"}

# The cases investigated by hand (site/investigations.qmd). Used only to score the
# verdicts afterwards: detection and the verdict rules never read this list.
KNOWN = [
    {"case": "USDC and USDT failure spike, Nov 2025", "metric": "failure_rate", "tokens": ["USDT", "USDC"],
     "start": "2025-11-22", "end": "2025-12-05", "families": ["bots"], "hand_verdict": "new automated senders"},
    {"case": "PYUSD failure spikes, Aug-Sep 2024", "metric": "failure_rate", "tokens": ["PYUSD"],
     "start": "2024-08-16", "end": "2024-09-16", "families": ["bots"], "hand_verdict": "bots, not users"},
    {"case": "600 trillion PYUSD, 15 Oct 2025", "metric": "volume", "tokens": ["PYUSD"],
     "start": "2025-10-15", "end": "2025-10-15", "families": ["supply"], "hand_verdict": "a real mint and burn"},
]
HAND_REWRITTEN_DAYS = 61


# ------------------------------------------------------------------ data

def fetch(data: Path, patterns: list[str], offline: bool) -> None:
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


def raw_views(con: duckdb.DuckDBPyConnection, data: Path, days: list[str]) -> None:
    """Views tr, tx and bl over the raw files of these days (empty if not downloaded)."""
    keep = {f"date={d}" for d in days}
    for name, folder in FOLDER.items():
        files = sorted(f for f in (data / folder).glob("date=*/*.parquet") if f.parent.name in keep)
        if files:
            listing = ", ".join(f"'{f.as_posix()}'" for f in files)
            extra = " replace (amount::double as amount)" if name == "tr" else ""
            con.execute(f"create or replace view {name} as select *{extra} from "
                        f"read_parquet([{listing}], hive_partitioning = false, union_by_name = true)")
        else:
            con.execute(f"create or replace view {name} as select {EMPTY[name]} where false")


def needed(ep: pd.Series) -> dict[str, list[str]]:
    """The raw days an episode needs: its peak day and the week before the episode began."""
    start = pd.Timestamp(ep.start)
    baseline = [(start - pd.Timedelta(days=i)).strftime("%Y-%m-%d") for i in range(BASELINE_DAYS, 0, -1)]
    peak = pd.Timestamp(ep.peak_date).strftime("%Y-%m-%d")
    return {"peak": peak, "baseline": baseline, "folder": RAW[ep.metric]}


def files_for(plan: dict) -> set[tuple[str, str]]:
    days = set(plan["baseline"]) | {plan["peak"]}
    return {(plan["folder"], d) for d in days} | {("blocks", plan["peak"])}


# ------------------------------------------------------------------ investigate one episode

def investigate(con, ep: pd.Series, plan: dict) -> tuple[dict, dict]:
    peak, baseline = plan["peak"], plan["baseline"]
    pf = checks.pipeline_facts(con, ep.metric, ep.token, peak, baseline)
    behaviour = {"failure_rate": checks.failure_facts, "transfers": checks.transfer_facts,
                 "volume": checks.volume_facts}[ep.metric]
    bf = behaviour(con, ep.token, peak, baseline)
    return verdicts.decide(ep.metric, ep.direction, pf, bf), {"pipeline": pf, "behaviour": bf}


# ------------------------------------------------------------------ report

def move(row) -> str:
    if row.metric == "failure_rate":
        return f"{100 * row.peak_value:.2f}% vs {100 * row.peak_usual:.2f}%"
    ratio = f"{verdicts.big(row.peak_ratio)}×" if row.peak_ratio >= 1000 else f"{row.peak_ratio:.2f}×"
    return f"{verdicts.big(row.peak_value)} vs {verdicts.big(row.peak_usual)} ({ratio})"


METRIC_NAMES = {"failure_rate": "failure rate", "transfers": "transfers", "volume": "value moved"}


def summary_table(eps: pd.DataFrame) -> str:
    lines = ["| Peak day | Token | Metric | Days | Peak vs usual | Verdict | Confidence | What the checks found |",
             "| --- | --- | --- | --- | --- | --- | --- | --- |"]
    for _, r in eps.iterrows():
        lines.append(f"| {r.peak_date:%Y-%m-%d} | {r.token} | {METRIC_NAMES[r.metric]} {r.direction} | {r.days} "
                     f"| {move(r)} | {r.label} | {r.confidence} | {r.title} |")
    return "\n".join(lines) + "\n"


def agreement(eps: pd.DataFrame, flagged: pd.DataFrame) -> tuple[str, list[dict]]:
    rows = []
    for k in KNOWN:
        for token in k["tokens"]:
            m = eps[(eps.metric == k["metric"]) & (eps.token == token)
                    & (eps.start <= pd.Timestamp(k["end"])) & (eps.end >= pd.Timestamp(k["start"]))]
            inv = m[m.investigated]
            best = inv.iloc[0] if len(inv) else (m.iloc[0] if len(m) else None)
            rows.append({
                "case": k["case"], "token": token, "found": len(m) > 0,
                "episodes": len(m), "investigated": len(inv) > 0,
                "verdict": best.label if best is not None and best.investigated else "",
                "agrees": bool(best is not None and best.investigated and best.family in k["families"]),
                "hand_verdict": k["hand_verdict"],
                "peak": f"{best.peak_date:%Y-%m-%d}" if best is not None else "",
            })
    rewritten = int((flagged.issue == "late or rewritten").sum()) if len(flagged) else 0
    lines = ["# Agreement with the investigations done by hand", "",
             "The rules never see these cases; this only scores what they decided on their own.", "",
             "| Case | Token | Found | Investigated | Peak day | Automatic verdict | Hand verdict | Agrees |",
             "| --- | --- | --- | --- | --- | --- | --- | --- |"]
    for r in rows:
        lines.append(f"| {r['case']} | {r['token']} | {'yes' if r['found'] else 'no'} "
                     f"| {'yes' if r['investigated'] else 'no'} | {r['peak']} | {r['verdict']} | {r['hand_verdict']} "
                     f"| {'yes' if r['agrees'] else 'no'} |")
    agreed = sum(r["agrees"] for r in rows)
    lines += ["", f"**{agreed} of {len(rows)}** token-cases found and given the same kind of verdict.", "",
              f"Days the pipeline mart flags as late or rewritten: {rewritten} "
              f"(the hand investigation counted {HAND_REWRITTEN_DAYS} token-transfer days)."]
    return "\n".join(lines) + "\n", rows


def details(eps: pd.DataFrame) -> str:
    out = []
    for _, r in eps[eps.investigated].iterrows():
        out.append(f"### {r.peak_date:%Y-%m-%d} · {r.token} · {METRIC_NAMES[r.metric]} {r.direction}\n")
        out.append(f"`{r.episode_id}` · {r.days} unusual day(s), {r.start:%Y-%m-%d} to {r.end:%Y-%m-%d} · "
                   f"peak {move(r)}, score {r.peak_score:+.1f}\n")
        out.append(f"**{r.label}** ({r.confidence}): {r.title}\n")
        out += [f"- {line}" for line in r.evidence]
        out += [f"- _Caveat:_ {line}" for line in r.caveats]
        out.append("")
    return "\n".join(out)


def upload(out: Path) -> None:
    from huggingface_hub import HfApi

    api = HfApi(token=os.environ["HF_TOKEN"])
    for attempt in range(1, 6):
        try:
            api.upload_folder(repo_id=HF_REPO, repo_type="dataset", folder_path=str(out), path_in_repo="watch",
                              delete_patterns="*", commit_message="Automatic investigation of every unusual day")
            return
        except Exception as exc:
            print(f"Upload attempt {attempt} failed: {exc}", file=sys.stderr)
            if attempt == 5:
                raise
            time.sleep(30 * attempt)


# ------------------------------------------------------------------ main

def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--out", default="watch")
    ap.add_argument("--data", help="Local copy of the dataset (offline)")
    ap.add_argument("--max-episodes", type=int, default=25, help="Investigate the most severe N episodes")
    ap.add_argument("--upload", action="store_true")
    args = ap.parse_args()

    offline = bool(args.data)
    data = Path(args.data or "data").resolve()
    out = Path(args.out).resolve()
    shutil.rmtree(out, ignore_errors=True)
    out.mkdir(parents=True)

    fetch(data, ["marts/*"], offline)
    con = duckdb.connect()
    for view, mart in (("payments", "fct_daily_payments"), ("health", "fct_daily_token_health"),
                       ("pipeline", "fct_daily_pipeline")):
        con.execute(f"create view {view} as select * from "
                    f"read_parquet('{data}/marts/{mart}/*/*.parquet', hive_partitioning = false)")

    _, eps, flagged = episodes.find(con)
    print(f"{len(eps)} episodes; investigating the {min(args.max_episodes, len(eps))} most severe")
    eps["investigated"] = False
    for col in ("family", "label", "title", "confidence"):
        eps[col] = ""
    eps["evidence"] = [[] for _ in range(len(eps))]
    eps["caveats"] = [[] for _ in range(len(eps))]

    chosen = list(eps.index[: args.max_episodes])
    plans = {i: needed(eps.loc[i]) for i in chosen}
    # Work through episodes in date order, so overlapping weeks are downloaded once.
    order = sorted(chosen, key=lambda i: plans[i]["peak"])
    facts = {}
    for n, i in enumerate(order):
        ep, plan = eps.loc[i], plans[i]
        t = time.time()
        fetch(data, [f"{folder}/date={day}/*" for folder, day in sorted(files_for(plan))], offline)
        raw_views(con, data, plan["baseline"] + [plan["peak"]])
        try:
            v, f = investigate(con, ep, plan)
        except Exception as exc:  # one failing episode must not stop the rest
            v, f = verdicts.verdict("doubt", f"Not investigated: {exc}", [], "none"), {}
        eps.at[i, "investigated"] = True
        for col in ("family", "label", "title", "confidence"):
            eps.at[i, col] = v[col]
        eps.at[i, "evidence"], eps.at[i, "caveats"] = v["evidence"], v["caveats"]
        facts[ep.episode_id] = f
        print(f"[{n + 1}/{len(order)}] {ep.episode_id}: {v['label']} - {v['title']} ({time.time() - t:.0f}s)")
        if not offline:  # free the disk: keep only files later episodes still need
            later = set().union(*(files_for(plans[j]) for j in order[n + 1:])) if n + 1 < len(order) else set()
            for folder, day in files_for(plan) - later:
                shutil.rmtree(data / folder / f"date={day}", ignore_errors=True)

    inv = eps[eps.investigated].sort_values("peak_score", key=abs, ascending=False)
    agree_md, agree_rows = agreement(eps, flagged)

    save = eps.drop(columns=["day_list", "pipeline_flagged_days"], errors="ignore").copy()
    save["evidence"] = save.evidence.map(lambda x: " | ".join(x))
    save["caveats"] = save.caveats.map(lambda x: " | ".join(x))
    con.register("eps_df", save)
    con.execute(f"copy eps_df to '{out}/episodes.parquet' (format parquet)")
    con.register("flag_df", flagged)
    con.execute(f"copy flag_df to '{out}/pipeline_days.parquet' (format parquet)")
    (out / "facts.jsonl").write_text("".join(json.dumps({"episode_id": k, **v}, default=str) + "\n"
                                             for k, v in facts.items()))
    (out / "agreement.json").write_text(json.dumps(agree_rows, indent=1))

    head = ["# Automatic investigation of every unusual day",
            f"Generated {dt.datetime.now(dt.timezone.utc):%Y-%m-%d %H:%M} UTC from `{HF_REPO}`.", "",
            f"{len(eps)} episodes found; the {len(inv)} most severe investigated. "
            f"{len(flagged)} days flagged by the pipeline mart on its own.", "",
            "Verdicts: " + ", ".join(f"{k} {v}" for k, v in inv.label.value_counts().items()) + "."]
    summary = "\n".join(head) + "\n\n## Investigated episodes\n\n" + summary_table(inv)
    (out / "report_summary.md").write_text(summary)
    (out / "report_agreement.md").write_text(agree_md)
    (out / "report.md").write_text(summary + "\n" + agree_md + "\n## Evidence, episode by episode\n\n" + details(eps)
                                   + "\n## Not investigated\n\n" + summary_table(eps[~eps.investigated].assign(
                                       label="", confidence="", title="")))
    print(summary)
    print(agree_md)
    if args.upload:
        upload(out)
    return 0


if __name__ == "__main__":
    sys.exit(main())
