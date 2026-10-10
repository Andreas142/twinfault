"""Turn the facts of one episode into a verdict, in a fixed order.

1. Pipeline first. If the rows are in doubt (missing blocks, an incomplete day, repeated
   rows, a mart that disagrees with its raw rows, amounts that changed units), that is the
   verdict, and no behavioural story is told.
2. Then look for a group that explains the move: one program, new senders, a few
   addresses, dust, the issuer minting, a few very large transfers. A group "explains" the
   move if it accounts for most of the extra (or missing) amount. If removing it also
   brings the metric back to its usual level, confidence is strong.
3. If nothing in the data is in doubt and no group explains it, the change is broad: many
   senders moved together. That is a real change, but the data alone does not say why.

Families, as shown on the site:
  pipeline    the data is wrong
  doubt       the data may have changed after it was first published
  bots        automated senders: one program, a fleet of new addresses, dust
  few         a handful of addresses or transfers
  supply      the issuer created or destroyed tokens
  newcomers   many new senders, with no sign of automation
  broad       a real change across many senders
"""
from __future__ import annotations

LABELS = {"pipeline": "Pipeline", "doubt": "Data in doubt", "bots": "Bots and automation",
          "few": "A few actors", "supply": "Supply event", "newcomers": "New senders",
          "broad": "Broad real change"}
EXPLAINS = 0.6       # a group explains a move if it accounts for 60% of the extra or missing amount
RESTORED = 1.5       # without the group, the metric is back within 1.5x of usual


def pct(x: float | None, digits: int = 1) -> str:
    if x is None:
        return "–"
    if 0 < abs(x) < 0.001:
        return f"{100 * x:.2g}%"
    return f"{100 * x:.{digits}f}%"


def big(n: float | None) -> str:
    if n is None:
        return "–"
    for size, suffix in ((1e12, "T"), (1e9, "B"), (1e6, "M"), (1e3, "k")):
        if abs(n) >= size:
            return f"{n / size:.3g}{suffix}"
    return f"{n:,.0f}"


def restored(without: float | None, usual: float) -> bool:
    """Without the group, is the rate back near usual? (Within 1.5x, or 0.1 points for tiny rates.)"""
    return without is not None and without <= max(RESTORED * usual, usual + 0.001)


def verdict(family: str, title: str, evidence: list[str], confidence: str, caveats: list[str] | None = None) -> dict:
    return {"family": family, "label": LABELS[family], "title": title, "evidence": evidence,
            "confidence": confidence, "caveats": caveats or []}


# ------------------------------------------------------------------ 1. pipeline

def pipeline_problems(metric: str, pf: dict, bf: dict) -> list[tuple[str, str]]:
    """(short title, evidence) for every way the rows themselves are wrong."""
    found = []
    if pf.get("missing_blocks"):
        found.append(("Blocks missing", f"{pf['missing_blocks']:,} block numbers are missing that day."))
    if pf.get("duplicate_blocks"):
        found.append(("Blocks loaded twice", f"{pf['duplicate_blocks']:,} blocks appear twice."))
    if pf.get("blocks") and not pf.get("day_complete"):
        found.append((f"Incomplete day: nothing after {pf['last_block_time']} UTC",
                      f"The day's last block is at {pf['last_block_time']} UTC: the rest of the day is not in the data."))
    if pf.get("duplicate_rows"):
        found.append(("Rows loaded twice", f"{pf['duplicate_rows']:,.0f} rows appear twice in the raw table."))
    if pf.get("mart_matches_raw") is False:
        found.append(("The model changed the number",
                      f"The mart shows {big(pf['mart_value'])} but the raw rows give {big(pf['raw_value'])}."))
    if metric == "volume" and bf.get("units_shift_power"):
        count_ratio = bf["transfers"] / bf["transfers_baseline_avg"] if bf.get("transfers_baseline_avg") else None
        if count_ratio and 0.5 <= count_ratio <= 2:
            k, h = bf["units_shift_power"], bf["units_shift_from_hour"]
            when = "all day" if h == 0 else f"from {h:02d}:00 UTC"
            found.append((f"Units changed {when}: amounts {10 ** k:,g}× too {'large' if k > 0 else 'small'}",
                          f"{when.capitalize()}, the typical transfer is exactly 10^{k} times the week before, while "
                          f"the number of transfers is normal ({count_ratio:.2f}× the week before)."))
    return found


def rewritten(pf: dict) -> bool:
    return (pf.get("pct_rows_late") or 0) >= 50 and (pf.get("pct_rows_late_baseline") or 0) < 10


# ------------------------------------------------------------------ 2. groups that explain the move

def explain_failures(direction: str, f: dict) -> dict | None:
    usual = f.get("usual_rate") or 0
    if direction == "down":
        absent = f.get("baseline_failures_from_absent_senders")
        if absent is not None and absent >= EXPLAINS:
            return verdict("few", "The senders that usually fail stopped sending",
                           [f"{pct(absent, 0)} of the previous week's failed calls came from addresses that sent "
                            f"nothing that day."], "moderate")
        return None

    lines = [f"Failure rate {pct(f['rate'], 2)} against {pct(usual, 2)} in the week before."]
    regular_ok = restored(f.get("rate_regular"), usual)
    if f.get("share_failures_new") is not None:
        lines.append(f"{pct(f['share_calls_new'], 0)} of calls came from addresses not seen the week before; "
                     f"they failed {pct(f['rate_new'], 1)} of the time, regular senders {pct(f['rate_regular'], 2)}.")

    fp_share, fp_base = f.get("fingerprint_share_of_failures"), f.get("fingerprint_share_baseline") or 0
    if fp_share is not None and fp_share >= 0.5 and fp_share >= 2 * fp_base:
        without = f.get("rate_without_fingerprint")
        back = restored(without, usual)
        lines.insert(0, f"{f['fingerprint_calls']:,} calls from {f['fingerprint_senders']:,} addresses used exactly "
                        f"{f['fingerprint_gas']:,} gas ({pct(fp_share, 0)} of failed calls, {pct(fp_base, 0)} the week "
                        f"before); without them the failure rate is {pct(without, 2)}.")
        return verdict("bots", f"One program: failed calls share an exact gas fingerprint ({f['fingerprint_gas']:,} gas)",
                       lines, "strong" if back else "moderate")

    if f.get("share_failures_new") is not None and f["share_failures_new"] >= EXPLAINS:
        title = "New addresses failing; regular senders unaffected" if regular_ok \
            else "New addresses made most of the failures (regular senders were also affected)"
        return verdict("bots", title, lines + [f"{pct(f['share_failures_new'], 0)} of failures came from new addresses."],
                       "strong" if regular_ok else "moderate")

    if (f.get("always_failing_share") or 0) >= EXPLAINS:
        without = f.get("rate_without_always_failing")
        lines.insert(0, f"{f['always_failing_senders']:,} addresses failed on 90% or more of 5+ calls each and made "
                        f"{pct(f['always_failing_share'], 0)} of all failures; without them the rate is {pct(without, 2)}.")
        return verdict("bots", "A few addresses failing over and over", lines,
                       "strong" if restored(without, usual) else "moderate")

    if (f.get("top10_share_of_failures") or 0) >= EXPLAINS:
        lines.insert(0, f"10 addresses made {pct(f['top10_share_of_failures'], 0)} of all failures.")
        return verdict("few", "Ten addresses made most of the failures", lines, "moderate")
    if f.get("rate_regular") is not None and f["rate_regular"] >= RESTORED * usual \
            and (f.get("share_failures_new") or 0) < 0.5:
        return verdict("broad", "Regular senders failed more too: not a new group, but most users", lines, "moderate")
    return None


def explain_transfers(direction: str, f: dict) -> dict | None:
    extra = f["transfers"] - (f.get("baseline_avg") or 0)
    if not extra:
        return None
    lines = [f"{f['transfers']:,} transfers against {f['baseline_avg']:,.0f} a day the week before ({extra:+,.0f})."]
    if direction == "down":
        dust_drop = (f.get("dust_baseline_avg") or 0) - (f.get("dust") or 0)
        if dust_drop >= EXPLAINS * -extra:
            return verdict("bots", "A dust wave ended: fewer transfers under 1 token",
                           lines + [f"{f['dust']:,.0f} transfers under 1 token, against {f['dust_baseline_avg']:,.0f} a day "
                                    f"the week before: {dust_drop / -extra:.0%} of the drop."], "strong")
        lost = f.get("lost_from_top50") or 0
        if lost >= EXPLAINS * -extra:
            return verdict("few", "The busiest senders sent much less",
                           lines + [f"The week's 50 busiest senders sent {lost:,.0f} fewer transfers: "
                                    f"{lost / -extra:.0%} of the drop."], "moderate")
        if (f.get("empty_hours") or 0) >= 2:
            return verdict("doubt", "Whole hours with no transfers at all",
                           lines + [f"{f['empty_hours']} hours had no transfers, where the week before always had some."],
                           "moderate")
        return None
    dust_extra = (f.get("dust") or 0) - (f.get("dust_baseline_avg") or 0)
    if dust_extra >= EXPLAINS * extra:
        return verdict("bots", "Dust: a wave of transfers under 1 token",
                       lines + [f"{f['dust']:,.0f} transfers under 1 token, against {f['dust_baseline_avg']:,.0f} a day "
                                f"the week before: {dust_extra / extra:.0%} of the extra transfers."], "strong")
    top_extra = (f.get("top10_sender_transfers") or 0) - (f.get("top10_sender_transfers_baseline") or 0)
    if top_extra >= EXPLAINS * extra:
        return verdict("few", "Ten senders made most of the extra transfers",
                       lines + [f"The day's 10 busiest senders made {f['top10_sender_transfers']:,.0f} transfers, against "
                                f"{f['top10_sender_transfers_baseline']:,.0f} for the busiest 10 on a usual day."], "strong")
    new_extra = (f.get("new_sender_transfers") or 0) - (f.get("new_sender_transfers_baseline") or 0)
    if new_extra >= EXPLAINS * extra:
        return verdict("newcomers", "New senders made most of the extra transfers",
                       lines + [f"Addresses not seen the week before made {f['new_sender_transfers']:,.0f} transfers, "
                                f"against about {f['new_sender_transfers_baseline']:,.0f} on a usual day."], "moderate")
    return None


def explain_volume(direction: str, f: dict) -> dict | None:
    extra = (f.get("volume") or 0) - (f.get("baseline_avg") or 0)
    if not extra:
        return None
    lines = [f"{big(f['volume'])} moved against {big(f['baseline_avg'])} a day the week before."]
    if direction == "down":
        missing = (f.get("top10_transfer_volume_baseline") or 0) - (f.get("top10_transfer_volume") or 0)
        if missing >= EXPLAINS * -extra:
            return verdict("few", "The usual very large transfers did not happen",
                           lines + [f"The 10 largest transfers moved {big(f['top10_transfer_volume'])}, against "
                                    f"{big(f['top10_transfer_volume_baseline'])} on a usual day."], "moderate")
        return None
    supply_extra = f["minted"] + f["burned"] - (f.get("supply_baseline_avg") or 0)
    if supply_extra >= EXPLAINS * extra:
        holders_ratio = (f.get("between_holders") or 0) / f["between_holders_baseline_avg"] \
            if f.get("between_holders_baseline_avg") else None
        return verdict("supply", "The issuer minted and burned tokens",
                       lines + [f"Minted {big(f['minted'])} and burned {big(f['burned'])} (from and to the zero address).",
                                f"Transfers between holders moved {big(f['between_holders'])}"
                                + (f", {holders_ratio:.2f}× the week before." if holders_ratio else ".")],
                       "strong" if holders_ratio is not None and holders_ratio <= 2 else "moderate")
    for side, verb in (("sender", "sent"), ("receiver", "received")):
        side_extra = (f.get(f"top10_{side}_volume") or 0) - (f.get(f"top10_{side}_volume_baseline") or 0)
        if side_extra >= EXPLAINS * extra:
            return verdict("few", f"Ten addresses {verb} most of the extra value",
                           lines + [f"The day's 10 biggest {side}s {verb} {big(f[f'top10_{side}_volume'])}, against "
                                    f"{big(f[f'top10_{side}_volume_baseline'])} for the biggest 10 on a usual day: "
                                    f"{side_extra / extra:.0%} of the extra value."], "strong")
    top_extra = (f.get("top10_transfer_volume") or 0) - (f.get("top10_transfer_volume_baseline") or 0)
    if top_extra >= EXPLAINS * extra:
        rest = (f.get("volume") or 0) - (f.get("top10_transfer_volume") or 0)
        rest_usual = (f.get("baseline_avg") or 0) - (f.get("top10_transfer_volume_baseline") or 0)
        return verdict("few", "A few very large transfers",
                       lines + [f"The 10 largest transfers moved {big(f['top10_transfer_volume'])}, against "
                                f"{big(f['top10_transfer_volume_baseline'])} on a usual day; everything else moved "
                                f"{big(rest)} against {big(rest_usual)}."],
                       "strong" if rest_usual and rest <= 2 * rest_usual else "moderate")
    return None


# ------------------------------------------------------------------ context

MOVES = {"transfers": 1.3, "volume": 1.5, "failure_rate": 1.5}


def context_lines(metric: str, direction: str, token: str, cf: dict) -> tuple[list[str], list[str]]:
    """Evidence about the day around the episode, and the other tokens that moved the same way."""
    lines, together = [], []
    m = MOVES[metric]
    for t, r in sorted((cf.get("token_ratios") or {}).items()):
        if t != token and r is not None and ((direction == "up" and r >= m) or (direction == "down" and r <= 1 / m)):
            together.append(f"{t} {r:.2f}×")
    if together:
        lines.append(f"Other stablecoins moved the same way that day: {', '.join(together)} the week before.")
    if cf.get("fee_ratio") and (cf["fee_ratio"] >= 2 or cf["fee_ratio"] <= 0.5):
        lines.append(f"The network's average fee was {cf['fee_ratio']:.1f}× the week before: "
                     f"{'a busy' if cf['fee_ratio'] >= 2 else 'a quiet'} day for all of Ethereum.")
    if cf.get("segment_share") is not None and abs(cf["segment_share"]) >= 0.5:
        lines.append(f"{cf['segment_share']:.0%} of the change came from transfers {cf['segment_band']} tokens, "
                     f"{cf['segment_route'].replace('via contract', 'through other contracts')}.")
    return lines, together


# ------------------------------------------------------------------ decide

def decide(metric: str, direction: str, pf: dict, bf: dict, cf: dict | None = None, token: str = "") -> dict:
    problems = pipeline_problems(metric, pf, bf)
    if problems:
        return verdict("pipeline", "; ".join(t for t, _ in problems), [e for _, e in problems], "strong")

    explain = {"failure_rate": explain_failures, "transfers": explain_transfers, "volume": explain_volume}[metric]
    found = explain(direction, bf)
    caveats = []
    if rewritten(pf):
        caveats.append(f"{pf['pct_rows_late']:.0f}% of the day's rows were written over 48 hours late: the source "
                       f"rewrote this day, so its numbers may differ from what was first published.")
        if metric == "failure_rate" and pf.get("pct_failed_rows_late") is not None \
                and abs(pf["pct_failed_rows_late"] - pf["pct_rows_late"]) < 10:
            caveats.append("Failed and successful rows were rewritten alike, so the rewrite did not create the failures.")
    context, together = context_lines(metric, direction, token, cf or {})
    if found:
        found["evidence"] += context
        found["caveats"] = caveats
        return found
    if caveats:
        return verdict("doubt", "The source rewrote this day, and no group explains the move", caveats + context,
                       "moderate")
    if together:
        return verdict("broad", "Market-wide: the other stablecoins moved the same way",
                       ["No pipeline problem, and no single group explains the move."] + context, "moderate")
    return verdict("broad", "Many senders moved together; no single group explains it",
                   ["No pipeline problem, and no group of senders, transfers or tokens accounts for most of the move."]
                   + context, "moderate")
