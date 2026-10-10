"""Find unusual days in the daily marts and group them into episodes.

Every token and day is scored for three metrics: transfers, value moved and the
failure rate of direct calls. The usual level is the median of the same weekday over
the previous 8 weeks, on a log scale, so a score measures a multiplicative move against
a robust spread. Consecutive unusual days (gaps of one quiet day allowed) in the same
direction form one episode.

Nothing here knows about any particular date: the investigator finds its own cases.
"""
from __future__ import annotations

import duckdb
import pandas as pd

METRICS = {
    # metric: (sql for the daily value, smallest move that counts, as a ratio to usual).
    # For the failure rate, calls and failed say how much chance alone can move it.
    "transfers": ("select date, token, sum(transfers)::double as value, null::double as calls, "
                  "null::double as failed from payments group by all", 1.3),
    "volume": ("select date, token, sum(volume) as value, null::double as calls, null::double as failed "
               "from payments group by all", 2.0),
    "failure_rate": ("select date, token, failure_rate as value, direct_calls::double as calls, "
                     "failed_calls::double as failed from health where direct_calls >= 500", 2.0),
}
MIN_SCORE = 5.0       # robust standard deviations
MIN_SPREAD = 0.05     # floor on the spread, in log units, so a very stable series does not flag 2% moves
WEEKS = 8


def scored_days(con: duckdb.DuckDBPyConnection) -> pd.DataFrame:
    """Every metric, token and day with its usual level and score."""
    parts = " union all ".join(f"select '{m}' as metric, * from ({sql})" for m, (sql, _) in METRICS.items())
    return con.execute(f"""
        with daily as ({parts}),
        logged as (
            select *, ln(greatest(value, 1e-12)) as lv from daily where value is not null
        ),
        scored as (
            select *, median(lv) over w as usual_lv, mad(lv) over w as mad_lv, count(*) over w as history
            from logged
            window w as (partition by metric, token, dayofweek(date) order by date
                         rows between {WEEKS} preceding and 1 preceding)
        )
        select date, metric, token, value, calls, failed, exp(usual_lv) as usual, value / exp(usual_lv) as ratio,
               (lv - usual_lv) / greatest(1.4826 * mad_lv, {MIN_SPREAD}) as score
        from scored where history = {WEEKS}
        order by metric, token, date
    """).df()


def unusual(days: pd.DataFrame) -> pd.DataFrame:
    """Days that move far (score) and by a meaningful amount (ratio)."""
    min_ratio = days.metric.map({m: r for m, (_, r) in METRICS.items()})
    far = days.score.abs() >= MIN_SCORE
    big = (days.ratio >= min_ratio) | (days.ratio <= 1 / min_ratio)
    # A failure rate built on a few failures moves a lot by chance: the number of failed calls
    # must differ from what the usual rate predicts by over 4 standard deviations, and by 50+.
    expected = days.usual * days.calls
    gap = (days.failed - expected).abs()
    chance = (days.metric != "failure_rate") | ((gap >= 50) & (gap >= 4 * expected.clip(lower=1) ** 0.5))
    out = days[far & big & chance].copy()
    out["direction"] = (out.score > 0).map({True: "up", False: "down"})
    return out


def group(flags: pd.DataFrame) -> pd.DataFrame:
    """Consecutive unusual days of one metric, token and direction become one episode."""
    if flags.empty:
        return pd.DataFrame(columns=["episode_id", "metric", "token", "direction", "start", "end", "days",
                                     "peak_date", "peak_value", "peak_usual", "peak_ratio", "peak_score"])
    rows = []
    for (metric, token, direction), g in flags.sort_values("date").groupby(["metric", "token", "direction"]):
        run = (g.date.diff().dt.days.fillna(99) > 2).cumsum()
        for _, e in g.groupby(run):
            peak = e.loc[e.score.abs().idxmax()]
            start, end = e.date.min(), e.date.max()
            rows.append({
                "episode_id": f"{metric}-{token}-{direction}-{start:%Y-%m-%d}",
                "metric": metric, "token": token, "direction": direction,
                "start": start, "end": end, "days": len(e), "day_list": [d.strftime("%Y-%m-%d") for d in e.date],
                "peak_date": peak.date, "peak_value": peak.value, "peak_usual": peak.usual,
                "peak_ratio": peak.ratio, "peak_score": peak.score,
            })
    return pd.DataFrame(rows).sort_values("peak_score", key=abs, ascending=False).reset_index(drop=True)


def pipeline_days(con: duckdb.DuckDBPyConnection) -> pd.DataFrame:
    """Days the pipeline mart itself flags: missing or duplicate blocks, or rows written very late."""
    return con.execute("""
        select date, missing_blocks, duplicate_blocks, max_load_lag_hours,
               max_load_lag_hours / (select median(max_load_lag_hours) from pipeline) as lag_vs_usual,
               case when missing_blocks > 0 then 'missing blocks'
                    when duplicate_blocks > 0 then 'duplicate blocks'
                    when max_load_lag_hours > 2 * (select median(max_load_lag_hours) from pipeline)
                        then 'late or rewritten'
               end as issue
        from pipeline
        where missing_blocks > 0 or duplicate_blocks > 0
           or max_load_lag_hours > 2 * (select median(max_load_lag_hours) from pipeline)
        order by date
    """).df()


def find(con: duckdb.DuckDBPyConnection) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """Scored days, episodes, and pipeline-flagged days."""
    days = scored_days(con)
    episodes = group(unusual(days))
    flagged = pipeline_days(con)
    bad = set(flagged.date.dt.strftime("%Y-%m-%d"))
    if not episodes.empty:
        episodes["pipeline_flagged_days"] = episodes.day_list.map(lambda ds: [d for d in ds if d in bad])
    return days, episodes, flagged
