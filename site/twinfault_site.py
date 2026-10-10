"""Shared data access and chart style for the twinfault website.

Every page reads the dbt marts (one Parquet file per month) through DuckDB,
so every number on the site is computed from the published data at build time.
"""
from __future__ import annotations

import json
import os
from pathlib import Path

import duckdb
import numpy as np
import pandas as pd
import plotly.graph_objects as go
import plotly.io as pio

HERE = Path(__file__).resolve().parent
MARTS = Path(os.environ.get("TWINFAULT_MARTS", HERE.parent / "hf_data" / "marts"))
MANIFEST = HERE.parent / "analytics" / "target" / "manifest.json"

TOKENS = ["USDT", "USDC", "PYUSD"]
TOKEN_COLORS = {"USDT": "#1a9e77", "USDC": "#2f6fd6", "PYUSD": "#e0a100"}
BANDS = ["under 10", "10 to 1k", "1k to 100k", "100k and over"]
BAND_COLORS = ["#c7d7f0", "#8fb0e3", "#4f7fcf", "#1d3f8f"]
INK = "#16202e"
MUTED = "#6b7685"
GRID = "#e9edf2"

# ---------------------------------------------------------------- data access

_con = duckdb.connect()
for _name in ("fct_daily_payments", "fct_daily_token_health", "fct_daily_pipeline"):
    _con.execute(
        f"create view {_name} as select * from "
        f"read_parquet('{(MARTS / _name).as_posix()}/*/*.parquet', hive_partitioning = false)"
    )

# One verdict per day: is the data itself in doubt?
# "Late or rewritten" means the slowest row was written more than twice the usual
# delay after its block: it arrived late, or the source rewrote that day later.
_con.execute("""
    create view pipeline_flags as
    select date,
           case
               when missing_blocks > 0 then 'Missing blocks'
               when duplicate_blocks > 0 then 'Duplicate blocks'
               when max_load_lag_hours > 2 * (select median(max_load_lag_hours) from fct_daily_pipeline)
                   then 'Late or rewritten'
               else 'OK'
           end as pipeline
    from fct_daily_pipeline
""")

# Token-days whose value moved is over 1,000 times that token's median day.
# They are left out of value figures and listed on the site, with the reason if known.
_con.execute("""
    create view value_outliers as
    with daily as (
        select date, token, sum(volume) as volume
        from fct_daily_payments group by all
    )
    select date, token, volume,
           volume / median(volume) over (partition by token) as times_usual
    from daily
    qualify volume > 1000 * median(volume) over (partition by token)
""")
_con.execute("""
    create view payments_value as
    select p.* from fct_daily_payments as p
    anti join value_outliers as o using (date, token)
""")

# Real events behind outliers, with a source. Checked by hand.
KNOWN_EVENTS = {
    ("PYUSD", "2025-10-15"): (
        "Paxos minted 300 trillion PYUSD by mistake and burned it within about 30 minutes",
        "https://www.theblock.co/news/ecosystems/2025-10-15-paxos-mistakely-mints-300-trillion-374870",
    ),
}


def sql(query: str) -> pd.DataFrame:
    """Run a query against the marts and return a DataFrame."""
    return _con.execute(query).df()


def value(query: str):
    """Run a query that returns a single value."""
    return _con.execute(query).fetchone()[0]


def last_full_month() -> str:
    """The newest month whose every day is in the data, as YYYY-MM."""
    return value("""
        select max(month) from (
            select month, count(distinct date) as days,
                   day(last_day(min(date))) as days_in_month
            from fct_daily_pipeline group by month
        ) where days = days_in_month
    """)


def unusual_days(source: str, metric: str, segment: str = "token", limit: int = 12) -> pd.DataFrame:
    """Rank days by how far a metric moved from its usual level.

    The usual level is the median of the same weekday over the previous
    8 weeks; the score divides the gap by a robust spread (1.4826 x MAD),
    so a score of 4 is roughly four standard deviations.
    """
    return sql(f"""
        with base as (
            select date, {segment} as segment, {metric} as value from ({source})
        ),
        scored as (
            select *,
                   median(value) over w as usual,
                   mad(value) over w as spread,
                   count(*) over w as history
            from base
            window w as (partition by segment, dayofweek(date) order by date
                         rows between 8 preceding and 1 preceding)
        )
        select s.date, s.segment, s.value, s.usual,
               (s.value - s.usual) / (1.4826 * s.spread) as score,
               coalesce(p.pipeline, 'No record') as pipeline
        from scored as s
        left join pipeline_flags as p using (date)
        where s.history = 8 and s.spread > 0
        order by abs(score) desc
        limit {limit}
    """)


def value_outliers() -> pd.DataFrame:
    """Outlier token-days, with the known reason and source where there is one."""
    df = sql("select * from value_outliers order by date")
    keys = list(zip(df.token, df.date.dt.strftime("%Y-%m-%d")))
    df["reason"] = [KNOWN_EVENTS.get(k, (None, None))[0] for k in keys]
    df["source"] = [KNOWN_EVENTS.get(k, (None, None))[1] for k in keys]
    return df


INVESTIGATIONS = MARTS.parent / "investigations"


def inv(name: str) -> pd.DataFrame:
    """One result table of scripts/investigate.py."""
    path = INVESTIGATIONS / f"{name}.parquet"
    if not path.exists():
        raise FileNotFoundError(f"{path} is missing: run the 'Phase 3c - investigate the unusual days' workflow first")
    df = _con.execute(f"select * from read_parquet('{path.as_posix()}')").df()
    for col in df.columns:
        if isinstance(df[col].dtype, pd.DatetimeTZDtype):
            df[col] = df[col].dt.tz_convert(None)
    return df


WATCH = MARTS.parent / "watch"


def watch(name: str) -> pd.DataFrame:
    """One result table of scripts/watch.py, the automatic investigation of every unusual day."""
    path = WATCH / f"{name}.parquet"
    if not path.exists():
        raise FileNotFoundError(f"{path} is missing: run the 'Phase 4 - investigate every unusual day' workflow first")
    df = _con.execute(f"select * from read_parquet('{path.as_posix()}')").df()
    for col in df.columns:
        if isinstance(df[col].dtype, pd.DatetimeTZDtype):
            df[col] = df[col].dt.tz_convert(None)
    return df


def watch_agreement() -> list[dict]:
    """How the automatic verdicts compare with the investigations done by hand."""
    return json.loads((WATCH / "agreement.json").read_text())


FAMILIES = {
    # family: (label, css class, one-line meaning)
    "pipeline": ("Pipeline", "fam-pipeline", "The rows themselves are wrong: missing, repeated, incomplete or in the wrong units."),
    "doubt": ("Data in doubt", "fam-doubt", "The source rewrote the day later; its numbers may have changed."),
    "bots": ("Bots and automation", "fam-bots", "One program, a fleet of new addresses, or a dust wave."),
    "few": ("A few actors", "fam-few", "A handful of addresses or transfers made most of the move."),
    "supply": ("Supply event", "fam-supply", "The issuer created or destroyed tokens."),
    "newcomers": ("New senders", "fam-newcomers", "Many new senders, with no sign of automation."),
    "broad": ("Broad real change", "fam-broad", "Real, and spread across many senders; the data alone does not say why."),
}


def family_pill(family: str) -> str:
    label, css, _ = FAMILIES.get(family, (family, "fam-broad", ""))
    return f'<span class="fam {css}">{label}</span>'


def etherscan(value: str, kind: str = "tx") -> str:
    """A short link to a transaction or address on Etherscan."""
    return f'<a class="eth" href="https://etherscan.io/{kind}/{value}" target="_blank"><code>{value[:8]}…{value[-6:]}</code></a>'


def dbt_test_count() -> int | None:
    """Number of dbt tests every month passed before it was published."""
    if not MANIFEST.exists():
        return None
    nodes = json.loads(MANIFEST.read_text())["nodes"].values()
    return sum(1 for node in nodes if node["resource_type"] == "test")

# ---------------------------------------------------------------- formatting


def month_label(month: str) -> str:
    """'2026-09' -> 'Sep 2026'."""
    return pd.Timestamp(month + "-01").strftime("%b %Y")


def compact(n: float, prefix: str = "") -> str:
    """1234567 -> 1.23M, with an optional prefix such as '$'."""
    n = float(n)
    for size, suffix in ((1e12, "T"), (1e9, "B"), (1e6, "M"), (1e3, "k")):
        if abs(n) >= size:
            return f"{prefix}{n / size:.3g}{suffix}"
    return f"{prefix}{n:,.0f}"


def small(x: float) -> str:
    """0.0000193 -> '0.000019': two significant digits, never scientific notation."""
    return np.format_float_positional(x, precision=2, unique=False, fractional=False, trim="-")


def change(new: float, old: float) -> str:
    """Percent change as text, such as '+3.2%'."""
    if not old:
        return ""
    return f"{(new - old) / old:+.1%}"


def money_unit(largest: float) -> tuple[float, str]:
    """Divisor and suffix for a dollar axis: millions, billions or trillions."""
    return (1e12, "T") if largest >= 1e12 else (1e9, "B") if largest >= 1e9 else (1e6, "M")


def pill(status: str) -> str:
    """A coloured label for a pipeline verdict."""
    kind = {"OK": "ok", "Late or rewritten": "warn"}.get(status, "bad")
    return f'<span class="pill pill-{kind}">{status}</span>'


def html_table(df: pd.DataFrame, numeric: tuple[str, ...] = ()) -> str:
    """A plain, styled HTML table; columns in `numeric` are right-aligned."""
    head = "".join(f'<th class="{"num" if c in numeric else ""}">{c}</th>' for c in df.columns)
    body = "".join(
        "<tr>" + "".join(f'<td class="{"num" if c in numeric else ""}">{v}</td>' for c, v in row.items()) + "</tr>"
        for _, row in df.iterrows()
    )
    return (f'<div class="tf-scroll"><table class="tf-table"><thead><tr>{head}</tr></thead>'
            f'<tbody>{body}</tbody></table></div>')

# ---------------------------------------------------------------- charts

pio.templates["twinfault"] = go.layout.Template(
    layout=dict(
        font=dict(family="Inter, -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif",
                  size=13, color=INK),
        colorway=[TOKEN_COLORS[t] for t in TOKENS] + ["#7b61c2", "#d1495b"],
        margin=dict(l=8, r=8, t=36, b=8),
        hovermode="x unified",
        hoverlabel=dict(bgcolor="white", bordercolor=GRID, font_size=12),
        legend=dict(orientation="h", yanchor="bottom", y=1.0, xanchor="right", x=1,
                    title_text="", traceorder="normal"),
        xaxis=dict(showgrid=False, linecolor=GRID, ticks="outside", tickcolor=GRID, title_text=""),
        yaxis=dict(gridcolor=GRID, zeroline=False, title_text="", tickfont=dict(color=MUTED)),
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
    )
)
pio.templates.default = "plotly_white+twinfault"

CONFIG = {"displaylogo": False, "responsive": True,
          "modeBarButtonsToRemove": ["select2d", "lasso2d", "autoScale2d"]}


def show(fig: go.Figure, height: int | None = None) -> None:
    """Display a chart with the site's defaults."""
    if height:
        fig.update_layout(height=height)
    # Fixed legend entry widths: dashboard cards can be drawn while hidden,
    # when the browser cannot measure text, which makes entries overlap.
    names = [t.name for t in fig.data if t.name and t.showlegend is not False]
    if names:
        fig.update_layout(legend_entrywidth=max(len(n) for n in names) * 7 + 42,
                          legend_entrywidthmode="pixels")
    fig.show(config=CONFIG)
