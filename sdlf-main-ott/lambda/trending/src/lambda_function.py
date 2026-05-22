import json
import os
import time
from datetime import date, datetime, timedelta, timezone

import boto3
from datalake_library.commons import init_logger

logger = init_logger(__name__)

athena = boto3.client("athena")
s3 = boto3.client("s3")
sns = boto3.client("sns")

DB                = os.environ["ATHENA_DATABASE"]
RESULTS           = os.environ["ATHENA_RESULTS"]
SNS_TOPIC_ARN     = os.environ["SNS_TOPIC_ARN"]
MIN_GROWTH        = float(os.environ["MIN_GROWTH_MULTIPLIER"])
MIN_VOLUME        = int(os.environ["MIN_SEARCH_VOLUME"])
MAX_TRENDING_ROWS = int(os.environ.get("MAX_TRENDING_ROWS", "500"))
WORKGROUP         = os.environ.get("ATHENA_WORKGROUP", "sdlf-ott")

_TRENDING_SQL = """
WITH current_window AS (
    SELECT keyword_norm, derived_genre, COUNT(*) AS current_cnt
    FROM {db}.curated
    WHERE dt >= '{cur_start}' AND dt < '{cur_end}' AND keyword_norm IS NOT NULL AND keyword_norm != ''
    GROUP BY keyword_norm, derived_genre
),
baseline_window AS (
    SELECT keyword_norm, derived_genre, COUNT(*) AS baseline_cnt
    FROM {db}.curated
    WHERE dt >= '{base_start}' AND dt < '{base_end}' AND keyword_norm IS NOT NULL AND keyword_norm != ''
    GROUP BY keyword_norm, derived_genre
)
SELECT c.keyword_norm,
       c.derived_genre,
       c.current_cnt,
       COALESCE(b.baseline_cnt, 0)                                               AS baseline_cnt,
       CASE WHEN COALESCE(b.baseline_cnt, 0) = 0 THEN NULL
            ELSE ROUND(CAST(c.current_cnt AS double) / b.baseline_cnt, 2)
       END                                                                        AS growth_multiplier,
       CASE WHEN b.baseline_cnt IS NULL THEN true ELSE false END                 AS is_new_keyword
FROM current_window c
LEFT JOIN baseline_window b
       ON c.keyword_norm = b.keyword_norm AND c.derived_genre = b.derived_genre
WHERE c.current_cnt >= {min_volume}
  AND (b.baseline_cnt IS NULL
       OR CAST(c.current_cnt AS double) / b.baseline_cnt >= {min_growth})
ORDER BY c.current_cnt DESC
LIMIT {max_rows}
"""

_FALLBACK_SQL = """
SELECT keyword_norm, derived_genre, COUNT(*) AS current_cnt,
       0 AS baseline_cnt, NULL AS growth_multiplier, true AS is_new_keyword
FROM {db}.curated
WHERE dt >= '{cur_start}' AND dt < '{cur_end}' AND keyword_norm IS NOT NULL AND keyword_norm != ''
GROUP BY keyword_norm, derived_genre
HAVING COUNT(*) >= {min_volume}
ORDER BY current_cnt DESC
LIMIT {max_rows}
"""

_BASELINE_CHECK_SQL = """
SELECT COUNT(*) AS cnt FROM {db}.curated
WHERE dt >= '{base_start}' AND dt < '{base_end}' AND keyword_norm IS NOT NULL AND keyword_norm != ''
"""


def athena_start(sql):
    return athena.start_query_execution(
        QueryString=sql,
        QueryExecutionContext={"Database": DB},
        ResultConfiguration={"OutputLocation": RESULTS},
        WorkGroup=WORKGROUP,
    )["QueryExecutionId"]


def athena_collect(qid):
    while True:
        st = athena.get_query_execution(QueryExecutionId=qid)["QueryExecution"]["Status"]["State"]
        if st == "SUCCEEDED":
            break
        if st in ("FAILED", "CANCELLED"):
            raise RuntimeError(f"Athena query {st}: {qid}")
        time.sleep(2)
    headers, rows = None, []
    for page in athena.get_paginator("get_query_results").paginate(QueryExecutionId=qid):
        for row in page["ResultSet"]["Rows"]:
            values = [col.get("VarCharValue", "") for col in row["Data"]]
            if headers is None:
                headers = values
                continue
            rows.append(dict(zip(headers, values)))
    return rows


def athena_query(sql):
    return athena_collect(athena_start(sql))


def latest_dt():
    rows = athena_query(f"SELECT MAX(dt) AS max_dt FROM {DB}.curated")
    val = rows[0].get("max_dt", "") if rows else ""
    if not val:
        raise RuntimeError("curated table has no data — cannot determine reference date")
    return date.fromisoformat(val)


def has_baseline_data(base_start, base_end):
    sql = _BASELINE_CHECK_SQL.format(db=DB, base_start=base_start, base_end=base_end)
    rows = athena_query(sql)
    return rows and int(rows[0].get("cnt", 0)) > 0


def run_trending_query(cur_start, cur_end, base_start, base_end, use_fallback):
    if use_fallback:
        sql = _FALLBACK_SQL.format(
            db=DB, cur_start=cur_start, cur_end=cur_end,
            min_volume=MIN_VOLUME, max_rows=MAX_TRENDING_ROWS,
        )
    else:
        sql = _TRENDING_SQL.format(
            db=DB, cur_start=cur_start, cur_end=cur_end,
            base_start=base_start, base_end=base_end,
            min_volume=MIN_VOLUME, min_growth=MIN_GROWTH,
            max_rows=MAX_TRENDING_ROWS,
        )
    return athena_query(sql)


_GENRE_COLOR = {
    "PHIM_VIET": "#68d391", "ANIME": "#9f7aea", "PHIM_TRUNG": "#f6ad55",
    "PHIM_HAN": "#76e4f7", "PHIM_AU_MY": "#fc8181", "TRUYEN_HINH": "#fbd38d",
    "UNKNOWN": "#4a5568", "NHAC": "#b794f4", "THE_THAO": "#4299e1",
    "EMPTY_QUERY": "#718096",
}


def _esc(s):
    return str(s).replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")


def _genre_pill(g):
    c = _GENRE_COLOR.get(g, "#718096")
    return (f'<span class="genre-pill" style="background:{c}22;color:{c};'
            f'border:1px solid {c}44">{_esc(g)}</span>')


_ABANDON_EXPR = ("ROUND(100.0 * SUM(CASE WHEN is_search_abandoned THEN 1 ELSE 0 END)"
                 " / COUNT(*), 1)")


def _dashboard_sql():
    """Every dashboard section's query, keyed by section. Run concurrently.

    These replace the former content-gap Lambda + analytics HTTP API: the
    dashboard is the single user-facing surface, so every section reads
    curated live at full depth (500 rows for content_gaps, 216 for the
    hour×genre heatmap).
    """
    ab = _ABANDON_EXPR
    return {
        # dt_window drives the dataset coverage caption: actual days present,
        # min/max date, and total row count. Used in the header + every
        # section's source tag.
        "dt_window": (
            f"SELECT MIN(dt) AS min_dt, MAX(dt) AS max_dt, "
            f"COUNT(DISTINCT dt) AS n_days, COUNT(*) AS total_rows, "
            f"ARRAY_JOIN(ARRAY_SORT(ARRAY_AGG(DISTINCT dt)), ',') AS dts "
            f"FROM {DB}.curated"
        ),
        "totals": (f"SELECT COUNT(*) AS total, COUNT(DISTINCT keyword_norm) AS kw, "
                   f"{ab} AS abandon FROM {DB}.curated"),
        "platform": (f"SELECT platform_group, COUNT(*) AS c, {ab} AS a "
                     f"FROM {DB}.curated GROUP BY platform_group ORDER BY c DESC"),
        "genre": (f"SELECT derived_genre, COUNT(*) AS c, {ab} AS a "
                  f"FROM {DB}.curated GROUP BY derived_genre ORDER BY c DESC"),
        "keywords": (f"SELECT keyword_norm, derived_genre, COUNT(*) AS c, {ab} AS a "
                     f"FROM {DB}.curated WHERE keyword_norm IS NOT NULL AND keyword_norm != '' "
                     f"GROUP BY keyword_norm, derived_genre ORDER BY c DESC LIMIT 20"),
        # Full depth — the API used to be the only path to >15 rows; now it's
        # the dashboard's own table with client-side filtering.
        "content_gaps": (
            f"SELECT keyword_norm, derived_genre, COUNT(*) AS searches, "
            f"SUM(CASE WHEN is_search_abandoned THEN 1 ELSE 0 END) AS abandoned, "
            f"{ab} AS abandon_rate_pct FROM {DB}.curated "
            f"WHERE derived_genre NOT IN ('UNKNOWN', 'EMPTY_QUERY') "
            f"AND keyword_norm IS NOT NULL AND keyword_norm != '' "
            f"GROUP BY keyword_norm, derived_genre "
            f"HAVING SUM(CASE WHEN is_search_abandoned THEN 1 ELSE 0 END) >= 5 "
            f"ORDER BY abandon_rate_pct DESC, abandoned DESC LIMIT 500"),
        "premium": (
            f"SELECT derived_genre, COUNT(*) AS total_searches, "
            f"SUM(CASE WHEN has_premium THEN 1 ELSE 0 END) AS premium_searches, "
            f"ROUND(100.0 * SUM(CASE WHEN has_premium THEN 1 ELSE 0 END) "
            f"/ CAST(COUNT(*) AS double), 1) AS premium_share_pct "
            f"FROM {DB}.curated GROUP BY derived_genre ORDER BY premium_share_pct DESC"),
        "repeat": (
            f"SELECT derived_genre, COUNT(*) AS total_searches, "
            f"SUM(CASE WHEN is_repeat_search THEN 1 ELSE 0 END) AS repeat_searches, "
            f"ROUND(100.0 * SUM(CASE WHEN is_repeat_search THEN 1 ELSE 0 END) "
            f"/ CAST(COUNT(*) AS double), 1) AS repeat_pct "
            f"FROM {DB}.curated GROUP BY derived_genre ORDER BY repeat_pct DESC"),
        "guest": (
            f"SELECT derived_genre, COUNT(*) AS total_searches, "
            f"SUM(CASE WHEN NOT user_is_authenticated THEN 1 ELSE 0 END) AS guest_searches, "
            f"ROUND(100.0 * SUM(CASE WHEN NOT user_is_authenticated THEN 1 ELSE 0 END) "
            f"/ CAST(COUNT(*) AS double), 1) AS guest_share_pct "
            f"FROM {DB}.curated WHERE derived_genre NOT IN ('UNKNOWN', 'EMPTY_QUERY') "
            f"GROUP BY derived_genre ORDER BY guest_share_pct DESC"),
        # Full hour × genre matrix (~216 rows) — drives both the heatmap and
        # the per-hour totals chart.
        "hour_genre": (
            f"SELECT hour_of_day_vn, derived_genre, COUNT(*) AS searches, "
            f"ROUND(100.0 * COUNT(*) / CAST(SUM(COUNT(*)) OVER (PARTITION BY derived_genre) AS double), 1) AS pct_of_genre "
            f"FROM {DB}.curated "
            f"WHERE hour_of_day_vn IS NOT NULL "
            f"GROUP BY hour_of_day_vn, derived_genre "
            f"ORDER BY derived_genre, hour_of_day_vn"),
    }


def _derive_window(dt_window_rows):
    """Turn the dt_window query result into a stamped caption + structured fields."""
    if not dt_window_rows:
        return {"label": "no data", "min_dt": "", "max_dt": "", "n_days": 0,
                "total_rows": 0, "missing": []}
    r = dt_window_rows[0]
    min_dt = r.get("min_dt", "") or ""
    max_dt = r.get("max_dt", "") or ""
    n_days = int(r.get("n_days", 0) or 0)
    total_rows = int(r.get("total_rows", 0) or 0)
    present = set((r.get("dts", "") or "").split(",")) if r.get("dts") else set()
    missing = []
    if min_dt and max_dt:
        try:
            a = date.fromisoformat(min_dt)
            b = date.fromisoformat(max_dt)
            d = a
            while d <= b:
                if d.isoformat() not in present:
                    missing.append(d.isoformat())
                d += timedelta(days=1)
        except ValueError:
            pass
    miss_label = ""
    if missing:
        sample = missing[:3]
        miss_label = "; ".join(sample)
        if len(missing) > 3:
            miss_label += f" (+{len(missing)-3} more)"
    label = (f"{min_dt} → {max_dt} • {n_days} days"
             + (f" • {len(missing)} missing ({miss_label})" if missing else "")
             + f" • {total_rows:,} rows")
    return {"label": label, "min_dt": min_dt, "max_dt": max_dt,
            "n_days": n_days, "total_rows": total_rows, "missing": missing}


def _section_src(base_sql, window, *, extra=""):
    """Source-attribution caption for a dashboard section.

    Renders the SQL hint, dt window, and (optionally) extra filter notes.
    """
    parts = [base_sql, window["label"]]
    if extra:
        parts.append(extra)
    return " &mdash; ".join(parts)


def _render_dashboard_html(data, window, trending_rows, trending_meta):
    """Render the centralized OTT search-analytics dashboard.

    Single user-facing surface. Every section is stamped with the dt window
    it actually aggregated (criterion #1: truthful coverage). Sections that
    exclude UNKNOWN+EMPTY_QUERY say so in their caption (criterion #1).
    The Trending section has its own window stamp (criterion #2: comparability).
    """
    totals = data["totals"][0] if data["totals"] else {}
    total_searches  = int(totals.get("total", 0) or 0)
    distinct_kw     = int(totals.get("kw", 0) or 0)
    overall_abandon = float(totals.get("abandon", 0) or 0)

    platform   = data["platform"]
    genre      = data["genre"]
    keywords   = data["keywords"]
    content_gaps = data["content_gaps"]
    premium    = data["premium"]
    repeat_kw  = data["repeat"]
    guest      = data["guest"]
    hour_genre = data["hour_genre"]

    plat = [{"p": r["platform_group"], "c": int(r["c"]), "a": float(r["a"] or 0)}
            for r in platform if r.get("platform_group")]
    byg  = [{"g": r["derived_genre"], "c": int(r["c"]), "a": float(r["a"] or 0)}
            for r in genre if r.get("derived_genre")]
    topk = [{"k": r["keyword_norm"], "g": r["derived_genre"],
             "c": int(r["c"]), "a": float(r["a"] or 0)} for r in keywords]
    top_genre = byg[0] if byg else {"g": "-", "c": 0, "a": 0.0}
    generated = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")

    def fmt_k(n):
        if n >= 1_000_000:
            return f"{n / 1_000_000:.2f}M"
        if n >= 1_000:
            return f"{n / 1_000:.0f}K"
        return str(n)

    def _int(r, k):
        try:
            return int(float(r.get(k, 0) or 0))
        except (TypeError, ValueError):
            return 0

    # ---- Trending: server-rendered, full 500 rows, in-page filterable ----
    trending_rows_html = ""
    for i, r in enumerate(trending_rows):
        gm = r.get("growth_multiplier") or ""
        is_new = str(r.get("is_new_keyword", "")).lower() == "true"
        growth = (f"{gm}&times;" if gm
                  else ("<span style='color:#68d391'>new</span>" if is_new else "&mdash;"))
        kw = _esc(r.get("keyword_norm", ""))
        g  = r.get("derived_genre", "")
        trending_rows_html += (
            f'<tr data-genre="{_esc(g)}" data-kw="{kw.lower()}">'
            f'<td class="rank">{i + 1}</td>'
            f'<td class="kw-cell" title="{kw}">{kw}</td>'
            f'<td>{_genre_pill(g)}</td>'
            f'<td>{_int(r, "current_cnt"):,}</td><td>{growth}</td></tr>'
        )
    trending_rows_html = trending_rows_html or '<tr><td colspan="5">No data</td></tr>'

    # ---- Content gaps: full 500 rows, in-page filterable by keyword + genre ----
    cg_rows_html = ""
    for i, r in enumerate(content_gaps):
        kw = _esc(r.get("keyword_norm", ""))
        g  = r.get("derived_genre", "")
        cg_rows_html += (
            f'<tr data-genre="{_esc(g)}" data-kw="{kw.lower()}">'
            f'<td class="rank">{i + 1}</td>'
            f'<td class="kw-cell" title="{kw}">{kw}</td>'
            f'<td>{_genre_pill(g)}</td>'
            f'<td>{_int(r, "searches"):,}</td><td>{_int(r, "abandoned"):,}</td>'
            f'<td style="color:#fc8181">{r.get("abandon_rate_pct", "")}%</td></tr>'
        )
    cg_rows_html = cg_rows_html or '<tr><td colspan="6">No data</td></tr>'

    def _share_rows(rows, pct_key, cnt_key, color):
        html_rows = ""
        for r in rows:
            try:
                pct = float(r.get(pct_key, 0) or 0)
            except (TypeError, ValueError):
                pct = 0.0
            html_rows += (
                f'<tr><td>{_genre_pill(r.get("derived_genre", ""))}</td>'
                f'<td>{_int(r, "total_searches"):,}</td>'
                f'<td>{_int(r, cnt_key):,}</td><td>{pct}%</td>'
                f'<td class="bar-cell"><div class="mini-bar"><div class="mini-bar-fill" '
                f'style="width:{min(pct, 100)}%;background:{color}"></div></div></td></tr>'
            )
        return html_rows or '<tr><td colspan="5">No data</td></tr>'

    premium_rows_html = _share_rows(premium, "premium_share_pct", "premium_searches", "#f6ad55")
    repeat_rows_html  = _share_rows(repeat_kw, "repeat_pct", "repeat_searches", "#fc8181")
    guest_rows_html   = _share_rows(guest, "guest_share_pct", "guest_searches", "#4299e1")

    # ---- Hour × genre heatmap: 24 cols × N genres, with per-hour totals row ----
    genres_in_order = [g["g"] for g in byg]
    matrix = {g: {h: 0 for h in range(24)} for g in genres_in_order}
    pct_matrix = {g: {h: 0.0 for h in range(24)} for g in genres_in_order}
    for r in hour_genre:
        try:
            h = int(r.get("hour_of_day_vn", -1))
            g = r.get("derived_genre", "")
            if 0 <= h < 24 and g in matrix:
                matrix[g][h] = int(r.get("searches", 0) or 0)
                pct_matrix[g][h] = float(r.get("pct_of_genre", 0) or 0)
        except (TypeError, ValueError):
            continue
    hour_totals = [sum(matrix[g][h] for g in genres_in_order) for h in range(24)]

    heat_thead = "<th>Genre</th>" + "".join(f'<th class="hcell">{h}h</th>' for h in range(24))
    heat_body = ""
    for g in genres_in_order:
        heat_body += f'<tr><td>{_genre_pill(g)}</td>'
        for h in range(24):
            pct = pct_matrix[g][h]
            # blue scale; cap at 15% for the visual midpoint (typical peak hour share)
            intensity = min(pct / 15.0, 1.0)
            r_ = int(26 + 30 * (1 - intensity))      # cool to mid
            g_ = int(32 + 80 * intensity)
            b_ = int(48 + 145 * intensity)
            cnt = matrix[g][h]
            heat_body += (
                f'<td class="hcell" title="{g} · {h:02d}:00 · {cnt:,} searches · {pct}% of genre" '
                f'style="background:rgb({r_},{g_},{b_})">{pct if pct >= 1 else ""}</td>'
            )
        heat_body += "</tr>"
    max_total = max(hour_totals) or 1
    heat_body += '<tr style="border-top:2px solid #2d3748"><td style="color:#718096;font-size:11px">All genres</td>'
    for h in range(24):
        c = hour_totals[h]
        bar_w = int(28 * c / max_total)
        heat_body += (
            f'<td class="hcell" style="background:#0f1117;color:#718096;font-size:10px" '
            f'title="{h:02d}:00 · {c:,} searches">{fmt_k(c)}</td>'
        )
    heat_body += "</tr>"

    return f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>FPT OTT Search Analytics</title>
<script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.0/dist/chart.umd.min.js"></script>
<style>
  * {{ box-sizing: border-box; margin: 0; padding: 0; }}
  body {{ font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif; background: #0f1117; color: #e2e8f0; min-height: 100vh; }}
  header {{ background: #1a1d27; border-bottom: 1px solid #2d3748; padding: 16px 32px; }}
  header h1 {{ font-size: 18px; font-weight: 600; color: #f7fafc; margin-bottom: 4px; }}
  .badge {{ background: #2d3748; color: #68d391; font-size: 11px; padding: 2px 8px; border-radius: 9999px; border: 1px solid #276749; margin-left: 8px; }}
  .header-stamp {{ font-size: 11px; color: #a0aec0; font-family: monospace; }}
  main {{ padding: 24px 32px; max-width: 1400px; margin: 0 auto; }}
  .kpis {{ display: grid; grid-template-columns: repeat(4, 1fr); gap: 16px; margin-bottom: 24px; }}
  .kpi {{ background: #1a1d27; border: 1px solid #2d3748; border-radius: 10px; padding: 20px; }}
  .kpi .label {{ font-size: 12px; color: #718096; text-transform: uppercase; letter-spacing: .05em; margin-bottom: 6px; }}
  .kpi .value {{ font-size: 28px; font-weight: 700; color: #f7fafc; }}
  .kpi .sub {{ font-size: 12px; color: #68d391; margin-top: 4px; }}
  .kpi .sub.warn {{ color: #f6ad55; }}
  .grid-2 {{ display: grid; grid-template-columns: 1fr 1fr; gap: 16px; margin-bottom: 24px; }}
  .grid-3 {{ display: grid; grid-template-columns: 2fr 1fr; gap: 16px; margin-bottom: 24px; }}
  .card {{ background: #1a1d27; border: 1px solid #2d3748; border-radius: 10px; padding: 20px; }}
  .card h2 {{ font-size: 14px; font-weight: 600; color: #a0aec0; text-transform: uppercase; letter-spacing: .05em; margin-bottom: 4px; }}
  .card .src {{ font-size: 11px; color: #4a5568; margin-bottom: 14px; font-family: monospace; line-height: 1.5; }}
  .chart-wrap {{ position: relative; height: 260px; }}
  .chart-wrap-sm {{ position: relative; height: 200px; }}
  table {{ width: 100%; border-collapse: collapse; font-size: 13px; }}
  thead th {{ text-align: left; color: #718096; font-weight: 500; padding: 6px 10px; border-bottom: 1px solid #2d3748; font-size: 11px; text-transform: uppercase; }}
  tbody tr:hover {{ background: rgba(255,255,255,0.03); }}
  tbody td {{ padding: 7px 10px; border-bottom: 1px solid #1e2130; }}
  .rank {{ color: #4a5568; width: 24px; text-align: right; padding-right: 12px; }}
  .genre-pill {{ display: inline-block; padding: 1px 7px; border-radius: 9999px; font-size: 10px; font-weight: 500; }}
  .bar-cell {{ width: 70px; }}
  .mini-bar {{ height: 5px; border-radius: 3px; background: #2d3748; overflow: hidden; }}
  .mini-bar-fill {{ height: 100%; border-radius: 3px; }}
  .scroll-wrap {{ max-height: 520px; overflow-y: auto; border: 1px solid #1e2130; border-radius: 6px; }}
  .scroll-wrap thead th {{ position: sticky; top: 0; background: #161922; z-index: 1; }}
  .filter-row {{ display: flex; gap: 8px; margin-bottom: 8px; }}
  .filter-row input, .filter-row select {{
    background: #2d3748; color: #e2e8f0; border: 1px solid #4a5568;
    border-radius: 6px; padding: 4px 8px; font-size: 12px;
  }}
  .filter-row .count {{ margin-left: auto; color: #718096; font-size: 11px; }}
  .kw-cell {{ max-width: 220px; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }}
  table.heat {{ table-layout: fixed; font-size: 10px; }}
  .heat td, .heat th {{ padding: 4px 2px; text-align: center; }}
  .heat td:first-child, .heat th:first-child {{ width: 110px; text-align: left; }}
  .hcell {{ width: 38px; }}
  footer {{ text-align: center; color: #4a5568; font-size: 11px; padding: 24px; border-top: 1px solid #1e2130; margin-top: 8px; }}
</style>
</head>
<body>
<header>
  <h1>FPT OTT &mdash; Search Analytics<span class="badge">fpt_ott_searchevents_analytics.curated</span></h1>
  <div class="header-stamp">Source: curated &mdash; {window["label"]} &mdash; generated {generated}</div>
</header>
<main>

<div class="kpis">
  <div class="kpi">
    <div class="label">Total Searches</div>
    <div class="value">{fmt_k(total_searches)}</div>
    <div class="sub">all curated search events</div>
  </div>
  <div class="kpi">
    <div class="label">Distinct Keywords</div>
    <div class="value">{distinct_kw:,}</div>
    <div class="sub">unique keyword_norm in curated</div>
  </div>
  <div class="kpi">
    <div class="label">Overall Abandon Rate</div>
    <div class="value">{overall_abandon}%</div>
    <div class="sub">curated weighted average</div>
  </div>
  <div class="kpi">
    <div class="label">Top Genre</div>
    <div class="value">{top_genre['g']}</div>
    <div class="sub warn">{top_genre['c']:,} searches &bull; {top_genre['a']}% abandon</div>
  </div>
</div>

<div class="grid-2">
  <div class="card">
    <h2>Searches by Platform</h2>
    <div class="src">curated &mdash; GROUP BY platform_group &mdash; {window["label"]}</div>
    <div class="chart-wrap"><canvas id="platformChart"></canvas></div>
  </div>
  <div class="card">
    <h2>Genre Distribution</h2>
    <div class="src">curated &mdash; GROUP BY derived_genre &mdash; {window["label"]}</div>
    <div class="chart-wrap-sm"><canvas id="genreChart"></canvas></div>
    <div style="margin-top:12px">
      <table>
        <thead><tr><th>Genre</th><th>Searches</th><th>Abandon%</th></tr></thead>
        <tbody id="genreTable"></tbody>
      </table>
    </div>
  </div>
</div>

<div class="grid-3">
  <div class="card">
    <h2>Top 20 Keywords by Volume</h2>
    <div class="src">curated &mdash; COUNT(*) by keyword_norm (keyword_norm IS NOT NULL) &mdash; {window["label"]}</div>
    <table>
      <thead><tr>
        <th class="rank">#</th><th>Keyword</th><th>Genre</th>
        <th>Searches</th><th>Abandon%</th><th class="bar-cell"></th>
      </tr></thead>
      <tbody id="keywordsTable"></tbody>
    </table>
  </div>
  <div class="card">
    <h2>Platform Abandon Rates</h2>
    <div class="src">curated &mdash; abandon_rate by platform &mdash; {window["label"]}</div>
    <div class="chart-wrap" style="height:280px"><canvas id="abandonChart"></canvas></div>
  </div>
</div>

<div style="font-size:13px;color:#718096;text-transform:uppercase;letter-spacing:.05em;margin:8px 0 14px;font-weight:600">
  Business Questions &mdash; stakeholder reports
</div>

<div class="card" style="margin-bottom:24px">
  <h2>Trending Keywords</h2>
  <div class="src">curated &mdash; {trending_meta["caption"]} &mdash; {trending_meta["mode"]}</div>
  <div class="filter-row">
    <input id="trFilter" placeholder="filter keyword..." />
    <select id="trGenre"><option value="">all genres</option>{"".join(f'<option value="{g}">{g}</option>' for g in genres_in_order)}</select>
    <div class="count" id="trCount"></div>
  </div>
  <div class="scroll-wrap">
    <table>
      <thead><tr><th class="rank">#</th><th>Keyword</th><th>Genre</th><th>Searches</th><th>Growth</th></tr></thead>
      <tbody id="trBody">{trending_rows_html}</tbody>
    </table>
  </div>
</div>

<div class="card" style="margin-bottom:24px">
  <h2>Content Gaps &mdash; Top Abandoned Titles</h2>
  <div class="src">curated &mdash; abandon rate by keyword (excludes UNKNOWN+EMPTY_QUERY, HAVING abandoned &ge; 5) &mdash; {window["label"]}</div>
  <div class="filter-row">
    <input id="cgFilter" placeholder="filter keyword..." />
    <select id="cgGenre"><option value="">all genres</option>{"".join(f'<option value="{g}">{g}</option>' for g in genres_in_order if g not in ("UNKNOWN","EMPTY_QUERY"))}</select>
    <div class="count" id="cgCount"></div>
  </div>
  <div class="scroll-wrap">
    <table>
      <thead><tr><th class="rank">#</th><th>Keyword</th><th>Genre</th><th>Searches</th><th>Abandoned</th><th>Abandon%</th></tr></thead>
      <tbody id="cgBody">{cg_rows_html}</tbody>
    </table>
  </div>
</div>

<div class="grid-2">
  <div class="card">
    <h2>Premium vs Free Demand by Genre</h2>
    <div class="src">curated &mdash; has_premium share per genre &mdash; {window["label"]}</div>
    <table>
      <thead><tr><th>Genre</th><th>Searches</th><th>Premium</th><th>Premium%</th><th class="bar-cell"></th></tr></thead>
      <tbody>{premium_rows_html}</tbody>
    </table>
  </div>
  <div class="card">
    <h2>Repeat Search Rate by Genre</h2>
    <div class="src">curated &mdash; is_repeat_search share per genre &mdash; {window["label"]}</div>
    <table>
      <thead><tr><th>Genre</th><th>Searches</th><th>Repeat</th><th>Repeat%</th><th class="bar-cell"></th></tr></thead>
      <tbody>{repeat_rows_html}</tbody>
    </table>
  </div>
</div>

<div class="card" style="margin-bottom:24px">
  <h2>Guest vs Authenticated Demand by Genre</h2>
  <div class="src">curated &mdash; guest (unauthenticated) share per genre (excludes UNKNOWN+EMPTY_QUERY) &mdash; {window["label"]}</div>
  <table>
    <thead><tr><th>Genre</th><th>Searches</th><th>Guest</th><th>Guest%</th><th class="bar-cell"></th></tr></thead>
    <tbody>{guest_rows_html}</tbody>
  </table>
</div>

<div class="card">
  <h2>Search Volume by Hour &times; Genre (Vietnam Time)</h2>
  <div class="src">curated &mdash; COUNT(*) by hour_of_day_vn &times; derived_genre, per-genre % shown in cell &mdash; {window["label"]}</div>
  <table class="heat">
    <thead><tr>{heat_thead}</tr></thead>
    <tbody>{heat_body}</tbody>
  </table>
</div>

</main>
<footer>
  Source: <code>fpt_ott_searchevents_analytics.curated</code> ({total_searches:,} search events) &nbsp;&bull;&nbsp;
  Renderer: <code>sdlf-ott-mainTR-report</code> Lambda (write_dashboard) &nbsp;&bull;&nbsp;
  Hosted: S3 + CloudFront (sdlf-pipeline-ott-dashboard) &nbsp;&bull;&nbsp;
  Window: {window["label"]} &nbsp;&bull;&nbsp;
  Generated: {generated}
</footer>

<script>
const GENRE_COLORS = {{
  PHIM_VIET:'#68d391', ANIME:'#9f7aea', PHIM_TRUNG:'#f6ad55',
  PHIM_HAN:'#76e4f7', PHIM_AU_MY:'#fc8181', TRUYEN_HINH:'#fbd38d',
  UNKNOWN:'#4a5568', NHAC:'#b794f4', THE_THAO:'#4299e1', EMPTY_QUERY:'#718096',
}};
const GENRE_LABELS = {{
  PHIM_VIET:'Phim Viet', UNKNOWN:'Unknown', PHIM_TRUNG:'Phim Trung',
  ANIME:'Anime', PHIM_HAN:'Phim Han', PHIM_AU_MY:'Phim Au My',
  TRUYEN_HINH:'Truyen Hinh', NHAC:'Nhac', THE_THAO:'The Thao', EMPTY_QUERY:'(empty)',
}};
Chart.defaults.color = '#a0aec0';
Chart.defaults.font = {{ family: "'Segoe UI', sans-serif", size: 12 }};

const platform = {json.dumps(plat, ensure_ascii=False)};
const byGenre = {json.dumps(byg, ensure_ascii=False)};
const topKeywords = {json.dumps(topk, ensure_ascii=False)};

const fmt = n => n >= 1e6 ? (n/1e6).toFixed(2)+'M' : n >= 1e3 ? (n/1e3).toFixed(1)+'K' : n;

new Chart(document.getElementById('platformChart'), {{
  type: 'bar',
  data: {{
    labels: platform.map(p => p.p),
    datasets: [{{
      label: 'Searches', data: platform.map(p => p.c),
      backgroundColor: ['#4299e1','#9f7aea','#68d391','#f6ad55','#fc8181','#fbd38d'],
      borderRadius: 4,
    }}]
  }},
  options: {{
    responsive: true, maintainAspectRatio: false,
    plugins: {{
      legend: {{ display: false }},
      tooltip: {{ callbacks: {{ afterLabel: ctx => `Abandon: ${{platform[ctx.dataIndex].a}}%` }} }}
    }},
    scales: {{
      x: {{ grid: {{ color: '#2d3748' }} }},
      y: {{ grid: {{ color: '#2d3748' }}, title: {{ display: true, text: 'Search Count' }} }},
    }}
  }}
}});

new Chart(document.getElementById('genreChart'), {{
  type: 'doughnut',
  data: {{
    labels: byGenre.map(g => GENRE_LABELS[g.g] || g.g),
    datasets: [{{
      data: byGenre.map(g => g.c),
      backgroundColor: byGenre.map(g => GENRE_COLORS[g.g] || '#718096'),
      borderWidth: 0, hoverOffset: 6
    }}]
  }},
  options: {{
    responsive: true, maintainAspectRatio: false, cutout: '65%',
    plugins: {{ legend: {{ position: 'right', labels: {{ boxWidth: 10, padding: 6, font: {{ size: 11 }} }} }} }}
  }}
}});

new Chart(document.getElementById('abandonChart'), {{
  type: 'bar',
  data: {{
    labels: platform.map(p => p.p),
    datasets: [{{
      label: 'Abandon Rate %', data: platform.map(p => p.a),
      backgroundColor: platform.map(p => p.a > 10 ? '#fc8181' : p.a > 5 ? '#f6ad55' : '#68d391'),
      borderRadius: 4,
    }}]
  }},
  options: {{
    responsive: true, maintainAspectRatio: false, indexAxis: 'y',
    plugins: {{ legend: {{ display: false }} }},
    scales: {{
      x: {{ grid: {{ color: '#2d3748' }}, title: {{ display: true, text: 'Abandon %' }}, max: 20 }},
      y: {{ grid: {{ display: false }} }},
    }}
  }}
}});

const genreTableEl = document.getElementById('genreTable');
byGenre.forEach(g => {{
  const tr = document.createElement('tr');
  const color = GENRE_COLORS[g.g] || '#718096';
  tr.innerHTML = `
    <td><span class="genre-pill" style="background:${{color}}22;color:${{color}};border:1px solid ${{color}}44">${{GENRE_LABELS[g.g]||g.g}}</span></td>
    <td>${{fmt(g.c)}}</td>
    <td style="color:${{g.a>10?'#fc8181':g.a>5?'#f6ad55':'#68d391'}}">${{g.a}}%</td>`;
  genreTableEl.appendChild(tr);
}});

const maxC = topKeywords[0] ? topKeywords[0].c : 1;
const kwTableEl = document.getElementById('keywordsTable');
topKeywords.forEach((kw, i) => {{
  const barW = Math.round((kw.c / maxC) * 100);
  const color = kw.a > 20 ? '#fc8181' : kw.a > 10 ? '#f6ad55' : '#68d391';
  const barColor = kw.a > 20 ? '#fc8181' : kw.a > 10 ? '#f6ad55' : '#4299e1';
  const tr = document.createElement('tr');
  const gc = GENRE_COLORS[kw.g] || '#718096';
  tr.innerHTML = `
    <td class="rank">${{i+1}}</td>
    <td class="kw-cell" title="${{kw.k}}">${{kw.k}}</td>
    <td><span class="genre-pill" style="background:${{gc}}22;color:${{gc}};border:1px solid ${{gc}}44">${{GENRE_LABELS[kw.g]||kw.g}}</span></td>
    <td>${{fmt(kw.c)}}</td>
    <td style="color:${{color}}">${{kw.a}}%</td>
    <td class="bar-cell"><div class="mini-bar"><div class="mini-bar-fill" style="width:${{barW}}%;background:${{barColor}}"></div></div></td>`;
  kwTableEl.appendChild(tr);
}});

function bindFilter(bodyId, kwId, genreId, countId, totalRows) {{
  const body = document.getElementById(bodyId);
  const kwIn = document.getElementById(kwId);
  const gIn = document.getElementById(genreId);
  const cnt = document.getElementById(countId);
  const apply = () => {{
    const q = (kwIn.value || '').toLowerCase();
    const g = gIn.value || '';
    let visible = 0;
    body.querySelectorAll('tr').forEach(tr => {{
      const kw = tr.dataset.kw || '';
      const tg = tr.dataset.genre || '';
      const show = (!q || kw.includes(q)) && (!g || tg === g);
      tr.style.display = show ? '' : 'none';
      if (show) visible++;
    }});
    cnt.textContent = `${{visible}} / ${{totalRows}} rows`;
  }};
  kwIn.addEventListener('input', apply);
  gIn.addEventListener('change', apply);
  apply();
}}
bindFilter('trBody', 'trFilter', 'trGenre', 'trCount', {len(trending_rows)});
bindFilter('cgBody', 'cgFilter', 'cgGenre', 'cgCount', {len(content_gaps)});
</script>
</body>
</html>
"""


def write_dashboard(trending_rows, trending_meta):
    """Render the centralized search-analytics dashboard and publish to S3.

    Single user-facing surface (the API is retired). All sections are sourced
    from the curated table; every section is stamped with the dt window it
    actually aggregated. Nine concurrent Athena queries — wall-clock time is
    the slowest query, not their sum.
    """
    bucket = os.environ.get("DASHBOARD_BUCKET", "")
    if not bucket:
        logger.warning("DASHBOARD_BUCKET not set — skipping dashboard write")
        return 0

    qids = {name: athena_start(sql) for name, sql in _dashboard_sql().items()}
    data = {name: athena_collect(qid) for name, qid in qids.items()}
    window = _derive_window(data["dt_window"])

    html = _render_dashboard_html(data, window, trending_rows, trending_meta)
    # Report the encoded byte count, not len(html) — the latter counts Python
    # str characters, which undercounts by ~8 KB because Vietnamese diacritics
    # are 2-byte UTF-8 sequences. The S3 object size is the encoded length.
    body = html.encode("utf-8")
    s3.put_object(
        Bucket=bucket,
        Key="index.html",
        Body=body,
        ContentType="text/html; charset=utf-8",
        CacheControl="public, max-age=300",
    )
    logger.info(f"Dashboard written -> s3://{bucket}/index.html ({len(body)} bytes, "
                f"window={window['label']})")
    return len(body)


def lambda_handler(event, context):
    """Pipeline entry: run trending + every dashboard query, render index.html.

    Triggered by the DQ state machine SUCCEEDED event. The Lambda is the
    single renderer of the user-facing dashboard — no CSVs, no parallel API.
    """
    logger.info(
        f"Dashboard refresh triggered — source: {event.get('source', '?')} "
        f"detail-type: {event.get('detail-type', '?')}"
    )
    ref = event.get("reference_date")
    today = date.fromisoformat(ref) if ref else latest_dt()
    cur_start  = str(today - timedelta(days=7))
    cur_end    = str(today)
    base_start = str(today - timedelta(days=35))
    base_end   = str(today - timedelta(days=28))
    dt = str(today)

    use_fallback = not has_baseline_data(base_start, base_end)
    if use_fallback:
        logger.warning("No baseline data found (< 4 weeks of pipeline runs) — volume-only fallback")
        trending_caption = (f"top keywords by volume in {cur_start} → {cur_end} "
                            f"(insufficient history for growth comparison)")
        mode_label = "fallback / volume-only"
    else:
        trending_caption = (f"current window {cur_start} → {cur_end} vs "
                            f"baseline {base_start} → {base_end}")
        mode_label = f"growth ≥ {MIN_GROWTH}x"

    errors = 0
    trending_rows = []
    try:
        trending_rows = run_trending_query(cur_start, cur_end, base_start, base_end, use_fallback)
    except Exception as e:
        logger.error(f"Trending query failed: {e}")
        errors += 1

    bytes_written = 0
    try:
        bytes_written = write_dashboard(
            trending_rows,
            {"caption": trending_caption, "mode": mode_label,
             "cur_start": cur_start, "cur_end": cur_end,
             "base_start": base_start, "base_end": base_end},
        )
    except Exception as e:
        logger.error(f"Dashboard write failed: {e}")
        errors += 1

    message = (
        f"OTT Search-Analytics Dashboard Refresh — {dt}\n"
        f"Mode: {mode_label}\n"
        f"Trending rows rendered: {len(trending_rows)}\n"
        f"Dashboard HTML bytes:   {bytes_written}\n"
        f"Errors: {errors}\n"
        f"Dashboard: {os.environ.get('DASHBOARD_URL', '(SSM /sdlf/pipeline/rDashboardUrl/ott)')}"
    )
    sns.publish(
        TopicArn=SNS_TOPIC_ARN,
        Subject=f"OTT Dashboard Refresh {dt}",
        Message=message,
    )
    result = {
        "dt": dt,
        "mode": mode_label,
        "trending_rows": len(trending_rows),
        "dashboard_bytes": bytes_written,
        "errors": errors,
    }
    logger.info(f"Dashboard refresh complete — trending:{len(trending_rows)} "
                f"bytes:{bytes_written} errors:{errors} mode:{mode_label}")
    return result
