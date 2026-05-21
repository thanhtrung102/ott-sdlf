import csv
import io
import json
import os
import time
from datetime import date, datetime, timedelta, timezone

import boto3
from datalake_library.commons import init_logger

logger = init_logger(__name__)

athena  = boto3.client("athena")
s3      = boto3.client("s3")
sns     = boto3.client("sns")
events  = boto3.client("events")

DB               = os.environ["ATHENA_DATABASE"]
RESULTS          = os.environ["ATHENA_RESULTS"]
STAGE_BUCKET     = os.environ["STAGE_BUCKET"]
ANALYTICS_PREFIX = os.environ["ANALYTICS_PREFIX"]
SNS_TOPIC_ARN    = os.environ["SNS_TOPIC_ARN"]
MIN_GROWTH       = float(os.environ["MIN_GROWTH_MULTIPLIER"])
MIN_VOLUME       = int(os.environ["MIN_SEARCH_VOLUME"])
MAX_TRENDING_ROWS = int(os.environ.get("MAX_TRENDING_ROWS", "1000"))
# IAM policy scopes athena:StartQueryExecution to workgroup/sdlf-ott; queries
# without a WorkGroup default to 'primary' and fail with AccessDenied.
WORKGROUP        = os.environ.get("ATHENA_WORKGROUP", "sdlf-ott")

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
{genre_filter}
ORDER BY c.current_cnt DESC
LIMIT {max_rows}
"""

_FALLBACK_SQL = """
SELECT keyword_norm, derived_genre, COUNT(*) AS current_cnt,
       0 AS baseline_cnt, NULL AS growth_multiplier, true AS is_new_keyword
FROM {db}.curated
WHERE dt >= '{cur_start}' AND dt < '{cur_end}' AND keyword_norm IS NOT NULL AND keyword_norm != ''
{genre_filter}
GROUP BY keyword_norm, derived_genre
HAVING COUNT(*) >= {min_volume}
ORDER BY current_cnt DESC
LIMIT {max_rows}
"""

_BASELINE_CHECK_SQL = """
SELECT COUNT(*) AS cnt FROM {db}.curated
WHERE dt >= '{base_start}' AND dt < '{base_end}' AND keyword_norm IS NOT NULL AND keyword_norm != ''
"""

# Gold-layer CTAS: per-keyword x platform x genre x date rankings.
# Written to staging first; swapped to production only after row count validates.
# Prevents an empty/partial gold table if the CTAS fails mid-write.
_GOLD_CTAS_SQL = """
CREATE TABLE {table_name}
WITH (
    format              = 'PARQUET',
    parquet_compression = 'SNAPPY',
    external_location   = '{gold_location}',
    partitioned_by      = ARRAY['trend_date']
)
AS
WITH agg AS (
    SELECT
        keyword_norm,
        platform_group,
        derived_genre,
        dt                                                                 AS trend_date,
        CAST(COUNT(*)                                           AS bigint) AS search_count,
        -- session_action is 'SUBMIT' in current ETL output; older partitions
        -- used the raw 'enter'. Match both so enter_count is correct across a
        -- mixed-vintage curated table and after a full re-classification run.
        CAST(SUM(CASE WHEN session_action IN ('SUBMIT', 'enter') THEN 1 ELSE 0 END)
                                                                AS bigint) AS enter_count,
        CAST(SUM(CASE WHEN is_search_abandoned THEN 1 ELSE 0 END)
                                                                AS bigint) AS abandoned_count,
        ROUND(CAST(SUM(CASE WHEN is_search_abandoned THEN 1 ELSE 0 END) AS double)
              / CAST(COUNT(*) AS double) * 100, 2)                        AS abandon_rate_pct,
        CAST(COUNT(DISTINCT user_id_hashed)                     AS bigint) AS unique_users
    FROM {curated_db}.curated
    WHERE keyword_norm IS NOT NULL AND keyword_norm != ''
    GROUP BY keyword_norm, platform_group, derived_genre, dt
    HAVING COUNT(*) >= {min_volume}
),
ranked AS (
    SELECT *,
        CAST(RANK() OVER (
            PARTITION BY platform_group, derived_genre, trend_date
            ORDER BY search_count DESC
        ) AS integer) AS rank_today
    FROM agg
),
prev_agg AS (
    SELECT
        keyword_norm,
        platform_group,
        derived_genre,
        trend_date                                                         AS trend_date_7d,
        CAST(RANK() OVER (
            PARTITION BY platform_group, derived_genre, trend_date
            ORDER BY search_count DESC
        ) AS integer) AS rank_prev
    FROM agg
),
prev_best AS (
    SELECT
        r.trend_date                                                       AS for_date,
        p.keyword_norm,
        p.platform_group,
        p.derived_genre,
        p.rank_prev,
        ROW_NUMBER() OVER (
            PARTITION BY r.trend_date, p.keyword_norm, p.platform_group, p.derived_genre
            ORDER BY ABS(date_diff('day', date(p.trend_date_7d), date(r.trend_date)) - 7)
        )                                                                  AS rn
    FROM ranked r
    JOIN prev_agg p
      ON r.keyword_norm   = p.keyword_norm
     AND r.platform_group = p.platform_group
     AND r.derived_genre  = p.derived_genre
     AND date_diff('day', date(p.trend_date_7d), date(r.trend_date)) BETWEEN 5 AND 9
)
SELECT
    r.keyword_norm,
    r.platform_group,
    r.derived_genre,
    r.search_count,
    r.enter_count,
    r.abandoned_count,
    r.abandon_rate_pct,
    r.unique_users,
    r.rank_today,
    p.rank_prev                                                            AS rank_7d_ago,
    CASE WHEN p.rank_prev IS NULL THEN true ELSE false END                 AS is_new_entrant,
    CASE WHEN p.rank_prev IS NULL THEN NULL
         ELSE p.rank_prev - r.rank_today END                              AS rank_improvement,
    r.trend_date
FROM ranked r
LEFT JOIN prev_best p
       ON p.for_date      = r.trend_date
      AND p.keyword_norm   = r.keyword_norm
      AND p.platform_group = r.platform_group
      AND p.derived_genre  = r.derived_genre
      AND p.rn             = 1
"""


def athena_query(sql):
    r = athena.start_query_execution(
        QueryString=sql,
        QueryExecutionContext={"Database": DB},
        ResultConfiguration={"OutputLocation": RESULTS},
        WorkGroup=WORKGROUP,
    )
    qid = r["QueryExecutionId"]
    while True:
        st = athena.get_query_execution(QueryExecutionId=qid)["QueryExecution"]["Status"]["State"]
        if st == "SUCCEEDED":
            break
        if st in ("FAILED", "CANCELLED"):
            raise RuntimeError(f"Athena query {st}: {qid}")
        time.sleep(3)
    headers, rows = None, []
    for page in athena.get_paginator("get_query_results").paginate(QueryExecutionId=qid):
        for row in page["ResultSet"]["Rows"]:
            values = [col.get("VarCharValue", "") for col in row["Data"]]
            if headers is None:
                headers = values
                continue
            rows.append(dict(zip(headers, values)))
    return headers or [], rows


def athena_ddl(sql):
    """Run a DDL statement (no result rows expected)."""
    r = athena.start_query_execution(
        QueryString=sql,
        ResultConfiguration={"OutputLocation": RESULTS},
        WorkGroup=WORKGROUP,
    )
    qid = r["QueryExecutionId"]
    while True:
        st = athena.get_query_execution(QueryExecutionId=qid)["QueryExecution"]["Status"]["State"]
        if st == "SUCCEEDED":
            return
        if st in ("FAILED", "CANCELLED"):
            raise RuntimeError(f"DDL {st}: {qid}")
        time.sleep(3)


def write_csv(key, headers, rows):
    buf = io.StringIO()
    writer = csv.DictWriter(buf, fieldnames=headers)
    writer.writeheader()
    writer.writerows(rows)
    s3.put_object(
        Bucket=STAGE_BUCKET,
        Key=key,
        Body=buf.getvalue().encode("utf-8"),
        ContentType="text/csv",
    )
    logger.info(f"Wrote {len(rows)} rows -> s3://{STAGE_BUCKET}/{key}")
    return len(rows)


def latest_dt():
    """Return the most recent dt partition in the curated table."""
    _, rows = athena_query(f"SELECT MAX(dt) AS max_dt FROM {DB}.curated")
    val = rows[0].get("max_dt", "") if rows else ""
    if not val:
        raise RuntimeError("curated table has no data — cannot determine reference date")
    return date.fromisoformat(val)


def has_baseline_data(base_start, base_end):
    sql = _BASELINE_CHECK_SQL.format(db=DB, base_start=base_start, base_end=base_end)
    _, rows = athena_query(sql)
    return rows and int(rows[0].get("cnt", 0)) > 0


def run_trending_query(cur_start, cur_end, base_start, base_end, use_fallback, genre_filter=""):
    if use_fallback:
        sql = _FALLBACK_SQL.format(
            db=DB, cur_start=cur_start, cur_end=cur_end,
            min_volume=MIN_VOLUME, genre_filter=genre_filter,
            max_rows=MAX_TRENDING_ROWS,
        )
    else:
        sql = _TRENDING_SQL.format(
            db=DB, cur_start=cur_start, cur_end=cur_end,
            base_start=base_start, base_end=base_end,
            min_volume=MIN_VOLUME, min_growth=MIN_GROWTH,
            genre_filter=genre_filter,
            max_rows=MAX_TRENDING_ROWS,
        )
    return athena_query(sql)


def _list_s3_keys(s3_url):
    bucket, prefix = s3_url.replace("s3://", "").split("/", 1)
    keys = set()
    for page in s3.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            keys.add(obj["Key"])
    return bucket, prefix, keys


def _empty_s3_prefix(s3_url):
    bucket, _, keys = _list_s3_keys(s3_url)
    if keys:
        batch = [{"Key": k} for k in keys]
        for i in range(0, len(batch), 1000):
            s3.delete_objects(Bucket=bucket, Delete={"Objects": batch[i:i+1000]})


def _copy_s3_prefix(src_url, dst_url):
    """Copy src→dst, return set of destination keys written."""
    src_bucket, src_prefix, _ = _list_s3_keys(src_url)
    dst_bucket, dst_prefix = dst_url.replace("s3://", "").split("/", 1)
    written = set()
    for page in s3.get_paginator("list_objects_v2").paginate(Bucket=src_bucket, Prefix=src_prefix):
        for obj in page.get("Contents", []):
            src_key = obj["Key"]
            dst_key = dst_prefix + src_key[len(src_prefix):]
            s3.copy_object(
                CopySource={"Bucket": src_bucket, "Key": src_key},
                Bucket=dst_bucket,
                Key=dst_key,
            )
            written.add(dst_key)
    return dst_bucket, written


def _ctas(table_name, location):
    athena_ddl(_GOLD_CTAS_SQL.format(
        table_name=table_name,
        gold_location=location,
        curated_db=DB,
        min_volume=MIN_VOLUME,
    ))


def write_gold_table():
    gold_db          = os.environ.get("GOLD_DATABASE", "fpt_ott_searchevents_gold")
    gold_location    = os.environ.get("GOLD_LOCATION", "")
    if not gold_location:
        logger.warning("GOLD_LOCATION not set — skipping gold table write")
        return 0

    prod_table       = f"{gold_db}.keyword_trends"
    staging_table    = f"{gold_db}.keyword_trends_staging"
    staging_location = gold_location.rstrip("/") + "_staging/"

    # Phase 1: CTAS to staging — production table untouched until validated.
    athena_ddl(f"DROP TABLE IF EXISTS {staging_table}")
    _empty_s3_prefix(staging_location)
    _ctas(staging_table, staging_location)
    _, cnt_rows = athena_query(f"SELECT COUNT(*) AS cnt FROM {staging_table}")
    count = int(cnt_rows[0]["cnt"]) if cnt_rows else 0
    if count == 0:
        athena_ddl(f"DROP TABLE IF EXISTS {staging_table}")
        _empty_s3_prefix(staging_location)
        raise RuntimeError("Staging CTAS produced 0 rows — gold table unchanged")

    # Phase 2: Atomic swap — copy first, prune stale after.
    # keyword_trends is a CFN-managed table with partition projection, so
    # Athena discovers partitions from S3 automatically. The old pattern
    # "empty production then copy" left a window where the gold table was
    # empty mid-flight. New pattern: enumerate existing keys, copy all new
    # keys (overwriting matching paths in place — each PutObject is atomic),
    # then delete only the existing keys that the copy didn't replace.
    _, _, before_keys = _list_s3_keys(gold_location)
    dst_bucket, written_keys = _copy_s3_prefix(staging_location, gold_location)
    stale = before_keys - written_keys
    if stale:
        batch = [{"Key": k} for k in stale]
        for i in range(0, len(batch), 1000):
            s3.delete_objects(Bucket=dst_bucket, Delete={"Objects": batch[i:i+1000]})

    # Phase 3: Cleanup staging.
    athena_ddl(f"DROP TABLE IF EXISTS {staging_table}")
    _empty_s3_prefix(staging_location)

    logger.info(f"Gold table written: {count} rows")
    return count


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


def _render_dashboard_html(totals, platform, genre, keywords, trending,
                           content_gaps, premium, repeat_kw, guest, hourly):
    """Render the centralized OTT search-analytics dashboard from curated.

    One stakeholder-facing page. Every figure is population-true — sourced
    from the unfiltered curated table. Sections: volume/genre/platform KPIs,
    plus the five content-gap business reports and trending. The gold
    keyword_trends table is a separate ranking product behind GET /trending.
    """
    total_searches  = int(totals.get("total", 0) or 0)
    distinct_kw     = int(totals.get("kw", 0) or 0)
    overall_abandon = float(totals.get("abandon", 0) or 0)
    max_dt          = totals.get("max_dt", "") or ""
    month_label = "unknown"
    if max_dt:
        try:
            month_label = date.fromisoformat(max_dt).strftime("%b %Y")
        except ValueError:
            pass

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

    # ---- business-question sections, rendered server-side ----
    trending_rows_html = ""
    for i, r in enumerate(trending):
        gm = r.get("growth_multiplier") or ""
        is_new = str(r.get("is_new_keyword", "")).lower() == "true"
        growth = (f"{gm}&times;" if gm
                  else ("<span style='color:#68d391'>new</span>" if is_new else "&mdash;"))
        kw = _esc(r.get("keyword_norm", ""))
        trending_rows_html += (
            f'<tr><td class="rank">{i + 1}</td>'
            f'<td style="max-width:220px;overflow:hidden;text-overflow:ellipsis;'
            f'white-space:nowrap" title="{kw}">{kw}</td>'
            f'<td>{_genre_pill(r.get("derived_genre", ""))}</td>'
            f'<td>{_int(r, "current_cnt"):,}</td><td>{growth}</td></tr>'
        )
    trending_rows_html = trending_rows_html or '<tr><td colspan="5">No data</td></tr>'

    cg_rows_html = ""
    for i, r in enumerate(content_gaps):
        kw = _esc(r.get("keyword_norm", ""))
        cg_rows_html += (
            f'<tr><td class="rank">{i + 1}</td>'
            f'<td style="max-width:220px;overflow:hidden;text-overflow:ellipsis;'
            f'white-space:nowrap" title="{kw}">{kw}</td>'
            f'<td>{_genre_pill(r.get("derived_genre", ""))}</td>'
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
    hours_json = json.dumps([
        {"h": int(r["hour_of_day_vn"]), "c": int(r["c"])}
        for r in hourly if str(r.get("hour_of_day_vn", "")).strip() != ""
    ])

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
  header {{ background: #1a1d27; border-bottom: 1px solid #2d3748; padding: 16px 32px; display: flex; align-items: center; gap: 12px; }}
  header h1 {{ font-size: 18px; font-weight: 600; color: #f7fafc; }}
  .badge {{ background: #2d3748; color: #68d391; font-size: 11px; padding: 2px 8px; border-radius: 9999px; border: 1px solid #276749; }}
  .source-tag {{ font-size: 11px; color: #718096; margin-left: auto; }}
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
  .card .src {{ font-size: 11px; color: #4a5568; margin-bottom: 14px; font-family: monospace; }}
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
  footer {{ text-align: center; color: #4a5568; font-size: 11px; padding: 24px; border-top: 1px solid #1e2130; margin-top: 8px; }}
</style>
</head>
<body>
<header>
  <h1>FPT OTT &mdash; Search Analytics</h1>
  <span class="badge">fpt_ott_searchevents_analytics.curated</span>
  <span class="source-tag">{month_label} &nbsp;|&nbsp; {distinct_kw:,} keywords &nbsp;|&nbsp; {len(plat)} platforms &nbsp;|&nbsp; generated {generated}</span>
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
    <div class="src">fpt_ott_searchevents_analytics.curated &mdash; GROUP BY platform_group</div>
    <div class="chart-wrap"><canvas id="platformChart"></canvas></div>
  </div>
  <div class="card">
    <h2>Genre Distribution</h2>
    <div class="src">fpt_ott_searchevents_analytics.curated &mdash; GROUP BY derived_genre</div>
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
    <div class="src">fpt_ott_searchevents_analytics.curated &mdash; COUNT(*) by keyword_norm</div>
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
    <div class="src">fpt_ott_searchevents_analytics.curated &mdash; abandon_rate by platform</div>
    <div class="chart-wrap" style="height:280px"><canvas id="abandonChart"></canvas></div>
  </div>
</div>

<div style="font-size:13px;color:#718096;text-transform:uppercase;letter-spacing:.05em;margin:8px 0 14px;font-weight:600">
  Business Questions &mdash; stakeholder reports
</div>

<div class="grid-2">
  <div class="card">
    <h2>Trending Keywords &mdash; Last 7 Days</h2>
    <div class="src">fpt_ott_searchevents_analytics.curated &mdash; 7-day window vs 4-week baseline</div>
    <table>
      <thead><tr><th class="rank">#</th><th>Keyword</th><th>Genre</th><th>Searches</th><th>Growth</th></tr></thead>
      <tbody>{trending_rows_html}</tbody>
    </table>
  </div>
  <div class="card">
    <h2>Content Gaps &mdash; Top Abandoned Titles</h2>
    <div class="src">curated &mdash; abandon rate by keyword, HAVING abandoned &ge; 5</div>
    <table>
      <thead><tr><th class="rank">#</th><th>Keyword</th><th>Genre</th><th>Searches</th><th>Abandoned</th><th>Abandon%</th></tr></thead>
      <tbody>{cg_rows_html}</tbody>
    </table>
  </div>
</div>

<div class="grid-2">
  <div class="card">
    <h2>Premium vs Free Demand by Genre</h2>
    <div class="src">curated &mdash; has_premium share per genre</div>
    <table>
      <thead><tr><th>Genre</th><th>Searches</th><th>Premium</th><th>Premium%</th><th class="bar-cell"></th></tr></thead>
      <tbody>{premium_rows_html}</tbody>
    </table>
  </div>
  <div class="card">
    <h2>Repeat Search Rate by Genre</h2>
    <div class="src">curated &mdash; is_repeat_search share per genre</div>
    <table>
      <thead><tr><th>Genre</th><th>Searches</th><th>Repeat</th><th>Repeat%</th><th class="bar-cell"></th></tr></thead>
      <tbody>{repeat_rows_html}</tbody>
    </table>
  </div>
</div>

<div class="grid-2">
  <div class="card">
    <h2>Guest vs Authenticated Demand by Genre</h2>
    <div class="src">curated &mdash; guest (unauthenticated) share per genre</div>
    <table>
      <thead><tr><th>Genre</th><th>Searches</th><th>Guest</th><th>Guest%</th><th class="bar-cell"></th></tr></thead>
      <tbody>{guest_rows_html}</tbody>
    </table>
  </div>
  <div class="card">
    <h2>Search Volume by Hour (Vietnam Time)</h2>
    <div class="src">curated &mdash; COUNT(*) by hour_of_day_vn</div>
    <div class="chart-wrap"><canvas id="hourlyChart"></canvas></div>
  </div>
</div>

</main>
<footer>
  Source: <code>fpt_ott_searchevents_analytics.curated</code> ({total_searches:,} search events) &nbsp;&bull;&nbsp;
  Produced by: <code>sdlf-ott-mainTR-report</code> Lambda (write_dashboard) &nbsp;&bull;&nbsp;
  Hosted: S3 + CloudFront (sdlf-pipeline-ott-dashboard) &nbsp;&bull;&nbsp;
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
    <td style="max-width:200px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap" title="${{kw.k}}">${{kw.k}}</td>
    <td><span class="genre-pill" style="background:${{gc}}22;color:${{gc}};border:1px solid ${{gc}}44">${{GENRE_LABELS[kw.g]||kw.g}}</span></td>
    <td>${{fmt(kw.c)}}</td>
    <td style="color:${{color}}">${{kw.a}}%</td>
    <td class="bar-cell"><div class="mini-bar"><div class="mini-bar-fill" style="width:${{barW}}%;background:${{barColor}}"></div></div></td>`;
  kwTableEl.appendChild(tr);
}});

const hourly = {hours_json};
new Chart(document.getElementById('hourlyChart'), {{
  type: 'bar',
  data: {{
    labels: hourly.map(h => h.h + 'h'),
    datasets: [{{
      label: 'Searches', data: hourly.map(h => h.c),
      backgroundColor: '#4299e1', borderRadius: 3,
    }}]
  }},
  options: {{
    responsive: true, maintainAspectRatio: false,
    plugins: {{ legend: {{ display: false }} }},
    scales: {{
      x: {{ grid: {{ display: false }} }},
      y: {{ grid: {{ color: '#2d3748' }}, title: {{ display: true, text: 'Search Count' }} }},
    }}
  }}
}});
</script>
</body>
</html>
"""


_ABANDON_EXPR = ("ROUND(100.0 * SUM(CASE WHEN is_search_abandoned THEN 1 ELSE 0 END)"
                 " / COUNT(*), 1)")


def _dashboard_sql():
    """Every dashboard section's query, keyed by section. Run concurrently."""
    ab = _ABANDON_EXPR
    return {
        "totals": (f"SELECT COUNT(*) AS total, COUNT(DISTINCT keyword_norm) AS kw, "
                   f"{ab} AS abandon, MAX(dt) AS max_dt FROM {DB}.curated"),
        "platform": (f"SELECT platform_group, COUNT(*) AS c, {ab} AS a "
                     f"FROM {DB}.curated GROUP BY platform_group ORDER BY c DESC"),
        "genre": (f"SELECT derived_genre, COUNT(*) AS c, {ab} AS a "
                  f"FROM {DB}.curated GROUP BY derived_genre ORDER BY c DESC"),
        "keywords": (f"SELECT keyword_norm, derived_genre, COUNT(*) AS c, {ab} AS a "
                     f"FROM {DB}.curated WHERE keyword_norm IS NOT NULL AND keyword_norm != '' "
                     f"GROUP BY keyword_norm, derived_genre ORDER BY c DESC LIMIT 20"),
        "content_gaps": (
            f"SELECT keyword_norm, derived_genre, COUNT(*) AS searches, "
            f"SUM(CASE WHEN is_search_abandoned THEN 1 ELSE 0 END) AS abandoned, "
            f"{ab} AS abandon_rate_pct FROM {DB}.curated "
            f"WHERE derived_genre NOT IN ('UNKNOWN', 'EMPTY_QUERY') "
            f"AND keyword_norm IS NOT NULL AND keyword_norm != '' "
            f"GROUP BY keyword_norm, derived_genre "
            f"HAVING SUM(CASE WHEN is_search_abandoned THEN 1 ELSE 0 END) >= 5 "
            f"ORDER BY abandon_rate_pct DESC, abandoned DESC LIMIT 15"),
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
        "hourly": (f"SELECT hour_of_day_vn, COUNT(*) AS c FROM {DB}.curated "
                   f"WHERE hour_of_day_vn IS NOT NULL "
                   f"GROUP BY hour_of_day_vn ORDER BY hour_of_day_vn"),
    }


def _collect_query(qid):
    """Block until an already-started Athena query finishes; return list of dict rows."""
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


def write_dashboard(trending_rows):
    """Render the centralized search-analytics dashboard and publish to S3.

    All sections are sourced from the unfiltered curated table so every figure
    is population-true. The dashboard bucket is fronted by CloudFront
    (sdlf-pipeline-ott-dashboard); index.html is rewritten on every pipeline
    run. The nine section queries are launched concurrently — wall-clock time
    is the slowest query, not their sum, keeping this well under the timeout.
    """
    bucket = os.environ.get("DASHBOARD_BUCKET", "")
    if not bucket:
        logger.warning("DASHBOARD_BUCKET not set — skipping dashboard write")
        return 0

    qids = {
        name: athena.start_query_execution(
            QueryString=sql,
            QueryExecutionContext={"Database": DB},
            ResultConfiguration={"OutputLocation": RESULTS},
            WorkGroup=WORKGROUP,
        )["QueryExecutionId"]
        for name, sql in _dashboard_sql().items()
    }
    data = {name: _collect_query(qid) for name, qid in qids.items()}

    html = _render_dashboard_html(
        data["totals"][0] if data["totals"] else {},
        data["platform"], data["genre"], data["keywords"],
        trending_rows[:15], data["content_gaps"], data["premium"],
        data["repeat"], data["guest"], data["hourly"],
    )
    s3.put_object(
        Bucket=bucket,
        Key="index.html",
        Body=html.encode("utf-8"),
        ContentType="text/html; charset=utf-8",
        CacheControl="public, max-age=300",
    )
    logger.info(f"Dashboard written -> s3://{bucket}/index.html ({len(html)} bytes)")
    return len(html)


_SUBDIR = {"trending_all": "all", "trending_unknown": "unknown"}


def lambda_handler(event, context):
    logger.info(
        f"Trending report triggered — source: {event.get('source', '?')} "
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
        logger.warning("No baseline data found (< 4 weeks of pipeline runs) — using volume-only fallback")
    else:
        logger.info(f"Baseline window {base_start} to {base_end} has data — running growth query")

    summary, errors = {}, 0
    trending_rows = []

    for name, genre_filter in [
        ("trending_all",     ""),
        ("trending_unknown", "AND c.derived_genre = 'UNKNOWN'" if not use_fallback
                             else "AND derived_genre = 'UNKNOWN'"),
    ]:
        try:
            headers, rows = run_trending_query(
                cur_start, cur_end, base_start, base_end, use_fallback, genre_filter,
            )
            if name == "trending_all":
                trending_rows = rows
            report_prefix = f"{ANALYTICS_PREFIX}{_SUBDIR[name]}/{dt}/"
            summary[name] = write_csv(f"{report_prefix}{name}.csv", headers, rows)
        except Exception as e:
            logger.error(f"{name} failed: {e}")
            summary[name] = -1
            errors += 1

    try:
        summary["gold_rows"] = write_gold_table()
    except Exception as e:
        logger.error(f"Gold table write failed: {e}")
        summary["gold_rows"] = -1
        errors += 1

    try:
        summary["dashboard_bytes"] = write_dashboard(trending_rows)
    except Exception as e:
        logger.error(f"Dashboard write failed: {e}")
        summary["dashboard_bytes"] = -1
        errors += 1

    mode = "fallback/volume-only" if use_fallback else f"growth >={MIN_GROWTH}x"
    message = (
        f"OTT Trending Keywords Report — {dt}\n"
        f"Mode: {mode}\n"
        f"Trending keywords (all genres): {summary.get('trending_all', 0)}\n"
        f"Trending UNKNOWN (LUT targets): {summary.get('trending_unknown', 0)}\n"
        f"Gold table rows written: {summary.get('gold_rows', 0)}\n"
        f"All:     s3://{STAGE_BUCKET}/{ANALYTICS_PREFIX}all/{dt}/\n"
        f"Unknown: s3://{STAGE_BUCKET}/{ANALYTICS_PREFIX}unknown/{dt}/\n"
        f"Errors: {errors}"
    )
    sns.publish(
        TopicArn=SNS_TOPIC_ARN,
        Subject=f"OTT Trending Keywords Report {dt}",
        Message=message,
    )
    result = {
        "dt": dt,
        "mode": mode,
        "reports": summary,
        "prefixes": {
            "all":     f"s3://{STAGE_BUCKET}/{ANALYTICS_PREFIX}all/{dt}/",
            "unknown": f"s3://{STAGE_BUCKET}/{ANALYTICS_PREFIX}unknown/{dt}/",
        },
        "errors": errors,
    }
    events.put_events(Entries=[{
        "Source": "sdlf.ott.trending",
        "DetailType": "Trending Report Completed",
        "Detail": json.dumps(result),
    }])
    logger.info(
        f"Trending report complete — all:{summary.get('trending_all',0)} "
        f"unknown:{summary.get('trending_unknown',0)} "
        f"gold:{summary.get('gold_rows',0)} errors:{errors} mode:{mode}"
    )
    return result
