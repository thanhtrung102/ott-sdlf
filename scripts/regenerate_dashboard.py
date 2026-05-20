"""Regenerate D:\\ott-sdlf\\dashboard\\index.html from the live gold table.

The dashboard is a static HTML/JS snapshot — its data arrays are hardcoded.
Run this script after a successful Trending Lambda execution to bring the
dashboard back in sync with sdlf_ott_gold.keyword_trends.

Fixes the long-standing label bug: the old dashboard called gold-table rows
"Unique Keywords", which they aren't — a row is (keyword × platform × genre
× trend_date). This version shows both counts correctly.

Usage:
    python D:/ott-sdlf/scripts/regenerate_dashboard.py
"""
import boto3
import json
import sys
import io
import time
from pathlib import Path

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")

REGION = "ap-southeast-1"
GOLD_DB = "fpt_ott_searchevents_gold"
WORKGROUP = "sdlf-ott"
OUTPUT = "s3://fpt-ott-ap-southeast-1-703668403514-athena-prod/dashboard-regen/"
HTML_PATH = Path(r"D:\ott-sdlf\dashboard\index.html")

ath = boto3.client("athena", region_name=REGION)


def run(sql):
    qid = ath.start_query_execution(
        QueryString=sql,
        WorkGroup=WORKGROUP,
        ResultConfiguration={"OutputLocation": OUTPUT},
    )["QueryExecutionId"]
    while True:
        st = ath.get_query_execution(QueryExecutionId=qid)["QueryExecution"]["Status"]["State"]
        if st == "SUCCEEDED":
            break
        if st in ("FAILED", "CANCELLED"):
            reason = ath.get_query_execution(QueryExecutionId=qid)["QueryExecution"]["Status"].get("StateChangeReason", "?")
            raise RuntimeError(f"Athena {st}: {reason[:200]}")
        time.sleep(1.5)
    rows = []
    for page in ath.get_paginator("get_query_results").paginate(QueryExecutionId=qid):
        for r in page["ResultSet"]["Rows"]:
            rows.append([c.get("VarCharValue", "") for c in r["Data"]])
    return rows[1:] if len(rows) > 1 else []


print(f"Querying {GOLD_DB}.keyword_trends ...")

# Totals
rows = run(f"""SELECT
    SUM(search_count) AS total_searches,
    COUNT(DISTINCT keyword_norm) AS distinct_keywords,
    COUNT(*) AS gold_rows,
    ROUND(100.0 * SUM(abandoned_count) / SUM(search_count), 1) AS overall_abandon
  FROM {GOLD_DB}.keyword_trends""")
total_searches = int(rows[0][0])
distinct_keywords = int(rows[0][1])
gold_rows = int(rows[0][2])
overall_abandon = float(rows[0][3])

# Top genre
rows = run(f"""SELECT derived_genre, SUM(search_count) AS cnt,
       ROUND(100.0 * SUM(abandoned_count) / SUM(search_count), 1) AS abandon
  FROM {GOLD_DB}.keyword_trends
  GROUP BY derived_genre ORDER BY cnt DESC LIMIT 1""")
top_genre_name = rows[0][0]
top_genre_count = int(rows[0][1])
top_genre_abandon = float(rows[0][2])

# By-platform
rows = run(f"""SELECT platform_group, SUM(search_count) AS cnt,
       ROUND(100.0 * SUM(abandoned_count) / SUM(search_count), 1) AS abandon
  FROM {GOLD_DB}.keyword_trends
  GROUP BY platform_group ORDER BY cnt DESC""")
platform = [{"p": r[0], "c": int(r[1]), "a": float(r[2])} for r in rows]

# By-genre
rows = run(f"""SELECT derived_genre, SUM(search_count) AS cnt,
       ROUND(100.0 * SUM(abandoned_count) / SUM(search_count), 1) AS abandon
  FROM {GOLD_DB}.keyword_trends
  GROUP BY derived_genre ORDER BY cnt DESC""")
by_genre = [{"g": r[0], "c": int(r[1]), "a": float(r[2])} for r in rows]

# Top-20 keywords (aggregated across platforms)
rows = run(f"""SELECT keyword_norm, derived_genre, SUM(search_count) AS cnt,
       ROUND(100.0 * SUM(abandoned_count) / SUM(search_count), 1) AS abandon
  FROM {GOLD_DB}.keyword_trends
  GROUP BY keyword_norm, derived_genre
  ORDER BY cnt DESC LIMIT 20""")
top_keywords = [{"k": r[0], "g": r[1], "c": int(r[2]), "a": float(r[3])} for r in rows]

# Reference date (max trend_date)
rows = run(f"SELECT MAX(trend_date) FROM {GOLD_DB}.keyword_trends")
trend_date = rows[0][0] if rows and rows[0][0] else "unknown"

# Month label for header (e.g. "Jun 2022")
month_label = "unknown"
if trend_date and trend_date != "unknown":
    from datetime import date
    try:
        d = date.fromisoformat(trend_date)
        month_label = d.strftime("%b %Y")
    except ValueError:
        pass

print(f"  total_searches    = {total_searches:,}")
print(f"  distinct_keywords = {distinct_keywords:,}")
print(f"  gold_rows         = {gold_rows:,}")
print(f"  overall_abandon   = {overall_abandon}%")
print(f"  top_genre         = {top_genre_name} ({top_genre_count:,}, {top_genre_abandon}% abandon)")
print(f"  platforms         = {len(platform)}")
print(f"  genres            = {len(by_genre)}")
print(f"  top_keywords      = {len(top_keywords)}")
print(f"  trend_date        = {trend_date}  ({month_label})")


def fmt_k(n):
    if n >= 1_000_000:
        return f"{n/1_000_000:.2f}M"
    if n >= 1_000:
        return f"{n/1_000:.0f}K"
    return str(n)


HTML = f"""<!DOCTYPE html>
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
  .delta-up {{ color: #68d391; }}
  .delta-down {{ color: #fc8181; }}
  .bar-cell {{ width: 70px; }}
  .mini-bar {{ height: 5px; border-radius: 3px; background: #2d3748; overflow: hidden; }}
  .mini-bar-fill {{ height: 100%; border-radius: 3px; }}
  footer {{ text-align: center; color: #4a5568; font-size: 11px; padding: 24px; border-top: 1px solid #1e2130; margin-top: 8px; }}
</style>
</head>
<body>
<header>
  <h1>FPT OTT &mdash; Search Analytics</h1>
  <span class="badge">sdlf_ott_gold.keyword_trends</span>
  <span class="source-tag">{month_label} &nbsp;|&nbsp; {distinct_keywords:,} keywords &nbsp;|&nbsp; {len(platform)} platforms &nbsp;|&nbsp; refreshed {trend_date}</span>
</header>
<main>

<div class="kpis">
  <div class="kpi">
    <div class="label">Tracked Searches</div>
    <div class="value">{fmt_k(total_searches)}</div>
    <div class="sub">aggregated across all keywords</div>
  </div>
  <div class="kpi">
    <div class="label">Distinct Keywords</div>
    <div class="value">{distinct_keywords:,}</div>
    <div class="sub">unique keyword_norm values</div>
  </div>
  <div class="kpi">
    <div class="label">Overall Abandon Rate</div>
    <div class="value">{overall_abandon}%</div>
    <div class="sub">gold-layer weighted avg</div>
  </div>
  <div class="kpi">
    <div class="label">Top Genre</div>
    <div class="value">{top_genre_name}</div>
    <div class="sub warn">{top_genre_count:,} searches &bull; {top_genre_abandon}% abandon</div>
  </div>
</div>

<div class="grid-2">
  <div class="card">
    <h2>Searches by Platform</h2>
    <div class="src">sdlf_ott_gold.keyword_trends &mdash; GROUP BY platform_group</div>
    <div class="chart-wrap"><canvas id="platformChart"></canvas></div>
  </div>
  <div class="card">
    <h2>Genre Distribution</h2>
    <div class="src">sdlf_ott_gold.keyword_trends &mdash; GROUP BY derived_genre</div>
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
    <div class="src">sdlf_ott_gold.keyword_trends &mdash; SUM(search_count), all platforms</div>
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
    <div class="src">sdlf_ott_gold.keyword_trends &mdash; abandon_rate by platform</div>
    <div class="chart-wrap" style="height:280px"><canvas id="abandonChart"></canvas></div>
  </div>
</div>

</main>
<footer>
  Source: <code>sdlf_ott_gold.keyword_trends</code> ({gold_rows:,} rows) &nbsp;&bull;&nbsp;
  Produced by: <code>sdlf-ott-mainTR-report</code> Lambda &nbsp;&bull;&nbsp;
  Upstream: <code>fpt_ott_searchevents_analytics.curated</code> (Stage B Glue ETL) &nbsp;&bull;&nbsp;
  Regen: <code>scripts/regenerate_dashboard.py</code> &nbsp;&bull;&nbsp;
  AWS Serverless Data Lake Framework
</footer>

<script>
const GENRE_COLORS = {{
  PHIM_VIET:'#68d391', ANIME:'#9f7aea', PHIM_TRUNG:'#f6ad55',
  PHIM_HAN:'#76e4f7', PHIM_AU_MY:'#fc8181', TRUYEN_HINH:'#fbd38d',
  UNKNOWN:'#4a5568', NHAC:'#b794f4', THE_THAO:'#4299e1', EMPTY_QUERY:'#718096',
}};
const GENRE_LABELS = {{
  PHIM_VIET:'Phim Việt', UNKNOWN:'Unknown', PHIM_TRUNG:'Phim Trung',
  ANIME:'Anime', PHIM_HAN:'Phim Hàn', PHIM_AU_MY:'Phim Âu Mỹ',
  TRUYEN_HINH:'Truyền Hình', NHAC:'Nhạc', THE_THAO:'Thể Thao', EMPTY_QUERY:'(empty)',
}};
Chart.defaults.color = '#a0aec0';
Chart.defaults.font = {{ family: "'Segoe UI', sans-serif", size: 12 }};

// --- Gold-layer data (regenerated from live query) ---
const platform = {json.dumps(platform, ensure_ascii=False)};
const byGenre = {json.dumps(by_genre, ensure_ascii=False)};
const topKeywords = {json.dumps(top_keywords, ensure_ascii=False)};

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
</script>
</body>
</html>
"""

HTML_PATH.write_text(HTML, encoding="utf-8")
print(f"\nWrote {HTML_PATH}  ({len(HTML):,} bytes)")
print(f"  Open in browser: file:///{HTML_PATH.as_posix()}")
