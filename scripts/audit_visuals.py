"""Reproducible audit-visual queries against the OTT analytics catalog.

Run after a successful Glue + DQ pipeline run. Each section answers a specific
business question and renders ASCII tables + bar charts so output reads cleanly
in a terminal or markdown context.

Notes on read access:
- raw_search_events is NOT queryable from the analyst Athena role (KMS-locked
  to the Lake Formation data-access role only). That's intentional — raw is the
  private "data lake" layer; analysts query curated.
- curated and dq_results are readable.

Usage:
    python D:/ott-sdlf/scripts/audit_visuals.py
"""
import boto3
import io
import sys
import time
from datetime import datetime, timedelta

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")

REGION = "ap-southeast-1"
DB = "fpt_ott_searchevents_analytics"
WORKGROUP = "sdlf-ott"
OUTPUT = "s3://fpt-ott-ap-southeast-1-703668403514-athena-prod/audit-visuals/"

ath = boto3.client("athena", region_name=REGION)
logs = boto3.client("logs", region_name=REGION)


def latest_lineage():
    """Most recent Glue LINEAGE line from /aws-glue/jobs/output, or None.

    Read live rather than hardcoded — retention swings with each ingest
    (a duplicate re-ingest pushes it well below the first-run ~0.88).
    """
    start = int((datetime.now() - timedelta(days=3)).timestamp() * 1000)
    try:
        events = []
        for page in logs.get_paginator("filter_log_events").paginate(
            logGroupName="/aws-glue/jobs/output",
            filterPattern="LINEAGE",
            startTime=start,
        ):
            events.extend(page.get("events", []))
    except Exception:
        return None
    # filter_log_events interleaves streams — events are not globally
    # time-ordered, so pick the newest LINEAGE line by event timestamp.
    runs = [e for e in events if "LINEAGE run_id" in e["message"]]
    if not runs:
        return None
    return max(runs, key=lambda e: e["timestamp"])["message"].strip()


def run(sql, label):
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
            err = ath.get_query_execution(QueryExecutionId=qid)["QueryExecution"]["Status"].get("StateChangeReason", "?")
            print(f"  [{label}] FAILED: {err[:120]}")
            return [], []
        time.sleep(1.5)
    rows = []
    for page in ath.get_paginator("get_query_results").paginate(QueryExecutionId=qid):
        for r in page["ResultSet"]["Rows"]:
            rows.append([c.get("VarCharValue", "") for c in r["Data"]])
    return (rows[0] if rows else []), (rows[1:] if len(rows) > 1 else [])


def print_table(headers, rows):
    if not rows:
        print("  (no rows)")
        return
    cols = list(zip(headers, *rows))
    widths = [min(max(len(str(c)) for c in col), 60) for col in cols]
    fmt = "  " + "  ".join("{:<" + str(w) + "}" for w in widths)
    print(fmt.format(*headers))
    print("  " + "  ".join("-" * w for w in widths))
    for r in rows:
        print(fmt.format(*[str(c)[:w] for c, w in zip(r, widths)]))


def print_bar(headers, rows, max_width=50):
    if not rows:
        print("  (no rows)")
        return
    vals = [float(r[1].replace(",", "")) for r in rows]
    mv = max(vals) if vals else 1
    name_w = max(len(r[0]) for r in rows)
    for r, v in zip(rows, vals):
        bar = "█" * int(v / mv * max_width)
        print(f"  {r[0]:<{name_w}}  {v:>10,.0f}  {bar}")


# ── Section 1: Quality gates across the lifecycle ─────────────────────────────
print("=" * 78)
print("1. QUALITY GATES — row counts at each lifecycle stage")
print("=" * 78)

print("\n1a. Curated layer: total rows per genre (all dt partitions)")
h, r = run(
    f"SELECT derived_genre, COUNT(*) FROM {DB}.curated GROUP BY derived_genre ORDER BY 2 DESC",
    "curated-by-genre",
)
print_bar(h, r)

print("\n1b. Curated retention vs the latest Glue LINEAGE log")
h, r = run(f"SELECT COUNT(*) FROM {DB}.curated", "curated-count")
cur_n = int(r[0][0]) if r else 0
print(f"  Curated rows: {cur_n:,}")
lineage = latest_lineage()
if lineage:
    print(f"  {lineage}")
else:
    print("  LINEAGE log: no recent Glue run found in /aws-glue/jobs/output")
print(f"  Note: curated table count may include rows from prior runs in partitions")
print(f"        that today's run did not overwrite (DYNAMIC partition mode).")

# ── Section 2: End-user value — what insights this delivers ───────────────────
print("\n" + "=" * 78)
print("2. END-USER VALUE — actual insights this delivers")
print("=" * 78)

print("\n2a. Top 10 search keywords (all dt partitions, excludes UNKNOWN/EMPTY)")
h, r = run(
    f"""SELECT keyword_norm, derived_genre, COUNT(*) AS searches
       FROM {DB}.curated
       WHERE keyword_norm IS NOT NULL AND keyword_norm != ''
         AND derived_genre NOT IN ('UNKNOWN', 'EMPTY_QUERY')
       GROUP BY keyword_norm, derived_genre
       ORDER BY searches DESC LIMIT 10""",
    "top-keywords",
)
print_table(h, r)

print("\n2b. Abandon rate by genre (sessions that quit before submitting)")
h, r = run(
    f"""SELECT derived_genre,
              COUNT(*) AS total,
              SUM(CASE WHEN is_search_abandoned THEN 1 ELSE 0 END) AS abandoned,
              ROUND(100.0 * SUM(CASE WHEN is_search_abandoned THEN 1 ELSE 0 END) / COUNT(*), 1) AS abandon_pct
       FROM {DB}.curated
       WHERE derived_genre NOT IN ('UNKNOWN', 'EMPTY_QUERY')
       GROUP BY derived_genre
       ORDER BY total DESC""",
    "abandon-by-genre",
)
print_table(h, r)

print("\n2c. Premium-user share by genre")
h, r = run(
    f"""SELECT derived_genre,
              COUNT(*) AS total,
              ROUND(100.0 * SUM(CASE WHEN has_premium THEN 1 ELSE 0 END) / COUNT(*), 1) AS premium_pct
       FROM {DB}.curated
       WHERE derived_genre NOT IN ('UNKNOWN', 'EMPTY_QUERY')
       GROUP BY derived_genre
       ORDER BY premium_pct DESC""",
    "premium-by-genre",
)
print_table(h, r)

print("\n2d. Search volume by Vietnam hour-of-day (peak windows)")
h, r = run(
    f"""SELECT CAST(hour_of_day_vn AS varchar) AS hour, COUNT(*) AS searches
       FROM {DB}.curated WHERE hour_of_day_vn IS NOT NULL
       GROUP BY hour_of_day_vn ORDER BY hour_of_day_vn""",
    "by-hour",
)
print_bar(h, r, max_width=40)

print("\n2e. Platform mix (where users search from)")
h, r = run(
    f"SELECT platform_group, COUNT(*) FROM {DB}.curated GROUP BY platform_group ORDER BY 2 DESC",
    "by-platform",
)
print_bar(h, r)

# ── Section 3: Data quality observations ──────────────────────────────────────
print("\n" + "=" * 78)
print("3. DATA QUALITY OBSERVATIONS")
print("=" * 78)

print("\n3a. Authentication mix (user_id present vs anonymous)")
h, r = run(
    f"SELECT user_is_authenticated, COUNT(*) FROM {DB}.curated GROUP BY user_is_authenticated",
    "auth-mix",
)
print_table(h, r)

print("\n3b. DQ outcomes (latest runs, cumulative)")
h, r = run(
    f"""SELECT outcome, COUNT(*) AS rules
       FROM {DB}.dq_results GROUP BY outcome ORDER BY rules DESC""",
    "dq-outcomes",
)
print_table(h, r)

print("\n3c. Classifier coverage — UNKNOWN/EMPTY_QUERY share")
h, r = run(
    f"""SELECT derived_genre, COUNT(*) AS rows,
              ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 1) AS pct
       FROM {DB}.curated GROUP BY derived_genre ORDER BY 2 DESC""",
    "classifier-coverage",
)
print_table(h, r)

print("\n" + "=" * 78)
print("DONE — queries logged to", OUTPUT)
print("=" * 78)
