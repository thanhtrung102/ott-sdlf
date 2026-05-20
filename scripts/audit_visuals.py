"""Reproducible audit-visual queries against the OTT analytics catalog.

Run after a successful Glue + DQ pipeline run. Each section answers a specific
business question and renders ASCII tables + bar charts so output reads cleanly
in a terminal or markdown context.

Notes on read access:
- raw_search_events is NOT queryable from the analyst Athena role (KMS-locked
  to the Lake Formation data-access role only). That's intentional — raw is the
  private "data lake" layer; analysts query curated/gold.
- curated, gold.keyword_trends, and dq_results are all readable.

Usage:
    python D:/ott-sdlf/scripts/audit_visuals.py
"""
import boto3
import io
import sys
import time

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")

REGION = "ap-southeast-1"
DB = "fpt_ott_searchevents_analytics"
GOLD_DB = "fpt_ott_searchevents_gold"
WORKGROUP = "sdlf-ott"
OUTPUT = "s3://fpt-ott-ap-southeast-1-703668403514-athena-prod/audit-visuals/"

ath = boto3.client("athena", region_name=REGION)


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

print("\n1a. Curated layer: total rows per genre across all 14 days")
h, r = run(
    f"SELECT derived_genre, COUNT(*) FROM {DB}.curated GROUP BY derived_genre ORDER BY 2 DESC",
    "curated-by-genre",
)
print_bar(h, r)

print("\n1b. Curated retention vs the published LINEAGE log")
h, r = run(f"SELECT COUNT(*) FROM {DB}.curated", "curated-count")
cur_n = int(r[0][0]) if r else 0
print(f"  Curated rows: {cur_n:,}")
print(f"  LINEAGE log (latest Glue run): raw=1,298,470 -> output=1,145,826 (retention=0.882)")
print(f"  Note: curated table count may include rows from prior runs in partitions")
print(f"        that today's run did not overwrite (DYNAMIC partition mode).")

# ── Section 2: End-user value — what insights this delivers ───────────────────
print("\n" + "=" * 78)
print("2. END-USER VALUE — actual insights this delivers")
print("=" * 78)

print("\n2a. Top 10 search keywords (14-day window, excludes UNKNOWN/EMPTY)")
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

# ── Section 3: Gold layer — pre-computed rankings for API ─────────────────────
print("\n" + "=" * 78)
print("3. GOLD LAYER — pre-computed rankings for end users")
print("=" * 78)

print("\n3a. Gold table size + sample of top-ranked rows")
h, r = run(
    f"""SELECT keyword_norm, derived_genre, platform_group, search_count, rank_today
       FROM {GOLD_DB}.keyword_trends ORDER BY search_count DESC LIMIT 5""",
    "gold-top",
)
print_table(h, r)
h, r = run(f"SELECT COUNT(*) FROM {GOLD_DB}.keyword_trends", "gold-count")
print(f"  Total gold rows: {int(r[0][0]):,}")

# ── Section 4: Data quality observations ──────────────────────────────────────
print("\n" + "=" * 78)
print("4. DATA QUALITY OBSERVATIONS")
print("=" * 78)

print("\n4a. Authentication mix (user_id present vs anonymous)")
h, r = run(
    f"SELECT user_is_authenticated, COUNT(*) FROM {DB}.curated GROUP BY user_is_authenticated",
    "auth-mix",
)
print_table(h, r)

print("\n4b. DQ outcomes (latest runs, cumulative)")
h, r = run(
    f"""SELECT outcome, COUNT(*) AS rules
       FROM {DB}.dq_results GROUP BY outcome ORDER BY rules DESC""",
    "dq-outcomes",
)
print_table(h, r)

print("\n4c. Classifier coverage — UNKNOWN/EMPTY_QUERY share")
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
