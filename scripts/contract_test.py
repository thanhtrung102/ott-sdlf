"""Post-deploy contract test for the OTT SDLF pipeline.

Asserts the end-user-facing contracts that prior incidents broke:

  1. CloudFront dashboard URL serves HTTP 200 with the full single-surface
     presentation (KPIs + every business-question section).
  2. Header carries a source-attribution stamp identifying the curated
     dt window the run aggregated (criterion #1: truthful coverage).
  3. Content Gaps and Trending tables hold their full-depth row counts
     (the analytics API used to back the deep-list use case; now the
     dashboard is the only surface, so the rows must live in HTML).
  4. Athena ad-hoc query on curated WHERE derived_genre = 'X' returns rows
     (regression test for L3 partition key drift).
  5. dq_results visible to the analyst role (regression test for L7).
  6. raw_search_events.action visible (regression test for L8).

Exit 0 on pass, non-zero on first failure. Designed for CI smoke or
manual post-deploy verification.
"""
import io
import re
import sys
import time
from urllib.request import urlopen

import boto3

# Force UTF-8 stdout — Windows cp1252 default chokes on Vietnamese diacritics
# that appear in real trending keywords (ữ, ố, ậ, …).
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")

REGION = "ap-southeast-1"
DB = "fpt_ott_searchevents_analytics"
ATHENA_OUTPUT = "s3://fpt-ott-ap-southeast-1-703668403514-athena-prod/contract-test/"

ssm = boto3.client("ssm", region_name=REGION)
ath = boto3.client("athena", region_name=REGION)

failures: list[str] = []


def check(label: str, ok: bool, detail: str = "") -> None:
    status = "PASS" if ok else "FAIL"
    msg = f"  [{status}] {label}"
    if detail:
        msg += f"  ({detail})"
    print(msg)
    if not ok:
        failures.append(label)


def athena_one(sql: str) -> list[str]:
    """Run a query, return the first non-header row's cell values."""
    qid = ath.start_query_execution(
        QueryString=sql,
        WorkGroup="sdlf-ott",
        ResultConfiguration={"OutputLocation": ATHENA_OUTPUT},
    )["QueryExecutionId"]
    while True:
        st = ath.get_query_execution(QueryExecutionId=qid)["QueryExecution"]["Status"]["State"]
        if st == "SUCCEEDED":
            break
        if st in ("FAILED", "CANCELLED"):
            raise RuntimeError(f"Athena {st}: {sql[:80]}")
        time.sleep(2)
    pages = ath.get_paginator("get_query_results").paginate(QueryExecutionId=qid)
    rows = []
    for page in pages:
        rows.extend(page["ResultSet"]["Rows"])
    if len(rows) < 2:
        return []
    return [c.get("VarCharValue", "") for c in rows[1]["Data"]]


print("=== Contract: CloudFront dashboard (single user-facing surface) ===")
dash_url = ssm.get_parameter(Name="/sdlf/pipeline/rDashboardUrl/ott")["Parameter"]["Value"]
check("Dashboard URL is published to SSM", dash_url.startswith("https://"), f"url={dash_url}")
with urlopen(dash_url, timeout=15) as r:
    body = r.read().decode("utf-8", errors="replace")
check(
    "Dashboard returns HTTP 200 + non-trivial HTML",
    r.status == 200 and len(body) > 8000 and "<!DOCTYPE html" in body,
    f"status={r.status} bytes={len(body)}",
)
# Single user-facing surface: every section must be present in the HTML, not
# offloaded to a JSON API.
for section in (
    "Total Searches", "Distinct Keywords", "Top 20 Keywords",
    "Trending Keywords", "Content Gaps", "Premium vs Free",
    "Repeat Search Rate", "Guest vs Authenticated",
    "Search Volume by Hour",
):
    check(f"Dashboard section present: {section}", section in body)

# Source-attribution stamp on every section (criterion: truthful coverage).
check("Header carries source-attribution caption",
      "Source: curated" in body and "days" in body)
# Section-level captions must also carry the dt window.
src_count = len(re.findall(r'class="src"', body))
check(f"Every section header has its own .src caption (>=9)",
      src_count >= 9, f"found={src_count}")

# Depth requirement — Content Gaps and Trending must hold full-depth lists in
# the HTML (the API previously served them; now they live here).
cg_rows = len(re.findall(r'<tr data-genre="[^"]+" data-kw="', body))
check(f"Filterable tables (Trending + Content Gaps) carry deep rows",
      cg_rows >= 100, f"data-kw tr count={cg_rows}")

# Hour×genre heatmap — 216 cells (24 hours × 9 genres) + an "All genres"
# totals row contribute 24 cells with the `hcell` class per row. A simple
# bar-chart-only version would have at most ~24 hcells.
hcell_count = body.count('class="hcell"')
check(f"Hour×genre heatmap rendered (>=200 cells)",
      hcell_count >= 200, f"hcell count={hcell_count}")

print()
print("=== Contract: Athena catalog ===")
# L3 regression: derived_genre must be queryable as a partition column
row = athena_one(
    f"SELECT derived_genre, COUNT(*) FROM {DB}.curated "
    f"WHERE dt = '2022-06-01' AND derived_genre = 'PHIM_HAN' GROUP BY derived_genre"
)
check(
    "Athena: derived_genre partition queryable (L3 regression)",
    bool(row) and row[0] == "PHIM_HAN" and int(row[1]) > 0,
    f"row={row}",
)

# L7 regression: dq_results visible (was LF-hidden)
row = athena_one(f"SELECT outcome, COUNT(*) FROM {DB}.dq_results GROUP BY outcome ORDER BY 1 LIMIT 1")
check("Athena: dq_results queryable (L7 LF regression)", bool(row), f"row={row}")

# raw_search_events.action visible (L8 regression)
row = athena_one(
    f"SELECT DISTINCT action FROM {DB}.raw_search_events WHERE dt = '20220601' LIMIT 1"
)
check("Athena: raw_search_events.action visible (L8 regression)", row == ["search"], f"row={row}")

# N1 regression: trending top row should be a real keyword, not EMPTY_QUERY.
# Source from the dashboard's trending section directly.
m = re.search(r'<tbody id="trBody">(.+?)</tbody>', body, re.S)
if m:
    first_tr = re.search(r'<tr data-genre="([^"]*)" data-kw="([^"]*)"', m.group(1))
    if first_tr:
        check(
            "Dashboard trending top row is NOT EMPTY_QUERY (N1 regression)",
            first_tr.group(1) != "EMPTY_QUERY" and bool(first_tr.group(2).strip()),
            f"top kw='{first_tr.group(2)}' genre={first_tr.group(1)}",
        )
    else:
        check("Dashboard trending top row is NOT EMPTY_QUERY (N1 regression)", False,
              "no trending rows found")
else:
    check("Dashboard trending top row is NOT EMPTY_QUERY (N1 regression)", False,
          "trending tbody missing")

print()
if failures:
    print(f"FAILED ({len(failures)}): " + "; ".join(failures))
    sys.exit(1)
print("All contracts passed.")
sys.exit(0)
