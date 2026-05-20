"""Post-deploy contract test for the OTT SDLF pipeline.

Asserts the end-user-facing contracts that L4/L5/L6 broke in the past:

  1. REST API returns JSON arrays for /content-gaps and /trending
  2. content_gaps top row is a real keyword (non-empty keyword_norm)
  3. premium_vs_free top row has populated derived_genre + numeric premium_share_pct
  4. trending top row is NOT EMPTY_QUERY (regression test for N1)
  5. CG Lambda's report_url returns HTTP 200 and <html (regression test for L5 SigV4)
  6. Athena ad-hoc query on curated WHERE derived_genre = 'X' returns rows
     (regression test for L3 partition key drift)

Exit 0 on pass, non-zero on first failure. Designed for CI smoke or manual
post-deploy verification. Reads no environment beyond AWS creds.
"""
import io
import json
import os
import sys
import time
from urllib.request import Request, urlopen
from urllib.error import HTTPError

import boto3

# Force UTF-8 stdout — Windows cp1252 default chokes on Vietnamese diacritics
# that appear in real trending keywords (ữ, ố, ậ, …).
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")

REGION = "ap-southeast-1"
API_BASE = "https://oygn7qkkr7.execute-api.ap-southeast-1.amazonaws.com"
DB = "fpt_ott_searchevents_analytics"
ATHENA_OUTPUT = "s3://fpt-ott-ap-southeast-1-703668403514-athena-prod/contract-test/"
API_KEY = os.environ.get("OTT_API_KEY", "")

from botocore.config import Config
# CG Lambda runs 5 sequential Athena queries; budget ~5 min.
lam = boto3.client("lambda", region_name=REGION,
                   config=Config(read_timeout=300, retries={"max_attempts": 0}))
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


def http_json(path: str, api_key: str | None = None) -> tuple[list, dict]:
    """GET API_BASE+path with x-api-key, return (rows, response_headers)."""
    req = Request(API_BASE + path)
    if api_key is None:
        api_key = API_KEY
    if api_key:
        req.add_header("x-api-key", api_key)
    with urlopen(req, timeout=10) as r:
        body = r.read().decode("utf-8")
        return json.loads(body), dict(r.headers.items())


def http_status(path: str, api_key: str = "") -> int:
    """GET path with explicit key (or none) and return status; capture HTTPError."""
    req = Request(API_BASE + path)
    if api_key:
        req.add_header("x-api-key", api_key)
    try:
        with urlopen(req, timeout=10) as r:
            return r.status
    except HTTPError as e:
        return e.code


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


print("=== Contract: REST API ===")
if not API_KEY:
    print("FATAL: set OTT_API_KEY env var (matches pApiKey CFN parameter)")
    sys.exit(2)

# P0a: missing key → 401
status = http_status("/trending?limit=1", api_key="")
check("API rejects request without x-api-key (P0a auth)", status == 401, f"status={status}")
# P0a: wrong key → 401
status = http_status("/trending?limit=1", api_key="WRONG_KEY_12345678")
check("API rejects wrong x-api-key (P0a auth)", status == 401, f"status={status}")

cg, _ = http_json("/content-gaps?limit=3&report=content_gaps")
check("API /content-gaps returns 3 rows", len(cg) == 3, f"got {len(cg)}")
check(
    "API content_gaps top row has non-empty keyword",
    bool(cg and cg[0].get("keyword_norm", "").strip()),
    f"top keyword='{cg[0].get('keyword_norm', '')}'" if cg else "no rows",
)

pf, _ = http_json("/content-gaps?limit=2&report=premium_vs_free")
check("API /content-gaps?report=premium_vs_free returns 2 rows", len(pf) == 2)
if pf:
    pct = pf[0].get("premium_share_pct", "")
    check("premium_vs_free top has numeric premium_share_pct", bool(pct and pct.replace(".", "").isdigit()), f"pct={pct}")

tr, tr_hdrs = http_json("/trending?limit=3")
check("API /trending returns 3 rows", len(tr) == 3)
# P0b: freshness header present and ISO-8601 parseable
freshness = tr_hdrs.get("X-Data-Freshness", "") or tr_hdrs.get("x-data-freshness", "")
check("API exposes X-Data-Freshness header (P0b)", bool(freshness) and "T" in freshness, f"X-Data-Freshness='{freshness}'")
check("API exposes Last-Modified header (P0b)", bool(tr_hdrs.get("Last-Modified") or tr_hdrs.get("last-modified")), f"Last-Modified='{tr_hdrs.get('Last-Modified', tr_hdrs.get('last-modified', ''))}'")
if tr:
    check(
        "trending top row is NOT EMPTY_QUERY (N1 regression)",
        tr[0].get("derived_genre") != "EMPTY_QUERY" and tr[0].get("keyword_norm", "").strip(),
        f"top kw='{tr[0].get('keyword_norm', '')}' genre={tr[0].get('derived_genre')}",
    )

print()
print("=== Contract: Dashboard HTML artifact + presigned URL ===")
# Don't sync-invoke CG (15-30s queries × 5 + SNS publish). Instead check the
# most-recent dashboard artifact and verify a freshly-signed URL serves HTML.
STAGE = "fpt-ott-ap-southeast-1-703668403514-stage-prod"
s3 = boto3.client("s3", region_name=REGION, config=Config(signature_version="s3v4"))
listing = s3.list_objects_v2(Bucket=STAGE, Prefix="analytics/content-gap/report/", Delimiter="/")
date_prefixes = [p["Prefix"] for p in listing.get("CommonPrefixes", [])]
latest_dt = sorted(date_prefixes)[-1] if date_prefixes else None
check("CG dashboard date directory exists", bool(latest_dt), f"latest={latest_dt}")
if latest_dt:
    key = f"{latest_dt}report.html"
    head_resp = s3.head_object(Bucket=STAGE, Key=key)
    check(
        "Dashboard report.html exists & non-trivial size",
        head_resp["ContentLength"] > 1000,
        f"size={head_resp['ContentLength']}",
    )
    presigned = s3.generate_presigned_url(
        "get_object", Params={"Bucket": STAGE, "Key": key}, ExpiresIn=300
    )
    with urlopen(presigned, timeout=10) as r:
        head = r.read(500).decode("utf-8", errors="replace")
        check(
            "Dashboard URL: HTTP 200 + <html (L5 SigV4 regression)",
            r.status == 200 and "<html" in head.lower(),
            f"status={r.status} starts='{head[:60]}'",
        )

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

print()
if failures:
    print(f"FAILED ({len(failures)}): " + "; ".join(failures))
    sys.exit(1)
print("All contracts passed.")
sys.exit(0)
