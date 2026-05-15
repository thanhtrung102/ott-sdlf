---
title: "3. Data Ingestion"
date: 2026-05-15
weight: 3
chapter: false
pre: <b>3. </b>
---

# Data Ingestion

The ingestion layer has two stages. **Stage A** routes the incoming S3 event through the SDLF orchestration framework. **Stage B** runs the PySpark enrichment job that transforms raw 10-column Parquet files into the 19-column curated schema.

---

## 3.1 Raw schema

Files land in the raw bucket under `ott/searchevents/YYYYMMDD/*.parquet`.

| Column | Type | Notes |
|---|---|---|
| `eventid` | STRING | UUID, primary deduplication key |
| `datetime` | STRING | Heterogeneous format: may contain Arabic-Indic numerals (٠–٩, ۰–۹) or Buddhist Era years |
| `keyword` | STRING | Raw user-typed query, unnormalised |
| `category` | STRING | User action: `quit` (abandoned), `enter` (submitted), other |
| `platform` | STRING | 35+ device type strings (see Section 3.3) |
| `networktype` | STRING | 9 variants: `4g`, `5g`, `wifi`, etc. |
| `userplansmap` | ARRAY\<STRING\> | Active subscription plans in `NAME:expiry-date` format, e.g. `["VIP:2024-12-31"]` |
| `proxy_isp` | STRING | ISP identifier |
| `user_id` | STRING | Null for unauthenticated (guest) users |
| `dt` | STRING | YYYYMMDD — extracted from the file path by the Glue job |

---

## 3.2 Stage A

**Source**: `pipeline-ott-mainA.yaml`

Stage A is the SDLF event-routing stage. When a new file lands in the raw S3 prefix, an S3 EventBridge notification fires and Stage A's Step Functions state machine starts.

The Lambda (`sdlf-ott-main-stagelambda-A`) performs lightweight work: it validates the event structure and marks the object as in-progress in the SDLF tracking DynamoDB table. It does not read or transform the Parquet data.

On success, Stage A emits a state machine `SUCCEEDED` event that triggers Stage B.

---

## 3.3 Stage B — Glue ETL job

**Source**: `glue/ott-search-glue-job.py`, referenced from `pipeline-ott-glue-job.yaml`

### Configuration

| Parameter | Value |
|---|---|
| Glue version | 4.0 (Spark 3.3, Python 3.10) |
| Worker type | G.1X |
| Number of workers | 10 |
| Job bookmarks | Enabled (production) |
| Extra Python files | `genre_classifier_pkg.zip` (from artifacts bucket) |
| Script location | `s3://[artifacts-bucket]/artifacts/ott-search-glue-job.py` |

### Transformation steps

The job reads raw Parquet files and applies 21 transformation steps to produce the curated table.

**Step 1 — Clean datetime**
Many source records contain Arabic-Indic numerals (٠–٩ Unicode block) or Buddhist Era year offsets (543 years ahead of Gregorian). The job translates both:

```python
# glue/ott-search-glue-job.py
_ARABIC_INDIC = str.maketrans("٠١٢٣٤٥٦٧٨٩۰۱۲۳۴۵۶۷۸۹", "01234567890123456789")
```

Buddhist Era detection: any year ≥ 2543 is subtracted by 543.

**Step 2 — Parse timestamp**
`to_timestamp(datetime_clean, 'yyyy-MM-dd HH:mm:ss.SSS')` in UTC.

**Step 3 — Drop corrupt rows**
`year(event_ts) < 2015` — filters out year-0004 spam that results from malformed datetime strings.

**Step 4 — Deduplicate**
`dropDuplicates(["eventid"])` — guards against Glue job retries double-processing records.

**Step 5 — Vietnam timezone hour**
`hour(event_ts + INTERVAL 7 HOURS)` → `hour_of_day_vn` (INT 0–23). Vietnam is UTC+7 with no daylight saving.

**Step 6 — Session action**
```python
when(col("category") == lit("quit"),  lit("ABANDON"))
.when(col("category") == lit("enter"), lit("SUBMIT"))
.when(col("category").isNotNull(),     F.upper(col("category")))
.otherwise(lit(None))
```

**Step 7 — Abandon flag**
`is_search_abandoned = (category == "quit")` — Boolean, used by all five content-gap queries.

**Step 8 — Authentication indicator**
`user_is_authenticated = (user_id IS NOT NULL)`

**Step 9 — User ID hash**
`SHA-256(user_id)` using PySpark's built-in `sha2(col("user_id"), 256)`. Null-safe: guest records get `user_id_hashed = null`.

**Step 10 — Keyword normalise**
`lower(trim(keyword))` → `keyword_norm`. Null-safe.

**Step 11 — Genre classification**
`genre_classifier.classify_keyword(keyword_norm)` — a Python UDF loaded from `genre_classifier_pkg.zip`. The classifier applies a four-layer waterfall:

1. **LUT** (`lut.json` + `lut_extended.json`) — exact-match dictionary, ~100k+ entries after Bedrock enrichment
2. **Regex rules** (`regex_rules.py`) — pattern matching for common title fragments
3. **Fuzzy matching** (`fuzzy_matcher.py`) — edit-distance fallback
4. **UNKNOWN** — keywords that pass all layers unclassified; these become candidates for the LUT Refresh Lambda

Nine output classes: `PHIM_TRUNG`, `PHIM_VIET`, `PHIM_HAN`, `PHIM_AU_MY`, `ANIME`, `THE_THAO`, `NHAC`, `TRUYEN_HINH`, `UNKNOWN`

**Step 12 — Platform grouping**
35+ raw platform strings bucketed into 6 canonical groups via `genre_classifier.bucket_platform()`.

**Step 13 — Network type normalise**
9 raw network variants → 5 canonical values via `genre_classifier.normalize_network_type()`.

**Step 14 — ISP segment**
`upper(proxy_isp)` → `isp_segment`

**Step 15 — Premium flag**
Scans `userplansmap` for `{VIP, HBO GO+, K+, MAX}` (case-insensitive, handles `NAME:date` format):

```python
_PREMIUM_PLANS = {"VIP", "HBO GO+", "K+", "MAX"}
```

**Step 16 — Subscription count**
`len(userplansmap)` → `subscription_count` (INT)

**Step 17 — Plan names**
Extracts the plan name (before `:` if present) from each entry in `userplansmap` → `plan_names` (ARRAY\<STRING\>)

**Step 18 — Search session ID**
30-minute bucket windowing:
- `half_hour_bucket = floor(unix_timestamp(event_ts) / 1800)`
- Authenticated: `SHA-256(user_id || half_hour_bucket)`
- Guest: `SHA-256(proxy_isp || platform || half_hour_bucket)`

This creates anonymous, time-bounded session identifiers without storing any persistent user token.

**Step 19 — Repeat search detection**
```sql
LAG(keyword_norm) OVER (PARTITION BY search_session_id ORDER BY event_ts)
```
`is_repeat_search = (keyword_norm == prev_keyword_norm)` — detects consecutive identical searches within a session, which signal user frustration.

**Step 20 — Pipeline lineage**
`_pipeline_run_id = JOB_RUN_ID` — Glue run ID stamped on every output row for traceability.

**Step 21 — Normalise dt partition key**
Converts `YYYYMMDD` (string, from folder name) → `yyyy-MM-dd` (DATE) so Athena can use date comparisons in partition pruning.

---

## 3.4 Curated schema

Output written to `s3://[analytics-bucket]/ott/searchevents/curated/dt=YYYY-MM-DD/`, Parquet, Snappy compressed, single partition key `dt`.

| Column | Type | Derived from |
|---|---|---|
| `event_id` | STRING | `eventid` alias |
| `event_ts` | TIMESTAMP | Parsed from cleaned `datetime` |
| `hour_of_day_vn` | INT | `hour(event_ts + 7h)` |
| `user_id_hashed` | STRING | `SHA-256(user_id)` or null |
| `user_is_authenticated` | BOOLEAN | `user_id IS NOT NULL` |
| `session_action` | STRING | ABANDON / SUBMIT / upper(category) |
| `is_search_abandoned` | BOOLEAN | `category == 'quit'` |
| `keyword_norm` | STRING | `lower(trim(keyword))` |
| `derived_genre` | STRING | Genre classifier UDF |
| `platform_group` | STRING | 35 strings → 6 buckets |
| `network_type_norm` | STRING | 9 variants → 5 buckets |
| `isp_segment` | STRING | `upper(proxy_isp)` |
| `has_premium` | BOOLEAN | `any plan in {VIP, HBO GO+, K+, MAX}` |
| `subscription_count` | INT | `len(userplansmap)` |
| `plan_names` | ARRAY\<STRING\> | Plan names extracted from map |
| `search_session_id` | STRING | SHA-256 session window hash |
| `is_repeat_search` | BOOLEAN | LAG window function |
| `_pipeline_run_id` | STRING | Glue JOB_RUN_ID |
| `dt` | DATE | Partition key, `yyyy-MM-dd` |

---

## 3.5 Glue catalog tables

Both tables are defined in `pipeline-ott-glue-job.yaml` with partition projection enabled, so Athena discovers new partitions automatically without `MSCK REPAIR TABLE`.

**`raw_search_events`**
- Format: Parquet
- Partition key: `dt` STRING (YYYYMMDD)
- Projection: `dt` — custom pattern, range from 2020-01-01 to NOW+1YEAR
- Location template: `s3://[analytics-bucket]/ott/searchevents/YYYYMMDD/`

**`curated`**
- Format: Parquet
- Partition key: `dt` DATE (yyyy-MM-dd)
- Projection: `dt` — date type, range from 2020-01-01 to NOW+1YEAR, interval 1 DAY
- Location template: `s3://[analytics-bucket]/ott/searchevents/curated/dt=${dt}/`
