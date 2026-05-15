---
title: "5. Analytics Pipelines"
date: 2026-05-15
weight: 5
chapter: false
pre: <b>5. </b>
---

# Analytics Pipelines

Three Lambda functions run in parallel after the curated DQ state machine succeeds. Each reads from the `curated` Athena table and produces a different type of output.

| Lambda | Template | Purpose | Output |
|---|---|---|---|
| `sdlf-ott-mainCG-report` | `pipeline-ott-contentgap.yaml` | Content gap analysis | 5 CSVs + HTML report |
| `sdlf-ott-mainLUT-refresh` | `pipeline-ott-lutrefresh.yaml` | Genre classifier maintenance | Updated `genre_classifier_pkg.zip` |
| `sdlf-ott-mainTR-report` | `pipeline-ott-trending.yaml` | Trending keyword detection | 2 CSVs + gold Parquet table |

---

## 5.1 Content Gap Lambda

**Source**: `lambda/content-gap/src/lambda_function.py`

### What it does

Runs five Athena queries against the curated table with a 90-day rolling lookback window. Writes each result as a CSV to the stage bucket, builds an HTML dashboard, generates a pre-signed URL (7-day TTL), and publishes a summary SNS message.

The reference date is `MAX(dt)` from the curated table (not `datetime.now()`), so the report date label always matches the most recent data partition — never skips or double-labels after a midnight run.

```python
# lambda/content-gap/src/lambda_function.py
def latest_dt():
    _, rows = athena_query(f"SELECT MAX(dt) AS max_dt FROM {DB}.curated")
    ...
    return date.fromisoformat(val)
```

### 90-day window

`start_dt = ref_date - timedelta(days=90)` — applied to all five queries via `WHERE dt >= '{start_dt}'`. This limits the scan to the last 90 days of partitions, keeping query cost bounded as the table grows.

### Queries

**1. Content gaps — abandoned keywords**

```sql
SELECT keyword_norm, derived_genre,
       COUNT(*) AS searches,
       SUM(CASE WHEN is_search_abandoned THEN 1 ELSE 0 END) AS abandoned,
       ROUND(100.0 * SUM(CASE WHEN is_search_abandoned THEN 1 ELSE 0 END)
             / CAST(COUNT(*) AS double), 1) AS abandon_rate_pct
FROM {db}.curated
WHERE dt >= '{start_dt}' AND derived_genre != 'UNKNOWN'
GROUP BY keyword_norm, derived_genre
HAVING SUM(CASE WHEN is_search_abandoned THEN 1 ELSE 0 END) >= 5
ORDER BY abandon_rate_pct DESC, abandoned DESC
LIMIT 500
```

> **Business use**: High `abandoned` count + high `abandon_rate_pct` = the platform almost certainly does not carry that title. Use as an acquisition shortlist.

**2. Premium vs free demand by genre**

```sql
SELECT derived_genre,
       COUNT(*) AS total_searches,
       SUM(CASE WHEN has_premium THEN 1 ELSE 0 END) AS premium_searches,
       SUM(CASE WHEN NOT has_premium THEN 1 ELSE 0 END) AS free_searches,
       ROUND(100.0 * SUM(CASE WHEN has_premium THEN 1 ELSE 0 END)
             / CAST(COUNT(*) AS double), 1) AS premium_share_pct
FROM {db}.curated
WHERE dt >= '{start_dt}'
GROUP BY derived_genre
ORDER BY premium_share_pct DESC
```

> **Business use**: High `premium_share_pct` genres justify higher licensing spend; premium users already paying are more likely to watch licensed content.

**3. Repeat search rate by genre**

```sql
SELECT derived_genre,
       COUNT(*) AS total_searches,
       SUM(CASE WHEN is_repeat_search THEN 1 ELSE 0 END) AS repeat_searches,
       ROUND(100.0 * SUM(CASE WHEN is_repeat_search THEN 1 ELSE 0 END)
             / CAST(COUNT(*) AS double), 1) AS repeat_pct
FROM {db}.curated
WHERE dt >= '{start_dt}'
GROUP BY derived_genre
ORDER BY repeat_pct DESC
```

> **Business use**: High `repeat_pct` means users are typing the same query multiple times without finding what they want — signals either missing content or a search UX problem.

**4. Hour-of-day heatmap (Vietnam time)**

```sql
SELECT hour_of_day_vn, derived_genre,
       COUNT(*) AS searches,
       ROUND(100.0 * COUNT(*) / CAST(SUM(COUNT(*)) OVER (PARTITION BY derived_genre) AS double), 1) AS pct_of_genre
FROM {db}.curated
WHERE dt >= '{start_dt}' AND derived_genre != 'UNKNOWN'
GROUP BY hour_of_day_vn, derived_genre
ORDER BY derived_genre, hour_of_day_vn
```

> **Business use**: `pct_of_genre` shows when each genre peaks throughout the day (Vietnam UTC+7). Use to schedule push notifications at peak demand hours per genre.

**5. Guest vs authenticated demand by genre**

```sql
SELECT derived_genre,
       COUNT(*) AS total_searches,
       SUM(CASE WHEN user_is_authenticated THEN 1 ELSE 0 END) AS auth_searches,
       SUM(CASE WHEN NOT user_is_authenticated THEN 1 ELSE 0 END) AS guest_searches,
       ROUND(100.0 * SUM(CASE WHEN NOT user_is_authenticated THEN 1 ELSE 0 END)
             / CAST(COUNT(*) AS double), 1) AS guest_share_pct
FROM {db}.curated
WHERE dt >= '{start_dt}' AND derived_genre != 'UNKNOWN'
GROUP BY derived_genre
ORDER BY guest_share_pct DESC
```

> **Business use**: High `guest_share_pct` = unauthenticated users are already expressing demand for this genre. These genres are the strongest targets for login-gate conversion campaigns.

### Output locations

| File | S3 path |
|---|---|
| `content_gaps.csv` | `analytics/content-gap/{dt}/content_gaps.csv` |
| `premium_vs_free.csv` | `analytics/content-gap/{dt}/premium_vs_free.csv` |
| `repeat_search_rate.csv` | `analytics/content-gap/{dt}/repeat_search_rate.csv` |
| `hour_of_day_heatmap.csv` | `analytics/content-gap/{dt}/hour_of_day_heatmap.csv` |
| `guest_vs_auth_demand.csv` | `analytics/content-gap/{dt}/guest_vs_auth_demand.csv` |
| `report.html` | `analytics/content-gap/{dt}/report.html` (pre-signed, 7-day TTL) |

All paths are under `s3://[stage-bucket]/`.

### Lambda configuration

| Parameter | Value |
|---|---|
| Runtime | Python 3.12 |
| Timeout | 300 s |
| Memory | 256 MB |
| Trigger | EventBridge — DQ SM `SUCCEEDED` |
| DLQ | `sdlf-ott-mainCG-dlq` (14-day retention) |
| Log group | `/aws/lambda/sdlf-ott-mainCG-report` (30-day retention) |

### Lake Formation access

The Content Gap role (`pContentGapRoleArn`, SSM: `/sdlf/pipeline/rRole/ott-mainCG`) has `SELECT` on `curated` with these columns **excluded**: `user_id_hashed`, `search_session_id`, `subscription_count`.

`has_premium` is **not** excluded because the `premium_vs_free` query depends on it.

---

## 5.2 Trending Lambda

See [Section 6 — Gold Layer](../6-gold-layer/) for the full trending query and gold table write logic.

---

## 5.3 LUT Refresh Lambda

**Source**: `lambda/lut-refresh/src/lambda_function.py`

### What it does

Keeps the genre classifier current by finding UNKNOWN keywords in the curated table and classifying them with Amazon Bedrock (Claude Haiku 4.5). The result is written back into `genre_classifier_pkg.zip` in the artifacts bucket, where it will be picked up by the Glue job on the next Stage B run.

### Execution steps

1. **Query UNKNOWN keywords** — Athena query against curated, filtered to `derived_genre = 'UNKNOWN'`, ordered by search count descending, limit `MAX_NEW_KEYWORDS × 3` (oversamples to account for keywords already in the LUT)

2. **Load existing LUT** — Download and unzip `genre_classifier_pkg.zip` from the artifacts bucket; parse `lut_extended.json`

3. **Find new keywords** — Subtract LUT entries from query results; take the top `MAX_NEW_KEYWORDS` (default: 20,000)

4. **Classify in batches of 50** — Call Bedrock `converse` API with each batch. Prompt: classify each keyword into one of the 9 genre classes or UNKNOWN

5. **Throttling and retry** — Exponential backoff: `sleep(2^n)` on throttle responses, up to 4 retries per batch. Inter-batch delay: 0.3 s

6. **Timeout safety** — Checks `context.get_remaining_time_in_millis() < 120_000`; stops batch processing with a log message if fewer than 120 s remain before the 900 s Lambda timeout

7. **Rebuild and upload** — Updates `lut_extended.json` with newly classified entries, rebuilds the ZIP, uploads to `s3://[artifacts-bucket]/ott/searchevents/genre_classifier_pkg.zip`

### Bedrock model

| Parameter | Value |
|---|---|
| Model ID | `global.anthropic.claude-haiku-4-5-20251001-v1:0` |
| API | `bedrock:InvokeModel` via `converse` |
| Batch size | 50 keywords per call |
| Max keywords per run | 20,000 |
| Throttle delay | 0.3 s between batches |
| Retry on throttle | Exponential backoff, max 4 retries |

### Genre classes

The model classifies into exactly 9 classes:

| Class | Content type |
|---|---|
| `PHIM_TRUNG` | Chinese films and series |
| `PHIM_VIET` | Vietnamese films and series |
| `PHIM_HAN` | Korean films and series |
| `PHIM_AU_MY` | Western (European/American) content |
| `ANIME` | Japanese animation |
| `THE_THAO` | Sports |
| `NHAC` | Music |
| `TRUYEN_HINH` | Television / broadcast |
| `UNKNOWN` | Cannot be classified |

### Lambda configuration

| Parameter | Value |
|---|---|
| Runtime | Python 3.12 |
| Timeout | 900 s (15 min) |
| Memory | 512 MB |
| Trigger | EventBridge — DQ SM `SUCCEEDED` |
| DLQ | `sdlf-ott-mainLUT-dlq` (14-day retention) |
| Log group | `/aws/lambda/sdlf-ott-mainLUT-refresh` (30-day retention) |

### Lake Formation access

The LUT Refresh role (`pLutRefreshRoleArn`, SSM: `/sdlf/pipeline/rRole/ott-mainLUT`) has `SELECT` on `curated` with four columns **excluded**: `user_id_hashed`, `search_session_id`, `has_premium`, `subscription_count`.

The LUT query only needs `keyword_norm` and `derived_genre`, so all user-fingerprinting columns are excluded.

### Environment variables

| Variable | Value |
|---|---|
| `BEDROCK_MODEL_ID` | `global.anthropic.claude-haiku-4-5-20251001-v1:0` |
| `MAX_NEW_KEYWORDS` | `20000` |
| `BATCH_SIZE` | `50` |
| `THROTTLE_SECONDS` | `0.3` |
| `ARTIFACTS_KEY` | `ott/searchevents/genre_classifier_pkg.zip` |
