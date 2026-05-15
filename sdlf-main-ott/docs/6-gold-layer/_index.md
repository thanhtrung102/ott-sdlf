---
title: "6. Gold Layer"
date: 2026-05-15
weight: 6
chapter: false
pre: <b>6. </b>
---

# Gold Layer

The gold layer is a daily Parquet table (`keyword_trends`) that stores per-keyword, per-platform, per-genre search rankings with 7-day rank change metrics. It is produced by the **Trending Lambda** (`sdlf-ott-mainTR-report`) and is the primary data source for the `/trending` API endpoint.

---

## Trending Lambda overview

**Source**: `lambda/trending/src/lambda_function.py`

### Execution flow

1. Query `MAX(dt)` from curated → `ref_date` (reference date)
2. Compute time windows:
   - Current window: `ref_date - 7d` to `ref_date`
   - Baseline window: `ref_date - 35d` to `ref_date - 28d` (same weekday 4 weeks earlier)
3. Check if baseline data exists (`_BASELINE_CHECK_SQL`)
4. If yes: run growth comparison query (`_TRENDING_SQL`)
5. If no: run volume-only fallback query (`_FALLBACK_SQL`) — active for the first 4 weeks after pipeline launch
6. Write two CSV reports (all genres, UNKNOWN only)
7. Write gold Parquet via staging-swap pattern
8. Publish SNS summary + EventBridge `"Trending Report Completed"` event

### Trending detection query

```sql
WITH current_window AS (
    SELECT keyword_norm, derived_genre, COUNT(*) AS current_cnt
    FROM {db}.curated
    WHERE dt >= '{cur_start}' AND dt < '{cur_end}'
    GROUP BY keyword_norm, derived_genre
),
baseline_window AS (
    SELECT keyword_norm, derived_genre, COUNT(*) AS baseline_cnt
    FROM {db}.curated
    WHERE dt >= '{base_start}' AND dt < '{base_end}'
    GROUP BY keyword_norm, derived_genre
)
SELECT c.keyword_norm,
       c.derived_genre,
       c.current_cnt,
       COALESCE(b.baseline_cnt, 0)                            AS baseline_cnt,
       CASE WHEN COALESCE(b.baseline_cnt, 0) = 0 THEN NULL
            ELSE ROUND(CAST(c.current_cnt AS double) / b.baseline_cnt, 2)
       END                                                     AS growth_multiplier,
       CASE WHEN b.baseline_cnt IS NULL THEN true ELSE false END AS is_new_keyword
FROM current_window c
LEFT JOIN baseline_window b
       ON c.keyword_norm = b.keyword_norm AND c.derived_genre = b.derived_genre
WHERE c.current_cnt >= {min_volume}
  AND (b.baseline_cnt IS NULL
       OR CAST(c.current_cnt AS double) / b.baseline_cnt >= {min_growth})
{genre_filter}
ORDER BY c.current_cnt DESC
LIMIT 1000
```

**Thresholds** (from environment variables):
- `MIN_SEARCH_VOLUME = 10` — filters noise (keywords searched fewer than 10 times)
- `MIN_GROWTH_MULTIPLIER = 3.0` — only keywords with ≥ 3× week-over-week growth appear

**UNKNOWN filter**: The `trending_unknown` report adds `AND c.derived_genre = 'UNKNOWN'` to catch trending keywords that the classifier has not yet labelled — these are high-priority targets for the LUT Refresh Lambda.

**Fallback query** (volume-only, active when baseline window is empty):
```sql
SELECT keyword_norm, derived_genre, COUNT(*) AS current_cnt,
       0 AS baseline_cnt, NULL AS growth_multiplier, true AS is_new_keyword
FROM {db}.curated
WHERE dt >= '{cur_start}' AND dt < '{cur_end}'
{genre_filter}
GROUP BY keyword_norm, derived_genre
HAVING COUNT(*) >= {min_volume}
ORDER BY current_cnt DESC
LIMIT 1000
```

---

## Gold table — keyword_trends

### Schema

| Column | Type | Description |
|---|---|---|
| `keyword_norm` | STRING | Normalised keyword |
| `platform_group` | STRING | 6-bucket canonical platform |
| `derived_genre` | STRING | 9-class genre |
| `search_count` | BIGINT | Total searches on `trend_date` |
| `enter_count` | BIGINT | Searches where user submitted (not abandoned) |
| `abandoned_count` | BIGINT | Searches abandoned |
| `abandon_rate_pct` | DOUBLE | `abandoned / search_count × 100` |
| `unique_users` | BIGINT | `COUNT(DISTINCT user_id_hashed)` |
| `rank_today` | INT | Search-count rank within `platform_group × derived_genre × trend_date` |
| `rank_7d_ago` | INT | Rank in the nearest day 5–9 days prior (closest match to 7d lookback) |
| `is_new_entrant` | BOOLEAN | `rank_7d_ago IS NULL` (no rank 7 days ago) |
| `rank_improvement` | INT | `rank_7d_ago - rank_today` (positive = moved up) |
| `trend_date` | DATE | Partition key |

### CTAS query (full)

```sql
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
        keyword_norm, platform_group, derived_genre,
        dt AS trend_date,
        CAST(COUNT(*)                                           AS bigint) AS search_count,
        CAST(SUM(CASE WHEN session_action = 'enter' THEN 1 ELSE 0 END)
                                                                AS bigint) AS enter_count,
        CAST(SUM(CASE WHEN is_search_abandoned THEN 1 ELSE 0 END)
                                                                AS bigint) AS abandoned_count,
        ROUND(CAST(SUM(CASE WHEN is_search_abandoned THEN 1 ELSE 0 END) AS double)
              / CAST(COUNT(*) AS double) * 100, 2)              AS abandon_rate_pct,
        CAST(COUNT(DISTINCT user_id_hashed)                     AS bigint) AS unique_users
    FROM {curated_db}.curated
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
        keyword_norm, platform_group, derived_genre,
        trend_date AS trend_date_7d,
        CAST(RANK() OVER (
            PARTITION BY platform_group, derived_genre, trend_date
            ORDER BY search_count DESC
        ) AS integer) AS rank_prev
    FROM agg   -- reads from agg, not from curated again
),
prev_best AS (
    SELECT
        r.trend_date AS for_date,
        p.keyword_norm, p.platform_group, p.derived_genre, p.rank_prev,
        ROW_NUMBER() OVER (
            PARTITION BY r.trend_date, p.keyword_norm, p.platform_group, p.derived_genre
            ORDER BY ABS(date_diff('day', date(p.trend_date_7d), date(r.trend_date)) - 7)
        ) AS rn
    FROM ranked r
    JOIN prev_agg p
      ON r.keyword_norm   = p.keyword_norm
     AND r.platform_group = p.platform_group
     AND r.derived_genre  = p.derived_genre
     AND date_diff('day', date(p.trend_date_7d), date(r.trend_date)) BETWEEN 5 AND 9
)
SELECT
    r.keyword_norm, r.platform_group, r.derived_genre,
    r.search_count, r.enter_count, r.abandoned_count, r.abandon_rate_pct, r.unique_users,
    r.rank_today,
    p.rank_prev                                            AS rank_7d_ago,
    CASE WHEN p.rank_prev IS NULL THEN true ELSE false END AS is_new_entrant,
    CASE WHEN p.rank_prev IS NULL THEN NULL
         ELSE p.rank_prev - r.rank_today END               AS rank_improvement,
    r.trend_date
FROM ranked r
LEFT JOIN prev_best p
       ON p.for_date      = r.trend_date
      AND p.keyword_norm   = r.keyword_norm
      AND p.platform_group = r.platform_group
      AND p.derived_genre  = r.derived_genre
      AND p.rn             = 1
```

> **Note on `prev_agg`**: `prev_agg` reads from the `agg` CTE (already computed in memory), not from the `curated` table again. This eliminates a full second table scan that the naive approach would require.

---

## Staging-swap write pattern

The Trending Lambda uses a three-phase write to guarantee the production gold table is never left in a partial state:

```
Phase 1: CTAS to staging table
    DROP TABLE IF EXISTS {gold_db}.keyword_trends_staging
    empty s3://[analytics-bucket]/.../keyword_trends_staging/
    CTAS → keyword_trends_staging
    SELECT COUNT(*) → validate > 0 rows

Phase 2: Swap S3 data to production location
    empty s3://[analytics-bucket]/.../keyword_trends/
    copy from keyword_trends_staging/ → keyword_trends/
    (CFN-managed keyword_trends table picks up new data via partition projection)

Phase 3: Clean up staging
    DROP TABLE IF EXISTS {gold_db}.keyword_trends_staging
    empty s3://[analytics-bucket]/.../keyword_trends_staging/
```

If the CTAS produces 0 rows (Phase 1 validation fails), the production table is left untouched and a `RuntimeError` is raised, which triggers the Lambda DLQ and the CloudWatch alarm.

---

## CFN-managed catalog table

`keyword_trends` is defined as `rKeywordTrendsTable` in `pipeline-ott-trending.yaml`:

```yaml
rKeywordTrendsTable:
  Type: AWS::Glue::Table
  Properties:
    DatabaseName: !Ref pGoldDatabaseName
    TableInput:
      Name: keyword_trends
      Parameters:
        projection.enabled: "true"
        projection.trend_date.type: date
        projection.trend_date.format: yyyy-MM-dd
        projection.trend_date.range: "2022-01-01,NOW+1YEARS"
        projection.trend_date.interval: "1"
        projection.trend_date.interval.unit: DAYS
        storage.location.template: !Sub s3://${pGoldBucket}/ott/searchevents/gold/keyword_trends/trend_date=${!trend_date}/
```

Because the table is CFN-managed with partition projection, the Trending Lambda never needs to `DROP TABLE` or run `MSCK REPAIR TABLE` against the production catalog entry. The Lambda only manages S3 data.

---

## Lambda configuration

| Parameter | Value |
|---|---|
| Runtime | Python 3.12 |
| Timeout | 300 s |
| Memory | 256 MB |
| Trigger | EventBridge — DQ SM `SUCCEEDED` |
| DLQ | `sdlf-ott-mainTR-dlq` (14-day retention) |
| Log group | `/aws/lambda/sdlf-ott-mainTR-report` (30-day retention) |

### Environment variables

| Variable | Default | Description |
|---|---|---|
| `MIN_GROWTH_MULTIPLIER` | `3.0` | Week-over-week growth threshold |
| `MIN_SEARCH_VOLUME` | `10` | Minimum searches to include in trending/gold |
| `GOLD_DATABASE` | `fpt_ott_searchevents_gold` | Glue gold database |
| `GOLD_LOCATION` | SSM-resolved | S3 URI for gold keyword_trends prefix |
| `ANALYTICS_PREFIX` | `analytics/trending/` | Stage bucket prefix for CSV reports |

### Lake Formation access

The Trending role (`pTrendingRoleArn`, SSM: `/sdlf/pipeline/rRole/ott-mainTR`) has full `SELECT` on `curated` (all columns). This is required because `COUNT(DISTINCT user_id_hashed)` in the gold CTAS needs access to `user_id_hashed`.
