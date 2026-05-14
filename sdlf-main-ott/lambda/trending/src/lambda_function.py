import csv
import io
import json
import os
import time
from datetime import date, timedelta

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

_TRENDING_SQL = """
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
       COALESCE(b.baseline_cnt, 0)                                               AS baseline_cnt,
       CASE WHEN COALESCE(b.baseline_cnt, 0) = 0 THEN NULL
            ELSE ROUND(CAST(c.current_cnt AS double) / b.baseline_cnt, 2)
       END                                                                        AS growth_multiplier,
       CASE WHEN b.baseline_cnt IS NULL THEN 'true' ELSE 'false' END             AS is_new_keyword
FROM current_window c
LEFT JOIN baseline_window b
       ON c.keyword_norm = b.keyword_norm AND c.derived_genre = b.derived_genre
WHERE c.current_cnt >= {min_volume}
  AND (b.baseline_cnt IS NULL
       OR CAST(c.current_cnt AS double) / b.baseline_cnt >= {min_growth})
{genre_filter}
ORDER BY c.current_cnt DESC
LIMIT 1000
"""

_FALLBACK_SQL = """
SELECT keyword_norm, derived_genre, COUNT(*) AS current_cnt,
       0 AS baseline_cnt, NULL AS growth_multiplier, 'true' AS is_new_keyword
FROM {db}.curated
WHERE dt >= '{cur_start}' AND dt < '{cur_end}'
{genre_filter}
GROUP BY keyword_norm, derived_genre
HAVING COUNT(*) >= {min_volume}
ORDER BY current_cnt DESC
LIMIT 1000
"""

_BASELINE_CHECK_SQL = """
SELECT COUNT(*) AS cnt FROM {db}.curated
WHERE dt >= '{base_start}' AND dt < '{base_end}'
"""

# Gold-layer CTAS: per-keyword x platform x genre x date rankings.
# Drops and recreates the external table each run so the gold layer
# reflects the most recent full-history aggregation.
_GOLD_DROP_SQL = "DROP TABLE IF EXISTS {gold_db}.keyword_trends"

_GOLD_CTAS_SQL = """
CREATE TABLE {gold_db}.keyword_trends
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
        CAST(SUM(CASE WHEN session_action = 'enter' THEN 1 ELSE 0 END)
                                                                AS bigint) AS enter_count,
        CAST(SUM(CASE WHEN is_search_abandoned THEN 1 ELSE 0 END)
                                                                AS bigint) AS abandoned_count,
        ROUND(CAST(SUM(CASE WHEN is_search_abandoned THEN 1 ELSE 0 END) AS double)
              / CAST(COUNT(*) AS double), 4)                              AS abandonment_rate,
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
        keyword_norm,
        platform_group,
        derived_genre,
        dt                                                                 AS trend_date_7d,
        CAST(RANK() OVER (
            PARTITION BY platform_group, derived_genre, dt
            ORDER BY COUNT(*) DESC
        ) AS integer) AS rank_prev
    FROM {curated_db}.curated
    GROUP BY keyword_norm, platform_group, derived_genre, dt
    HAVING COUNT(*) >= {min_volume}
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
    r.abandonment_rate,
    r.unique_users,
    r.rank_today,
    p.rank_prev                                                            AS rank_7d_ago,
    CASE WHEN p.rank_prev IS NULL THEN true ELSE false END                 AS is_new_entrant,
    CASE WHEN p.rank_prev IS NULL THEN NULL
         ELSE p.rank_prev - r.rank_today END                              AS rank_delta,
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
        )
    else:
        sql = _TRENDING_SQL.format(
            db=DB, cur_start=cur_start, cur_end=cur_end,
            base_start=base_start, base_end=base_end,
            min_volume=MIN_VOLUME, min_growth=MIN_GROWTH,
            genre_filter=genre_filter,
        )
    return athena_query(sql)


def _empty_s3_prefix(s3_url):
    """Delete all objects under an s3://bucket/prefix/ URL before CTAS."""
    parts = s3_url.replace("s3://", "").split("/", 1)
    bucket, prefix = parts[0], parts[1] if len(parts) > 1 else ""
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        keys = [{"Key": o["Key"]} for o in page.get("Contents", [])]
        if keys:
            s3.delete_objects(Bucket=bucket, Delete={"Objects": keys})
            logger.info(f"Deleted {len(keys)} objects from s3://{bucket}/{prefix}")


def write_gold_table():
    gold_db       = os.environ.get("GOLD_DATABASE", "sdlf_ott_gold")
    gold_location = os.environ.get("GOLD_LOCATION", "")
    if not gold_location:
        logger.warning("GOLD_LOCATION not set — skipping gold table write")
        return 0

    logger.info(f"Writing gold table {gold_db}.keyword_trends -> {gold_location}")
    athena_ddl(_GOLD_DROP_SQL.format(gold_db=gold_db))
    _empty_s3_prefix(gold_location)
    ctas = _GOLD_CTAS_SQL.format(
        gold_db=gold_db,
        gold_location=gold_location,
        curated_db=DB,
        min_volume=MIN_VOLUME,
    )
    athena_ddl(ctas)
    _, count_rows = athena_query(f"SELECT COUNT(*) AS cnt FROM {gold_db}.keyword_trends")
    count = int(count_rows[0]["cnt"]) if count_rows else 0
    logger.info(f"Gold table written: {count} rows")
    return count


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

    for name, genre_filter in [
        ("trending_all",     ""),
        ("trending_unknown", "AND c.derived_genre = 'UNKNOWN'" if not use_fallback
                             else "AND derived_genre = 'UNKNOWN'"),
    ]:
        try:
            headers, rows = run_trending_query(
                cur_start, cur_end, base_start, base_end, use_fallback, genre_filter,
            )
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
