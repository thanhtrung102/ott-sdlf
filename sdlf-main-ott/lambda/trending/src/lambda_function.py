import csv
import io
import os
import time
from datetime import date, timedelta

import boto3
from datalake_library.commons import init_logger

logger = init_logger(__name__)

athena = boto3.client("athena")
s3     = boto3.client("s3")
sns    = boto3.client("sns")

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

_BASELINE_CHECK_SQL = """
SELECT COUNT(*) AS cnt FROM {db}.curated
WHERE dt >= '{base_start}' AND dt < '{base_end}'
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


def has_baseline_data(cur_end, base_start, base_end):
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


def lambda_handler(event, context):
    logger.info(
        f"Trending report triggered — source: {event.get('source', '?')} "
        f"detail-type: {event.get('detail-type', '?')}"
    )
    ref = event.get("reference_date")
    today = date.fromisoformat(ref) if ref else date.today()
    cur_start  = str(today - timedelta(days=7))
    cur_end    = str(today)
    base_start = str(today - timedelta(days=35))
    base_end   = str(today - timedelta(days=28))
    dt = str(today)
    prefix = f"{ANALYTICS_PREFIX}{dt}/"

    use_fallback = not has_baseline_data(cur_end, base_start, base_end)
    if use_fallback:
        logger.warning("No baseline data found (< 4 weeks of pipeline runs) — using volume-only fallback")
    else:
        logger.info(f"Baseline window {base_start} to {base_end} has data — running growth query")

    summary, errors = {}, 0

    try:
        headers, rows = run_trending_query(cur_start, cur_end, base_start, base_end, use_fallback)
        count_all = write_csv(f"{prefix}trending_all.csv", headers, rows)
        summary["trending_all"] = count_all
    except Exception as e:
        logger.error(f"trending_all failed: {e}")
        summary["trending_all"] = -1
        errors += 1

    try:
        headers, rows = run_trending_query(
            cur_start, cur_end, base_start, base_end, use_fallback,
            genre_filter="AND c.derived_genre = 'UNKNOWN'" if not use_fallback
                         else "AND derived_genre = 'UNKNOWN'",
        )
        count_unknown = write_csv(f"{prefix}trending_unknown.csv", headers, rows)
        summary["trending_unknown"] = count_unknown
    except Exception as e:
        logger.error(f"trending_unknown failed: {e}")
        summary["trending_unknown"] = -1
        errors += 1

    all_rows_sample = []
    try:
        _, sample = athena_query(
            f"SELECT keyword_norm, derived_genre, COUNT(*) AS cnt "
            f"FROM {DB}.curated "
            f"WHERE dt >= '{cur_start}' AND dt < '{cur_end}' "
            f"GROUP BY keyword_norm, derived_genre ORDER BY cnt DESC LIMIT 5"
        )
        all_rows_sample = [f"{r['keyword_norm']} ({r['derived_genre']}, {r['cnt']})" for r in sample]
    except Exception:
        pass

    mode = "fallback/volume-only" if use_fallback else f"growth >={MIN_GROWTH}x"
    message = (
        f"OTT Trending Keywords Report — {dt}\n"
        f"Mode: {mode}\n"
        f"Trending keywords (all genres): {summary.get('trending_all', 0)}\n"
        f"Trending UNKNOWN (LUT targets): {summary.get('trending_unknown', 0)}\n"
        f"Top 5 current: {', '.join(all_rows_sample)}\n"
        f"Reports: s3://{STAGE_BUCKET}/{prefix}\n"
        f"Errors: {errors}"
    )
    sns.publish(
        TopicArn=SNS_TOPIC_ARN,
        Subject=f"OTT Trending Keywords Report {dt}",
        Message=message,
    )
    logger.info(
        f"Trending report complete — all:{summary.get('trending_all',0)} "
        f"unknown:{summary.get('trending_unknown',0)} errors:{errors} mode:{mode}"
    )
    return {
        "dt": dt,
        "mode": mode,
        "reports": summary,
        "prefix": f"s3://{STAGE_BUCKET}/{prefix}",
        "errors": errors,
    }
