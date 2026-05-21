import csv
import io
import os
import time
from datetime import date, timedelta

import boto3
from datalake_library.commons import init_logger

logger = init_logger(__name__)

athena = boto3.client("athena")
s3 = boto3.client("s3")
sns = boto3.client("sns")

DB = os.environ["ATHENA_DATABASE"]
RESULTS = os.environ["ATHENA_RESULTS"]
STAGE_BUCKET = os.environ["STAGE_BUCKET"]
ANALYTICS_PREFIX = os.environ["ANALYTICS_PREFIX"]
SNS_TOPIC_ARN = os.environ["SNS_TOPIC_ARN"]
# IAM policy scopes athena:StartQueryExecution to workgroup/sdlf-ott; queries
# without a WorkGroup default to 'primary' and fail with AccessDenied.
WORKGROUP = os.environ.get("ATHENA_WORKGROUP", "sdlf-ott")
# Published to SSM by the dashboard stack. This Lambda no longer renders its
# own HTML page — the CloudFront dashboard (refreshed by the Trending Lambda)
# is the single stakeholder-facing presentation surface. This Lambda's job is
# to keep the five report CSVs fresh; those back the HTTP API and the
# dashboard's business-question sections.
DASHBOARD_URL = os.environ.get("DASHBOARD_URL", "")

QUERIES = {
    "content_gaps": """
        SELECT keyword_norm, derived_genre,
               COUNT(*) AS searches,
               SUM(CASE WHEN is_search_abandoned THEN 1 ELSE 0 END) AS abandoned,
               ROUND(100.0 * SUM(CASE WHEN is_search_abandoned THEN 1 ELSE 0 END)
                     / CAST(COUNT(*) AS double), 1) AS abandon_rate_pct
        FROM {db}.curated
        WHERE dt >= '{start_dt}' AND derived_genre NOT IN ('UNKNOWN', 'EMPTY_QUERY')
              AND keyword_norm IS NOT NULL AND keyword_norm != ''
        GROUP BY keyword_norm, derived_genre
        HAVING SUM(CASE WHEN is_search_abandoned THEN 1 ELSE 0 END) >= 5
        ORDER BY abandon_rate_pct DESC, abandoned DESC
        LIMIT 500
    """,
    "premium_vs_free": """
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
    """,
    "repeat_search_rate": """
        SELECT derived_genre,
               COUNT(*) AS total_searches,
               SUM(CASE WHEN is_repeat_search THEN 1 ELSE 0 END) AS repeat_searches,
               ROUND(100.0 * SUM(CASE WHEN is_repeat_search THEN 1 ELSE 0 END)
                     / CAST(COUNT(*) AS double), 1) AS repeat_pct
        FROM {db}.curated
        WHERE dt >= '{start_dt}'
        GROUP BY derived_genre
        ORDER BY repeat_pct DESC
    """,
    "hour_of_day_heatmap": """
        SELECT hour_of_day_vn,
               derived_genre,
               COUNT(*) AS searches,
               ROUND(100.0 * COUNT(*) / CAST(SUM(COUNT(*)) OVER (PARTITION BY derived_genre) AS double), 1) AS pct_of_genre
        FROM {db}.curated
        WHERE dt >= '{start_dt}' AND derived_genre NOT IN ('UNKNOWN', 'EMPTY_QUERY')
        GROUP BY hour_of_day_vn, derived_genre
        ORDER BY derived_genre, hour_of_day_vn
    """,
    "guest_vs_auth_demand": """
        SELECT derived_genre,
               COUNT(*) AS total_searches,
               SUM(CASE WHEN user_is_authenticated THEN 1 ELSE 0 END) AS auth_searches,
               SUM(CASE WHEN NOT user_is_authenticated THEN 1 ELSE 0 END) AS guest_searches,
               ROUND(100.0 * SUM(CASE WHEN NOT user_is_authenticated THEN 1 ELSE 0 END)
                     / CAST(COUNT(*) AS double), 1) AS guest_share_pct
        FROM {db}.curated
        WHERE dt >= '{start_dt}' AND derived_genre NOT IN ('UNKNOWN', 'EMPTY_QUERY')
        GROUP BY derived_genre
        ORDER BY guest_share_pct DESC
    """,
}


def latest_dt():
    _, rows = athena_query(f"SELECT MAX(dt) AS max_dt FROM {DB}.curated")
    val = rows[0].get("max_dt", "") if rows else ""
    if not val:
        raise RuntimeError("curated table has no data — cannot determine reference date")
    return date.fromisoformat(val)


def athena_query(sql):
    r = athena.start_query_execution(
        QueryString=sql,
        QueryExecutionContext={"Database": DB},
        ResultConfiguration={"OutputLocation": RESULTS},
        WorkGroup=WORKGROUP,
    )
    qid = r["QueryExecutionId"]
    while True:
        st = athena.get_query_execution(QueryExecutionId=qid)["QueryExecution"]["Status"]["State"]
        if st == "SUCCEEDED":
            break
        if st in ("FAILED", "CANCELLED"):
            raise RuntimeError(f"Athena query {st}: {qid}")
        time.sleep(3)
    headers = None
    rows = []
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


def lambda_handler(event, context):
    logger.info(
        f"Content gap report triggered — source: {event.get('source', '?')} "
        f"detail-type: {event.get('detail-type', '?')}"
    )
    ref_date = latest_dt()
    dt = str(ref_date)
    start_dt = str(ref_date - timedelta(days=90))
    summary = {}
    errors = 0

    for name, sql_tpl in QUERIES.items():
        try:
            headers, rows = athena_query(sql_tpl.format(db=DB, start_dt=start_dt))
            summary[name] = write_csv(f"{ANALYTICS_PREFIX}{name}/{dt}/{name}.csv", headers, rows)
        except Exception as e:
            logger.error(f"{name} failed: {e}")
            summary[name] = -1
            errors += 1

    message = (
        f"OTT Content Gap Report — {dt}\n"
        f"Content gaps (abandoned keywords): {summary.get('content_gaps', 0)}\n"
        f"Premium vs free: {summary.get('premium_vs_free', 0)} genres\n"
        f"Repeat search rate: {summary.get('repeat_search_rate', 0)} genres\n"
        f"Hour heatmap: {summary.get('hour_of_day_heatmap', 0)} rows\n"
        f"Guest vs auth: {summary.get('guest_vs_auth_demand', 0)} genres\n"
        f"Errors: {errors}\n\n"
        f"Dashboard: {DASHBOARD_URL or '(dashboard URL not configured)'}"
    )
    sns.publish(
        TopicArn=SNS_TOPIC_ARN,
        Subject=f"OTT Content Gap Report {dt}",
        Message=message,
    )
    logger.info(f"Report complete — {sum(v for v in summary.values() if v > 0)} total rows, {errors} errors")
    return {
        "dt": dt,
        "reports": summary,
        "prefix": f"s3://{STAGE_BUCKET}/{ANALYTICS_PREFIX}",
        "dashboard_url": DASHBOARD_URL,
        "errors": errors,
    }
