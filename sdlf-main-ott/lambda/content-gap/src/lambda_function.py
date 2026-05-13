import csv
import io
import os
import time
from datetime import datetime, timezone

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

QUERIES = {
    "content_gaps": """
        SELECT keyword_norm, derived_genre,
               COUNT(*) AS searches,
               SUM(CASE WHEN is_search_abandoned THEN 1 ELSE 0 END) AS abandoned,
               ROUND(100.0 * SUM(CASE WHEN is_search_abandoned THEN 1 ELSE 0 END)
                     / CAST(COUNT(*) AS double), 1) AS abandon_rate_pct
        FROM {db}.curated
        WHERE derived_genre != 'UNKNOWN'
          AND is_search_abandoned = true
        GROUP BY keyword_norm, derived_genre
        ORDER BY abandoned DESC
        LIMIT 500
    """,
    "genre_demand_30d": """
        SELECT derived_genre,
               COUNT(*) AS total_searches,
               ROUND(100.0 * COUNT(*) / CAST(SUM(COUNT(*)) OVER () AS double), 1) AS share_pct
        FROM {db}.curated
        WHERE dt >= DATE_FORMAT(CURRENT_DATE - INTERVAL '30' DAY, '%Y-%m-%d')
        GROUP BY derived_genre
        ORDER BY total_searches DESC
    """,
    "premium_vs_free": """
        SELECT derived_genre,
               COUNT(*) AS total_searches,
               SUM(CASE WHEN has_premium THEN 1 ELSE 0 END) AS premium_searches,
               SUM(CASE WHEN NOT has_premium THEN 1 ELSE 0 END) AS free_searches,
               ROUND(100.0 * SUM(CASE WHEN has_premium THEN 1 ELSE 0 END)
                     / CAST(COUNT(*) AS double), 1) AS premium_share_pct
        FROM {db}.curated
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
        GROUP BY derived_genre
        ORDER BY repeat_pct DESC
    """,
    "platform_demand_7d": """
        SELECT platform_group, derived_genre,
               COUNT(*) AS searches
        FROM {db}.curated
        WHERE dt >= DATE_FORMAT(CURRENT_DATE - INTERVAL '7' DAY, '%Y-%m-%d')
        GROUP BY platform_group, derived_genre
        ORDER BY platform_group, searches DESC
    """,
}


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
    dt = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    prefix = f"{ANALYTICS_PREFIX}{dt}/"
    summary = {}
    errors = 0

    for name, sql_tpl in QUERIES.items():
        try:
            headers, rows = athena_query(sql_tpl.format(db=DB))
            count = write_csv(f"{prefix}{name}.csv", headers, rows)
            summary[name] = count
        except Exception as e:
            logger.error(f"{name} failed: {e}")
            summary[name] = -1
            errors += 1

    message = (
        f"OTT Content Gap Report — {dt}\n"
        f"Content gaps (abandoned keywords): {summary.get('content_gaps', 0)}\n"
        f"Genre demand 30d: {summary.get('genre_demand_30d', 0)} genres\n"
        f"Premium vs free: {summary.get('premium_vs_free', 0)} genres\n"
        f"Repeat search rate: {summary.get('repeat_search_rate', 0)} genres\n"
        f"Platform demand 7d: {summary.get('platform_demand_7d', 0)} rows\n"
        f"Reports: s3://{STAGE_BUCKET}/{prefix}\n"
        f"Errors: {errors}"
    )
    sns.publish(
        TopicArn=SNS_TOPIC_ARN,
        Subject=f"OTT Content Gap Report {dt}",
        Message=message,
    )
    logger.info(f"Report complete — {sum(v for v in summary.values() if v > 0)} total rows, {errors} errors")
    return {"dt": dt, "reports": summary, "prefix": f"s3://{STAGE_BUCKET}/{prefix}", "errors": errors}
