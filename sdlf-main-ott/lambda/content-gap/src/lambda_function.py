import csv
import io
import os
import time
from datetime import datetime, timezone

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
REPORT_TTL = int(os.environ.get("REPORT_PRESIGN_TTL_SECONDS", "604800"))  # 7 days

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
    "hour_of_day_heatmap": """
        SELECT hour_of_day_vn,
               derived_genre,
               COUNT(*) AS searches,
               ROUND(100.0 * COUNT(*) / CAST(SUM(COUNT(*)) OVER (PARTITION BY derived_genre) AS double), 1) AS pct_of_genre
        FROM {db}.curated
        WHERE derived_genre != 'UNKNOWN'
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
        WHERE derived_genre != 'UNKNOWN'
        GROUP BY derived_genre
        ORDER BY guest_share_pct DESC
    """,
}

QUERY_TITLES = {
    "content_gaps": "Content Gaps — Top Abandoned Keywords",
    "premium_vs_free": "Premium vs Free Demand by Genre",
    "repeat_search_rate": "Repeat Search Rate by Genre",
    "hour_of_day_heatmap": "Search Volume by Hour (Vietnam Time)",
    "guest_vs_auth_demand": "Guest vs Authenticated Demand by Genre",
}

QUERY_DESCRIPTIONS = {
    "content_gaps": "High abandoned + high abandon_rate_pct = title missing from platform. Use as acquisition shortlist.",
    "premium_vs_free": "High premium_share_pct genres justify higher licensing spend.",
    "repeat_search_rate": "High repeat_pct = users keep searching without finding. Content or UX problem.",
    "hour_of_day_heatmap": "Use pct_of_genre to find peak hours per genre for push notification scheduling.",
    "guest_vs_auth_demand": "High guest_share_pct genres are strongest login-gate conversion opportunities.",
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


def _html_table(headers, rows):
    if not headers:
        return "<p><em>No data</em></p>"
    ths = "".join(f"<th>{h}</th>" for h in headers)
    trs = ""
    for row in rows:
        trs += "<tr>" + "".join(f"<td>{row.get(h, '')}</td>" for h in headers) + "</tr>"
    return f"<table><thead><tr>{ths}</tr></thead><tbody>{trs}</tbody></table>"


def build_html_report(dt, results):
    sections = ""
    for name, (headers, rows, count) in results.items():
        title = QUERY_TITLES.get(name, name)
        desc = QUERY_DESCRIPTIONS.get(name, "")
        status = f'<span class="badge ok">{count} rows</span>' if count >= 0 else '<span class="badge err">FAILED</span>'
        table_html = _html_table(headers, rows) if count >= 0 else "<p><em>Query failed</em></p>"
        sections += f"""
        <section>
            <h2>{title} {status}</h2>
            <p class="desc">{desc}</p>
            {table_html}
        </section>"""

    return f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>OTT Content Gap Report — {dt}</title>
<style>
  body {{ font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif; margin: 0; background: #f5f5f5; color: #222; }}
  header {{ background: #1a1a2e; color: #fff; padding: 24px 32px; }}
  header h1 {{ margin: 0 0 4px; font-size: 1.4rem; }}
  header p {{ margin: 0; opacity: .7; font-size: .9rem; }}
  main {{ max-width: 1200px; margin: 0 auto; padding: 24px 32px; }}
  section {{ background: #fff; border-radius: 8px; padding: 24px; margin-bottom: 24px; box-shadow: 0 1px 4px rgba(0,0,0,.08); }}
  h2 {{ margin: 0 0 8px; font-size: 1.1rem; display: flex; align-items: center; gap: 10px; }}
  .desc {{ margin: 0 0 16px; color: #555; font-size: .85rem; }}
  table {{ width: 100%; border-collapse: collapse; font-size: .83rem; }}
  th {{ background: #f0f0f0; text-align: left; padding: 8px 10px; border-bottom: 2px solid #ddd; white-space: nowrap; }}
  td {{ padding: 7px 10px; border-bottom: 1px solid #eee; }}
  tr:hover td {{ background: #fafafa; }}
  .badge {{ font-size: .75rem; padding: 2px 8px; border-radius: 12px; font-weight: 600; }}
  .badge.ok {{ background: #d4edda; color: #155724; }}
  .badge.err {{ background: #f8d7da; color: #721c24; }}
  .note {{ color: #888; font-size: .8rem; margin: 6px 0 0; }}
</style>
</head>
<body>
<header>
  <h1>OTT Content Gap Report</h1>
  <p>Pipeline run: {dt} &nbsp;|&nbsp; Source: {DB}.curated</p>
</header>
<main>
{sections}
</main>
</body>
</html>"""


def write_html_report(key, html):
    s3.put_object(
        Bucket=STAGE_BUCKET,
        Key=key,
        Body=html.encode("utf-8"),
        ContentType="text/html",
    )
    url = s3.generate_presigned_url(
        "get_object",
        Params={"Bucket": STAGE_BUCKET, "Key": key},
        ExpiresIn=REPORT_TTL,
    )
    logger.info(f"HTML report -> s3://{STAGE_BUCKET}/{key} (presigned {REPORT_TTL}s)")
    return url


def lambda_handler(event, context):
    logger.info(
        f"Content gap report triggered — source: {event.get('source', '?')} "
        f"detail-type: {event.get('detail-type', '?')}"
    )
    dt = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    prefix = f"{ANALYTICS_PREFIX}{dt}/"
    summary = {}
    errors = 0
    results = {}

    for name, sql_tpl in QUERIES.items():
        try:
            headers, rows = athena_query(sql_tpl.format(db=DB))
            count = write_csv(f"{prefix}{name}.csv", headers, rows)
            summary[name] = count
            results[name] = (headers, rows, count)
        except Exception as e:
            logger.error(f"{name} failed: {e}")
            summary[name] = -1
            results[name] = ([], [], -1)
            errors += 1

    html = build_html_report(dt, results)
    report_url = write_html_report(f"{prefix}report.html", html)

    message = (
        f"OTT Content Gap Report — {dt}\n"
        f"Content gaps (abandoned keywords): {summary.get('content_gaps', 0)}\n"
        f"Premium vs free: {summary.get('premium_vs_free', 0)} genres\n"
        f"Repeat search rate: {summary.get('repeat_search_rate', 0)} genres\n"
        f"Hour heatmap: {summary.get('hour_of_day_heatmap', 0)} rows\n"
        f"Guest vs auth: {summary.get('guest_vs_auth_demand', 0)} genres\n"
        f"Errors: {errors}\n\n"
        f"Dashboard: {report_url}"
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
        "prefix": f"s3://{STAGE_BUCKET}/{prefix}",
        "report_url": report_url,
        "errors": errors,
    }
