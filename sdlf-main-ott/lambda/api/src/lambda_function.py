import csv, io, json, os
from email.utils import format_datetime
import boto3

s3 = boto3.client("s3")
BUCKET = os.environ["STAGE_BUCKET"]
TOP_N = int(os.environ.get("TOP_N", "50"))
# Required for production. Compared against the x-api-key header.
# Stored as a Lambda env var (encrypted at rest by AWS-managed Lambda key).
EXPECTED_API_KEY = os.environ.get("API_KEY", "")

def latest_date_prefix(base):
    resp = s3.list_objects_v2(Bucket=BUCKET, Prefix=base, Delimiter="/")
    folders = sorted(p["Prefix"] for p in resp.get("CommonPrefixes", []))
    return folders[-1] if folders else None

def read_csv(key, limit):
    obj = s3.get_object(Bucket=BUCKET, Key=key)
    reader = csv.DictReader(io.StringIO(obj["Body"].read().decode("utf-8")))
    rows = [row for _, row in zip(range(limit), reader)]
    return rows, obj["LastModified"]

def ok(rows, last_modified):
    return {
        "statusCode": 200,
        "headers": {
            "Content-Type": "application/json",
            "Last-Modified": format_datetime(last_modified),
            "X-Data-Freshness": last_modified.isoformat(),
            "Cache-Control": "public, max-age=300",
        },
        "body": json.dumps(rows),
    }

def err(code, msg):
    return {"statusCode": code, "body": json.dumps({"error": msg})}

_VALID_TRENDING_TYPES = {"all", "unknown"}
_VALID_CG_REPORTS = {
    "content_gaps", "premium_vs_free", "repeat_search_rate",
    "hour_of_day_heatmap", "guest_vs_auth_demand",
}


def _check_auth(event):
    if not EXPECTED_API_KEY:
        return err(500, "API_KEY env var not configured")
    headers = {k.lower(): v for k, v in (event.get("headers") or {}).items()}
    presented = headers.get("x-api-key", "")
    if presented != EXPECTED_API_KEY:
        return err(401, "missing or invalid x-api-key header")
    return None


def handler(event, context):
    auth_failure = _check_auth(event)
    if auth_failure is not None:
        return auth_failure

    path = event.get("rawPath", "")
    qs = event.get("queryStringParameters") or {}
    try:
        limit = min(int(qs.get("limit", TOP_N)), 500)
    except (ValueError, TypeError):
        return err(400, "limit must be an integer")
    date = qs.get("date")

    try:
        if path == "/trending":
            trend_type = qs.get("type", "all")
            if trend_type not in _VALID_TRENDING_TYPES:
                return err(400, "type must be 'all' or 'unknown'")
            base = f"analytics/trending/{trend_type}/"
            prefix = f"{base}{date}/" if date else latest_date_prefix(base)
            if not prefix:
                return err(404, "No trending data available yet")
            rows, last_modified = read_csv(f"{prefix}trending_{trend_type}.csv", limit)
            return ok(rows, last_modified)

        elif path == "/content-gaps":
            report = qs.get("report", "content_gaps")
            if report not in _VALID_CG_REPORTS:
                return err(400, f"report must be one of: {', '.join(sorted(_VALID_CG_REPORTS))}")
            base = f"analytics/content-gap/{report}/"
            prefix = f"{base}{date}/" if date else latest_date_prefix(base)
            if not prefix:
                return err(404, "No content-gap data available yet")
            rows, last_modified = read_csv(f"{prefix}{report}.csv", limit)
            return ok(rows, last_modified)

        else:
            return err(404, "Not found — use /trending or /content-gaps")

    except Exception as exc:
        return err(500, str(exc))
