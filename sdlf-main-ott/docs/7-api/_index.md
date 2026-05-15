---
title: "7. HTTP API"
date: 2026-05-15
weight: 7
chapter: false
pre: <b>7. </b>
---

# HTTP API

The analytics API is a lightweight **Amazon API Gateway HTTP API** backed by a Python Lambda function. It exposes the daily CSV reports as JSON without requiring callers to have Athena or S3 access.

**Source**: `pipeline-ott-api.yaml`

---

## Base URL

```
https://{api-id}.execute-api.ap-southeast-1.amazonaws.com
```

The deployed URL is stored in SSM at `/sdlf/pipeline/rApiUrl/ott`.

---

## Endpoints

### GET /trending

Returns trending keyword data from the stage bucket CSV files.

**Query parameters**

| Parameter | Type | Required | Default | Notes |
|---|---|---|---|---|
| `type` | string | No | `all` | `all` — all genres; `unknown` — UNKNOWN genre only |
| `limit` | integer | No | `50` | 1–500 |
| `date` | string | No | Latest available | Format: `YYYY-MM-DD` |

**Request example**

```
GET /trending?type=all&limit=100&date=2026-05-14
```

**Response — 200 OK**

```json
[
  {
    "keyword_norm": "captain america",
    "derived_genre": "PHIM_AU_MY",
    "current_cnt": "4821",
    "baseline_cnt": "1203",
    "growth_multiplier": "4.01",
    "is_new_keyword": "false"
  },
  ...
]
```

**CSV source path**: `s3://[stage-bucket]/analytics/trending/{type}/{date}/trending_{type}.csv`

---

### GET /content-gaps

Returns content gap analysis data from the stage bucket CSV files.

**Query parameters**

| Parameter | Type | Required | Default | Notes |
|---|---|---|---|---|
| `report` | string | No | `content_gaps` | See report names below |
| `limit` | integer | No | `50` | 1–500 |
| `date` | string | No | Latest available | Format: `YYYY-MM-DD` |

**Available report names**

| `report` value | Columns returned | Business question |
|---|---|---|
| `content_gaps` | keyword_norm, derived_genre, searches, abandoned, abandon_rate_pct | What titles are missing? |
| `premium_vs_free` | derived_genre, total_searches, premium_searches, free_searches, premium_share_pct | Where do premium users search? |
| `repeat_search_rate` | derived_genre, total_searches, repeat_searches, repeat_pct | Where is search frustration highest? |
| `hour_of_day_heatmap` | hour_of_day_vn, derived_genre, searches, pct_of_genre | When does each genre peak? |
| `guest_vs_auth_demand` | derived_genre, total_searches, auth_searches, guest_searches, guest_share_pct | Which genres attract the most guests? |

**Request example**

```
GET /content-gaps?report=premium_vs_free&limit=10
```

**Response — 200 OK**

```json
[
  {
    "derived_genre": "PHIM_HAN",
    "total_searches": "182034",
    "premium_searches": "121882",
    "free_searches": "60152",
    "premium_share_pct": "66.9"
  },
  ...
]
```

**CSV source path**: `s3://[stage-bucket]/analytics/content-gap/{date}/{report}.csv`

---

## Error responses

| Status | Condition | Body |
|---|---|---|
| `400` | `limit` out of range (< 1 or > 500) | `{"error": "limit must be between 1 and 500"}` |
| `400` | Unknown `report` value | `{"error": "unknown report: {value}"}` |
| `404` | No data for requested date | `{"error": "no data found for date {date}"}` |
| `500` | S3 read error or unhandled exception | `{"error": "internal error"}` |

---

## Latest date resolution

When `date` is not specified, the Lambda lists the date-prefixed folders under the relevant S3 prefix, sorts them lexicographically, and returns the most recent:

```python
# pipeline-ott-api.yaml (inline Lambda code)
def latest_date_prefix(bucket, prefix):
    resp = s3.list_objects_v2(Bucket=bucket, Prefix=prefix, Delimiter='/')
    prefixes = [p['Prefix'] for p in resp.get('CommonPrefixes', [])]
    return sorted(prefixes)[-1] if prefixes else None
```

This means the API always serves the most recently completed daily run without requiring the caller to know the date.

---

## Lambda configuration

| Parameter | Value |
|---|---|
| Runtime | Python 3.12 |
| Timeout | 30 s |
| Memory | 128 MB |
| Default limit (`TOP_N`) | `50` (overridable via query parameter up to 500) |
| Log group | `/aws/lambda/sdlf-ott-api` (30-day retention) |

---

## CORS

```yaml
CorsConfiguration:
  AllowOrigins: ["*"]
  AllowMethods: ["GET"]
```

All origins are allowed for `GET` requests. No authentication is required for the API endpoints (internal use assumed). Add an API key or Cognito authoriser if external exposure is planned.

---

## IAM permissions

The API Lambda role has:
- `s3:GetObject` and `s3:ListBucket` on the stage bucket (content-gap and trending prefixes only)
- No Athena, Glue, or curated table access — it only reads pre-computed CSV files
