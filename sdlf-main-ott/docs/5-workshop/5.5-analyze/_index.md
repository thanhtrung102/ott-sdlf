---
title: "Analyze"
date: 2026-05-20
weight: 5
chapter: false
pre: " <b> 5.5 </b> "
---

The DQ state machine just emitted `DQ SUCCEEDED`. Three Lambdas fan out from that event. This chapter shows each one running and reads back the concrete output.

---

## 5.5.1 What happens automatically

```
DQ SM SUCCEEDED
   │
   ├─► Trending Lambda  ── Athena ── gold.keyword_trends + CSVs ── EventBridge "Trending Report Completed"
   │                                                                       │
   │                                                                       ▼
   │                                                              Gold DQ State Machine
   │
   ├─► Content Gap Lambda ── 5 Athena queries ── CSVs + HTML dashboard ── SNS
   │
   └─► LUT-Refresh Lambda ── Top-N UNKNOWN keywords ── Bedrock Claude ── classifier zip
```

All three Lambdas finish within ~5 minutes total of wall-clock time after `DQ SUCCEEDED`.

---

## 5.5.2 Trending Lambda — week-over-week growth

The three Lambda invocations in §5.5.2–§5.5.4 share a payload. Write it to a file once — that sidesteps PowerShell's quote stripping on native CLI args *and* AWS CLI v2's default base64 expectation for inline `--payload` strings (CLI v2 raises `Invalid base64` on the raw JSON).

```powershell
'{"source":"workshop","detail-type":"Manual Trigger"}' |
  Out-File -Encoding ASCII -NoNewline C:\tmp\lambda-payload.json
```

Then invoke Trending:

```powershell
aws lambda invoke --function-name sdlf-ott-mainTR-report --region ap-southeast-1 `
  --cli-read-timeout 0 --payload fileb://C:\tmp\lambda-payload.json `
  C:\tmp\trending-out.json
Get-Content C:\tmp\trending-out.json
```

**Expected** (live 2026-05-20 — rows vary with the latest dt partition you ingested):

```json
{
  "dt": "2022-06-22",
  "mode": "fallback/volume-only",
  "reports": {
    "trending_all": 545,
    "trending_unknown": 25,
    "gold_rows": 13546
  },
  "prefixes": {
    "all": "s3://...-stage-prod/analytics/trending/all/2022-06-22/",
    "unknown": "s3://...-stage-prod/analytics/trending/unknown/2022-06-22/"
  },
  "errors": 0
}
```

> `mode: fallback/volume-only` indicates fewer than 4 weeks of historical data — the growth comparison falls back to raw volume ranking. With ≥4 weeks ingested, `mode` becomes `growth >=3.0x`.

**Verify** the gold table:

```sql
SELECT COUNT(*) AS gold_rows,
       COUNT(DISTINCT keyword_norm) AS unique_keywords
FROM fpt_ott_searchevents_gold.keyword_trends;
```

**Expected** (live 2026-05-20 — exact numbers vary):

```
gold_rows    unique_keywords
13546        1686
```

---

## 5.5.3 Content Gap Lambda — 5 daily reports + HTML dashboard

```powershell
aws lambda invoke --function-name sdlf-ott-mainCG-report --region ap-southeast-1 `
  --cli-read-timeout 0 --payload fileb://C:\tmp\lambda-payload.json `
  C:\tmp\cg-out.json
Get-Content C:\tmp\cg-out.json
```

**Expected** (live 2026-05-20):

```json
{
  "dt": "2022-06-22",
  "reports": {
    "content_gaps": 500,
    "premium_vs_free": 10,
    "repeat_search_rate": 10,
    "hour_of_day_heatmap": 216,
    "guest_vs_auth_demand": 9
  },
  "report_url": "https://...-stage-prod.s3.ap-southeast-1.amazonaws.com/analytics/content-gap/report/2022-06-22/report.html?X-Amz-Algorithm=...",
  "errors": 0
}
```

**Open the dashboard URL** from `report_url` — that's a 7-day presigned link to an HTML report with all 5 tables rendered.

> 📷 **Screenshot —** the content-gap HTML report open in a browser: KPI tiles across the top, the five report tables (content gaps, premium-vs-free, repeat-search, hourly heatmap, guest-vs-auth) below.
> *Placeholder: capture and save as `01-content-gap-report.png` in this chapter folder, then replace this block with `![Content-gap HTML report](01-content-gap-report.png)`.*

---

## 5.5.4 LUT-Refresh Lambda — Bedrock classification

```powershell
aws lambda invoke --function-name sdlf-ott-mainLUT-refresh --region ap-southeast-1 `
  --invocation-type Event --payload fileb://C:\tmp\lambda-payload.json `
  C:\tmp\lut-out.json
Write-Host "LUT refresh fired asynchronously. Watch CloudWatch /aws/lambda/sdlf-ott-mainLUT-refresh."
```

The LUT refresh takes ~10-15 minutes (depends on how many UNKNOWN keywords need classification). Tail the logs:

```powershell
aws logs tail /aws/lambda/sdlf-ott-mainLUT-refresh --since 5m --follow --region ap-southeast-1
```

**Expected log lines** (sample; numbers grow with each run — current LUT has 125k+ entries as of 2026-05-20):

```
2026-05-20T... INFO Loaded existing LUT: 122665 entries
2026-05-20T... INFO Fetched 20000 UNKNOWN keywords from Athena
2026-05-20T... INFO Bedrock batch 1/200 classified — added 47 PHIM_VIET, 23 ANIME, 12 PHIM_TRUNG ...
...
2026-05-20T... INFO Uploaded refreshed classifier to s3://...-artifacts-prod/ott/searchevents/genre_classifier_pkg.zip
2026-05-20T... INFO LUT refresh complete. Added 4823 new entries (new total: 127488).
```

> The LUT-Refresh Lambda writes the refreshed classifier zip to **both** the SDLF artifacts bucket (provenance) **and** the project-specific bucket `ott-search-${ACCT}-prod/ott/searchevents/` (which the Glue job reads `--extra-py-files` from). The mirror is wired inside `save_lut()` via the `PROJECT_BUCKET` env var + an `s3:PutObject` grant in `pipeline-ott-lutrefresh.yaml` — no manual copy or CICD step is needed, and the next Stage B run automatically uses the enriched LUT. See [§5.2.5](../5.2-prerequisites/#525-the-genre-classifier-zip-and-glue-script) for why the project bucket is separate from the SDLF artifacts bucket.

---

## 5.5.5 HTTP API — read the trending data programmatically

```powershell
$API = aws ssm get-parameter --name /sdlf/pipeline/rApiUrl/ott --region ap-southeast-1 --query Parameter.Value --output text
$KEY = aws ssm get-parameter --name /sdlf/ott/api-key/prod --region ap-southeast-1 --query Parameter.Value --output text

# Top 5 trending keywords (all genres)
Invoke-RestMethod -Uri "$API/trending?limit=5" -Headers @{"x-api-key" = $KEY}
```

**Expected** (live 2026-05-20 — counts vary with the latest dt; diacritics preserved end-to-end):

```
keyword_norm                                       derived_genre   current_cnt
-------------------------------------------------- --------------  -----------
nữ thanh tra tài ba                                PHIM_VIET       470
sao băng                                           PHIM_HAN        459
liên minh công lý: phiên bản của zack snyder       PHIM_AU_MY      417
fairy tail                                         ANIME           334
giữa thanh xuân                                    PHIM_VIET       230
```

**Verify the freshness header**:

```powershell
$r = Invoke-WebRequest -Uri "$API/trending?limit=1" -Headers @{"x-api-key" = $KEY}
$r.Headers.'X-Data-Freshness'
$r.Headers.'Last-Modified'
$r.Headers.'Cache-Control'
```

**Expected** (live 2026-05-20 — your timestamp will be from the latest Trending Lambda invocation):

```
2026-05-20T09:11:24+00:00
Wed, 20 May 2026 09:11:24 +0000
public, max-age=300
```

**Verify auth is enforced** — request without the header should return 401:

```powershell
try {
  Invoke-WebRequest -Uri "$API/trending?limit=1" -ErrorAction Stop
} catch {
  "Status: $($_.Exception.Response.StatusCode.value__)"
}
```

**Expected**:

```
Status: 401
```

---

## 5.5.6 Browse the 5 Content-Gap reports via API

```powershell
foreach ($r in @("content_gaps","premium_vs_free","repeat_search_rate","hour_of_day_heatmap","guest_vs_auth_demand")) {
  $rows = Invoke-RestMethod -Uri "$API/content-gaps?report=$r&limit=2" -Headers @{"x-api-key" = $KEY}
  Write-Host "`n=== $r (top 2) ==="
  $rows | Format-List
}
```

**Expected** (live 2026-05-20 — top 2 of `premium_vs_free`):

```
=== premium_vs_free (top 2) ===
derived_genre     : PHIM_AU_MY
total_searches    : 96763
premium_searches  : 6120
free_searches     : 90643
premium_share_pct : 6.3

derived_genre     : EMPTY_QUERY
total_searches    : 95447
premium_searches  : 4227
free_searches     : 91220
premium_share_pct : 4.4
```

---

**Next**: [5.6 — Verify](../5.6-verify/).
