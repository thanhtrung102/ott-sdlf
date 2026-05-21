---
title: "Verify"
date: 2026-05-20
weight: 6
chapter: false
pre: " <b> 5.6 </b> "
---

The pipeline ran, the analytics fired, the API answered. This chapter runs three reproducible verification tools that turn "looks fine" into "16 assertions passed, here's the proof".

---

## 5.6.1 The contract test (16 assertions)

`scripts/contract_test.py` is the regression test that runs on every CI/CD deploy. It checks the end-user-facing contracts that have broken in past iterations.

> 💡 **TIP:** This is the single most useful verification command in the workshop. If `All contracts passed.` prints, the pipeline is healthy end-to-end (API + dashboard + Athena catalog).

```powershell
$env:OTT_API_KEY = (aws ssm get-parameter --name /sdlf/ott/api-key/prod `
  --region ap-southeast-1 --query Parameter.Value --output text)

python D:\ott-sdlf\scripts\contract_test.py
```

**Expected** (live output, captured 2026-05-20):

```
=== Contract: REST API ===
  [PASS] API rejects request without x-api-key (P0a auth)  (status=401)
  [PASS] API rejects wrong x-api-key (P0a auth)  (status=401)
  [PASS] API /content-gaps returns 3 rows  (got 3)
  [PASS] API content_gaps top row has non-empty keyword  (top keyword='nguyen l')
  [PASS] API /content-gaps?report=premium_vs_free returns 2 rows
  [PASS] premium_vs_free top has numeric premium_share_pct  (pct=6.3)
  [PASS] API /trending returns 3 rows
  [PASS] API exposes X-Data-Freshness header (P0b)  (X-Data-Freshness='2026-05-20T09:11:24+00:00')
  [PASS] API exposes Last-Modified header (P0b)  (Last-Modified='Wed, 20 May 2026 09:11:24 +0000')
  [PASS] trending top row is NOT EMPTY_QUERY (N1 regression)  (top kw='nữ thanh tra tài ba' genre=PHIM_VIET)

=== Contract: Dashboard HTML artifact + presigned URL ===
  [PASS] CG dashboard date directory exists  (latest=analytics/content-gap/report/2022-06-22/)
  [PASS] Dashboard report.html exists & non-trivial size  (size=60322)
  [PASS] Dashboard URL: HTTP 200 + <html (L5 SigV4 regression)  (status=200 starts='<!DOCTYPE html>...')

=== Contract: Athena catalog ===
  [PASS] Athena: derived_genre partition queryable (L3 regression)  (row=['PHIM_HAN', '6270'])
  [PASS] Athena: dq_results queryable (L7 LF regression)  (row=['Failed', '41'])
  [PASS] Athena: raw_search_events.action visible (L8 regression)  (row=['search'])

All contracts passed.
```

If any assertion fails, the script exits non-zero and the CI/CD `Deploy` stage fails the build.

---

## 5.6.2 Live data visuals (`scripts/audit_visuals.py`)

This script runs a battery of Athena queries and renders ASCII charts so you can see actual numbers without opening the Athena console.

```powershell
python D:\ott-sdlf\scripts\audit_visuals.py
```

**Expected** (live 2026-05-20 — abbreviated to key sections; numbers are for the 19-partition reference deploy):

```
==============================================================================
1. QUALITY GATES — row counts at each lifecycle stage
==============================================================================

1a. Curated layer: total rows per genre
  PHIM_VIET       371,723  ██████████████████████████████████████████████████
  UNKNOWN         161,209  █████████████████████
  PHIM_TRUNG      153,399  ████████████████████
  ANIME           143,269  ███████████████████
  PHIM_AU_MY       96,763  █████████████
  EMPTY_QUERY      95,447  ████████████
  PHIM_HAN         93,674  ████████████
  NHAC             45,893  ██████
  TRUYEN_HINH      45,277  ██████
  THE_THAO         34,619  ████

1b. Curated retention vs the published LINEAGE log
  Curated rows: 1,241,273
  LINEAGE log (latest Glue run): raw=1,298,470 -> output=1,145,826 (retention=0.882)

==============================================================================
2. END-USER VALUE — actual insights this delivers
==============================================================================

2b. Abandon rate by genre (sessions that quit before submitting)
  derived_genre  total   abandoned  abandon_pct
  -------------  ------  ---------  -----------
  PHIM_VIET      371721  36748      9.9
  PHIM_TRUNG     153401  12012      7.8
  ANIME          143268  16676      11.6
  ...

2d. Search volume by Vietnam hour-of-day (peak windows)
  0      62,302  █████████████████████████
  2      92,809  ██████████████████████████████████████
  3      96,560  ████████████████████████████████████████   ← peak (3 AM VN)
  11      5,771  ██                                          ← trough (11 AM)
  17     67,356  ███████████████████████████
  18     75,094  ███████████████████████████████
```

The full output is ~80 lines. Use it whenever you want a quick eyeball check on data shape without composing Athena queries by hand.

---

## 5.6.3 The dashboard

Two paths to view the dashboard.

### Path A: the hosted search-analytics dashboard

The dashboard is **deployed**, not a local file. The `sdlf-pipeline-ott-dashboard` stack hosts it on a private S3 bucket fronted by CloudFront (Origin Access Control). The Trending Lambda regenerates `index.html` into that bucket on **every pipeline run** (`write_dashboard`), so the hosted page is never more than one run stale.

Every figure is **population-true** — sourced from the unfiltered `fpt_ott_searchevents_analytics.curated` table (~1.24 M search events), not the volume-thresholded gold table. The gold `keyword_trends` table keeps its own job: the trending-ranking product behind `GET /trending`.

Open the live dashboard (URL is published to SSM by the stack):

```powershell
$URL = aws ssm get-parameter --name /sdlf/pipeline/rDashboardUrl/ott `
  --region ap-southeast-1 --query Parameter.Value --output text
Write-Host "Dashboard: $URL"
Start-Process $URL
```

To save a local copy for offline viewing — `regenerate_dashboard.py` now just downloads the live page (it no longer queries Athena or bakes numbers):

```powershell
python D:\ott-sdlf\scripts\regenerate_dashboard.py
```

**Expected console output**:

```
Live dashboard: https://<id>.cloudfront.net
Saved local copy -> D:\ott-sdlf\dashboard\index.html  (NN,NNN bytes)
```

The dashboard shows four KPIs (total searches, distinct keywords, overall abandon rate, top genre), a platform bar chart, a genre donut + table, a top-20 keyword table, and a platform-abandon horizontal bar.

> 📷 **Screenshot —** the dashboard open in a browser at its CloudFront URL: KPI tiles, platform bar chart, genre donut, and the top-20 keyword table.
> *Placeholder: capture and save as `01-dashboard.png` in this chapter folder, then replace this block with `![Search-analytics dashboard](01-dashboard.png)`.*

### Path B: the daily content-gap HTML report (Lambda-generated)

The Content-Gap Lambda writes an HTML report with all 5 report tables embedded, every time it runs. Stored in the stage bucket; accessed via 7-day presigned URL.

```powershell
$STAGE = aws ssm get-parameter --name /sdlf/storage/rStageBucket/prod --query Parameter.Value --output text
$LATEST = aws s3 ls "s3://$STAGE/analytics/content-gap/report/" --region ap-southeast-1 |
  ForEach-Object { ($_ -split ' ')[-1] } | Sort-Object | Select-Object -Last 1
$KEY = "analytics/content-gap/report/$LATEST" + "report.html"

# Generate a presigned URL with KMS support (SigV4 is required for SSE-KMS)
python -c "import boto3; from botocore.config import Config; s3 = boto3.client('s3', region_name='ap-southeast-1', config=Config(signature_version='s3v4')); print(s3.generate_presigned_url('get_object', Params={'Bucket': '$STAGE', 'Key': '$KEY'}, ExpiresIn=300))"
```

Open the URL in a browser. **Expected**: the same content as the SNS-emailed presigned URL — KPIs, 5 report tables, dark-mode styled.

---

## 5.6.4 CloudWatch dashboard + alarms

```powershell
Start-Process "https://ap-southeast-1.console.aws.amazon.com/cloudwatch/home?region=ap-southeast-1#dashboards:name=sdlf-ott-searchevents-pipeline"
```

**Expected widgets** (live, what you should actually see):
- Stage A/B/DQ SM execution counts (Succeeded / Failed)
- Stage B SM duration percentiles (p50, p90, p99)
- Stage A + Stage B DLQ depth
- Glue job elapsed time + bytes read
- Per-Lambda Invocations / Errors / Duration (LUT-Refresh, Content Gap, Trending)
- Gold DQ SM executions

> 📷 **Screenshot —** CloudWatch console: the `sdlf-ott-searchevents-pipeline` dashboard with all widgets populated.
> *Placeholder: capture and save as `02-cloudwatch-dashboard.png` in this chapter folder, then replace this block with `![CloudWatch pipeline dashboard](02-cloudwatch-dashboard.png)`.*

```powershell
aws cloudwatch describe-alarms --alarm-name-prefix sdlf-ott `
  --region ap-southeast-1 --query "MetricAlarms | length(@)" --output text
```

**Expected**: `17` — the full alarm count.

```powershell
aws cloudwatch describe-alarms --alarm-name-prefix sdlf-ott `
  --region ap-southeast-1 --query "MetricAlarms[?StateValue=='ALARM'].AlarmName" --output text
```

**Expected** (live): the two near-timeout alarms may show — `sdlf-ott-mainCG-report-near-timeout` and `sdlf-ott-mainLUT-refresh-near-timeout`. These fire when a Lambda's p90 duration nears its timeout; they are an early-warning signal, not an outage. Any `*-errors` or `*-sm-execution-failed` alarm in `ALARM`, however, must be fixed before declaring success. Chapter 5.7 covers this in detail.

---

## 5.6.5 Live API hit with the freshness header

The most concise end-to-end verification: a single curl-equivalent against the API.

```powershell
$API = aws ssm get-parameter --name /sdlf/pipeline/rApiUrl/ott --region ap-southeast-1 --query Parameter.Value --output text
$KEY = aws ssm get-parameter --name /sdlf/ott/api-key/prod --region ap-southeast-1 --query Parameter.Value --output text

$resp = Invoke-WebRequest -Uri "$API/trending?limit=3" -Headers @{"x-api-key" = $KEY}
Write-Host "Status:           $($resp.StatusCode)"
Write-Host "X-Data-Freshness: $($resp.Headers.'X-Data-Freshness')"
Write-Host "Cache-Control:    $($resp.Headers.'Cache-Control')"
Write-Host "Top result:"
($resp.Content | ConvertFrom-Json)[0] | Format-List
```

**Expected** (live):

```
Status:           200
X-Data-Freshness: 2026-05-19T15:41:26+00:00
Cache-Control:    public, max-age=300
Top result:

keyword_norm      : nữ thanh tra tài ba
derived_genre     : PHIM_VIET
current_cnt       : 4831
baseline_cnt      : 0
growth_multiplier : 
is_new_keyword    : true
```

If `Status: 200` + a parseable `X-Data-Freshness` timestamp + a non-empty top result come back, your pipeline is end-to-end healthy.

---

**Next**: [5.7 — Live Verification](../5.7-verification/).
