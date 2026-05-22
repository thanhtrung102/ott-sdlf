---
title: "Verify"
date: 2026-05-20
weight: 6
chapter: false
pre: " <b> 5.6 </b> "
---

The pipeline ran, the analytics fired, the dashboard refreshed. This chapter runs three reproducible verification tools that turn "looks fine" into "every contract assertion passed, here's the proof".

---

## 5.6.1 The contract test

`scripts/contract_test.py` is the regression test that runs on every CI/CD deploy. It checks the end-user-facing contracts that have broken in past iterations — now centred on the CloudFront dashboard as the **single user-facing surface**.

> 💡 **TIP:** This is the single most useful verification command in the workshop. If `All contracts passed.` prints, the pipeline is healthy end-to-end (dashboard + Athena catalog).

```powershell
python D:\ott-sdlf\scripts\contract_test.py
```

**Expected** (live output, sample):

```
=== Contract: CloudFront dashboard (single user-facing surface) ===
  [PASS] Dashboard URL is published to SSM  (url=https://d3bdq70ai5wf18.cloudfront.net)
  [PASS] Dashboard returns HTTP 200 + non-trivial HTML  (status=200 bytes=390800)
  [PASS] Dashboard section present: Total Searches
  [PASS] Dashboard section present: Distinct Keywords
  [PASS] Dashboard section present: Top 20 Keywords
  [PASS] Dashboard section present: Trending Keywords
  [PASS] Dashboard section present: Content Gaps
  [PASS] Dashboard section present: Premium vs Free
  [PASS] Dashboard section present: Repeat Search Rate
  [PASS] Dashboard section present: Guest vs Authenticated
  [PASS] Dashboard section present: Search Volume by Hour
  [PASS] Header carries source-attribution caption
  [PASS] Every section header has its own .src caption (>=9)  (found=10)
  [PASS] Filterable tables (Trending + Content Gaps) carry deep rows  (data-kw tr count=...)
  [PASS] Hour×genre heatmap rendered (>=200 cells)  (hcell count=...)

=== Contract: Athena catalog ===
  [PASS] Athena: derived_genre partition queryable (L3 regression)  (row=['PHIM_HAN', '6270'])
  [PASS] Athena: dq_results queryable (L7 LF regression)  (row=['Failed', '41'])
  [PASS] Athena: raw_search_events.action visible (L8 regression)  (row=['search'])
  [PASS] Dashboard trending top row is NOT EMPTY_QUERY (N1 regression)

All contracts passed.
```

If any assertion fails, the script exits non-zero and the CI/CD `Deploy` stage fails the build.

---

## 5.6.2 Live data visuals (`scripts/audit_visuals.py`)

This script runs a battery of Athena queries and renders ASCII charts so you can see actual numbers without opening the Athena console.

```powershell
python D:\ott-sdlf\scripts\audit_visuals.py
```

**Expected** (live — abbreviated; numbers are for the reference deploy):

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
  ...

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
```

Use it whenever you want a quick eyeball check on data shape without composing Athena queries by hand.

---

## 5.6.3 The dashboard

The CloudFront dashboard is the **single stakeholder-facing presentation surface** — it contains every insight the pipeline produces. The `sdlf-pipeline-ott-dashboard` stack hosts it on a private S3 bucket fronted by CloudFront (Origin Access Control). The dashboard renderer Lambda (Trending Lambda) regenerates `index.html` into that bucket on **every pipeline run** (`write_dashboard`), running its nine section queries concurrently against Athena.

Every figure is **population-true** — sourced from the unfiltered `fpt_ott_searchevents_analytics.curated` table. Every section carries a `.src` caption stating exactly which dt window was aggregated, the row count, and any missing days — so a stakeholder can never mis-read "1.15M searches" without seeing that the data spans Jun 1–Jun 23, 2022 with two days missing.

Sections rendered:

- **Header strip** — overall dt window stamp: `Source: curated — 2022-06-01 → 2022-06-23 • 21 days • 2 missing (2022-06-16; 2022-06-19) • 1,145,826 rows`
- **KPIs** — total searches, distinct keywords, overall abandon rate, top genre
- **Volume** — searches by platform, genre distribution, top-20 keywords, platform abandon rates
- **Business questions** — every section stamped with its dt window:
  - **Trending Keywords** — 500-row in-page-filterable list (by keyword + genre); window caption shows both the current 7-day slice and the 4-week baseline
  - **Content Gaps** — 500-row filterable list of top-abandoned titles
  - **Premium vs Free / Repeat Search / Guest vs Authenticated** — per-genre share tables
  - **Search Volume by Hour × Genre** — 24 × 9 heatmap with per-hour totals row

Open the live dashboard (URL is published to SSM by the stack):

```powershell
$URL = aws ssm get-parameter --name /sdlf/pipeline/rDashboardUrl/ott `
  --region ap-southeast-1 --query Parameter.Value --output text
Write-Host "Dashboard: $URL"
Start-Process $URL
```

To save a local copy for offline viewing — `regenerate_dashboard.py` downloads the live page:

```powershell
python D:\ott-sdlf\scripts\regenerate_dashboard.py
```

**Expected console output**:

```
Live dashboard: https://<id>.cloudfront.net
Saved local copy -> D:\ott-sdlf\dashboard\index.html  (NN,NNN bytes)
```

> 📷 **Screenshot —** the dashboard open in a browser at its CloudFront URL: header source-attribution stamp, KPI tiles, platform/genre charts, top-20 keyword table, then the business-question sections (trending 500-row, content gaps 500-row, premium-vs-free, repeat-search, guest-vs-auth, hour×genre heatmap).
> *Placeholder: capture and save as `01-dashboard.png` in this chapter folder, then replace this block with `![Search-analytics dashboard](01-dashboard.png)`.*

The retired analytics HTTP API and the standalone content-gap Lambda used to provide deep JSON access; their job is now done by the dashboard's in-page-filterable 500-row tables. No JSON endpoint to maintain, no key rotation, no duplicate truth.

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
- Per-Lambda Invocations / Errors / Duration (LUT-Refresh, Dashboard renderer)

> 📷 **Screenshot —** CloudWatch console: the `sdlf-ott-searchevents-pipeline` dashboard with all widgets populated.
> *Placeholder: capture and save as `02-cloudwatch-dashboard.png` in this chapter folder, then replace this block with `![CloudWatch pipeline dashboard](02-cloudwatch-dashboard.png)`.*

```powershell
aws cloudwatch describe-alarms --alarm-name-prefix sdlf-ott `
  --region ap-southeast-1 --query "MetricAlarms | length(@)" --output text
```

**Expected**: `11` — the full alarm count.

```powershell
aws cloudwatch describe-alarms --alarm-name-prefix sdlf-ott `
  --region ap-southeast-1 --query "MetricAlarms[?StateValue=='ALARM'].AlarmName" --output text
```

**Expected** (live): no alarms firing on a healthy pipeline. Any `*-errors`, `*-sm-execution-failed`, or `*-dlq-not-empty` alarm in `ALARM` must be fixed before declaring success. Chapter 5.7 covers this in detail.

---

## 5.6.5 Spot-check the dashboard live

The most concise end-to-end verification: a single HTTP GET against the CloudFront dashboard.

```powershell
$URL = aws ssm get-parameter --name /sdlf/pipeline/rDashboardUrl/ott --region ap-southeast-1 --query Parameter.Value --output text
$resp = Invoke-WebRequest -Uri $URL
Write-Host "Status:           $($resp.StatusCode)"
Write-Host "Cache-Control:    $($resp.Headers.'Cache-Control')"
Write-Host "Bytes:            $($resp.RawContentLength)"
if ($resp.Content -match '(?s)Source: curated.+?(\d+) days.+?(\d[\d,]*) rows') {
  Write-Host "Window stamp:     $($matches[1]) days, $($matches[2]) rows"
}
```

**Expected** (live):

```
Status:           200
Cache-Control:    public, max-age=300
Bytes:            390800
Window stamp:     21 days, 1,145,826 rows
```

If `Status: 200` + a parseable source-attribution window stamp comes back, the pipeline is end-to-end healthy.

---

**Next**: [5.7 — Live Verification](../5.7-verification/).
