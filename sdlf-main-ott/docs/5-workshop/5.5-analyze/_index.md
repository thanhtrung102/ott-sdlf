---
title: "Analyze"
date: 2026-05-20
weight: 5
chapter: false
pre: " <b> 5.5 </b> "
---

The DQ state machine just emitted `DQ SUCCEEDED`. Two Lambdas fan out from that event. This chapter shows each one running and reads back the concrete output.

---

## 5.5.1 What happens automatically

```
DQ SM SUCCEEDED
   │
   ├─► Dashboard renderer (Trending Lambda)
   │      ─ 9 concurrent Athena queries over curated
   │      ─ trending + KPIs + every business-question section
   │      ─ Writes index.html → S3 → CloudFront
   │
   └─► LUT-Refresh Lambda ── Top-N UNKNOWN keywords ── Bedrock Claude ── classifier zip
```

Both Lambdas finish within ~5 minutes total wall-clock of `DQ SUCCEEDED`. The CloudFront dashboard is the **single user-facing surface** — every business-question section reads `curated` live, with the dt window stamped on each section caption.

---

## 5.5.2 Dashboard renderer (Trending Lambda) — single user-facing surface

Write the shared invocation payload once. This sidesteps PowerShell's quote stripping on native CLI args *and* AWS CLI v2's default base64 expectation for inline `--payload` strings (CLI v2 raises `Invalid base64` on the raw JSON).

```powershell
'{"source":"workshop","detail-type":"Manual Trigger"}' |
  Out-File -Encoding ASCII -NoNewline C:\tmp\lambda-payload.json
```

Then invoke the dashboard renderer:

```powershell
aws lambda invoke --function-name sdlf-ott-mainTR-report --region ap-southeast-1 `
  --cli-read-timeout 0 --payload fileb://C:\tmp\lambda-payload.json `
  C:\tmp\trending-out.json
Get-Content C:\tmp\trending-out.json
```

**Expected** (live — values vary with the latest dt partition you ingested):

```json
{
  "dt": "2022-06-23",
  "mode": "fallback / volume-only",
  "trending_rows": 517,
  "dashboard_bytes": 390800,
  "errors": 0
}
```

> `mode: fallback / volume-only` indicates fewer than 4 weeks of historical data — the growth comparison falls back to raw volume ranking. With ≥4 weeks ingested, `mode` becomes `growth ≥ 3x`.

`dashboard_bytes` is the UTF-8 byte size of the freshly-rendered `index.html` published to the CloudFront-fronted dashboard bucket — it equals the S3 object's `ContentLength` exactly, and matches the `bytes=` figure the contract test and `verify_live.py` report. See [§5.6.3](../5.6-verify/#563-the-dashboard).

---

## 5.5.3 What the renderer queries

The Lambda runs nine Athena queries concurrently against `curated`, then assembles the result into one HTML document:

| Section | Query | Depth |
|---|---|---|
| `dt_window` | `MIN(dt) / MAX(dt) / COUNT(DISTINCT dt)` — drives the source-attribution caption | scalar |
| `totals` | overall row count, distinct keywords, abandon rate | scalar |
| `platform` | `GROUP BY platform_group` | ~6 rows |
| `genre` | `GROUP BY derived_genre` | ~10 rows |
| `keywords` | top 20 keywords by volume | 20 rows |
| `content_gaps` | top-abandoned titles (excludes UNKNOWN/EMPTY_QUERY, HAVING abandoned ≥5) | 500 rows |
| `premium` | premium share per genre | all genres |
| `repeat` | repeat-search share per genre | all genres |
| `guest` | guest demand per genre | all genres |
| `hour_genre` | hour × genre matrix | 216 rows (24 × 9) |

All sections are stamped with the dt window they actually aggregated. The header caption reads:

```
Source: curated — 2022-06-01 → 2022-06-23 • 21 days • 2 missing (2022-06-16; 2022-06-19) • 1,145,826 rows — generated ...
```

If any days are missing from the window, the caption lists them (e.g., `• 2 missing (2022-06-16; 2022-06-19)`).

---

## 5.5.4 LUT-Refresh Lambda — Bedrock classification

```powershell
aws lambda invoke --function-name sdlf-ott-mainLUT-refresh --region ap-southeast-1 `
  --invocation-type Event --payload fileb://C:\tmp\lambda-payload.json `
  C:\tmp\lut-out.json
Write-Host "LUT refresh fired asynchronously. Watch CloudWatch /aws/lambda/sdlf-ott-mainLUT-refresh."
```

Runtime depends entirely on how many *new* UNKNOWN keywords the run finds. Only keywords absent from **both** the existing LUT and the resolved-unknown set are sent to Bedrock (50 per `converse` call, `MAX_NEW_KEYWORDS=15000` ceiling, 900 s Lambda timeout):

- **First run on a fresh deploy** — the LUT is near-empty, thousands of keywords are new, so the run goes the full ~10-15 min and may hit the timeout buffer and resume on the next trigger.
- **Steady-state run** — the LUT already covers the bulk of demand, so a run typically finds only a few dozen new keywords and finishes in **~15-30 seconds**.

Tail the logs:

```powershell
aws logs tail /aws/lambda/sdlf-ott-mainLUT-refresh --since 5m --follow --region ap-southeast-1
```

**Expected log lines** (live — steady-state run, numbers vary per run):

```
INFO LUT refresh triggered — source: workshop detail-type: Manual Trigger
INFO Athena: 33161 UNKNOWN keyword_norm values in curated table
INFO Loaded 128596 LUT entries, 31157 resolved-unknown markers
INFO Candidates: 50 | skipped as unclassifiable: 0 | to classify via Bedrock: 50
INFO   [1] classified: 3, unresolved: 47, batch errors: 0
INFO Classification complete: 3 new genres, 47 confirmed-unclassifiable, 0 batch errors
INFO Zip rebuilt: 128599 LUT entries, 31204 resolved-unknown -> s3://...-artifacts-prod/ott/searchevents/genre_classifier_pkg.zip
INFO Zip mirrored to project bucket -> s3://ott-search-<ACCT>-prod/ott/searchevents/genre_classifier_pkg.zip
```

`Athena: N UNKNOWN ...` counts *every* distinct UNKNOWN keyword in `curated`; `Candidates` is the subset not already in the LUT or the resolved set; `classified` is how many of those Bedrock assigned a genre. The rest are recorded as `resolved-unknown` so they are never re-sent — which is why a mature deployment classifies only a handful per run.

> The LUT-Refresh Lambda writes the refreshed classifier zip to **both** the SDLF artifacts bucket (provenance) **and** the project-specific bucket `ott-search-${ACCT}-prod/ott/searchevents/` (which the Glue job reads `--extra-py-files` from). The mirror is wired inside `save_lut()` via the `PROJECT_BUCKET` env var + an `s3:PutObject` grant in `pipeline-ott-lutrefresh.yaml` — no manual copy or CICD step is needed, and the next Stage B run automatically uses the enriched LUT. See [§5.2.5](../5.2-prerequisites/#525-the-genre-classifier-zip-and-glue-script) for why the project bucket is separate from the SDLF artifacts bucket.

---

## 5.5.5 Open the dashboard

```powershell
$DASH = aws ssm get-parameter --name /sdlf/pipeline/rDashboardUrl/ott --region ap-southeast-1 --query Parameter.Value --output text
Start-Process $DASH
```

You'll see one page with:

- **Four KPI cards** — total searches, distinct keywords, overall abandon rate, top genre.
- **Volume views** — searches by platform, genre distribution, top 20 keywords, platform abandon rates.
- **Business-question sections** (each stamped with its dt window):
  - **Trending Keywords** — 500-row filterable list, by keyword + genre.
  - **Content Gaps** — 500-row filterable list of top-abandoned titles.
  - **Premium vs Free / Repeat Search / Guest vs Authenticated** — per-genre share tables.
  - **Search Volume by Hour × Genre** — 24 × 9 heatmap with per-hour totals row at the bottom.

Every section header carries a `.src` caption like:

```
curated — abandon rate by keyword (excludes UNKNOWN+EMPTY_QUERY, HAVING abandoned ≥ 5)
        — 2022-06-01 → 2022-06-23 • 21 days • 2 missing (2022-06-16; 2022-06-19) • 1,145,826 rows
```

That caption is the contract: it tells the stakeholder exactly what slice of `curated` the figures came from. If the dataset later grows or has gaps, the caption updates automatically on the next refresh.

---

**Next**: [5.6 — Verify](../5.6-verify/).
