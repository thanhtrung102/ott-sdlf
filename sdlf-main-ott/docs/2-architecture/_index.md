---
title: "2. Architecture"
date: 2026-05-15
weight: 2
chapter: false
pre: <b>2. </b>
---

# Architecture

## End-to-end flow

```
┌─────────────────────────────────────────────────────────────────────────────┐
│  UPSTREAM                                                                   │
│  S3 ObjectCreated → s3://[raw-bucket]/ott/searchevents/YYYYMMDD/*.parquet  │
└─────────────────────────────┬───────────────────────────────────────────────┘
                              │ EventBridge S3 notification
                              ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│  STAGE A  (pipeline-ott-mainA.yaml)                                         │
│  EventBridge Rule → Step Functions SM (sdlf-ott-main-sm-A)                 │
│  Lambda: sdlf-ott-main-stagelambda-A                                        │
│  Role: validates event structure, marks object as in-progress               │
└─────────────────────────────┬───────────────────────────────────────────────┘
                              │ SM Execution SUCCEEDED
                              │ (OR: Cron 19:00 UTC daily as fallback)
                              ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│  STAGE B  (pipeline-ott-mainB.yaml + pipeline-ott-glue-job.yaml)           │
│  EventBridge Rule → Step Functions SM (sdlf-ott-main-sm-B)                 │
│  Glue Job: sdlf-ott-searchevents-glue-job                                  │
│  10 × G.1X workers, Glue 4.0 (PySpark 3.3)                                │
│  Reads:  s3://[raw-bucket]/ott/searchevents/YYYYMMDD/*.parquet              │
│  Writes: s3://[analytics-bucket]/ott/searchevents/curated/dt=YYYY-MM-DD/   │
│  19-column enrichment (see Section 3)                                       │
└─────────────────────────────┬───────────────────────────────────────────────┘
                              │ SM Execution SUCCEEDED
                              ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│  DATA QUALITY  (pipeline-ott-dq-stage.yaml — instance: mainDQ)             │
│  Step Functions SM (sdlf-ott-main-sm-mainDQ)                                │
│  Glue DQ: profile + evaluate curated table                                  │
│  Glue Crawler: refresh curated partition list in Glue Catalog              │
│  Outputs: dq_results table, SNS alert on failure                           │
└──────────────┬──────────────────────────────────────────────────────────────┘
               │ DQ SM Execution SUCCEEDED — triggers all three in parallel
     ┌─────────┼──────────────────────────┐
     ▼         ▼                          ▼
┌──────────┐ ┌──────────────────────┐ ┌──────────────────────────────────────┐
│ CONTENT  │ │  LUT REFRESH         │ │  TRENDING                            │
│ GAP      │ │  (pipeline-ott-      │ │  (pipeline-ott-trending.yaml)        │
│ (pipeline│ │  lutrefresh.yaml)    │ │  Lambda: sdlf-ott-mainTR-report      │
│ -ott-    │ │  Lambda: sdlf-ott-   │ │  Athena: growth comparison query     │
│ content  │ │  mainLUT-refresh     │ │  (current 7d vs baseline 28-35d ago) │
│ gap.yaml)│ │  Athena: UNKNOWN     │ │  → trending_all.csv                  │
│ Lambda:  │ │  keywords query      │ │  → trending_unknown.csv              │
│ sdlf-ott │ │  Bedrock: classify   │ │  → keyword_trends (Parquet gold)     │
│ -mainCG  │ │  in batches of 50    │ │  EventBridge: "Trending Completed"   │
│ 5 Athena │ │  Rebuild LUT zip     │ │                    │                 │
│ queries  │ │  → artifacts bucket  │ │                    ▼                 │
│ 5 CSVs + │ │                      │ │  GOLD DQ (pipeline-ott-dq-stage.yaml │
│ HTML     │ │                      │ │  instance: mainGoldDQ)               │
│ report   │ │                      │ │  Profile + evaluate keyword_trends   │
└──────────┘ └──────────────────────┘ └──────────────────────────────────────┘
                                                    │
                                                    ▼
                              ┌─────────────────────────────────────────────┐
                              │  HTTP API  (pipeline-ott-api.yaml)          │
                              │  API Gateway HTTP API + Lambda              │
                              │  GET /trending?type=all|unknown             │
                              │  GET /content-gaps?report=...               │
                              │  Reads latest CSVs from stage bucket        │
                              └─────────────────────────────────────────────┘
```

---

## Component map

| Component | AWS Resource | CloudFormation Template |
|---|---|---|
| Stage A SM | `sdlf-ott-main-sm-A` | `pipeline-ott-mainA.yaml` |
| Stage B SM | `sdlf-ott-main-sm-B` | `pipeline-ott-mainB.yaml` |
| Glue ETL job | `sdlf-ott-searchevents-glue-job` | `pipeline-ott-glue-job.yaml` |
| Curated catalog table | `curated` in Glue DB `searchevents` | `pipeline-ott-glue-job.yaml` |
| Raw catalog table | `raw_search_events` in Glue DB `searchevents` | `pipeline-ott-glue-job.yaml` |
| DQ SM (curated) | `sdlf-ott-main-sm-mainDQ` | `pipeline-ott-dq-stage.yaml` |
| DQ SM (gold) | `sdlf-ott-main-sm-mainGoldDQ` | `pipeline-ott-dq-stage.yaml` |
| Content Gap Lambda | `sdlf-ott-mainCG-report` | `pipeline-ott-contentgap.yaml` |
| LUT Refresh Lambda | `sdlf-ott-mainLUT-refresh` | `pipeline-ott-lutrefresh.yaml` |
| Trending Lambda | `sdlf-ott-mainTR-report` | `pipeline-ott-trending.yaml` |
| Gold table | `keyword_trends` in DB `fpt_ott_searchevents_gold` | `pipeline-ott-trending.yaml` |
| HTTP API | `sdlf-ott-api` (API Gateway + Lambda) | `pipeline-ott-api.yaml` |
| Lake Formation grants | 7 principal grants on `curated` | `pipeline-ott-lakeformation.yaml` |
| Monitoring | Dashboard + 12 alarms | `pipeline-ott-monitoring.yaml` |

---

## Data stores

| Store | S3 Prefix | Format | Partition key |
|---|---|---|---|
| Raw events | `ott/searchevents/YYYYMMDD/` | Parquet | Bare date folder (YYYYMMDD) |
| Curated events | `ott/searchevents/curated/dt=YYYY-MM-DD/` | Parquet, Snappy | `dt` (DATE) |
| Gold layer | `ott/searchevents/gold/keyword_trends/trend_date=YYYY-MM-DD/` | Parquet, Snappy | `trend_date` (DATE) |
| Content Gap CSVs | `analytics/content-gap/YYYY-MM-DD/` | CSV | Date folder |
| Trending CSVs | `analytics/trending/{all\|unknown}/YYYY-MM-DD/` | CSV | Date folder |
| DQ results | `ott/searchevents/dq-results/run_date=YYYY-MM-DD/` | JSON | `run_date` |
| Athena temp results | `athena-results/{content-gap\|trending\|lut-refresh}/` | Athena output | None |
| Genre classifier | `ott/searchevents/genre_classifier_pkg.zip` | ZIP | None |

> All S3 paths are relative to the bucket resolved from SSM at deploy time. Actual bucket names are not hardcoded.

---

## Glue databases

| Database | Contents | Created by |
|---|---|---|
| `[searchevents DB]` | `raw_search_events`, `curated`, `dq_results`, 5 content-gap tables, 2 trending tables | SDLF dataset module + CFN templates |
| `fpt_ott_searchevents_gold` | `keyword_trends` | `pipeline-ott-trending.yaml` |

> The searchevents database name is resolved from SSM parameter `/sdlf/dataset/rAnalyticsGlueDataCatalog/searchevents`.

---

## Event routing summary

| Trigger | EventBridge rule | Target |
|---|---|---|
| S3 ObjectCreated on raw prefix | Stage A event rule | Stage A Step Functions SM |
| Stage A SM SUCCEEDED | Stage B event rule | Stage B Step Functions SM |
| Cron 19:00 UTC daily | Stage B cron rule | Stage B Step Functions SM (fallback) |
| Stage B SM SUCCEEDED | DQ event rule | DQ Step Functions SM |
| DQ SM SUCCEEDED | Analytics event rules (×3) | Content Gap, LUT Refresh, Trending Lambdas |
| Trending Lambda completes | `sdlf.ott.trending` event | Gold DQ Step Functions SM |

---

## Key design decisions

**Single partition key on curated (`dt` only)**
The curated table is partitioned only by `dt`, not by `dt + derived_genre`. This avoids writing ~10,000 small-file directories per day (one per genre per day combination), which would degrade Athena query performance. Genre filtering is done at query time, not at partition level.

**CFN-managed gold table with partition projection**
`keyword_trends` is defined as a CloudFormation resource (`rKeywordTrendsTable`) with partition projection enabled. The Trending Lambda only writes S3 data; Athena discovers new partitions automatically via projection. This eliminates the need for `MSCK REPAIR TABLE` and prevents catalog conflicts from Lambda-managed DROP/CTAS cycles.

**Staging-swap pattern for gold writes**
The Trending Lambda writes to a staging S3 location first, validates the row count, then swaps the data to the production gold location. The production CFN-managed table is never dropped. This guarantees no window of missing data if the CTAS fails mid-write.

**Bedrock for LUT refresh (not a static lookup table)**
Genre classification uses a layered waterfall: LUT → regex → fuzzy matching → UNKNOWN. Keywords that remain UNKNOWN are batched and sent to Claude Haiku 4.5 nightly. This keeps the LUT current without manual curation.
