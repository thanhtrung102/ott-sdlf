---
title: "Overview"
date: 2026-05-20
weight: 1
chapter: false
pre: " <b> 5.1 </b> "
---

In this chapter you'll see *what* the pipeline does and *which* AWS services it uses, so when commands later reference (say) `sdlf-stage-glue`, you know what's underneath.

---

## What the pipeline does

A user types a query into the FPT Play OTT app. The app emits a `log_search` event to S3 in Parquet format. From there:

1. **Ingest** — A new Parquet file landing in `s3://raw-bucket/ott/searchevents/{YYYYMMDD}/` fires an EventBridge rule that starts Stage A (event routing) → Stage B (Glue ETL).
2. **Enrich** — A 19-step Glue ETL job repairs corrupt dates, hashes user IDs, classifies the search keyword into one of 10 genres, normalises the platform string, computes session boundaries, and writes the result to a curated Parquet table partitioned by `(dt, derived_genre)`.
3. **Quality-gate** — Glue Data Quality runs over the curated table; if it fails, the downstream analytics don't fire.
4. **Analyse** — Two Lambdas fan out from a successful DQ:
   - **Dashboard renderer (Trending Lambda)** — runs the trending Athena query plus every dashboard section query (KPIs, platform, genre, top 20 keywords, content gaps at full 500-row depth, premium vs free, repeat search, guest vs auth, hour×genre heatmap) concurrently against `curated`, then rewrites `index.html` to the CloudFront-fronted dashboard bucket. Every section is stamped with the dt window it actually aggregated (min/max date, missing days, total rows).
   - **LUT Refresh** — sends `UNKNOWN`-classified keywords to Bedrock Claude Haiku, rebuilds the classifier zip for the next Glue run, persists confirmed-unclassifiable verdicts so each run progresses.
5. **Serve** — CloudFront serves the rendered dashboard. This is the **single user-facing surface**: there is no parallel JSON API. Content Gaps and Trending sections carry their full-depth row lists in HTML, with in-page filtering.

---

## Services used

This pipeline consumes 12 AWS services. Skim the table; you'll see each one in action by chapter 5.5.

| Service | Why this pipeline needs it |
|---|---|
| **Amazon S3** | Five buckets — raw / stage / analytics / artifacts / dashboard — partitioned the medallion way: raw (immutable), curated (enriched), with `dashboard-prod` holding the rendered `index.html`. |
| **AWS Lake Formation** | Column-level RBAC on the `curated` table. Once `IAM_ALLOWED_PRINCIPALS` is revoked (chapter 3.5), every read goes through LF. |
| **AWS Glue** | (a) The 4.0 / Spark 3.3 ETL job, G.1X × 10 workers, ~25 min for the 14-day set; (b) the catalog hosting `raw_search_events`, `curated`, `dq_results`. |
| **AWS Glue Data Quality** | One ruleset evaluation over the curated table (post-Stage B). |
| **AWS Step Functions** | Three state machines (`mainA` event routing, `mainB` Glue orchestration, `mainDQ` quality gate). |
| **Amazon EventBridge** | Routes S3 ObjectCreated → Stage A; chains `Stage A SUCCEEDED` → `Stage B`; fans `DQ SUCCEEDED` to the two analytics Lambdas. |
| **AWS Lambda** | Two Python 3.12 functions — `mainTR-report` (dashboard renderer) and `mainLUT-refresh`, both x86_64 on the SDLF datalake-library Layer. Both have X-Ray active tracing. |
| **Amazon Athena** | Query path for the dashboard renderer (workgroup `sdlf-ott` with KMS encryption) — 9 concurrent section queries per refresh. |
| **Amazon Bedrock** | LUT Refresh Lambda uses Claude Haiku 4.5 (`global.anthropic.claude-haiku-4-5-20251001-v1:0`) to classify previously-UNKNOWN search keywords. |
| **Amazon CloudFront** | OAC-fronted, private-origin distribution of the rendered dashboard. The single user-facing surface. |
| **Amazon SQS** | Four DLQs (Stage A/B + the two analytics Lambdas) — all with depth alarms. |
| **Amazon SNS** | Operational notifications: SM failures, DLQ depth, dashboard refresh delivery (linking to the CloudFront dashboard). |
| **AWS KMS** | One customer-managed key encrypts every S3 bucket, every SQS DLQ, every CloudWatch log group. |
| **Amazon CloudWatch** | A single dashboard + 11 alarms covering Step Functions failures (Stage A/B/DQ), Glue runtime, Lambda errors/duration, and all 4 DLQ depths. |

---

## Architecture at a glance

```
                                  Raw Parquet
                                       │
                                 ObjectCreated
                                       │
                                       ▼
                              ┌────────────────┐
                              │ EventBridge    │
                              └────────┬───────┘
                                       │
                                       ▼
                              ┌────────────────┐
                              │ Stage A SM     │  (sdlf-stage-lambda)
                              │ event routing  │
                              └────────┬───────┘
                                       │ SUCCEEDED event
                                       ▼
                              ┌────────────────┐
                              │ Stage B SM     │  (sdlf-stage-glue)
                              │ orchestrate    │
                              └────────┬───────┘
                                       │
                                       ▼
                              ┌────────────────┐
                              │ Glue ETL       │  19-step enrichment
                              │ G.1X × 10      │  ~25 min for 14 days
                              └────────┬───────┘
                                       │
                                       ▼   Curated S3 (dt × derived_genre)
                              ┌────────────────┐
                              │ DQ State Mach. │  GlueDataQuality
                              └────────┬───────┘
                                       │ SUCCEEDED event
                       ┌───────────────┴───────────────┐
                       ▼                               ▼
              ┌────────────────┐               ┌────────────────┐
              │ Dashboard λ    │               │ LUT Refresh λ  │
              │ (Trending)     │               │ Bedrock Claude │
              │ 9 Athena qs +  │               │ → classifier   │
              │ HTML render    │               │   zip          │
              └────────┬───────┘               └────────┬───────┘
                       │                                 │
                       ▼                                 ▼
              ┌────────────────┐               ┌────────────────┐
              │ S3 → CloudFront│               │ S3 artifacts   │
              │ index.html     │               │ classifier zip │
              │ (SOLE surface) │               │                │
              └────────────────┘               └────────────────┘
```

You'll touch every box in this diagram by the time you finish chapter 5.6.

---

**Next**: [5.2 — Prerequisites](../5.2-prerequisites/).
