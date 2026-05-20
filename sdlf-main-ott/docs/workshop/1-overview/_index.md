---
title: "1. Overview"
date: 2026-05-20
weight: 1
chapter: false
pre: <b>1. </b>
---

# 1. Overview

In this chapter you'll see *what* the pipeline does and *which* AWS services it uses, so when commands later reference (say) `sdlf-stage-glue`, you know what's underneath.

---

## What the pipeline does

A user types a query into the FPT Play OTT app. The app emits a `log_search` event to S3 in Parquet format. From there:

1. **Ingest** — A new Parquet file landing in `s3://raw-bucket/ott/searchevents/{YYYYMMDD}/` fires an EventBridge rule that starts Stage A (event routing) → Stage B (Glue ETL).
2. **Enrich** — A 19-step Glue ETL job repairs corrupt dates, hashes user IDs, classifies the search keyword into one of 10 genres, normalises the platform string, computes session boundaries, and writes the result to a curated Parquet table partitioned by `(dt, derived_genre)`.
3. **Quality-gate** — Glue Data Quality runs over the curated table; if it fails, the downstream analytics don't fire.
4. **Analyse** — Three Lambdas fan out from a successful DQ:
   - **Trending** — week-over-week growth analysis, writes a gold-layer `keyword_trends` table + CSVs.
   - **Content Gap** — 5 reports (abandon rate, premium vs free, repeat search, hourly heatmap, guest vs auth) + a static HTML dashboard with a 7-day presigned URL.
   - **LUT Refresh** — sends `UNKNOWN`-classified keywords to Bedrock Claude Haiku, rebuilds the classifier zip for the next Glue run.
5. **Serve** — A key-protected HTTP API exposes the trending + content-gap reports as JSON with a `X-Data-Freshness` header.

---

## Services used

This pipeline consumes 14 AWS services. Skim the table; you'll see each one in action by chapter 5.

| Service | Why this pipeline needs it |
|---|---|
| **Amazon S3** | Four buckets — raw / stage / analytics / artifacts — partitioned the medallion way: raw (immutable), curated (enriched), gold (pre-computed). |
| **AWS Lake Formation** | Column-level RBAC on the `curated` table. `IAM_ALLOWED_PRINCIPALS` is revoked, so every read goes through LF. |
| **AWS Glue** | (a) The 4.0 / Spark 3.3 ETL job, G.1X × 10 workers, ~25 min for the 14-day set; (b) the catalog hosting `raw_search_events`, `curated`, `keyword_trends`, `dq_results`, and the 5 content-gap report tables. |
| **AWS Glue Data Quality** | Two ruleset evaluations — one over the curated table (post-Stage B), one over the gold table (post-Trending Lambda). |
| **AWS Step Functions** | Three state machines (`mainA` event routing, `mainB` Glue orchestration, `mainDQ` + `mainGoldDQ` quality gates). |
| **Amazon EventBridge** | Routes S3 ObjectCreated → Stage A; chains `Stage A SUCCEEDED` → `Stage B`; fans `DQ SUCCEEDED` to the three analytics Lambdas. |
| **AWS Lambda** | Four Python 3.12 functions — `mainTR-report`, `mainCG-report`, `mainLUT-refresh` (x86_64, on the SDLF datalake-library Layer) + `sdlf-ott-api` (arm64, no Layer). All four have X-Ray active tracing. |
| **Amazon API Gateway (HTTP API v2)** | `GET /trending` and `GET /content-gaps`. Auth via in-Lambda `x-api-key` check (the key is in SSM at `/sdlf/ott/api-key/prod`). |
| **Amazon Athena** | Query path for the three analytics Lambdas (workgroup `sdlf-ott` with KMS encryption). |
| **Amazon Bedrock** | LUT Refresh Lambda uses Claude Haiku 4.5 (`global.anthropic.claude-haiku-4-5-20251001-v1:0`) to classify previously-UNKNOWN search keywords. |
| **Amazon SQS** | Five DLQs (Stage A/B + the three analytics Lambdas) — all with depth alarms. |
| **Amazon SNS** | Operational notifications: SM failures, DLQ depth, daily report delivery (with presigned URLs). |
| **AWS KMS** | One customer-managed key encrypts every S3 bucket, every SQS DLQ, every CloudWatch log group. |
| **Amazon CloudWatch** | A single dashboard + 14 alarms covering Step Functions failures, Glue runtime, Lambda errors/throttles/duration, and all 5 DLQ depths. |

Reference: [Architecture](../../2-architecture/) for the full component dependency map.

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
                  ┌────────────────────┼────────────────────┐
                  ▼                    ▼                    ▼
         ┌────────────────┐   ┌────────────────┐   ┌────────────────┐
         │ Trending λ     │   │ Content Gap λ  │   │ LUT Refresh λ  │
         │ Athena + CTAS  │   │ 5 reports +    │   │ Bedrock Claude │
         │ → gold table   │   │ HTML dashboard │   │ → classifier   │
         └────────┬───────┘   └────────┬───────┘   └────────┬───────┘
                  │                    │                    │
                  └─────────┬──────────┘                    │
                            ▼                               ▼
                  ┌────────────────┐                ┌────────────────┐
                  │ HTTP API       │                │ S3 artifacts   │
                  │ x-api-key auth │                │ classifier zip │
                  └────────────────┘                └────────────────┘
```

You'll touch every box in this diagram by the time you finish chapter 6.

---

**Next**: [chapter 2 — Prerequisites](../2-prerequisites/).
