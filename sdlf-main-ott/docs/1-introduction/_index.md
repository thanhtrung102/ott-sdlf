---
title: "1. Introduction"
date: 2026-05-15
weight: 1
chapter: false
pre: <b>1. </b>
---

# Introduction

## Business context

The platform is a Vietnamese OTT streaming service. Its search bar generates hundreds of thousands of events per day: users type keywords, select results, abandon searches, and repeat the same queries across sessions.

This raw event stream contains high-value signals that were previously unused:

- A keyword searched 10,000 times in a week with a 90 % abandon rate is almost certainly a title the platform does not carry
- A genre with 60 % guest-user demand is the strongest conversion opportunity for a login gate
- A keyword whose searches tripled this week versus four weeks ago is a trending title that deserves a push notification

The pipeline makes these signals visible, updated, and queryable every day.

---

## Goals

| Goal | Addressed by |
|---|---|
| Identify missing titles (acquisition shortlist) | Content Gap — `content_gaps` query |
| Prioritise licensing spend by premium-user demand | Content Gap — `premium_vs_free` query |
| Flag user frustration (high repeat-search) | Content Gap — `repeat_search_rate` query |
| Detect trending keywords for push notifications | Trending Lambda + gold layer |
| Target login-gate campaigns by genre | Content Gap — `guest_vs_auth_demand` query |
| Keep genre classification current without manual effort | LUT Refresh Lambda (Bedrock) |
| Expose analytics to internal tools without direct Athena access | HTTP API (GET /trending, GET /content-gaps) |

---

## Scope

**In scope**

- Raw search-event ingestion from S3 (Parquet files, YYYYMMDD partitions)
- PySpark enrichment: 19-column curated schema with genre classification, session stitching, and privacy hashing
- Automated data quality profiling and evaluation
- Five Athena-based content-gap queries (90-day lookback)
- Trending keyword detection with week-over-week growth comparison
- Bedrock-powered genre classifier refresh (Claude Haiku 4.5)
- Gold-layer Parquet table with daily keyword rankings
- HTTP API serving analytics as JSON
- Column-level access control via Lake Formation
- CloudWatch monitoring with 12 alarms

**Out of scope**

- Real-time (sub-minute) event streaming
- User-level recommendation models
- Content metadata enrichment (title catalogue)
- A/B test assignment or experiment tracking

---

## Dataset

| Field | Value |
|---|---|
| SDLF team | `ott` |
| SDLF dataset | `searchevents` |
| Raw prefix | `ott/searchevents/YYYYMMDD/` |
| Raw format | Parquet, 10 columns |
| Curated prefix | `ott/searchevents/curated/dt=YYYY-MM-DD/` |
| Curated format | Parquet, 19 columns |
| Frequency | Once daily (Glue job at 19:00 UTC or on upstream S3 event) |
| Region | ap-southeast-1 |

---

## Technology stack

| Layer | Technology |
|---|---|
| Orchestration | **AWS Step Functions** (Stage A, Stage B, DQ state machines) |
| ETL | **AWS Glue** 4.0, PySpark 3.3 |
| Query | **Amazon Athena** (workgroup `sdlf-ott`) |
| Storage | **Amazon S3** (raw, stage, analytics, artifacts buckets) |
| Catalog | **AWS Glue Data Catalog** |
| Access control | **AWS Lake Formation** column-level permissions |
| Encryption | **AWS KMS** (S3 SSE, SQS, Athena workgroup) |
| Analytics compute | **AWS Lambda** Python 3.12 (×4 functions) |
| AI classification | **Amazon Bedrock** — `global.anthropic.claude-haiku-4-5-20251001-v1:0` |
| API | **Amazon API Gateway** HTTP API + Lambda |
| Notifications | **Amazon SNS** |
| Event routing | **Amazon EventBridge** |
| Configuration | **AWS Systems Manager Parameter Store** |
| IaC | **AWS CloudFormation** (SDLF module pattern) |
| CI/CD | **AWS CodePipeline** (`sdlf-ott-cicd`) |
