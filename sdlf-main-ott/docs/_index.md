---
title: "OTT Search Analytics Pipeline"
date: 2026-05-15
weight: 1
chapter: true
description: "OTT Search Analytics Pipeline — AWS SDLF"
---

# OTT Search Analytics Pipeline

A production-grade serverless data lake built on the **AWS Serverless Data Lake Framework (SDLF)** that ingests raw search-event data from an OTT streaming platform, enriches it with automated genre classification, and surfaces business analytics through five analytical outputs and a live HTTP API.

---

## What this pipeline does

Every day, millions of search queries are issued on the platform. This pipeline captures every event, normalises and enriches it, then answers four strategic questions for the content and product teams:

- **What content is missing?** — keywords users search for and abandon without clicking a result
- **Who is searching?** — premium subscribers vs free users, authenticated vs guest, by genre
- **What is trending?** — keywords whose search volume grew ≥ 3× compared to the same week four weeks ago
- **When do users search?** — hour-of-day demand by genre in Vietnam time (UTC+7)

---

## Sections

| # | Section | What you will find |
|---|---|---|
| 1 | [Introduction](1-introduction/) | Business context, goals, scope |
| 2 | [Architecture](2-architecture/) | End-to-end system diagram and component map |
| 3 | [Data Ingestion](3-ingestion/) | Stage A event routing, Stage B Glue ETL, data schema |
| 4 | [Data Quality](4-quality/) | Glue DQ state machine, ruleset, crawler |
| 5 | [Analytics Pipelines](5-analytics/) | Content Gap, Trending, LUT Refresh |
| 6 | [Gold Layer](6-gold-layer/) | keyword_trends Parquet table, CTAS pattern |
| 7 | [HTTP API](7-api/) | /trending and /content-gaps endpoints |
| 8 | [Security](8-security/) | Lake Formation column-level access, KMS, IAM |
| 9 | [Monitoring](9-monitoring/) | CloudWatch dashboard, alarms, DLQs |
| 10 | [Deployment](10-deployment/) | Prerequisites, stack order, CI/CD pipeline |
| 11 | [Workshop](workshop/) | **FCJ-format guided walkthrough — 7 chapters, ~90 min, with reproducible commands + live output** |

---

> **Region**: ap-southeast-1 (Singapore)
> **Framework**: AWS SDLF v2
> **Runtime**: Python 3.12 (Lambda), Glue 4.0 / Spark 3.3 (ETL)
