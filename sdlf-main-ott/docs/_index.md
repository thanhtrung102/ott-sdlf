---
title: "OTT Search Analytics Pipeline"
date: 2026-05-20
weight: 1
chapter: true
description: "OTT Search Analytics Pipeline — AWS SDLF · FCJ Workshop"
---

# OTT Search Analytics Pipeline

A production-grade serverless data lake on **AWS SDLF** that ingests OTT search-event Parquet, enriches it with Vietnamese-keyword genre classification, and serves five analytics reports + a key-protected HTTP API.

> **This site is a workshop.** The [**Workshop**](workshop/) chapter is the fully-written, hands-on path. Sections 1-10 below are short reference stubs that point you at the relevant workshop chapter.

---

## Start here

| | |
|---|---|
| **[Workshop](workshop/)** | 8 chapters, ~100 min, copy-paste reproducible commands with live output for every step. Build the whole pipeline end-to-end on your own AWS account. |

---

## Reference sections

Short stubs of each subsystem; deeper hands-on coverage is in the workshop.

| # | Section | Workshop pointer |
|---|---|---|
| 1 | [Introduction](1-introduction/) | [Workshop §1 — Overview](workshop/1-overview/) |
| 2 | [Architecture](2-architecture/) | [Workshop §1 — Overview](workshop/1-overview/) |
| 3 | [Data Ingestion](3-ingestion/) | [Workshop §4 — Ingest](workshop/4-ingest/) |
| 4 | [Data Quality](4-quality/) | [Workshop §4 — Ingest](workshop/4-ingest/) |
| 5 | [Analytics Pipelines](5-analytics/) | [Workshop §5 — Analyze](workshop/5-analyze/) |
| 6 | [Gold Layer](6-gold-layer/) | [Workshop §5 — Analyze](workshop/5-analyze/) |
| 7 | [HTTP API](7-api/) | [Workshop §5 — Analyze](workshop/5-analyze/) |
| 8 | [Security](8-security/) | [Workshop §3 — Deploy](workshop/3-deploy/) |
| 9 | [Monitoring](9-monitoring/) | [Workshop §6 — Verify](workshop/6-verify/) |
| 10 | [Deployment](10-deployment/) | [Workshop §3 — Deploy](workshop/3-deploy/) |

---

> **Region**: `ap-southeast-1` (Singapore) ·  **Framework**: AWS SDLF v2 · **Runtime**: Python 3.12 (Lambda), Glue 4.0 / Spark 3.3 (ETL)
