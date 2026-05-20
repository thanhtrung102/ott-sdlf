---
title: "OTT Search Analytics Pipeline"
date: 2026-05-20
weight: 1
chapter: false
description: "First Cloud Journey report — OTT Search Analytics Pipeline on AWS SDLF"
---

# First Cloud Journey — Internship Report

This site follows the **First Cloud Journey (FCJ)** report template. The seven sections below mirror the standard FCJ structure; the fully-worked deliverable is **section 5, the Workshop**.

---

## Report sections

| # | Section | Status |
|---|---|---|
| 1 | [Worklog](1-worklog/) | Placeholder |
| 2 | [Proposal](2-proposal/) | Placeholder |
| 3 | [Translated Blogs](3-translated-blogs/) | Placeholder |
| 4 | [Events Participated](4-events-participated/) | Placeholder |
| 5 | **[Workshop](5-workshop/)** | **Complete — the live project** |
| 6 | [Self-Assessment](6-self-assessment/) | Placeholder |
| 7 | [Sharing and Feedback](7-sharing-and-feedback/) | Placeholder |

---

## Section 5 — the Workshop

A production-grade serverless data lake on **AWS SDLF** that ingests OTT search-event Parquet, enriches it with Vietnamese-keyword genre classification, and serves five analytics reports + a key-protected HTTP API.

The [Workshop](5-workshop/) is 8 chapters, ~100 minutes, every step copy-paste reproducible with live-captured output:

| | Chapter |
|---|---|
| 5.1 | [Overview](5-workshop/5.1-overview/) — architecture + the 14 AWS services |
| 5.2 | [Prerequisites](5-workshop/5.2-prerequisites/) — account, IAM, Bedrock, source data |
| 5.3 | [Deploy](5-workshop/5.3-deploy/) — 11 CloudFormation stacks via CI/CD |
| 5.4 | [Ingest](5-workshop/5.4-ingest/) — Stage A → B → DQ |
| 5.5 | [Analyze](5-workshop/5.5-analyze/) — the three analytics Lambdas + API |
| 5.6 | [Verify](5-workshop/5.6-verify/) — 16-assertion contract test |
| 5.7 | [Live Verification](5-workshop/5.7-verification/) — every deployed resource + business insights |
| 5.8 | [Cleanup](5-workshop/5.8-cleanup/) — tear it all down |

---

> **Region**: `ap-southeast-1` (Singapore) ·  **Framework**: AWS SDLF v2 · **Runtime**: Python 3.12 (Lambda), Glue 4.0 / Spark 3.3 (ETL)
