---
title: "Workshop"
date: 2026-05-20
weight: 11
chapter: true
description: "Guided walkthrough — build the OTT Search Analytics pipeline end-to-end on AWS"
---

# Workshop — OTT Search Analytics Pipeline on SDLF

> **Format**: First Cloud Journey (FCJ) workshop. Linear, copy-paste-friendly, with concrete output for every step.
>
> **Time budget**: ~90 minutes if everything works first try. Most of that is the Glue ETL run (~25 minutes wall-clock).
>
> **Audience**: An AWS engineer who has used CloudFormation and IAM, but has not worked with SDLF or Vietnamese OTT data before.

---

## What you will build

A production-grade serverless data lake that turns raw OTT search-event Parquet into:

- a curated table partitioned by date and genre,
- a gold table of week-over-week keyword rankings,
- five daily analytics reports (content gap, premium-vs-free, repeat-search, hourly heatmap, guest-vs-auth),
- a static dashboard,
- a key-protected HTTP API,
- and 14 CloudWatch alarms covering every failure mode.

The reference dataset is **14 days of June 2022 FPT Play search events (~1.3 M events/day)**. The pipeline is region-locked to **`ap-southeast-1` (Singapore)**.

---

## Workshop chapters

| # | Chapter | Time | What you'll do |
|---|---|---|---|
| 1 | [Overview](1-overview/) | 5 min | Architecture map + the 14 AWS services this pipeline uses |
| 2 | [Prerequisites](2-prerequisites/) | 10 min | AWS account, IAM bootstrap, region, Bedrock model enablement |
| 3 | [Deploy](3-deploy/) | 15 min | SDLF foundation + 11 OTT CloudFormation stacks (via CI/CD or PowerShell) |
| 4 | [Ingest](4-ingest/) | 30 min | Drop a raw Parquet, watch Stage A → B → DQ fire automatically |
| 5 | [Analyze](5-analyze/) | 10 min | Trigger the three analytics Lambdas, read the JSON + dashboard |
| 6 | [Verify](6-verify/) | 5 min | 16-assertion contract test, audit visuals, live API call |
| 7 | [Cleanup](7-cleanup/) | 5 min | Delete the stacks, empty buckets, revoke LF |

---

## Cost estimate

For the full 14-day reference dataset, end-to-end:

| Service | Driver | One-time cost |
|---|---|---|
| Glue (G.1X × 10 workers, ~25 min) | One ETL run | ~$1.30 |
| Athena (scanned data) | ~5 reports × 1 GB scan | ~$0.025 |
| Lambda (4 functions × ~3 min total invocations) | Per run | <$0.01 |
| S3 (raw + stage + analytics + gold, ~5 GB total) | Storage | ~$0.12/month |
| CloudWatch (logs + dashboard + 14 alarms) | Standing | ~$1.00/month |
| Bedrock (Claude Haiku, LUT-refresh) | Per refresh, ~10 k tokens | ~$0.05 |
| **Total per full run** | | **<$3** |

Numbers above are observed, not theoretical — measured on the deployed reference pipeline.

---

## How to follow the workshop

Every chapter follows this pattern:

1. **What we're doing** — one paragraph.
2. **Step N** — numbered commands you can copy and paste.
3. **Expected output** — a code block showing what you should actually see. If you don't see this, something's wrong.
4. **Verify** — an independent check (CLI command, Athena query, or browser hit).
5. **Reference** — a link to the deeper [docs section](../) if you want to understand *why*.

When you finish chapter 7 (Cleanup), nothing of the workshop should remain in your AWS account.

> **Tip**: Run every step from a single PowerShell session with `$env:AWS_PROFILE` and `$env:AWS_REGION = "ap-southeast-1"` set up-front. Most of the commands below assume those are in place.

---

## Reference architecture

The complete component map is in [Architecture](../2-architecture/). Short version:

```
S3 raw drop ─► EventBridge ─► Stage A SM ─► Stage B SM ─► Glue ETL
                                                              │
                                                              ▼
                                                         Curated S3 + Glue catalog
                                                              │
                                                              ▼
                                                         DQ State Machine
                                                              │
                ┌──────────────────────────┬──────────────────┴──────────────────┐
                ▼                          ▼                                     ▼
        Trending Lambda          Content Gap Lambda                     LUT-Refresh Lambda
                │                          │                                     │
                ▼                          ▼                                     ▼
        gold.keyword_trends    CSVs + HTML dashboard               classifier zip (S3)
                │                          │
                └──────────────┬───────────┘
                               ▼
                       HTTP API (x-api-key)
```

Ready? Start with [chapter 1 — Overview](1-overview/).
