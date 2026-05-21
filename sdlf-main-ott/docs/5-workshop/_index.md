---
title: "Workshop"
date: 2026-05-20
weight: 5
chapter: false
pre: " <b> 5. </b> "
---

> **Format**: First Cloud Journey (FCJ) workshop. Linear, copy-paste-friendly, with concrete output for every step.
>
> **Time budget**: ~100 minutes if everything works first try. Most of that is the Glue ETL run (~25 minutes wall-clock).
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
- and 17 CloudWatch alarms covering every failure mode.

The reference dataset is **14 days of June 2022 FPT Play search events (~1.3 M events/day)**. The pipeline is region-locked to **`ap-southeast-1` (Singapore)**.

---

## Workshop chapters

| # | Chapter | Time | What you'll do |
|---|---|---|---|
| 5.1 | [Overview](5.1-overview/) | 5 min | Architecture map + the 14 AWS services this pipeline uses |
| 5.2 | [Prerequisites](5.2-prerequisites/) | 10 min | AWS account, IAM bootstrap, region, Bedrock model enablement |
| 5.3 | [Deploy](5.3-deploy/) | 15 min | SDLF foundation + 12 OTT CloudFormation stacks (via CI/CD or PowerShell) |
| 5.4 | [Ingest](5.4-ingest/) | 30 min | Drop a raw Parquet, watch Stage A → B → DQ fire automatically |
| 5.5 | [Analyze](5.5-analyze/) | 10 min | Trigger the three analytics Lambdas, read the JSON + dashboard |
| 5.6 | [Verify](5.6-verify/) | 5 min | 16-assertion contract test, audit visuals, live API call |
| 5.7 | [Live Verification](5.7-verification/) | 10 min | Verify every deployed functionality live + read the business insights |
| 5.8 | [Cleanup](5.8-cleanup/) | 5 min | Delete the stacks, empty buckets, revoke LF |

---

## Cost estimate

For the full 14-day reference dataset, end-to-end:

| Service | Driver | One-time cost |
|---|---|---|
| Glue (G.1X × 10 workers, ~25 min) | One ETL run | ~$1.30 |
| Athena (scanned data) | ~5 reports × 1 GB scan | ~$0.025 |
| Lambda (4 functions × ~3 min total invocations) | Per run | <$0.01 |
| S3 (raw + stage + analytics + gold, ~5 GB total) | Storage | ~$0.12/month |
| CloudWatch (logs + dashboard + 17 alarms) | Standing | ~$1.00/month |
| Bedrock (Claude Haiku, LUT-refresh) | Per refresh, ~10 k tokens | ~$0.05 |
| **Total per full run** | | **<$3** |

Numbers above are observed, not theoretical — measured on the deployed reference pipeline.

---

## How to follow

Each chapter is: **command → expected output → verify**. Copy-paste, check the output matches, move on.

> **Set up your session once**: `$env:AWS_REGION = "ap-southeast-1"` and `$env:AWS_PROFILE = "<your-profile>"`. Every command below assumes both are set.

---

## Reference architecture

The pipeline at a glance:

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

Ready? Start with [5.1 — Overview](5.1-overview/).
