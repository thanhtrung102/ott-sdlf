---
title: "5. Analytics Pipelines"
date: 2026-05-20
weight: 5
chapter: false
pre: <b>5. </b>
---

# 5. Analytics Pipelines

> **Reference content — placeholder.** Trending, Content Gap, and LUT-Refresh Lambda internals live here. For the hands-on path, see [Workshop chapter 5 — Analyze](../workshop/5-analyze/).

## What fans out from `DQ SUCCEEDED`

| Lambda | What it does | Output |
|---|---|---|
| `mainTR-report` | Week-over-week growth analysis (Athena CTAS) | gold `keyword_trends` table + CSVs |
| `mainCG-report` | 5 daily reports (content gap, premium-vs-free, repeat-search, hourly heatmap, guest-vs-auth) | 5 catalog tables + HTML dashboard (7-day presigned URL via SNS) |
| `mainLUT-refresh` | Top-N UNKNOWN keywords → Bedrock Claude Haiku 4.5 → rebuild classifier zip | `genre_classifier_pkg.zip` in artifacts bucket |

All three Lambdas finish within ~5 min of wall-clock after `DQ SUCCEEDED` (LUT-Refresh adds ~10-15 min depending on UNKNOWN count).
