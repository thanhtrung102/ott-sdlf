---
title: "2. Architecture"
date: 2026-05-20
weight: 2
chapter: false
pre: <b>2. </b>
---

# 2. Architecture

> **Reference content — placeholder.** Component map and dependencies live here. For the hands-on path, see [Workshop chapter 1 — Overview](../workshop/1-overview/) which includes the same diagram.

## At a glance

```
S3 raw drop ─► EventBridge ─► Stage A SM ─► Stage B SM ─► Glue ETL ─► Curated S3
                                                                          │
                                                                          ▼
                                                                     DQ State Machine
                                                                          │
                ┌──────────────────────────┬──────────────────────────────┘
                ▼                          ▼                              ▼
        Trending Lambda          Content Gap Lambda                 LUT-Refresh Lambda
                │                          │                              │
                ▼                          ▼                              ▼
        gold.keyword_trends    CSVs + HTML dashboard           classifier zip (S3)
                │                          │
                └──────────────┬───────────┘
                               ▼
                       HTTP API (x-api-key)
```

14 AWS services, 11 CloudFormation stacks, ap-southeast-1.
