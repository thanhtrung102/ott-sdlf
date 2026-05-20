---
title: "4. Data Quality"
date: 2026-05-20
weight: 4
chapter: false
pre: <b>4. </b>
---

# 4. Data Quality

> **Reference content — placeholder.** Glue Data Quality ruleset, the DQ Step Function, and crawler details live here. For the hands-on path, see [Workshop chapter 4 — Ingest](../workshop/4-ingest/) §4.6 DQ state machine.

## Auto-recommended ruleset drift

Glue DQ's auto-recommended ruleset is generated from initial small-sample data; over time it drifts as real data shape evolves. Common drift signals on this pipeline:

| Cluster | Why it fails | What to do |
|---|---|---|
| `subscription_count` bound rules | Recommended `<= 15` and `<= 12` from initial sample; real users have up to 16+ plans. | Widen the rule, or accept as data-shape drift. |
| `derived_genre` enum | Auto-recommended 7 genres; real classifier emits 10. | Update the ruleset enum. |
| `event_id` Uniqueness `> 0.95` | Real signal — re-ingesting the same source into a new `dt` partition creates cross-partition duplicates. | Accept (workshop re-trigger pattern) or add `INSERT OVERWRITE` dedup. |

A `Failed` outcome ≠ pipeline failure — the DQ SM passes when the overall score threshold is met.
