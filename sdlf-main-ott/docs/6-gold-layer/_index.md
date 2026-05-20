---
title: "6. Gold Layer"
date: 2026-05-20
weight: 6
chapter: false
pre: <b>6. </b>
---

# 6. Gold Layer

> **Reference content — placeholder.** `keyword_trends` schema, CTAS pattern, and Gold DQ details live here. For the hands-on path, see [Workshop chapter 5 — Analyze](../workshop/5-analyze/) §5.2 Trending Lambda.

## `fpt_ott_searchevents_gold.keyword_trends`

Built by the Trending Lambda via Athena CTAS. One row per `(keyword × platform × derived_genre × trend_date)` tuple.

| Column | Type | Notes |
|---|---|---|
| `keyword_norm` | string | Lowercase, diacritics preserved |
| `derived_genre` | string | One of 10 enum values |
| `platform` | string | Normalised (`PHIM_VIET`, `PHIM_AU_MY`, …) |
| `trend_date` | date | Sunday of the trend week |
| `current_cnt` | int | Searches this week |
| `baseline_cnt` | int | Searches 4 weeks ago |
| `growth_multiplier` | double | `current / baseline`; null for new keywords |
| `is_new_keyword` | boolean | True if `baseline_cnt = 0` |

After the table writes, the Gold DQ state machine fires automatically.
