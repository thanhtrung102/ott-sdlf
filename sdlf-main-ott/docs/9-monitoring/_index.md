---
title: "9. Monitoring"
date: 2026-05-20
weight: 9
chapter: false
pre: <b>9. </b>
---

# 9. Monitoring

> **Reference content — placeholder.** Dashboard widgets, alarm thresholds, and DLQ wiring live here. For the hands-on path, see [Workshop chapter 6 — Verify](../workshop/6-verify/) §6.4 CloudWatch dashboard + alarms.

## What's deployed

| Resource | Count | Owner stack |
|---|---|---|
| CloudWatch dashboard | 1 (`sdlf-ott-searchevents-pipeline`) | `pipeline-ott-monitoring.yaml` |
| CloudWatch alarms | 14 | `pipeline-ott-monitoring.yaml` |
| DLQs | 5 (Stage A, Stage B, Trending, Content Gap, LUT-Refresh) | per-Lambda + per-SM stacks |
| X-Ray tracing | Active on all 4 analytics Lambdas + API Lambda | per-Lambda stacks |

All 14 alarms publish to the `/SDLF/SNS/ott/Notifications` topic on `ALARM` state.
