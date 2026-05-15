---
title: "9. Monitoring"
date: 2026-05-15
weight: 9
chapter: false
pre: <b>9. </b>
---

# Monitoring

**Source**: `pipeline-ott-monitoring.yaml`

All pipeline observability is centralised in a single CloudWatch dashboard (`sdlf-ott-searchevents-pipeline`) with 12 alarms publishing to the OTT SNS notifications topic.

---

## CloudWatch dashboard

The dashboard contains 10 metric widgets covering the full pipeline end-to-end.

| Widget | Metrics shown |
|---|---|
| Stage A/B/DQ SM executions | Succeeded, Failed, TimedOut — per state machine |
| Stage B SM duration | p50, p90, p99 execution duration |
| DLQ depth (Stage A) | `ApproximateNumberOfMessagesVisible` on `sdlf-ott-mainA-dlq` |
| DLQ depth (Stage B) | `ApproximateNumberOfMessagesVisible` on `sdlf-ott-mainB-dlq` |
| Glue job elapsed time | `glue.driver.aggregate.elapsedTime` |
| Glue job bytes read | `glue.driver.aggregate.bytesRead` |
| LUT Refresh Lambda | Invocations, Errors, Duration |
| Content Gap Lambda | Invocations, Errors, Duration |
| Trending Lambda | Invocations, Errors, Duration |
| Gold DQ SM executions | Succeeded, Failed |

---

## Alarms

All alarms publish to the SNS topic resolved from `/SDLF/SNS/ott/Notifications`. Eleven alarms are defined:

| Alarm name | Metric | Threshold | Rationale |
|---|---|---|---|
| Stage B SM failed | `ExecutionsFailed` (Stage B SM) | ≥ 1 in 5 min | Any Glue job failure blocks the downstream pipeline |
| DQ SM failed | `ExecutionsFailed` (DQ SM) | ≥ 1 in 5 min | DQ failure means curated data is suspect; analytics should not run |
| Stage A DLQ not empty | `ApproximateNumberOfMessagesVisible` (Stage A DLQ) | ≥ 1 | Event routing failure — file landed but pipeline did not start |
| Stage B DLQ not empty | `ApproximateNumberOfMessagesVisible` (Stage B DLQ) | ≥ 1 | Stage B routing failure |
| Stage B SM duration > 60 min | `ExecutionTime` p90 (Stage B SM) | > 3,600,000 ms | Glue job runtime regression or data volume spike |
| LUT Refresh errors | `Errors` (LUT Refresh Lambda) | ≥ 1 in 5 min | Bedrock classification failure or S3 write failure |
| LUT Refresh duration > 720 s | `Duration` p90 (LUT Refresh Lambda) | > 720,000 ms | 80 % of the 900 s Lambda timeout — early warning before hard timeout |
| Content Gap errors | `Errors` (Content Gap Lambda) | ≥ 1 in 5 min | Athena query failure or S3 write failure |
| Content Gap duration > 270 s | `Duration` p90 (Content Gap Lambda) | > 270,000 ms | 90 % of the 300 s Lambda timeout |
| Trending errors | `Errors` (Trending Lambda) | ≥ 1 in 5 min | Trending query or gold write failure |
| Trending duration > 270 s | `Duration` p90 (Trending Lambda) | > 270,000 ms | 90 % of the 300 s Lambda timeout |
| Gold DQ SM failed | `ExecutionsFailed` (Gold DQ SM) | ≥ 1 in 5 min | Gold table quality failure |

---

## Dead-letter queues

Five SQS DLQs capture failed event deliveries (not Lambda failures — those go to Lambda DLQs):

| DLQ name | Captures | Retention |
|---|---|---|
| `sdlf-ott-mainA-dlq` | Stage A Step Functions routing failures | 14 days |
| `sdlf-ott-mainB-dlq` | Stage B Step Functions routing failures | 14 days |
| `sdlf-ott-mainCG-dlq` | Content Gap Lambda invocation failures | 14 days |
| `sdlf-ott-mainLUT-dlq` | LUT Refresh Lambda invocation failures | 14 days |
| `sdlf-ott-mainTR-dlq` | Trending Lambda invocation failures | 14 days |

All DLQs use KMS encryption (shared OTT KMS key). The DLQ-not-empty alarms fire within 5 minutes of a message landing, so failures are visible before the next pipeline run.

---

## CloudWatch log groups

| Log group | Source | Retention |
|---|---|---|
| `/aws/lambda/sdlf-ott-mainCG-report` | Content Gap Lambda | 30 days |
| `/aws/lambda/sdlf-ott-mainLUT-refresh` | LUT Refresh Lambda | 30 days |
| `/aws/lambda/sdlf-ott-mainTR-report` | Trending Lambda | 30 days |
| `/aws/lambda/sdlf-ott-api` | HTTP API Lambda | 30 days |
| `/sdlf/statemachine/ott-mainDQ` | DQ state machine | 30 days |
| `/sdlf/statemachine/ott-mainGoldDQ` | Gold DQ state machine | 30 days |
| `/aws-glue/jobs/output` | Glue job stdout | [Glue default] |
| `/aws-glue/jobs/error` | Glue job stderr | [Glue default] |

---

## SNS notifications

All three analytics Lambdas publish an SNS message on every run (success or failure):

**Content Gap notification example**:
```
OTT Content Gap Report — 2026-05-14
Content gaps (abandoned keywords): 487
Premium vs free: 9 genres
Repeat search rate: 9 genres
Hour heatmap: 216 rows
Guest vs auth: 9 genres
Errors: 0

Dashboard: https://[stage-bucket].s3.ap-southeast-1.amazonaws.com/...
```

**Trending notification example**:
```
OTT Trending Keywords Report — 2026-05-14
Mode: growth >=3.0x
Trending keywords (all genres): 312
Trending UNKNOWN (LUT targets): 47
Gold table rows written: 28450
All:     s3://[stage-bucket]/analytics/trending/all/2026-05-14/
Unknown: s3://[stage-bucket]/analytics/trending/unknown/2026-05-14/
Errors: 0
```

The `Dashboard:` URL in the Content Gap notification is a pre-signed S3 URL with a 7-day TTL pointing to `report.html`.
