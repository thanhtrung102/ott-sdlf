---
title: "4. Data Quality"
date: 2026-05-15
weight: 4
chapter: false
pre: <b>4. </b>
---

# Data Quality

The data quality layer runs after Stage B completes. It uses **AWS Glue Data Quality** to profile and evaluate the curated table, then triggers a Glue Crawler to refresh partition metadata. A second DQ instance runs against the gold `keyword_trends` table after the Trending Lambda completes.

---

## Template

**Source**: `pipeline-ott-dq-stage.yaml` — this template is reusable and deployed twice:

| Instance name | Table evaluated | Trigger |
|---|---|---|
| `sdlf-ott-main-sm-mainDQ` | `curated` | Stage B SM `SUCCEEDED` |
| `sdlf-ott-main-sm-mainGoldDQ` | `keyword_trends` | EventBridge `sdlf.ott.trending` / `Trending Report Completed` |

---

## Step Functions state machine

The DQ state machine runs these states in sequence:

```
FormatRunDate
     │
     ▼
StartRecommendationRun ──► WaitForRecommendation ──► GetRecommendationStatus
                                                            │
                                                    RecommendationComplete
                                                            │
                                                            ▼
StartEvaluationRun ──────► WaitForEvaluation ─────► GetEvaluationStatus
                                                            │
                                             ┌──────────────┴──────────────┐
                                             ▼                             ▼
                                       EvaluationComplete              DQFailed
                                             │                             │
                                       StartCrawler                  SNS Notification
                                             │
                                       WaitForCrawler
```

**States explained**

| State | What it does |
|---|---|
| `FormatRunDate` | Formats today's date as `yyyy-MM-dd` for partition key use |
| `StartRecommendationRun` | Calls `glue:StartDataQualityRuleRecommendationRun` — auto-generates a ruleset on first run |
| `WaitForRecommendation` | Polls every 30s for `SUCCEEDED` or `FAILED` |
| `StartEvaluationRun` | Calls `glue:StartDataQualityRuleEvaluationRun` against the curated/gold table |
| `WaitForEvaluation` | Polls every 30s |
| `EvaluationComplete` | Writes DQ result JSON to S3, then starts the Glue Crawler |
| `StartCrawler` | Triggers `rDatalakeCrawlerRole` to refresh Glue Catalog partitions |
| `DQFailed` | Publishes failure message to SNS notifications topic |

---

## DQ results table

Evaluation output is written to the Glue Catalog table `dq_results` in the searchevents database.

| Column | Type | Description |
|---|---|---|
| `catalog_id` | STRING | AWS account ID |
| `database_name` | STRING | Glue database name |
| `table_name` | STRING | `curated` or `keyword_trends` |
| `dq_run_id` | STRING | Glue DQ run identifier |
| `evaluation_started_on` | STRING | ISO timestamp |
| `evaluation_completed_on` | STRING | ISO timestamp |
| `rule` | STRING | Rule expression evaluated |
| `outcome` | STRING | `Pass` or `Fail` |
| `failure_reason` | STRING | Populated on `Fail` |
| `evaluated_metrics` | STRING | Metric values as JSON string |

Partition key: `run_date` (DATE), projection enabled — range from 2020-01-01 to NOW+1YEAR.

---

## IAM roles

| Role | Purpose |
|---|---|
| `rDQExecutionRole` | Step Functions execution role — starts Glue DQ runs, reads results, starts crawler |
| `rGlueDQRole` | Glue DQ worker role — reads curated/gold table, writes DQ results to S3 |
| `rEventBridgeRole` | Triggers the DQ SM on upstream events |

Both `rDQExecutionRole` and `rGlueDQRole` ARNs are exported to SSM:
- `/sdlf/pipeline/rRole/ott-mainDQExec`
- `/sdlf/pipeline/rRole/ott-mainDQGlue`

These SSM values are consumed by `pipeline-ott-lakeformation.yaml` to grant column-level Lake Formation access.

---

## Monitoring

A CloudWatch alarm fires when either DQ SM has a failed execution:

- Alarm: `sdlf-ott-mainDQ-sm-failed` — triggers when `ExecutionsFailed >= 1`
- Alarm: `sdlf-ott-mainGoldDQ-sm-failed` — same threshold, gold table

Both alarms publish to the OTT SNS notifications topic. Log group: `/sdlf/statemachine/ott-mainDQ` and `/sdlf/statemachine/ott-mainGoldDQ` (30-day retention).
