---
title: "Ingest"
date: 2026-05-20
weight: 4
chapter: false
pre: " <b> 5.4 </b> "
---

You have raw Parquet in the raw bucket and a deployed pipeline waiting. This chapter drops a single file to trigger Stage A → B → DQ, then verifies each stage with concrete output.

---

## 5.4.1 What's about to happen

```
PutObject → EventBridge → Stage A SM ── EventBridge ──► Stage B SM ──► Glue Job
                              │                                            │
                              ▼                                            ▼
                     PEH row in DDB                            Curated S3 (dt × derived_genre)
                                                                           │
                                                                           ▼
                                                                  DQ State Machine
                                                                           │
                                                                EventBridge "DQ SUCCEEDED"
```

Three state machines fire automatically; you don't invoke anything by hand.

---

## 5.4.2 Trigger ingestion

Copy one existing partition (`20220614`) into a new partition key (`20220615`). The S3 ObjectCreated event fires Stage A.

```powershell
$RAW = aws ssm get-parameter --name /sdlf/storage/rRawBucket/prod --query Parameter.Value --output text
$SRC = "s3://$RAW/ott/searchevents/20220614/part-00000-6e07374d-dd20-4533-83f6-7c32c5bc60c4-c000.snappy.parquet"
$DST = "s3://$RAW/ott/searchevents/20220615/part-00000-6e07374d-dd20-4533-83f6-7c32c5bc60c4-c000.snappy.parquet"

# Record trigger time so we can filter for the SM execution started AFTER this.
$trigger = (Get-Date).ToUniversalTime().AddSeconds(-5)

aws s3 cp $SRC $DST --region ap-southeast-1
Write-Host "Ingestion triggered at $trigger"
```

**Expected**:

```
copy: s3://...-raw-prod/ott/searchevents/20220614/part-00000-... to s3://...-raw-prod/ott/searchevents/20220615/part-00000-...
Ingestion triggered at 05/20/2026 09:32:41
```

---

## 5.4.3 Stage A — event routing (~30 s)

Stage A's Step Function records the file as a pipeline-execution-history (PEH) row in the Octagon DDB table, then emits an EventBridge SUCCEEDED event.

```powershell
$smArn = "arn:aws:states:ap-southeast-1:$(aws sts get-caller-identity --query Account --output text):stateMachine:sdlf-ott-mainA-sm"
aws stepfunctions list-executions --state-machine-arn $smArn --region ap-southeast-1 `
  --max-results 1 --query "executions[0].{name:name, status:status, startDate:startDate}" --output table
```

**Expected** (status reaches `SUCCEEDED` within ~30 s):

```
-------------------------------------------------------------------
| name                              | status     | startDate      |
|-----------------------------------|------------|----------------|
| sdlf-ott-mainA-...                | SUCCEEDED  | 2026-05-20...  |
-------------------------------------------------------------------
```

If it hangs in `RUNNING` for >2 min, check the Stage A DLQ:

```powershell
aws sqs get-queue-attributes --queue-url "$(aws sqs get-queue-url --queue-name sdlf-ott-mainA-dlq.fifo --query QueueUrl --output text --region ap-southeast-1)" `
  --attribute-names ApproximateNumberOfMessages --region ap-southeast-1
```

`ApproximateNumberOfMessages: 0` is healthy. (Stage A/B DLQs are FIFO queues — note the `.fifo` suffix.)

---

## 5.4.4 Stage B — Glue ETL (~25-33 min)

Stage B picks up the EventBridge event, starts the Glue job, waits for completion. Spark reads all dt partitions under SOURCE_LOCATION (not just the new one), so wall-clock depends on cumulative day count.

> ℹ️ **NOTE:** This is the longest step in the workshop. Walk away for ~25 minutes; the rest of the chapter can wait. The Glue job emits a `LINEAGE` log line when it finishes — that's the signal to come back.

```powershell
$smArn = "arn:aws:states:ap-southeast-1:$(aws sts get-caller-identity --query Account --output text):stateMachine:sdlf-ott-mainB-sm"
aws stepfunctions list-executions --state-machine-arn $smArn --region ap-southeast-1 `
  --max-results 1 --query "executions[0].{status:status, startDate:startDate}" --output table
```

While Stage B runs, watch the underlying Glue job in CloudWatch:

```powershell
# Job-run ID once Stage B starts it
aws glue get-job-runs --job-name sdlf-ott-searchevents-glue-job --region ap-southeast-1 `
  --max-results 1 --query "JobRuns[0].{Id:Id, State:JobRunState, ExecutionTime:ExecutionTime}" --output table
```

**Expected progression**: `STARTING` → `RUNNING` (~20-25 min) → `SUCCEEDED`.

> 📷 **Screenshot —** Glue console: the `sdlf-ott-searchevents-glue-job` run page showing run state `SUCCEEDED` with the elapsed time.
> *Placeholder: capture and save as `01-glue-job-succeeded.png` in this chapter folder, then replace this block with `![Glue job run succeeded](01-glue-job-succeeded.png)`.*

When the Glue job finishes, look for the LINEAGE log line:

```powershell
aws logs filter-log-events --region ap-southeast-1 `
  --log-group-name /aws-glue/jobs/output --filter-pattern "LINEAGE" `
  --start-time ([DateTimeOffset]::Now.AddMinutes(-30).ToUnixTimeMilliseconds()) `
  --query "events[-1].message" --output text
```

**Expected** (numbers vary):

```
LINEAGE run_id=jr_<id> raw=1298470 post_year=1298261 post_dedup=1145826 output=1145826 retention=0.882
```

`retention ≥ 0.7` is required; below that, `LINEAGE_ALARM` fires SNS.

---

## 5.4.5 Verify the curated layer landed

```powershell
$ANALYTICS = aws ssm get-parameter --name /sdlf/storage/rAnalyticsBucket/prod --query Parameter.Value --output text
aws s3 ls "s3://$ANALYTICS/ott/searchevents/curated/" --region ap-southeast-1
```

**Expected** — Hive partitions by `dt`:

```
                           PRE dt=2022-06-01/
                           PRE dt=2022-06-02/
                           PRE dt=2022-06-03/
                           ...
                           PRE dt=2022-06-14/
                           PRE dt=2022-06-15/
```

Drilling into one partition, you should see Hive sub-partitions by `derived_genre`:

```powershell
aws s3 ls "s3://$ANALYTICS/ott/searchevents/curated/dt=2022-06-15/" --region ap-southeast-1
```

**Expected**:

```
                           PRE derived_genre=ANIME/
                           PRE derived_genre=EMPTY_QUERY/
                           PRE derived_genre=NHAC/
                           PRE derived_genre=PHIM_AU_MY/
                           PRE derived_genre=PHIM_HAN/
                           PRE derived_genre=PHIM_TRUNG/
                           PRE derived_genre=PHIM_VIET/
                           PRE derived_genre=THE_THAO/
                           PRE derived_genre=TRUYEN_HINH/
                           PRE derived_genre=UNKNOWN/
```

Each `(dt, derived_genre)` directory contains exactly **1 Parquet file of ~1 MB**, thanks to the `repartition(dt, derived_genre)` shuffle in the Glue script.

---

## 5.4.6 DQ state machine — quality gate (~3 min)

After Stage B succeeds, the curated DQ state machine fires automatically.

```powershell
$smArn = "arn:aws:states:ap-southeast-1:$(aws sts get-caller-identity --query Account --output text):stateMachine:sdlf-ott-mainDQ-sm"
aws stepfunctions list-executions --state-machine-arn $smArn --region ap-southeast-1 `
  --max-results 1 --query "executions[0].{status:status, startDate:startDate}" --output table
```

**Expected**: `SUCCEEDED` within ~3 min. If `FAILED`, the downstream analytics Lambdas won't fire — investigate the SM execution in the console for which rule failed.

You can query the resulting DQ results directly:

```sql
SELECT outcome, COUNT(*) AS rules
FROM fpt_ott_searchevents_analytics.dq_results
GROUP BY outcome;
```

**Expected** (live output, observed today — across all DQ runs to date):

```
outcome   rules
--------- -----
Passed    205
Failed     25
```

A `Failed` count > 0 is not a pipeline failure — the DQ SM passes when the score threshold is met. To see which rules failed:

```sql
SELECT rule, COUNT(*) AS occurrences
FROM fpt_ott_searchevents_analytics.dq_results
WHERE outcome = 'Failed'
GROUP BY rule
ORDER BY occurrences DESC;
```

> The `dq_results` table's metrics column is named `evaluated_metrics` (with underscore), not `evaluatedmetrics`. Trips up first-time queries.

**Expected** (live output — top failing rules; numbers vary):

```
occurrences  rule
-----------  ------------------------------------------------------------------------------
          4  ColumnValues "subscription_count" <= 15
          4  ColumnValues "derived_genre" in [...7-genre subset]
          4  Uniqueness "event_id" > 0.85
          1  Uniqueness "event_id" > 0.95
```

> These failures are auto-recommended-ruleset drift — `subscription_count` bounds and the `derived_genre` enum were tuned on a small initial sample — plus a real cross-partition `event_id` duplication from re-ingesting the same source file into a new `dt`. None is a pipeline break.

---

## 5.4.7 Sanity-check by counting curated rows

```sql
SELECT derived_genre, COUNT(*) AS rows
FROM fpt_ott_searchevents_analytics.curated
GROUP BY derived_genre
ORDER BY rows DESC;
```

**Expected** (live — cumulative across all dt partitions, exact numbers will vary):

```
derived_genre        rows
---------------- --------
PHIM_VIET         371,721
UNKNOWN           161,209
PHIM_TRUNG        153,401
ANIME             143,268
PHIM_AU_MY         96,764
PHIM_HAN           93,674
EMPTY_QUERY        85,913
NHAC               45,893
TRUYEN_HINH        45,277
THE_THAO           34,619
TOTAL           1,231,739
```

10 genres, ~1.2M rows. A single Glue run outputs ~1.15M per the LINEAGE log; the rest is cumulative across re-triggers.

---

**Next**: [5.5 — Analyze](../5.5-analyze/).
