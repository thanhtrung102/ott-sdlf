---
title: "4. Ingest"
date: 2026-05-20
weight: 4
chapter: false
pre: <b>4. </b>
---

# 4. Ingest

You have raw Parquet in the raw bucket and a deployed pipeline waiting. This chapter drops a single file to trigger Stage A → B → DQ, then verifies each stage with concrete output.

---

## 4.1 What's about to happen

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

## 4.2 Trigger ingestion

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

## 4.3 Stage A — event routing (~30 s)

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
aws sqs get-queue-attributes --queue-url "$(aws sqs get-queue-url --queue-name sdlf-ott-mainA-dlq --query QueueUrl --output text --region ap-southeast-1)" `
  --attribute-names ApproximateNumberOfMessagesVisible --region ap-southeast-1
```

`ApproximateNumberOfMessagesVisible: 0` is healthy.

---

## 4.4 Stage B — Glue ETL (~25-33 min)

> The Glue job processes all dt partitions Spark sees under SOURCE_LOCATION (not just the new one), because the raw layer uses the bare-YYYYMMDD directory layout and Spark's directory-recursive read picks all of them up. With 16 partitions on disk, observed run-time was 2020 s (~33 min). With the reference 14 days only, expect ~25 min.

Stage B picks up the EventBridge event, starts the Glue job with the right SOURCE_LOCATION/OUTPUT_LOCATION arguments, then waits for completion.

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

When the Glue job finishes, look for the LINEAGE log line:

```powershell
aws logs filter-log-events --region ap-southeast-1 `
  --log-group-name /aws-glue/jobs/output --filter-pattern "LINEAGE" `
  --start-time ([DateTimeOffset]::Now.AddMinutes(-30).ToUnixTimeMilliseconds()) `
  --query "events[-1].message" --output text
```

**Expected** (numbers will vary slightly run-to-run):

```
LINEAGE run_id=jr_<id> raw=1298470 post_year=1298261 post_dedup=1145826 output=1145826 retention=0.882
```

The `retention=0.882` is what we want: above the 0.7 alarm threshold. If it's below, an `LINEAGE_ALARM` log entry will fire and an SNS notification goes out.

---

## 4.5 Verify the curated layer landed

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

## 4.6 DQ state machine — quality gate (~3 min)

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

A `Failed` count > 0 is **not** automatically a pipeline failure. Glue Data Quality's auto-recommended ruleset includes statistical assertions (e.g. cardinality bounds, completeness ratios) that can drift over time as data shape evolves. The DQ Step Function is configured to PASS the overall SM execution when the *score threshold* is met even if individual rules fail.

To see which rules failed:

```sql
SELECT rule, COUNT(*) AS occurrences
FROM fpt_ott_searchevents_analytics.dq_results
WHERE outcome = 'Failed'
GROUP BY rule
ORDER BY occurrences DESC;
```

> The `dq_results` table's metrics column is named `evaluated_metrics` (with underscore), not `evaluatedmetrics`. Trips up first-time queries.

**Expected** (live output, observed today — the actual failing rules on this pipeline):

```
occurrences  rule
-----------  ------------------------------------------------------------------------------
          4  ColumnValues "subscription_count" in ["0","1","2","3","4","5","6","7","8","9","10","11","12","13","14","15"]
          4  ColumnValues "subscription_count" <= 15
          4  ColumnValues "derived_genre" in ["PHIM_VIET","UNKNOWN","PHIM_TRUNG","ANIME","PHIM_AU_MY","PHIM_HAN","NHAC"] with threshold...
          4  Uniqueness "event_id" > 0.85
          4  ColumnValues "subscription_count" in ["0","1"] with threshold >= 0.93
          1  ColumnValues "subscription_count" <= 12
          1  Uniqueness "event_id" > 0.95
          1  ColumnValues "dt" in [...]
```

The failures cluster into three real issues — every workshop participant will see them and they're worth understanding:

| Cluster | Why it fails | What to do |
|---|---|---|
| **`subscription_count` bound rules** | Glue DQ auto-recommended `<= 15` and `<= 12` from initial small-sample data; real data has users with up to 16+ plans. | Either widen the rule (manual edit of ruleset) or accept these as data-shape drift. |
| **`derived_genre` enum** | Auto-recommended a 7-genre subset (no `EMPTY_QUERY`, `TRUYEN_HINH`, `THE_THAO`); real classifier now emits 10. | Update the ruleset enum to all 10 genres. |
| **`event_id` Uniqueness > 0.95** | Real signal — uniqueness is **0.9303**. The Glue script's `dropDuplicates(["eventid"])` is *per-run*, not per-table; re-ingesting the same source file into a new `dt` partition produces cross-partition duplicates (1,231,739 cumulative rows ÷ 1,145,826 distinct = 85,913 dupes). | Either accept (workshop re-trigger pattern intentionally creates this) or add a downstream `INSERT OVERWRITE` dedup step. The `>0.85` rule passes; the `>0.95` rule doesn't. |

A `Failed` outcome ≠ pipeline failure. The DQ Step Function passes the overall execution when the score threshold is met, even if individual rules fail.

---

## 4.7 Sanity-check by counting curated rows

```sql
SELECT derived_genre, COUNT(*) AS rows
FROM fpt_ott_searchevents_analytics.curated
GROUP BY derived_genre
ORDER BY rows DESC;
```

**Expected** (live output, observed today — June 2022 dataset post-dedup, cumulative across all dt partitions):

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

**1,231,739 curated rows across 10 genres**. (Note: this is the cumulative table count after multiple runs; a single Glue run on 14 days outputs ~1,145,826 rows per the LINEAGE log.)

The single biggest takeaway: **PHIM_VIET (Vietnamese films) is 30 % of all searches** — the platform's audience is strongly local-content-first.

> Reference: [Data Ingestion deep-dive](../../3-ingestion/) and [Data Quality deep-dive](../../4-quality/).

---

**Next**: [chapter 5 — Analyze](../5-analyze/).
