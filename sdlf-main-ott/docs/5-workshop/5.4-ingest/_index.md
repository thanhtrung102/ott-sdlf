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

> ℹ️ **NOTE — this trigger copies an *existing* file.** `20220615`'s rows carry the same `eventid` values as `20220614`. The Glue job deduplicates by `eventid` across **all** `dt` partitions, so the new partition adds ~0 unique rows — `post_dedup` stays flat — and overall `retention` drops on every repeat run. On a deployment that has already been triggered a few times, `retention` can fall below the `0.7` threshold and fire `LINEAGE_ALARM` (see §5.4.4). This is expected behaviour from re-ingesting duplicates, **not a pipeline failure** — Stage B still `SUCCEEDED`. To exercise the pipeline with genuinely new rows, stage a distinct day's Parquet instead of copying one.

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

## 5.4.4 Stage B — Glue ETL (~25-35 min)

Stage B picks up the EventBridge event, starts the Glue job, waits for completion. Spark reads all dt partitions under `SOURCE_LOCATION` (not just the new one), so wall-clock depends on cumulative day count. Across recent history this lands at 1159–2148 s (≈ 19–36 min).

> ℹ️ **NOTE:** This is the longest step in the workshop. Walk away for ~30 minutes; the rest of the chapter can wait. The Glue job emits a `LINEAGE` log line when it finishes — that's the signal to come back.
>
> **Failure semantics (live as of 2026-05-20):** the Stage B state machine's Map state uses `ToleratedFailurePercentage: 0` — when the Glue job fails, Stage B fails. Earlier deployments had this at 100 (Map "succeeded" on Glue failures). This value is a hardcoded literal in the SDLF stage-glue module's SM definition (`sdlf-stage-glue/src/state-machine/stage-glue.asl.json`) — it is **not** exposed as a CFN parameter, so `pipeline-ott-mainB.yaml` cannot override it. If you see a `SUCCEEDED` Stage B with a `FAILED` Glue run, your deployment is on the broken version. Remediation: patch that file to `0`, **redeploy the SDLF framework** so it republishes the `stageglue` module, then redeploy `sdlf-pipeline-ott-mainB`. Redeploying `mainB` alone is not sufficient — it resolves the module from `{{resolve:ssm:/sdlf/stageglue/main}}`, which keeps serving the unpatched definition until the framework is redeployed.
>
> **Write semantics:** the Glue script sets `spark.sql.sources.partitionOverwriteMode = DYNAMIC` and defaults `PUSH_DOWN_PREDICATE` to `None` (full backfill when omitted). With the bookmark enabled, an incremental run only rewrites partitions touched by the new file. Earlier script revisions used static overwrite + a `today-2` predicate fallback, which silently wiped curated to 0 rows on Stage-B-triggered runs.

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

`retention` is `output / raw` — the fraction of raw rows that survived year-filtering and `eventid` dedup. `retention ≥ 0.7` is the healthy band; below it, the Glue script logs a `LINEAGE_ALARM` line.

> ℹ️ `retention` falls every time you re-run the §5.4.2 trigger, because copying an existing file adds raw rows that all dedup away (`raw` grows, `post_dedup` stays flat). A deployment triggered several times can show `retention ≈ 0.68` and a `LINEAGE_ALARM` line — that is the duplicate-ingest side-effect described in §5.4.2, not a Stage B failure. A first ingest of 14 genuinely distinct days lands around `0.88`; a single duplicate re-trigger on top of that pulls it toward `0.7`.

---

## 5.4.5 Verify the curated layer landed

```powershell
$ANALYTICS = aws ssm get-parameter --name /sdlf/storage/rAnalyticsBucket/prod --query Parameter.Value --output text
aws s3 ls "s3://$ANALYTICS/ott/searchevents/curated/" --region ap-southeast-1
```

**Expected** — Hive partitions by `dt` (your set will include any re-ingestion partitions you've added; the reference deploy currently has 19):

```
                           PRE dt=2022-06-01/
                           PRE dt=2022-06-02/
                           PRE dt=2022-06-03/
                           ...
                           PRE dt=2022-06-14/
                           PRE dt=2022-06-17/
                           PRE dt=2022-06-18/
                           PRE dt=2022-06-20/
                           PRE dt=2022-06-21/
                           PRE dt=2022-06-22/
```

Drilling into one partition, you should see Hive sub-partitions by `derived_genre`:

```powershell
aws s3 ls "s3://$ANALYTICS/ott/searchevents/curated/dt=2022-06-22/" --region ap-southeast-1
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

> `derived_genre=EMPTY_QUERY/` may be absent from a given `dt=` partition if that day's input had no events with an empty keyword string. Live verification of `dt=2022-06-22` shows 9 of the 10 genres present, no `EMPTY_QUERY/`; older partitions like `dt=2022-06-01` have all 10.

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

**Expected** (live, 2026-05-20 — cumulative across all DQ runs to date):

```
outcome   rules
--------- -----
Passed    281
Failed     41
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

**Expected** (live, 2026-05-20 — top failing rules; numbers vary):

```
occurrences  rule
-----------  ------------------------------------------------------------------------------
          6  ColumnValues "derived_genre" in [...subset]
          6  ColumnValues "subscription_count" <= 15
          6  ColumnValues "subscription_count" in ["0","1","2",...,"15"]
          6  ColumnValues "subscription_count" in ["0","1"] with threshold >= 0.93
          4  Uniqueness "event_id" > 0.85
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

**Expected** (live, 2026-05-20 — across 19 dt partitions in the reference deploy; exact numbers will vary with your partition set):

```
derived_genre        rows
---------------- --------
PHIM_VIET         371,723
UNKNOWN           161,209
PHIM_TRUNG        153,399
ANIME             143,269
PHIM_AU_MY         96,763
EMPTY_QUERY        95,447
PHIM_HAN           93,674
NHAC               45,893
TRUYEN_HINH        45,277
THE_THAO           34,619
TOTAL           1,241,273
```

10 genres, ~1.24M rows across 19 dt partitions (14 reference 20220601–20220614 + 20220617, 20220618, 20220620, 20220621, 20220622 added by re-ingestion tests). A single full-history Glue run outputs ~1.15M per the LINEAGE log.

---

**Next**: [5.5 — Analyze](../5.5-analyze/).
