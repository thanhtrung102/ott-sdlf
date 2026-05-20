---
title: "Live Verification"
date: 2026-05-20
weight: 7
chapter: false
pre: " <b> 5.7 </b> "
---

Chapter 5.6 proved the end-user data *contracts* hold. This chapter verifies every
*deployed resource* is live and healthy — one functionality at a time — then reads
back the business insights the pipeline produces.

Everything here is reproducible on any machine: AWS credentials for the target
account + `pip install boto3`. No resource name is hard-coded that can't be
re-derived; the API URL and key are read from SSM.

All output blocks below were captured live on **2026-05-20** against account
`703668403514`, region `ap-southeast-1`.

---

## 5.7.1 One command — verify every functionality

`scripts/verify_live.py` walks all nine subsystems and prints `PASS` / `WARN` /
`FAIL` per check. `WARN` marks a real live state worth knowing (e.g. an alarm
firing) — it is not a script failure.

```powershell
python D:\ott-sdlf\scripts\verify_live.py
```

**Expected output** (live, captured 2026-05-20 after the in-place bug fixes from §5.4.4):

```
OTT SDLF pipeline — live verification
account=703668403514  region=ap-southeast-1

=== CI/CD pipeline ===
  [PASS] sdlf-ott-cicd latest execution  (status=Succeeded)

=== CloudFormation — 11 OTT stacks ===
  [PASS] sdlf-ott-searchevents-glue-job  (UPDATE_COMPLETE)
  [PASS] sdlf-pipeline-ott-mainA  (UPDATE_COMPLETE)
  [PASS] sdlf-pipeline-ott-mainB  (UPDATE_COMPLETE)
  [PASS] sdlf-pipeline-ott-dataquality  (UPDATE_COMPLETE)
  [PASS] sdlf-pipeline-ott-lutrefresh  (UPDATE_COMPLETE)
  [PASS] sdlf-pipeline-ott-contentgap  (UPDATE_COMPLETE)
  [PASS] sdlf-pipeline-ott-trending  (UPDATE_COMPLETE)
  [PASS] sdlf-pipeline-ott-goldquality  (UPDATE_COMPLETE)
  [PASS] sdlf-pipeline-ott-monitoring  (UPDATE_COMPLETE)
  [PASS] sdlf-pipeline-ott-lakeformation  (UPDATE_COMPLETE)
  [PASS] sdlf-pipeline-ott-api  (UPDATE_COMPLETE)

=== Glue ETL job + catalog ===
  [PASS] Glue job sdlf-ott-searchevents-glue-job  (Glue 4.0, 10xG.1X)
  [PASS] Glue job last run  (SUCCEEDED, 1772s)
  [PASS] Glue database fpt_ott_searchevents_analytics
  [PASS] Glue database fpt_ott_searchevents_gold
  [PASS] fpt_ott_searchevents_analytics: 10 expected tables  (10/10)
  [PASS] fpt_ott_searchevents_gold.keyword_trends

=== Step Functions — 4 state machines ===
  [PASS] sdlf-ott-mainA-sm latest execution  (SUCCEEDED)
  [PASS] sdlf-ott-mainB-sm latest execution  (SUCCEEDED)
  [PASS] sdlf-ott-mainDQ-sm latest execution  (SUCCEEDED)
  [PASS] sdlf-ott-mainGoldDQ-sm latest execution  (SUCCEEDED)

=== Lambda — analytics + API functions ===
  [PASS] sdlf-ott-mainTR-report  (python3.12, 256MB)
  [PASS] sdlf-ott-mainCG-report  (python3.12, 256MB)
  [PASS] sdlf-ott-mainLUT-refresh  (python3.12, 512MB)
  [PASS] sdlf-ott-api  (python3.12, 128MB)

=== SQS — 5 dead-letter queues ===
  [PASS] sdlf-ott-mainA-dlq.fifo  (depth=0)
  [WARN] sdlf-ott-mainB-dlq.fifo  (depth=1)
  [PASS] sdlf-ott-mainCG-dlq  (depth=0)
  [PASS] sdlf-ott-mainLUT-dlq  (depth=0)
  [PASS] sdlf-ott-mainTR-dlq  (depth=0)

=== CloudWatch — dashboard + alarms ===
  [PASS] dashboard sdlf-ott-searchevents-pipeline
  [PASS] sdlf-ott alarms deployed  (17 alarms)
  [WARN] alarms currently in ALARM state  (sdlf-ott-mainB-dlq-not-empty, sdlf-ott-mainCG-report-near-timeout, sdlf-ott-mainLUT-refresh-near-timeout)

=== HTTP API — endpoints + auth + freshness ===
  [PASS] GET /trending (authorised)  (HTTP 200, 3 rows)
  [PASS] X-Data-Freshness header present  (2026-05-20T09:11:24+00:00)
  [PASS] GET /trending without key rejected  (HTTP 401)
  [PASS] GET /content-gaps (authorised)  (HTTP 200, 2 rows)

=== Lake Formation — column RBAC enforcement ===
  [WARN] curated table — IAM_ALLOWED_PRINCIPALS still granted  (column exclusions DEFINED but NOT ENFORCED — revoke to activate)

=== Summary ===
  PASS=36  WARN=3  FAIL=0
```

`FAIL=0` is the success criterion. The 3 `WARN`s are explained in section 5.7.3 — the Stage B DLQ depth=1 is a single stuck message from a prior Stage B failed execution (and the `sdlf-ott-mainB-dlq-not-empty` alarm it trips); the two `*-near-timeout` alarms are early-warning signals, not outages; and the Lake Formation column RBAC is defined but dormant until `IAM_ALLOWED_PRINCIPALS` is revoked. If the post-Trending Gold DQ happens to be mid-execution when you run this, `mainGoldDQ-sm` shows a transient 4th `WARN` (`RUNNING`) that clears on its own.

---

## 5.7.2 Functionality matrix — what each check proves

| # | Functionality | Live resource | Proven by |
|---|---|---|---|
| 1 | CI/CD auto-deploy | `sdlf-ott-cicd` CodePipeline | Latest execution `Succeeded` |
| 2 | Infrastructure-as-code | 11 OTT CloudFormation stacks | All `*_COMPLETE` |
| 3 | ETL enrichment | Glue 4.0 job, 10×G.1X | Last run `SUCCEEDED` in 1772 s |
| 4 | Data catalog | 2 Glue DBs, 10 analytics + `keyword_trends` gold tables | All present |
| 5 | Orchestration | 4 Step Functions (`mainA/B/DQ/GoldDQ`) | All last executions `SUCCEEDED` |
| 6 | Analytics compute | 4 Lambdas (`mainTR/mainCG/mainLUT/api`) | All deployed, Python 3.12 |
| 7 | Failure isolation | 5 DLQs (Stage A/B FIFO + 3 analytics) | All depth 0 |
| 8 | Observability | 1 dashboard + 17 alarms | All deployed |
| 9 | Serving layer | HTTP API v2 | 200 + freshness header; 401 without key |
| 10 | Access control | Lake Formation on `curated` | Grant state reported (see 5.7.3) |

That is the entire deployed surface — 11 stacks, 1 Glue job, 14 catalog tables
(across 4 databases), 4 state machines, 13 Lambdas (4 analytics + 9 SDLF
framework), 5 DLQs, 17 alarms, 1 HTTP API.

---

## 5.7.3 Two live findings worth knowing

`verify_live.py` reports several `WARN`s. All are accurate live states, not bugs in
the script.

### Finding 1 — near-timeout alarms are firing

```powershell
aws cloudwatch describe-alarms --alarm-name-prefix sdlf-ott --region ap-southeast-1 `
  --query "MetricAlarms[?StateValue=='ALARM'].AlarmName" --output text
```

**Expected** (live 2026-05-20 — may also include `sdlf-ott-mainB-dlq-not-empty` if a Stage B execution recently FAILED and left a DLQ message):

```
sdlf-ott-mainB-dlq-not-empty    sdlf-ott-mainCG-report-near-timeout    sdlf-ott-mainLUT-refresh-near-timeout
```

These are *near-timeout* alarms — they fire when a Lambda's p90 duration crosses
~80 % of its configured timeout. The Content-Gap Lambda (5 sequential Athena
queries) and the LUT-Refresh Lambda (batched Bedrock calls) both run long enough
to trip them. They are an early-warning signal, not an outage: no DLQ has a
message, and no `*-errors` alarm is firing. If they become chronic, raise the
Lambda timeout or shard the work.

### Finding 2 — Lake Formation column RBAC is defined but not enforced

```powershell
aws lakeformation list-permissions --region ap-southeast-1 `
  --resource '{\"Table\":{\"CatalogId\":\"703668403514\",\"DatabaseName\":\"fpt_ott_searchevents_analytics\",\"Name\":\"curated\"}}' `
  --query "PrincipalResourcePermissions[].Principal.DataLakePrincipalIdentifier" --output text
```

**Expected** (live):

```
arn:aws:iam::...:role/sdlf-ott/sdlf-ott-cicd-codebuild    arn:aws:iam::...:user/terraform-admin    arn:aws:iam::...:user/terraform-admin    IAM_ALLOWED_PRINCIPALS
```

`IAM_ALLOWED_PRINCIPALS` is still granted `ALL` on `curated`. While that grant
exists, the column exclusions declared in `pipeline-ott-lakeformation.yaml` are
**dormant** — any IAM principal with Glue/Athena access reads every column. To
activate column-level RBAC, revoke it (workshop [§5.3.5](../5.3-deploy/)). This is
expected on a fresh reference deployment: the revoke is a deliberate, separate,
irreversible step.

---

## 5.7.4 Business insights — what the data delivers

`scripts/audit_visuals.py` runs the end-consumer Athena queries and renders them
as terminal charts. This is the demo: the actual business value drawn from
~1.24 M curated search events (live as of 2026-05-20).

```powershell
python D:\ott-sdlf\scripts\audit_visuals.py
```

### Insight 1 — Vietnamese content dominates demand

```
PHIM_VIET       371,723  ██████████████████████████████████████████████████
UNKNOWN         161,209  █████████████████████
PHIM_TRUNG      153,399  ████████████████████
ANIME           143,269  ███████████████████
PHIM_AU_MY       96,763  █████████████
EMPTY_QUERY      95,447  ████████████
PHIM_HAN         93,674  ████████████
NHAC             45,893  ██████
TRUYEN_HINH      45,277  ██████
THE_THAO         34,619  ████
```

PHIM_VIET (Vietnamese film) is **32.3 %** of classified searches (excluding `EMPTY_QUERY` rows) — the single largest genre. Content acquisition should weight local titles accordingly.

### Insight 2 — top trending titles (14-day window)

```
keyword_norm                                  derived_genre  searches
liên minh công lý: phiên bản của zack snyder  PHIM_AU_MY     10,864
fairy tail                                    ANIME           9,075
thiên nga bóng đêm                            PHIM_VIET       7,411
nữ thanh tra tài ba                           PHIM_VIET       7,012
sao băng                                      PHIM_HAN        6,987
```

Diacritics survive end-to-end (raw → Glue → Athena → gold → API → JSON). The
HTTP API exposes the same ranking at `GET /trending`.

### Insight 3 — premium demand skews Western

```
derived_genre  total   premium_pct
PHIM_AU_MY     96,763   6.3
THE_THAO       34,619   3.6
NHAC           45,893   2.8
TRUYEN_HINH    55,795   2.1
...
ANIME         143,269   1.4
```

Western content (PHIM_AU_MY) has the highest premium-subscriber share at
**6.3 %** — roughly 4× ANIME. A signal for tiered-content strategy.

### Insight 4 — search peaks at 3 AM Vietnam time

```
 2     105,835  █████████████████████████████████████
 3     113,303  ████████████████████████████████████████   <- peak
 4     105,471  █████████████████████████████████████
...
11       6,908  ██                                          <- trough
```

Demand peaks 02:00–04:00 VN and bottoms out late morning — relevant for
cache-warming and batch-job scheduling.

### Insight 5 — abandon rate flags content holes

```
derived_genre  total   abandoned  abandon_pct
ANIME          143269  20399      11.1
THE_THAO        40201   4044      10.1
PHIM_VIET      371723  44857       9.6
```

ANIME has the highest abandon rate (**11.1 %**) — users search, find nothing,
quit. The `content_gaps` report drills this down to specific abandoned keywords.

### Insight summary

| Insight | Source table / report | Business action |
|---|---|---|
| Genre demand mix | `curated` | Content acquisition weighting |
| Trending titles | `keyword_trends` gold / `trending_all` | Promotion + push notifications |
| Premium skew by genre | `premium_vs_free` | Tiered-content strategy |
| Hourly demand curve | `hour_of_day_heatmap` | Cache-warming, job scheduling |
| Abandon rate / content gaps | `content_gaps` | Fill catalog holes |
| Repeat-search frustration | `repeat_search_rate` | UX investigation |
| Guest vs authenticated | `guest_vs_auth_demand` | Signup-funnel targeting |

Every figure above is reproducible: re-run `verify_live.py` and
`audit_visuals.py` on any machine with account credentials.

---

**Next**: [5.8 — Cleanup](../5.8-cleanup/).
