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
re-derived; the dashboard URL is read from SSM.

---

## 5.7.1 One command — verify every functionality

`scripts/verify_live.py` walks every subsystem and prints `PASS` / `WARN` /
`FAIL` per check. `WARN` marks a real live state worth knowing (e.g. an alarm
firing) — it is not a script failure.

```powershell
python D:\ott-sdlf\scripts\verify_live.py
```

**Expected output** (sample):

```
OTT SDLF pipeline — live verification
account=703668403514  region=ap-southeast-1

=== CI/CD pipeline ===
  [PASS] sdlf-ott-cicd latest execution  (status=Succeeded)

=== CloudFormation — 9 OTT stacks ===
  [PASS] sdlf-ott-searchevents-glue-job  (UPDATE_COMPLETE)
  [PASS] sdlf-pipeline-ott-mainA  (UPDATE_COMPLETE)
  [PASS] sdlf-pipeline-ott-mainB  (UPDATE_COMPLETE)
  [PASS] sdlf-pipeline-ott-dataquality  (UPDATE_COMPLETE)
  [PASS] sdlf-pipeline-ott-lutrefresh  (UPDATE_COMPLETE)
  [PASS] sdlf-pipeline-ott-trending  (UPDATE_COMPLETE)
  [PASS] sdlf-pipeline-ott-monitoring  (UPDATE_COMPLETE)
  [PASS] sdlf-pipeline-ott-lakeformation  (UPDATE_COMPLETE)
  [PASS] sdlf-pipeline-ott-dashboard  (UPDATE_COMPLETE)

=== Glue ETL job + catalog ===
  [PASS] Glue job sdlf-ott-searchevents-glue-job  (Glue 4.0, 10xG.1X)
  [PASS] Glue job last run  (SUCCEEDED, 1691s)
  [PASS] Glue database fpt_ott_searchevents_analytics
  [PASS] fpt_ott_searchevents_analytics: 3 expected tables  (3/3)

=== Step Functions — 3 state machines ===
  [PASS] sdlf-ott-mainA-sm latest execution  (SUCCEEDED)
  [PASS] sdlf-ott-mainB-sm latest execution  (SUCCEEDED)
  [PASS] sdlf-ott-mainDQ-sm latest execution  (SUCCEEDED)

=== Lambda — analytics + API functions ===
  [PASS] sdlf-ott-mainTR-report  (python3.12, 512MB)
  [PASS] sdlf-ott-mainLUT-refresh  (python3.12, 512MB)

=== SQS — 4 dead-letter queues ===
  [PASS] sdlf-ott-mainA-dlq.fifo  (depth=0)
  [PASS] sdlf-ott-mainB-dlq.fifo  (depth=0)
  [PASS] sdlf-ott-mainLUT-dlq  (depth=0)
  [PASS] sdlf-ott-mainTR-dlq  (depth=0)

=== CloudWatch — dashboard + alarms ===
  [PASS] dashboard sdlf-ott-searchevents-pipeline
  [PASS] sdlf-ott alarms deployed  (11 alarms)
  [PASS] no alarms in ALARM state

=== CloudFront dashboard — single user-facing surface ===
  [PASS] Dashboard URL published to SSM  (https://...cloudfront.net)
  [PASS] Dashboard HTTP 200 + non-trivial HTML  (status=200 bytes=390800)
  [PASS] Header source-attribution stamp present
  [PASS] Dashboard section present: Total Searches
  [PASS] Dashboard section present: Distinct Keywords
  [PASS] Dashboard section present: Top 20 Keywords
  [PASS] Dashboard section present: Trending Keywords
  [PASS] Dashboard section present: Content Gaps
  [PASS] Dashboard section present: Premium vs Free
  [PASS] Dashboard section present: Repeat Search Rate
  [PASS] Dashboard section present: Guest vs Authenticated
  [PASS] Dashboard section present: Search Volume by Hour

=== Lake Formation — column RBAC enforcement ===
  [PASS] curated table — IAM_ALLOWED_PRINCIPALS revoked  (column-level RBAC enforced)

=== Summary ===
  PASS=30+  WARN=0  FAIL=0
```

`FAIL=0` is the success criterion. Any `WARN` is a real live state worth investigating but does not break the contract.

---

## 5.7.2 Functionality matrix — what each check proves

| # | Functionality | Live resource | Proven by |
|---|---|---|---|
| 1 | CI/CD auto-deploy | `sdlf-ott-cicd` CodePipeline | Latest execution `Succeeded` |
| 2 | Infrastructure-as-code | 9 OTT CloudFormation stacks | All `*_COMPLETE` |
| 3 | ETL enrichment | Glue 4.0 job, 10×G.1X | Last run `SUCCEEDED` |
| 4 | Data catalog | 1 Glue DB, 3 physical tables | All present |
| 5 | Orchestration | 3 Step Functions (`mainA/B/DQ`) | All last executions `SUCCEEDED` |
| 6 | Analytics compute | 2 Lambdas (`mainTR` dashboard renderer + `mainLUT-refresh`) | Both deployed, Python 3.12 |
| 7 | Failure isolation | 4 DLQs (Stage A/B FIFO + 2 analytics) | All depth 0 |
| 8 | Observability | 1 CloudWatch dashboard + 11 alarms | All deployed |
| 9 | Serving layer | CloudFront dashboard (single surface) | 200 + every section + source-attribution stamp |
| 10 | Access control | Lake Formation on `curated` | Grant state reported (see 5.7.3) |

That is the entire deployed surface — 9 stacks, 1 Glue job, 3 catalog tables in `fpt_ott_searchevents_analytics` (`raw_search_events`, `curated`, `dq_results`), 3 state machines, 2 analytics Lambdas + the SDLF framework Lambdas, 4 DLQs, 11 alarms, 1 CloudFront dashboard.

---

## 5.7.3 Lake Formation column RBAC enforcement

If you completed [§5.3.5](../5.3-deploy/), `IAM_ALLOWED_PRINCIPALS` is revoked and the 5 `TableWithColumns` grants are live. Verify:

```powershell
python D:\ott-sdlf\scripts\verify_monitoring_and_lf.py
```

**Expected** (live after activation):

```
=== LAKE FORMATION ===
  OK L1 expected role grants present  (5/5)
  OK L2 IAM_ALLOWED_PRINCIPALS revoked  (column-level RBAC is ACTIVELY ENFORCED)
  OK L3 lutrefresh grant matches template  (excludes: ['has_premium', 'search_session_id', 'subscription_count', 'user_id_hashed'])
  OK L3 trending grant matches template  (all columns)
  OK L3 rDQExecu grant matches template  (all columns)
  OK L3 rGlueDQRol grant matches template  (all columns)
  OK L3 searchevents-glue-role grant matches template  (all columns)
```

Why this needs an imperative companion to the CFN stack: when `AWS::LakeFormation::PrincipalPermissions` declares a `TableWithColumns` grant while `IAM_ALLOWED_PRINCIPALS` is active on the same table, CloudFormation reports `CREATE_COMPLETE` but the underlying LF grant does not land — `ListPermissions(ResourceType=TABLE_WITH_COLUMNS)` returns 0. `activate_lakeformation.py` calls `lakeformation:GrantPermissions` directly to install the grants, then revokes `IAM_ALLOWED_PRINCIPALS`. After both steps run, all 5 grants become visible and enforcement is active.

If you skip §5.3.5 on a fresh deployment, `verify_monitoring_and_lf.py` flags it:

```
  !! L2 IAM_ALLOWED_PRINCIPALS SELECT still granted  (column exclusions DEFINED but NOT ENFORCED — see activation command)
```

That state is *operationally fine* (Lambdas keep working via the bypass) but the column-level exclusions are inert. Don't try to revoke `IAM_ALLOWED_PRINCIPALS` without running the activate script first — you will lock the Lambdas out.

---

## 5.7.4 Business insights — what the data delivers

`scripts/audit_visuals.py` runs the end-consumer Athena queries and renders them
as terminal charts. The dashboard renders the same numbers in the browser;
this script is for the headless / terminal context.

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

### Insight 2 — top trending titles (7-day window)

```
keyword_norm                                  derived_genre  searches
liên minh công lý: phiên bản của zack snyder  PHIM_AU_MY     10,864
fairy tail                                    ANIME           9,075
thiên nga bóng đêm                            PHIM_VIET       7,411
nữ thanh tra tài ba                           PHIM_VIET       7,012
sao băng                                      PHIM_HAN        6,987
```

Diacritics survive end-to-end (raw → Glue → Athena → HTML). The dashboard's Trending Keywords section exposes the full 500-row list with in-page filtering.

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

Demand peaks 02:00–04:00 VN and bottoms out late morning. The dashboard's hour×genre heatmap shows *which* genres peak when (e.g., PHIM_VIET dominates the 02–04 window; ANIME has a smaller evening secondary peak).

### Insight 5 — abandon rate flags content holes

```
derived_genre  total   abandoned  abandon_pct
ANIME          143269  20399      11.1
THE_THAO        40201   4044      10.1
PHIM_VIET      371723  44857       9.6
```

ANIME has the highest abandon rate (**11.1 %**) — users search, find nothing,
quit. The dashboard's Content Gaps section drills this down to the top 500 abandoned keywords with in-page filtering.

### Insight summary

| Insight | Dashboard section | Business action |
|---|---|---|
| Genre demand mix | Genre Distribution | Content acquisition weighting |
| Trending titles | Trending Keywords (500-row) | Promotion + push notifications |
| Premium skew by genre | Premium vs Free | Tiered-content strategy |
| Hourly demand curve | Search Volume by Hour × Genre (heatmap) | Cache-warming, job scheduling |
| Abandon rate / content gaps | Content Gaps (500-row) | Fill catalog holes |
| Repeat-search frustration | Repeat Search Rate | UX investigation |
| Guest vs authenticated | Guest vs Authenticated | Signup-funnel targeting |

Every figure above is reproducible: re-run `verify_live.py` and
`audit_visuals.py` on any machine with account credentials.

---

**Next**: [5.8 — Cleanup](../5.8-cleanup/).
