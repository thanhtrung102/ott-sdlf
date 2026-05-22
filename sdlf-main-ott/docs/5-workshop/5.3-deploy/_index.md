---
title: "Deploy"
date: 2026-05-20
weight: 3
chapter: false
pre: " <b> 5.3 </b> "
---

The repo already has CI/CD set up — that's the recommended deploy path. You push to `main` and the `sdlf-ott-cicd` CodePipeline takes care of validating + deploying every CloudFormation stack in dependency order.

> 💡 **TIP:** CI/CD is the recommended path (section 5.3.2). The local PowerShell path (section 5.3.9) is only for first-time bootstrap before CI/CD exists, or for rapid iteration when you don't want to wait on a `git push`.

| Path | Use when |
|---|---|
| **A. CI/CD (`sdlf-ott-cicd` CodePipeline)** ✓ recommended | Every routine deploy. Push to `main`, walk away, watch CodePipeline. |
| **B. Local PowerShell (`ott-pipeline.ps1`)** | First-time bootstrap before CI/CD exists, or rapid iteration when you don't want to wait on a git push. |

This chapter walks path A. Path B is at section 5.3.9 for the bootstrap / iteration case.

---

## 5.3.1 What gets deployed

Nine CloudFormation stacks, in dependency order:

```
1.  sdlf-ott-searchevents-glue-job   — Glue job + IAM role + raw/curated catalog tables
2.  sdlf-pipeline-ott-mainA          — Stage A state machine (event routing)
3.  sdlf-pipeline-ott-mainB          — Stage B state machine (Glue orchestration)
4.  sdlf-pipeline-ott-dataquality    — Curated-layer DQ state machine
5.  sdlf-pipeline-ott-lutrefresh     — LUT-Refresh Lambda + DLQ + EventBridge rule
6.  sdlf-pipeline-ott-trending       — Dashboard renderer (Trending Lambda) + DLQ
7.  sdlf-pipeline-ott-monitoring     — CloudWatch dashboard + 11 alarms
8.  sdlf-pipeline-ott-lakeformation  — Column-level RBAC on curated
9.  sdlf-pipeline-ott-dashboard      — S3 + CloudFront hosting for the search-analytics dashboard
```

The analytics HTTP API and the standalone content-gap Lambda were retired in favour of dashboard-with-full-depth: the dashboard renderer (formerly Trending Lambda) now executes every section query at full depth and the CloudFront dashboard is the single user-facing surface.

The deploy path handles artifact staging, stack ordering, and parameter wiring automatically.

---

## 5.3.2 Trigger the deploy via git push

```powershell
cd D:\ott-sdlf
git push origin main
```

That's it. The CodePipeline picks up the push within ~30 seconds and runs four stages:

```
Source   → CodeStar connection pulls the commit
Validate → CodeBuild runs cfn-lint on every pipeline-ott-*.yaml
Deploy   → CodeBuild stages the Glue script + Lambda zips to S3, runs
           `aws cloudformation deploy` for each of 9 stacks, then pushes live Lambda code
Notify   → Lambda publishes the success/failure summary to SNS
```

**Total time**: ~15 min for the deploy stage (each `cloudformation deploy` waits for stack completion); ~30 s each for the other stages.

---

## 5.3.3 Watch the pipeline run

```powershell
aws codepipeline get-pipeline-state --name sdlf-ott-cicd --region ap-southeast-1 `
  --query "stageStates[].{Stage:stageName, Status:latestExecution.status}" --output table
```

**Expected output during a run** (status moves left-to-right over ~15 min):

```
---------------------------------
| Stage    | Status              |
|----------|---------------------|
| Source   | Succeeded           |
| Validate | Succeeded           |
| Deploy   | InProgress          |   ← watch this one
| Notify   | (previous Succeeded) |
---------------------------------
```

Or tail the CodeBuild logs for the Deploy stage directly:

```powershell
$BUILD_ID = aws codebuild list-builds-for-project --project-name sdlf-ott-cicd-deploy `
  --region ap-southeast-1 --query "ids[0]" --output text
aws codebuild batch-get-builds --ids $BUILD_ID --region ap-southeast-1 `
  --query "builds[0].{status:buildStatus, phases:currentPhase}" --output table
```

**Expected**: `status: IN_PROGRESS, phases: BUILD` while running; `status: SUCCEEDED` when done.

---

## 5.3.4 When the pipeline succeeds

You'll receive an SNS notification at the email subscribed to `sdlf-ott-cicd-notifications`. Or check directly:

```powershell
aws codepipeline get-pipeline-state --name sdlf-ott-cicd --region ap-southeast-1 `
  --query "stageStates[].{Stage:stageName, Status:latestExecution.status}" --output table
```

**Expected (all green)**:

```
---------------------------------
| Stage    | Status              |
|----------|---------------------|
| Source   | Succeeded           |
| Validate | Succeeded           |
| Deploy   | Succeeded           |
| Notify   | Succeeded           |
---------------------------------
```

> 📷 **Screenshot —** CodePipeline console: `sdlf-ott-cicd` with all four stages (Source · Validate · Deploy · Notify) green.
> *Placeholder: capture and save as `01-pipeline-green.png` in this chapter folder, then replace this block with `![CodePipeline all stages green](01-pipeline-green.png)`.*

---

## 5.3.5 Activate Lake Formation column-level RBAC

`pipeline-ott-lakeformation.yaml` declares 5 `AWS::LakeFormation::PrincipalPermissions` resources with `TableWithColumns` ColumnWildcards. CloudFormation will report all 5 as `CREATE_COMPLETE`, but **the underlying LF grants will not land** until `IAM_ALLOWED_PRINCIPALS` is revoked. CFN reports success because the resource creation API call succeeded; the column-level grant collapses to a no-op silently. Result: 0 `TABLE_WITH_COLUMNS` grants visible via `ListPermissions` after the deploy.

Activation is therefore a 2-script step that runs *after* `5.3.2`:

**Step 1 — pre-grant admin + CI/CD principals.** `lf_grants.py` grants `ALL` on every OTT table to `terraform-admin` and `sdlf-ott-cicd-codebuild`, so neither loses access after the revoke. Idempotent.

```powershell
python D:\ott-sdlf\scripts\lf_grants.py --apply
```

**Step 2 — grant the OTT pipeline roles imperatively + revoke `IAM_ALLOWED_PRINCIPALS`.** `activate_lakeformation.py` works around the CFN no-op by calling `lakeformation:GrantPermissions` directly for each ott-main\* role with the exact column exclusions declared in `pipeline-ott-lakeformation.yaml`, then revokes `IAM_ALLOWED_PRINCIPALS` to turn on enforcement.

```powershell
python D:\ott-sdlf\scripts\activate_lakeformation.py            # dry-run, prints what it would do
python D:\ott-sdlf\scripts\activate_lakeformation.py --apply    # commits
```

**Expected output** (`--apply` mode):

```
Step 1: Grant ott-main* roles with column exclusions
  OK      LutRefresh    ->  excludes ['user_id_hashed', 'search_session_id', 'has_premium', 'subscription_count']
  OK      Trending      ->  all columns
  OK      DQExec        ->  all columns
  OK      DQGlue        ->  all columns
Step 2: Grant Glue ETL role (SELECT/INSERT/ALTER, all columns)
  OK      GlueETL      ->  SELECT all columns
  OK      GlueETL      ->  INSERT (table-level)
  OK      GlueETL      ->  ALTER (table-level)
Step 3: Grant Crawler role (table-level ALTER/DESCRIBE/INSERT)
  OK      Crawler      ->  ALTER+DESCRIBE+INSERT
Step 4: Revoke IAM_ALLOWED_PRINCIPALS to activate enforcement
  OK      IAM_ALLOWED_PRINCIPALS  ->  ALL  (revoked)

=== Post-state verification ===
  TableWithColumns grants now visible: 5
```

> On a **first** activation, step 4 prints `OK`. The script is idempotent — on any **re-run** (the table is already enforced) step 4 prints `ALREADY_REVOKED` instead, which is also success. The post-state count is the 5 pipeline-role grants plus any admin grant `lf_grants.py` added, so it may read `5` or `6`; `verify_monitoring_and_lf.py` (below) is the authoritative check.

> ⚠️ **WARNING:** Once `IAM_ALLOWED_PRINCIPALS` is revoked, only principals explicitly granted in step 1 + step 2 can read `curated`. Any role outside that set loses access.

**Verify enforcement** — the empirical check, runs in <1 second:

```powershell
python D:\ott-sdlf\scripts\verify_monitoring_and_lf.py
```

**Expected**: `L2 IAM_ALLOWED_PRINCIPALS revoked  (column-level RBAC is ACTIVELY ENFORCED)` and 5 × `L3 ... grant matches template` lines.

> The CFN approach alone (without `activate_lakeformation.py`) leaves you in the worst of both worlds: `IAM_ALLOWED_PRINCIPALS` not revoked + 0 actual column grants. The Lambdas keep working only via the bypass — and pulling that bypass would lock every Lambda out. The activate script fixes both at once.

---

## 5.3.6 Subscribe to the alarm SNS topic

The 11 CloudWatch alarms deployed by `pipeline-ott-monitoring.yaml` publish to the SNS topic at `/SDLF/SNS/ott/Notifications`. The topic exists, but a fresh deployment has **0 subscribers** — alarms will fire silently. Add an endpoint:

```powershell
$TOPIC = aws ssm get-parameter --name /SDLF/SNS/ott/Notifications --region ap-southeast-1 --query Parameter.Value --output text
aws sns subscribe --topic-arn $TOPIC --protocol email `
  --notification-endpoint your-email@example.com --region ap-southeast-1
```

You'll receive an AWS Notification email — click **Confirm subscription** in it. Until you do, the subscription stays in `PendingConfirmation` and alarm notifications are dropped.

**Verify**:

```powershell
aws sns list-subscriptions-by-topic --topic-arn $TOPIC --region ap-southeast-1 `
  --query "Subscriptions[?SubscriptionArn != 'PendingConfirmation'].[Protocol,Endpoint]" --output table
```

**Expected** — at least one confirmed row with your protocol + endpoint.

---

## 5.3.7 Sanity-check what's deployed

```powershell
aws cloudformation list-stacks --region ap-southeast-1 `
  --stack-status-filter CREATE_COMPLETE UPDATE_COMPLETE `
  --query "StackSummaries[?starts_with(StackName, 'sdlf-pipeline-ott') || starts_with(StackName, 'sdlf-ott-searchevents-glue-job')].StackName" `
  --output text
```

**Expected** — 9 top-level OTT stacks + 4 SDLF MODULE nested stacks (created automatically by the Stage A/B MODULE constructs; do not deploy them directly):

```
sdlf-ott-searchevents-glue-job
sdlf-pipeline-ott-dashboard
sdlf-pipeline-ott-dataquality
sdlf-pipeline-ott-lakeformation
sdlf-pipeline-ott-lutrefresh
sdlf-pipeline-ott-mainA
sdlf-pipeline-ott-mainA-rMainA-<suffix>                      (SDLF nested)
sdlf-pipeline-ott-mainA-rMainA-<suffix>-rPipelineInterface-<suffix>  (SDLF nested)
sdlf-pipeline-ott-mainB
sdlf-pipeline-ott-mainB-rMainB-<suffix>                      (SDLF nested)
sdlf-pipeline-ott-mainB-rMainB-<suffix>-rPipelineInterface-<suffix>  (SDLF nested)
sdlf-pipeline-ott-monitoring
sdlf-pipeline-ott-trending
```

13 names total.

---

## 5.3.8 Read the live state — what the deploy gave you

```powershell
# CloudFront dashboard URL — the single user-facing surface (you'll open it in 5.5)
aws ssm get-parameter --name /sdlf/pipeline/rDashboardUrl/ott --region ap-southeast-1 --query Parameter.Value --output text
# Live example: https://d3bdq70ai5wf18.cloudfront.net
# Your distribution id will differ; the rDashboardUrl SSM parameter is the source of truth.

# CloudWatch operational dashboard
Write-Host "Dashboard: https://ap-southeast-1.console.aws.amazon.com/cloudwatch/home?region=ap-southeast-1#dashboards:name=sdlf-ott-searchevents-pipeline"

# Glue job name
aws glue get-job --job-name sdlf-ott-searchevents-glue-job --region ap-southeast-1 --query "Job.Name" --output text
# Expected: sdlf-ott-searchevents-glue-job
```

---

## 5.3.9 (Alternative) Deploy without CI/CD — local PowerShell

For first-time bootstrap (before CI/CD exists in the account) or rapid iteration when waiting on a git push isn't acceptable:

```powershell
cd D:\ott-sdlf
.\ott-pipeline.ps1
```

Time: ~15 min for first deploy; ~5 min for subsequent runs (no-op CFN updates).

**Expected output (truncated)**:

```
=== Deploy stacks ===
  uploading Glue script to s3://ott-search-...-prod/ott/searchevents/ ...
  packaging analytics Lambdas to s3://...-artifacts-prod/lambda/ ...
[trending]   uploaded ... (12000+ bytes)
[lutrefresh] uploaded ... (3263 bytes)
  deploying sdlf-ott-searchevents-glue-job ... OK
  deploying sdlf-pipeline-ott-mainA ........ OK
  ... (all 9 stacks)
  [OK]   All 9 stacks deployed
```

`ott-pipeline.ps1` does what the CI/CD buildspec does, plus ingests one raw file and runs the analytics Lambdas — useful for end-to-end iteration in one command. Both paths stage the same artifacts and deploy identical CloudFormation.

---

**Next**: [5.4 — Ingest](../5.4-ingest/).
