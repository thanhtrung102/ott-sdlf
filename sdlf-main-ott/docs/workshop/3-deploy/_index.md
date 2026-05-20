---
title: "3. Deploy"
date: 2026-05-20
weight: 3
chapter: false
pre: <b>3. </b>
---

# 3. Deploy

The repo already has CI/CD set up — that's the recommended deploy path. You push to `main` and the `sdlf-ott-cicd` CodePipeline takes care of validating + deploying every CloudFormation stack in dependency order.

> 💡 **TIP:** CI/CD is the recommended path (section 3.2). The local PowerShell path (section 3.8) is only for first-time bootstrap before CI/CD exists, or for rapid iteration when you don't want to wait on a `git push`.

| Path | Use when |
|---|---|
| **A. CI/CD (`sdlf-ott-cicd` CodePipeline)** ✓ recommended | Every routine deploy. Push to `main`, walk away, watch CodePipeline. |
| **B. Local PowerShell (`ott-pipeline.ps1`)** | First-time bootstrap before CI/CD exists, or rapid iteration when you don't want to wait on a git push. |

This chapter walks path A. Path B is at section 3.8 for the bootstrap / iteration case.

---

## 3.1 What gets deployed

Eleven CloudFormation stacks, in dependency order:

```
1.  sdlf-ott-searchevents-glue-job   — Glue job + IAM role + raw/curated catalog tables
2.  sdlf-pipeline-ott-mainA          — Stage A state machine (event routing)
3.  sdlf-pipeline-ott-mainB          — Stage B state machine (Glue orchestration)
4.  sdlf-pipeline-ott-dataquality    — Curated-layer DQ state machine
5.  sdlf-pipeline-ott-lutrefresh     — LUT-Refresh Lambda + DLQ + EventBridge rule
6.  sdlf-pipeline-ott-contentgap     — Content-Gap Lambda + 5 catalog tables + DLQ
7.  sdlf-pipeline-ott-trending       — Trending Lambda + gold catalog + DLQ
8.  sdlf-pipeline-ott-goldquality    — Gold-layer DQ state machine
9.  sdlf-pipeline-ott-monitoring     — CloudWatch dashboard + 17 alarms
10. sdlf-pipeline-ott-lakeformation  — Column-level RBAC on curated
11. sdlf-pipeline-ott-api            — HTTP API (x-api-key, freshness headers, arm64)
```

The deploy script handles ordering and parameter wiring automatically.

---

## 3.2 Trigger the deploy via git push

```powershell
cd D:\ott-sdlf
git push origin main
```

That's it. The CodePipeline picks up the push within ~30 seconds and runs four stages:

```
Source   → CodeStar connection pulls the commit
Validate → CodeBuild runs cfn-lint on every pipeline-ott-*.yaml
Deploy   → CodeBuild runs `aws cloudformation deploy` for each of 11 stacks
Notify   → Lambda publishes the success/failure summary to SNS
```

**Total time**: ~15 min for the deploy stage (each `cloudformation deploy` waits for stack completion); ~30 s each for the other stages.

---

## 3.3 Watch the pipeline run

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

## 3.4 When the pipeline succeeds

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

## 3.5 Activate Lake Formation column-level RBAC

`pipeline-ott-lakeformation.yaml` declares column-level grants on the `curated` table, but they stay **dormant** until the default `IAM_ALLOWED_PRINCIPALS` grant is revoked. Two steps.

**Step 1 — pre-grant the admin + CI/CD principals.** `lf_grants.py` grants `ALL` on every OTT table to `terraform-admin` and `sdlf-ott-cicd-codebuild`, so neither loses access after the revoke (and so the next CI/CD deploy doesn't fail with an LF permission error). It does *not* revoke anything — that's step 2.

```powershell
python D:\ott-sdlf\scripts\lf_grants.py --apply
```

**Expected output** (one line per table × principal; counts depend on how many catalog tables exist — 10 analytics + 2 gold = 24 grants today):

```
=== fpt_ott_searchevents_analytics (10 tables) ===
  OK   ALL on curated -> terraform-admin
  OK   ALL on curated -> sdlf-ott-cicd-codebuild
  ... (one pair per table)
=== fpt_ott_searchevents_gold (2 tables) ===
  OK   ALL on keyword_trends -> terraform-admin
  ... (one pair per table)

Summary: granted=24 failed=0
```

**Step 2 — revoke `IAM_ALLOWED_PRINCIPALS` on `curated`.** The exact command, with the account ID and database already substituted, is published as the `oActivationCommand` output of the Lake Formation stack:

```powershell
aws cloudformation describe-stacks --stack-name sdlf-pipeline-ott-lakeformation `
  --region ap-southeast-1 `
  --query "Stacks[0].Outputs[?OutputKey=='oActivationCommand'].OutputValue" --output text
```

It prints the revoke command — run what it gives you:

```
aws lakeformation revoke-permissions --principal '{"DataLakePrincipalIdentifier":"IAM_ALLOWED_PRINCIPALS"}' --resource '{"Table":{"CatalogId":"<account>","DatabaseName":"fpt_ott_searchevents_analytics","Name":"curated"}}' --permissions SELECT --region ap-southeast-1
```

> ⚠️ **WARNING:** Once revoked, only the principals explicitly granted in the Lake Formation stack can read `curated`. Any role that previously read it via plain IAM loses access.

**Verify enforcement** — `IAM_ALLOWED_PRINCIPALS` must be gone from `curated`:

```powershell
aws lakeformation list-permissions --region ap-southeast-1 `
  --resource '{\"Table\":{\"CatalogId\":\"<account>\",\"DatabaseName\":\"fpt_ott_searchevents_analytics\",\"Name\":\"curated\"}}' `
  --query "PrincipalResourcePermissions[?Principal.DataLakePrincipalIdentifier=='IAM_ALLOWED_PRINCIPALS'] | length(@)" --output text
```

**Expected**: `0`. If it returns `1`, step 2 didn't apply — re-run it. Chapter 7 §7.3 checks this on the reference deployment (where, by default, it has not yet been run).

---

## 3.6 Sanity-check what's deployed

```powershell
aws cloudformation list-stacks --region ap-southeast-1 `
  --stack-status-filter CREATE_COMPLETE UPDATE_COMPLETE `
  --query "StackSummaries[?starts_with(StackName, 'sdlf-pipeline-ott') || starts_with(StackName, 'sdlf-ott-searchevents-glue-job')].StackName" `
  --output text
```

**Expected** — 11 top-level OTT stacks + 4 SDLF MODULE nested stacks (created automatically by the Stage A/B MODULE constructs; do not deploy them directly):

```
sdlf-ott-searchevents-glue-job
sdlf-pipeline-ott-api
sdlf-pipeline-ott-contentgap
sdlf-pipeline-ott-dataquality
sdlf-pipeline-ott-goldquality
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

15 names total.

---

## 3.7 Read the live state — what the deploy gave you

```powershell
# API base URL (you'll use it in chapter 5)
aws ssm get-parameter --name /sdlf/pipeline/rApiUrl/ott --region ap-southeast-1 --query Parameter.Value --output text
# Live example: https://oygn7qkkr7.execute-api.ap-southeast-1.amazonaws.com
# Your api-id will differ; the rApiUrl SSM parameter is the source of truth.

# CloudWatch dashboard
Write-Host "Dashboard: https://ap-southeast-1.console.aws.amazon.com/cloudwatch/home?region=ap-southeast-1#dashboards:name=sdlf-ott-searchevents-pipeline"

# Glue job name
aws glue get-job --job-name sdlf-ott-searchevents-glue-job --region ap-southeast-1 --query "Job.Name" --output text
# Expected: sdlf-ott-searchevents-glue-job
```

---

## 3.8 (Alternative) Deploy without CI/CD — local PowerShell

For first-time bootstrap (before CI/CD exists in the account) or rapid iteration when waiting on a git push isn't acceptable:

```powershell
cd D:\ott-sdlf
.\ott-pipeline.ps1
```

Time: ~15 min for first deploy; ~5 min for subsequent runs (no-op CFN updates).

**Expected output (truncated)**:

```
=== Deploy stacks ===
  uploading Glue script to s3://...-artifacts-prod/ott/searchevents/ ...
  packaging analytics Lambdas to s3://...-artifacts-prod/lambda/ ...
[contentgap] uploaded ... (4823 bytes)
[trending]   uploaded ... (4964 bytes)
[lutrefresh] uploaded ... (3204 bytes)
[api]        uploaded ... (1532 bytes)
  deploying sdlf-ott-searchevents-glue-job ... OK
  deploying sdlf-pipeline-ott-mainA ........ OK
  ... (all 11 stacks)
  [OK]   All 11 stacks deployed
```

`ott-pipeline.ps1` does what the CI/CD buildspec does, plus ingests one raw file and runs the analytics Lambdas — useful for end-to-end iteration in one command. Both paths deploy identical CloudFormation.

---

**Next**: [chapter 4 — Ingest](../4-ingest/).
