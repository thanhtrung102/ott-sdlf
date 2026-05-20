---
title: "3. Deploy"
date: 2026-05-20
weight: 3
chapter: false
pre: <b>3. </b>
---

# 3. Deploy

The repo already has CI/CD set up — that's the recommended deploy path. You push to `main` and the `sdlf-ott-cicd` CodePipeline takes care of validating + deploying every CloudFormation stack in dependency order.

| Path | Use when |
|---|---|
| **A. CI/CD (`sdlf-ott-cicd` CodePipeline)** ✓ recommended | Every routine deploy. Push to `main`, walk away, watch CodePipeline. |
| **B. Local PowerShell (`ott-pipeline.ps1`)** | First-time bootstrap before CI/CD exists, or rapid iteration when you don't want to wait on a git push. |

This chapter walks path A. Path B is at section 3.7 for the bootstrap / iteration case.

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
9.  sdlf-pipeline-ott-monitoring     — CloudWatch dashboard + 14 alarms
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

---

## 3.5 Activate Lake Formation column-level RBAC

The Lake Formation grants are declared in `pipeline-ott-lakeformation.yaml` but stay dormant until you revoke `IAM_ALLOWED_PRINCIPALS` on each protected table.

```powershell
python D:\ott-sdlf\scripts\lf_grants.py --apply
```

**Expected output**:

```
=== fpt_ott_searchevents_analytics (3 tables) ===
  OK   ALL on curated -> terraform-admin
  OK   ALL on curated -> sdlf-ott-cicd-codebuild
  OK   ALL on raw_search_events -> terraform-admin
  OK   ALL on raw_search_events -> sdlf-ott-cicd-codebuild
  OK   ALL on dq_results -> terraform-admin
  OK   ALL on dq_results -> sdlf-ott-cicd-codebuild

=== fpt_ott_searchevents_gold (1 tables) ===
  OK   ALL on keyword_trends -> terraform-admin
  OK   ALL on keyword_trends -> sdlf-ott-cicd-codebuild

Summary: granted=8 failed=0
```

**Verify enforcement** — `IAM_ALLOWED_PRINCIPALS` must NOT be in the grant list of any protected table:

```python
python -c "
import boto3
lf = boto3.client('lakeformation', region_name='ap-southeast-1')
for db, t in [('fpt_ott_searchevents_analytics','curated'),
              ('fpt_ott_searchevents_gold','keyword_trends')]:
    r = lf.list_permissions(Resource={'Table':{'CatalogId':'<your-account>','DatabaseName':db,'Name':t}})
    iam = any(p.get('Principal',{}).get('DataLakePrincipalIdentifier','').endswith(':IAMAllowedPrincipals')
              for p in r.get('PrincipalResourcePermissions', []))
    print(f'{db}.{t}: IAM_ALLOWED_PRINCIPALS granted? {iam}')
"
```

**Expected**: all entries print `False`.

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
