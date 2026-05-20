---
title: "3. Deploy"
date: 2026-05-20
weight: 3
chapter: false
pre: <b>3. </b>
---

# 3. Deploy

Two equivalent deploy paths:

| Path | Use when |
|---|---|
| **A. CI/CD (`sdlf-ott-cicd` CodePipeline)** | You've pushed your branch to GitHub and the connection is configured. Hands-off. |
| **B. Local PowerShell (`ott-pipeline.ps1`)** | You want full control, are iterating, or don't want to wait on GitHub. |

This chapter walks path B (faster for first-time workshop runs). Path A is described at the end.

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

## 3.2 Run the deploy

From the repo root:

```powershell
.\ott-pipeline.ps1
```

Time: ~15 minutes for first deploy (~5 min for subsequent runs since most stacks are no-op).

**Expected output (truncated to one stack per group)**:

```
=== Deploy stacks ===
  uploading Glue script to s3://fpt-ott-ap-southeast-1-703668403514-artifacts-prod/ott/searchevents/ ...
  packaging analytics Lambdas to s3://fpt-ott-ap-southeast-1-703668403514-artifacts-prod/lambda/ ...
[contentgap] uploaded s3://...-artifacts-prod/lambda/contentgap.zip (4823 bytes, sha256=...)
[trending] uploaded s3://...-artifacts-prod/lambda/trending.zip (4964 bytes, sha256=...)
[lutrefresh] uploaded s3://...-artifacts-prod/lambda/lutrefresh.zip (3204 bytes, sha256=...)
[api] uploaded s3://...-artifacts-prod/lambda/api.zip (1532 bytes, sha256=...)
  deploying sdlf-ott-searchevents-glue-job ... OK
  deploying sdlf-pipeline-ott-mainA ........ OK
  deploying sdlf-pipeline-ott-mainB ........ OK
  deploying sdlf-pipeline-ott-dataquality .. OK
  deploying sdlf-pipeline-ott-lutrefresh ... OK
  deploying sdlf-pipeline-ott-contentgap ... OK
  deploying sdlf-pipeline-ott-trending ..... OK
  deploying sdlf-pipeline-ott-goldquality .. OK
  deploying sdlf-pipeline-ott-monitoring ... OK
  deploying sdlf-pipeline-ott-lakeformation OK
  deploying sdlf-pipeline-ott-api .......... OK
  [OK]   All 11 stacks deployed
```

---

## 3.3 Activate Lake Formation column-level RBAC

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

## 3.4 Sanity-check what's deployed

```powershell
aws cloudformation list-stacks --region ap-southeast-1 `
  --stack-status-filter CREATE_COMPLETE UPDATE_COMPLETE `
  --query "StackSummaries[?starts_with(StackName, 'sdlf-pipeline-ott') || starts_with(StackName, 'sdlf-ott-searchevents-glue-job')].StackName" `
  --output text
```

**Expected** — the 11 top-level OTT stacks below, plus 4 SDLF MODULE nested stacks (one each for the Stage A and Stage B framework constructs). The framework's `awslabs::sdlf::stageA::MODULE` and `stageB::MODULE` resolve to nested CloudFormation stacks that show up with random-suffix names:

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

15 names total — 11 OTT-managed + 4 nested. The nested ones are created automatically by the MODULE construct; you don't deploy them directly.

---

## 3.5 Read the live state — what the deploy gave you

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

## 3.6 (Alternative) Deploy via CI/CD

If you have GitHub access:

1. Push your branch to `main`.
2. The `sdlf-ott-cicd` CodePipeline auto-triggers: Source → Validate (cfn-lint) → Deploy → Notify.
3. Watch in the AWS console at CodePipeline → `sdlf-ott-cicd` → View pipeline.

**Verify a successful run**:

```powershell
aws codepipeline get-pipeline-state --name sdlf-ott-cicd --region ap-southeast-1 `
  --query "stageStates[].{Stage:stageName, Status:latestExecution.status}" --output table
```

**Expected**:

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

**Next**: [chapter 4 — Ingest](../4-ingest/).
