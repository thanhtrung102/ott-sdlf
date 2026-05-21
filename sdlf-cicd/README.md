# sdlf-cicd/ — CI/CD for the OTT pipeline

The CI/CD stack for the OTT search-analytics pipeline. Deploys a 4-stage AWS CodePipeline that auto-deploys every push to `main`.

This README explains what's in this directory and how the pieces fit together. For *how to use* the pipeline, see [docs/10-deployment](../sdlf-main-ott/docs/10-deployment/) and [docs/workshop/3-deploy](../sdlf-main-ott/docs/workshop/3-deploy/).

---

## Files

| File | Role |
|---|---|
| `template-cicd.yaml` | CloudFormation stack `sdlf-ott-cicd`. Defines the CodePipeline, two CodeBuild projects, the artifact bucket + KMS key, the CodeBuild IAM role, the notifier Lambda + SNS topic, and the EventBridge rule that fires the Lambda on pipeline failures. |
| `buildspec-validate.yml` | The Validate stage's CodeBuild script. Runs `cfn-lint` against every `pipeline-ott-*.yaml`. ~30 s. |
| `buildspec-deploy.yml` | The Deploy stage's CodeBuild script. Fetches SSM parameters (bucket names, KMS key), then runs `aws cloudformation deploy` for each of the 9 OTT stacks in dependency order. ~15 min. |

---

## Pipeline shape

```
GitHub push to main
        │ (CodeStar Connection)
        ▼
┌────────────────┐    ┌──────────────────┐    ┌────────────────┐    ┌─────────────────┐
│ Source         │ →  │ Validate         │ →  │ Deploy         │ →  │ Notify          │
│ pull commit    │    │ cfn-lint × 9     │    │ deploy × 9     │    │ Lambda → SNS    │
└────────────────┘    └──────────────────┘    └────────────────┘    └─────────────────┘
   ~10 s                  ~30 s                  ~15 min                ~5 s
```

EventBridge rule `sdlf-ott-cicd-failure` ALSO fires the notifier on any pipeline FAILED state (covers Validate failures and Deploy failures alike).

---

## The CodeBuild IAM role (`sdlf-ott-cicd-codebuild`)

Path `/sdlf-ott/`. Grants the role needs to deploy 9 stacks:

| Capability | Why it's needed |
|---|---|
| `cloudformation:*` on `sdlf-pipeline-ott-*` + `sdlf-ott-searchevents-glue-job` | Deploy / update / describe the 9 OTT stacks. |
| `iam:Create/Delete/Update/Get/PassRole` scoped to `/sdlf-ott/` path | Create the Lambda execution roles each pipeline stack defines. |
| `lambda:*` on `sdlf-ott-*` functions | Create + update the 2 analytics Lambdas + the SDLF nested-stack Lambdas. |
| `states:*` on `sdlf-ott-*` state machines | Create + update the 3 Step Functions (Stage A, B, DQ). |
| `events:*` on `sdlf-ott-*` rules | Create + update the EventBridge fan-out rules. |
| `sqs:*` on `sdlf-ott-*` queues + KMS access for queue encryption | Create + update the 4 DLQs. |
| `sns:*` on the notifications topic | Subscribe analytics Lambdas to publish. |
| `glue:CreateJob/UpdateJob/DeleteJob/GetJob/TagResource` on `job/sdlf-ott-*` | Manage the ETL Glue job. |
| `glue:GetDatabase/GetTable/GetPartitions/DeleteTable` on `catalog`+`database/*`+`table/*/*` | Read existing catalog state and idempotently drop legacy CSV-backed tables. |
| `glue:CreateDatabase` / **`glue:UpdateDatabase`** / `DeleteDatabase`, `CreateTable`/`UpdateTable`/`DeleteTable`, `CreateCrawler`/`UpdateCrawler`/`DeleteCrawler`/`GetCrawler` on `catalog`+`database/*`+`table/*/*`+`crawler/*` | Manage Glue databases + tables + crawlers across stacks. |
| `athena:CreateWorkGroup/...` (scoped) + Bedrock invoke + SSM read on `/sdlf/...` | Workgroup management, classifier model, parameter lookup. |
| `cloudfront:CreateDistribution/UpdateDistribution/...` scoped to the dashboard distribution | Manage the OAC-fronted dashboard distribution. |
| Lake Formation: this role is also a data lake admin (added via `pipeline-ott-lakeformation.yaml`) | So it can run the `lakeformation:revoke-permissions` step on `IAM_ALLOWED_PRINCIPALS`. |

> **Important gotcha**: `glue:UpdateDatabase` is required even though no template explicitly modifies the database — CFN's stack-update flow calls `UpdateDatabase` to verify drift on every no-op update. Missing it makes any subsequent deploy fail with `is not authorized to perform: glue:UpdateDatabase`. We hit this on 2026-05-20 — added it to template-cicd.yaml as part of commit 32c5b9d.

---

## Buildspec hierarchy

### `buildspec-validate.yml`
1. `pip install cfn-lint`
2. `cfn-lint sdlf-main-ott/pipeline-ott-*.yaml`
3. Exit 0 if all templates parse cleanly; non-zero blocks the Deploy stage.

### `buildspec-deploy.yml`
Pre-build resolves these SSM parameters into env vars:

| Env var | SSM source |
|---|---|
| `KMS_KEY` | `/sdlf/storage/rKMSKey/prod` |
| `STAGE_BUCKET` | `/sdlf/storage/rStageBucket/prod` |
| `ANALYTICS_BUCKET` | `/sdlf/storage/rAnalyticsBucket/prod` |
| `CURATED_DB` | `/sdlf/dataset/rAnalyticsGlueDataCatalog/searchevents` |
| `CURATED_CRAWLER` | `/sdlf/dataset/rAnalyticsGlueCrawler/searchevents` |
| `CRAWLER_ROLE` | `/sdlf/dataset/rDatalakeCrawlerRole/searchevents` |
| `STAGE_B_SM_ARN` | `/sdlf/pipeline/rStateMachine/ott-mainB` |
| `ATHENA_RESULTS_BUCKET` + `ATHENA_WG_KMS` | derived from `aws athena get-work-group --work-group sdlf-ott` |

Build runs `aws cloudformation deploy` for each of the 9 stacks in dependency order. The order matches the workshop's [chapter 3 deployment list](../sdlf-main-ott/docs/workshop/3-deploy/) exactly.

Post-build verifies each stack reached `CREATE_COMPLETE` or `UPDATE_COMPLETE`; exits 1 if any didn't.

---

## Deploying the CI/CD stack itself

You can't deploy the CI/CD stack via the CI/CD pipeline — chicken and egg. Deploy it manually once, then it bootstraps itself for all future deploys:

```powershell
aws cloudformation deploy --template-file sdlf-cicd/template-cicd.yaml `
  --stack-name sdlf-ott-cicd `
  --capabilities CAPABILITY_NAMED_IAM CAPABILITY_AUTO_EXPAND `
  --region ap-southeast-1 `
  --parameter-overrides `
    pGitHubOwner=thanhtrung102 `
    pGitHubRepo=ott-sdlf `
    pGitHubBranch=main `
    pNotificationEmail=<your-email> `
    pGitHubConnectionArn=<your-codestar-connection-arn> `
    pMaxNewKeywords=15000 `
    pBedrockModelId=global.anthropic.claude-haiku-4-5-20251001-v1:0 `
    pPipelineReference=initial-deploy
```

**CodeStar connection prerequisite**: create a CodeStar GitHub connection in the AWS console *before* deploying; it requires browser-based OAuth approval that can't be scripted. Use the resulting connection ARN as `pGitHubConnectionArn`.

To re-deploy the CI/CD stack later (e.g. to update the IAM policy), run the same command with `pPipelineReference` set to a new tag. Other parameters reuse the previous values automatically (CFN remembers them).

---

## Failure-recovery playbook

When the CI/CD Deploy stage fails:

1. **Check which stack** — look at the CodeBuild log for `ERROR: <stack-name> in unexpected state <state>`.
2. **If `UPDATE_ROLLBACK_FAILED`** — the update failed AND the rollback failed too (usually IAM-related). Force-skip the failing resource:
   ```bash
   aws cloudformation continue-update-rollback --stack-name <stack> --resources-to-skip <ResourceLogicalId>
   ```
3. **If `UPDATE_ROLLBACK_COMPLETE`** — the stack reverted cleanly. Fix the underlying issue and re-push.
4. **If it's an IAM permission denial** — the fix usually goes in `template-cicd.yaml` (this directory). Deploy it manually with admin credentials, then re-push.
5. **If it's a CFN template syntax/semantic error** — fix the affected `pipeline-ott-*.yaml` and re-push. The Validate stage will catch most syntax errors before Deploy.

The notifier Lambda (`sdlf-ott-cicd-notify`) publishes a structured failure summary to `sdlf-ott-cicd-notifications` SNS topic on every failed run.

---

## Cost

| Resource | Cost driver | Monthly |
|---|---|---|
| CodePipeline | 1 active pipeline | $1.00 (first one free, second onwards) |
| CodeBuild (Validate × 0.5 min + Deploy × 15 min per run) | Per-minute, ARM-AL2 small | ~$0.05/run; ~$1.50/month at one push/day |
| Artifact S3 bucket + KMS encryption | ~10 MB per run, 30-day retention | ~$0.01/month |
| Notifier Lambda + SNS | Invoked only on FAILED state | <$0.01/month |
| **Total** | | **~$2.50/month** at one push/day |
