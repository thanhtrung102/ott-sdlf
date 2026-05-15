---
title: "10. Deployment"
date: 2026-05-15
weight: 10
chapter: false
pre: <b>10. </b>
---

# Deployment

The pipeline is deployed through **AWS CodePipeline** (`sdlf-ott-cicd`). Every push to the `main` branch triggers a four-stage CI/CD pipeline that validates and deploys all CloudFormation stacks.

---

## Prerequisites

Before the first deployment, the following must be in place:

| Prerequisite | Notes |
|---|---|
| SDLF core stacks deployed | Stage A/B Lambda modules, SDLF dataset module for `searchevents` |
| S3 buckets created and registered in SSM | Raw, stage, analytics, artifacts buckets; SSM paths under `/sdlf/storage/` |
| KMS key created and registered in SSM | `/sdlf/storage/rKMSKey/prod` |
| SNS topic created and registered in SSM | `/SDLF/SNS/ott/Notifications` |
| SDLF Lambda layer available | `/SDLF/Lambda/LatestDatalakeLibraryLayer` |
| `genre_classifier_pkg.zip` uploaded | `s3://[artifacts-bucket]/ott/searchevents/genre_classifier_pkg.zip` — required by the Glue job |
| Bedrock model enabled in region | `anthropic.claude-haiku-4-5-20251001-v1:0` must be enabled in ap-southeast-1 |
| Lake Formation data lake admin configured | Required for the `IAM_ALLOWED_PRINCIPALS` revocation step |

---

## CodePipeline stages

| Stage | Actions | On failure |
|---|---|---|
| **Source** | Pull from CodeCommit/GitHub repository | Pipeline stops |
| **Validate** | CloudFormation `validate-template` on all YAML files | Pipeline stops; no resources changed |
| **Deploy** | `cloudformation deploy` for each stack in order | Pipeline stops; previous stacks remain |
| **Notify** | SNS notification (success or failure summary) | Always runs |

---

## Stack deployment order

Stacks must be deployed in dependency order. The CodePipeline Deploy stage handles this sequencing automatically.

```
1. datasets.yaml              — SDLF dataset module (searchevents)
2. pipeline-ott-glue-job.yaml — Glue job, IAM role, catalog tables (raw + curated)
3. pipeline-ott-mainA.yaml    — Stage A state machine + Lambda
4. pipeline-ott-mainB.yaml    — Stage B state machine
5. pipeline-ott-dq-stage.yaml (mainDQ instance)  — Curated DQ state machine
6. pipeline-ott-contentgap.yaml   — Content Gap Lambda + catalog tables
7. pipeline-ott-lutrefresh.yaml   — LUT Refresh Lambda
8. pipeline-ott-trending.yaml     — Trending Lambda + gold catalog tables
9. pipeline-ott-dq-stage.yaml (mainGoldDQ instance) — Gold DQ state machine
10. pipeline-ott-api.yaml         — HTTP API
11. pipeline-ott-lakeformation.yaml — Lake Formation grants
12. pipeline-ott-monitoring.yaml   — Dashboard + alarms
```

> Stack 9 (Gold DQ) depends on the gold Glue database created by stack 8.
> Stack 11 (Lake Formation) depends on all role ARNs exported by stacks 3–9.

---

## Pre-deployment catalog cleanup

If the `keyword_trends` table already exists in the gold Glue database from a previous Lambda CTAS run, CloudFormation will fail to create `rKeywordTrendsTable` in `pipeline-ott-trending.yaml`. Delete it before deploying:

```bash
aws glue delete-table \
  --database-name fpt_ott_searchevents_gold \
  --name keyword_trends \
  --region ap-southeast-1
```

Similarly for the `curated` table if deploying the Glue job stack for the first time over an environment that already has a Lambda-created table.

---

## Activating Lake Formation column-level restrictions

After `pipeline-ott-lakeformation.yaml` is deployed, the column-level exclusions are defined but not enforced. To enforce them, a Lake Formation administrator must revoke the default `IAM_ALLOWED_PRINCIPALS` SELECT grant:

```bash
# Substitute ACCOUNT with your AWS account ID and DB with the searchevents database name
aws lakeformation revoke-permissions \
  --principal '{"DataLakePrincipalIdentifier":"IAM_ALLOWED_PRINCIPALS"}' \
  --resource '{"Table":{"CatalogId":"ACCOUNT","DatabaseName":"DB","Name":"curated"}}' \
  --permissions SELECT \
  --region ap-southeast-1
```

The exact command (with substituted account ID and database name) is available in the CloudFormation stack output `oActivationCommand`.

> **Warning**: Running this command immediately restricts all access to the `curated` table to the seven principals defined in the Lake Formation stack. Any other role that previously read the table via IAM will lose access.

---

## CloudFormation parameters

All templates use `AWS::SSM::Parameter::Value<String>` parameter types, meaning parameter values are resolved from SSM at deploy time. You do not pass bucket names or ARNs directly — they are pulled automatically from the SSM paths listed in [Section 8 — Security](../8-security/).

The one manual parameter common to all templates:

| Parameter | Default | Notes |
|---|---|---|
| `pPipelineReference` | `none` | Set to the CodePipeline execution ID for traceability; visible in each stack's `oPipelineReference` output |

---

## CI/CD pipeline

**Pipeline name**: `sdlf-ott-cicd`

Every push to `main` runs all four stages. The pipeline was verified end-to-end on execution `e83eace2-09d3-4849-9514-8d42b4fb8601`:

```
Stage     Status
Source    Succeeded
Validate  Succeeded
Deploy    Succeeded
Notify    Succeeded
```

Overall status: **Succeeded**

---

## Verifying a successful deployment

After all stacks deploy:

1. **Glue job exists**
   ```bash
   aws glue get-job --job-name sdlf-ott-searchevents-glue-job --region ap-southeast-1
   ```

2. **Catalog tables exist**
   ```bash
   aws glue get-tables --database-name [searchevents-db] --region ap-southeast-1 \
     --query 'TableList[].Name'
   ```
   Expected: `raw_search_events`, `curated`, `dq_results`, `content_gaps`, `premium_vs_free`, `repeat_search_rate`, `hour_of_day_heatmap`, `guest_vs_auth_demand`, `trending_all`, `trending_unknown`

3. **Gold database and table**
   ```bash
   aws glue get-table --database-name fpt_ott_searchevents_gold --name keyword_trends \
     --region ap-southeast-1
   ```

4. **API URL in SSM**
   ```bash
   aws ssm get-parameter --name /sdlf/pipeline/rApiUrl/ott --region ap-southeast-1
   ```

5. **Smoke-test the API**
   ```bash
   curl "$(aws ssm get-parameter --name /sdlf/pipeline/rApiUrl/ott \
     --query Parameter.Value --output text)/content-gaps?limit=5"
   ```
   Returns `[]` until the first pipeline run completes.

---

## Triggering a manual pipeline run

To trigger Stage B manually (bypassing the S3 event):

```bash
aws events put-events --entries '[{
  "Source": "sdlf.ott",
  "DetailType": "Manual trigger",
  "Detail": "{}"
}]' --region ap-southeast-1
```

Or start a Glue job run directly:

```bash
aws glue start-job-run \
  --job-name sdlf-ott-searchevents-glue-job \
  --arguments '{"--PUSH_DOWN_PREDICATE":"dt=20260514"}' \
  --region ap-southeast-1
```
