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
| `genre_classifier_pkg.zip` uploaded | `s3://[artifacts-bucket]/ott/searchevents/genre_classifier_pkg.zip` — required by the Glue job (a baked-in minimal LUT fallback is now compiled into the Glue script, so a missing/corrupt zip degrades classification but no longer crashes the job) |
| Bedrock model enabled in region | `anthropic.claude-haiku-4-5-20251001-v1:0` must be enabled in ap-southeast-1 |
| Lake Formation data lake admin configured | Required for the `IAM_ALLOWED_PRINCIPALS` revocation — already executed on `curated`, `raw_search_events`, and `keyword_trends`; re-run for any new table added later |
| API key seeded in SSM | `/sdlf/ott/api-key/prod` — 32-char random string; the buildspec passes it to the API stack as `pApiKey` |

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

Stacks must be deployed in dependency order. The CodePipeline Deploy stage (`sdlf-cicd/buildspec-deploy.yml`) handles this sequencing automatically — the list below matches that buildspec exactly.

**Prerequisite (deployed once during framework setup, not by this CI/CD)**:
`sdlf-main/foundations-ott-prod.yaml`, `team-ott-prod.yaml`, `dataset-searchevents-prod.yaml` — buckets, KMS, Lake Formation registration, team + dataset Glue DBs.

**OTT CI/CD-managed stacks (11)**:

```
1.  pipeline-ott-glue-job.yaml      — Glue job, IAM role, catalog tables (raw + curated)
2.  pipeline-ott-mainA.yaml         — Stage A state machine + Lambda
3.  pipeline-ott-mainB.yaml         — Stage B state machine (orchestrates Glue)
4.  pipeline-ott-dq-stage.yaml      — Curated DQ state machine (mainDQ instance)
5.  pipeline-ott-lutrefresh.yaml    — LUT Refresh Lambda + DLQ + EventBridge rule
6.  pipeline-ott-contentgap.yaml    — Content Gap Lambda + 5 catalog tables + DLQ
7.  pipeline-ott-trending.yaml      — Trending Lambda + gold catalog table + DLQ
8.  pipeline-ott-dq-stage.yaml      — Gold DQ state machine (mainGoldDQ instance)
9.  pipeline-ott-monitoring.yaml    — CloudWatch dashboard + 14 alarms
10. pipeline-ott-lakeformation.yaml — Column-level RBAC on curated
11. pipeline-ott-api.yaml           — HTTP API (x-api-key, freshness headers, arm64)
```

> Stack 8 (Gold DQ) depends on the gold Glue database created by stack 7.
> Stack 10 (Lake Formation) depends on all role ARNs exported by stacks 2–7.
> Stack 11 (API) is last so its IAM role can be granted against already-created stage-bucket paths.

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
   KEY=$(aws ssm get-parameter --name /sdlf/ott/api-key/prod \
     --query Parameter.Value --output text --region ap-southeast-1)
   curl -H "x-api-key: $KEY" "$(aws ssm get-parameter --name /sdlf/pipeline/rApiUrl/ott \
     --query Parameter.Value --output text)/content-gaps?limit=5"
   ```
   Returns `[]` until the first pipeline run completes. The response includes
   `X-Data-Freshness` and `Last-Modified` headers so callers can detect stale data.

6. **Run the full contract test (16 assertions)**
   ```bash
   $env:OTT_API_KEY = (aws ssm get-parameter --name /sdlf/ott/api-key/prod \
     --query Parameter.Value --output text --region ap-southeast-1)
   python D:/ott-sdlf/scripts/contract_test.py
   ```

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
