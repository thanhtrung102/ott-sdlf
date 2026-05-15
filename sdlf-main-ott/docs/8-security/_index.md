---
title: "8. Security"
date: 2026-05-15
weight: 8
chapter: false
pre: <b>8. </b>
---

# Security

The pipeline enforces data access at three layers: **column-level Lake Formation permissions**, **KMS encryption at rest**, and **least-privilege IAM roles**. Each Lambda has only the permissions required by the specific Athena queries it runs.

---

## Lake Formation column-level permissions

**Source**: `pipeline-ott-lakeformation.yaml`

Seven principal grants are defined on the `curated` table. Column-level exclusions prevent roles from reading sensitive user-identity columns they do not need.

| CFN Resource | Principal | Columns excluded | Permissions |
|---|---|---|---|
| `rLFContentGap` | Content Gap Lambda role | `user_id_hashed`, `search_session_id`, `subscription_count` | SELECT |
| `rLFLutRefresh` | LUT Refresh Lambda role | `user_id_hashed`, `search_session_id`, `has_premium`, `subscription_count` | SELECT |
| `rLFTrending` | Trending Lambda role | None (all columns) | SELECT |
| `rLFDQExec` | DQ execution role | None (all columns) | SELECT |
| `rLFDQGlue` | Glue DQ worker role | None (all columns) | SELECT |
| `rLFCrawler` | Glue Crawler role | N/A (table-level) | ALTER, DESCRIBE, INSERT |
| `rLFGlueJob` | Glue ETL job role | None (all columns) | SELECT, INSERT, ALTER |

### Exclusion rationale

**Content Gap Lambda** (`rLFContentGap`):
- Needs `has_premium` for the `premium_vs_free` query — **not excluded**
- Does not need user identity (`user_id_hashed`), session IDs (`search_session_id`), or raw subscription count (`subscription_count`)

**LUT Refresh Lambda** (`rLFLutRefresh`):
- Only reads `keyword_norm` and `derived_genre`
- All four user-fingerprinting columns excluded: `user_id_hashed`, `search_session_id`, `has_premium`, `subscription_count`

**Trending Lambda** (`rLFTrending`):
- Requires `user_id_hashed` for `COUNT(DISTINCT user_id_hashed)` in the gold CTAS (`unique_users` column)
- No columns excluded

### Activation requirement

Lake Formation column-level restrictions are **prepared but not enforced** until `IAM_ALLOWED_PRINCIPALS` is revoked by a Lake Formation administrator:

```bash
aws lakeformation revoke-permissions \
  --principal '{"DataLakePrincipalIdentifier":"IAM_ALLOWED_PRINCIPALS"}' \
  --resource '{"Table":{"CatalogId":"ACCOUNT","DatabaseName":"DB","Name":"curated"}}' \
  --permissions SELECT \
  --region ap-southeast-1
```

The exact command is exported as `oActivationCommand` in the CloudFormation stack outputs.

> **Important**: Until this command is run, IAM permissions continue to govern access and the column exclusions have no effect.

---

## KMS encryption

| Resource | Encryption | Key source |
|---|---|---|
| S3 buckets (all four) | SSE-KMS | `/sdlf/storage/rKMSKey/prod` |
| Athena workgroup `sdlf-ott` | SSE-KMS | Separate Athena workgroup key |
| SQS DLQs | SSE-KMS | Shared OTT KMS key |

All KMS key ARNs are resolved from SSM at deploy time. No key IDs are hardcoded in any CloudFormation template or Lambda code.

---

## Privacy design

The raw `user_id` field is never written to any storage layer:

1. The Glue job computes `user_id_hashed = sha2(user_id, 256)` and drops the original column
2. `search_session_id` is a SHA-256 hash of a 30-minute time bucket, not a persistent token
3. Even `user_id_hashed` is excluded from Content Gap and LUT Refresh queries via Lake Formation

This means no personally identifiable user identifier leaves the Glue job unmasked.

---

## IAM roles summary

| Role | Resource | Used by |
|---|---|---|
| `sdlf-ott-searchevents-glue-role` | Glue ETL job | Stage B Glue job |
| `rDQExecutionRole` | Step Functions | DQ state machine execution |
| `rGlueDQRole` | Glue DQ | Data quality profile runs |
| `sdlf-ott-mainCG-role` | Lambda | Content Gap Lambda |
| `sdlf-ott-mainLUT-role` | Lambda | LUT Refresh Lambda |
| `sdlf-ott-mainTR-role` | Lambda | Trending Lambda |
| `sdlf-ott-api-role` | Lambda | HTTP API Lambda |

All roles follow least-privilege: each is scoped to the specific S3 prefixes, Athena workgroup, Glue databases, and KMS keys it requires.

---

## SSM parameter access

All cross-stack references use SSM Parameter Store. No stack directly `!ImportValue`s outputs from another stack. This decouples deployment order and allows individual stacks to be redeployed without affecting others.

Key SSM paths used:

| SSM path | Contains |
|---|---|
| `/sdlf/storage/rRawBucket/prod` | Raw S3 bucket name |
| `/sdlf/storage/rAnalyticsBucket/prod` | Analytics S3 bucket name |
| `/sdlf/storage/rStageBucket/prod` | Stage S3 bucket name |
| `/sdlf/storage/rArtifactsBucket/prod` | Artifacts S3 bucket name |
| `/sdlf/storage/rKMSKey/prod` | KMS key ARN |
| `/sdlf/dataset/rAnalyticsGlueDataCatalog/searchevents` | Glue database name |
| `/sdlf/pipeline/rRole/ott-mainCG` | Content Gap role ARN |
| `/sdlf/pipeline/rRole/ott-mainLUT` | LUT Refresh role ARN |
| `/sdlf/pipeline/rRole/ott-mainTR` | Trending role ARN |
| `/sdlf/pipeline/rRole/ott-mainDQExec` | DQ execution role ARN |
| `/sdlf/pipeline/rRole/ott-mainDQGlue` | DQ Glue role ARN |
| `/SDLF/SNS/ott/Notifications` | SNS topic ARN for alerts |
