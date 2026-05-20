---
title: "8. Security"
date: 2026-05-20
weight: 8
chapter: false
pre: <b>8. </b>
---

# 8. Security

> **Reference content — placeholder.** Lake Formation grants, KMS key wiring, IAM scoping, and SSM parameter inventory live here. For the hands-on path, see [Workshop chapter 3 — Deploy](../workshop/3-deploy/) §3.5 Activate Lake Formation column-level RBAC.

## Layers

1. **Lake Formation column-level RBAC** — `IAM_ALLOWED_PRINCIPALS` revoked on `curated`, `raw_search_events`, `dq_results`, `keyword_trends`; explicit grants to the 7 service principals.
2. **KMS** — one customer-managed key (`/sdlf/storage/rKMSKey/prod`) encrypts every S3 bucket, every SQS DLQ, every CloudWatch log group.
3. **IAM** — every Lambda execution role + the CodeBuild role lives under path `/sdlf-ott/` for scoped policy management.
4. **API auth** — `x-api-key` header validated in-Lambda; key in SSM (`/sdlf/ott/api-key/prod`).

## SSM parameter inventory

All bucket names, ARNs, and the API key are resolved at deploy time from SSM (`AWS::SSM::Parameter::Value<String>`). No secret in any CloudFormation template.
