# OTT Search Analytics — SDLF Data Lake

Production-grade serverless data lake built on the [AWS Serverless Data Lake Framework (SDLF)](https://github.com/awslabs/aws-serverless-data-lake-framework) that ingests FPT Play OTT search-event Parquet, enriches it (genre classification, Vietnamese timezone, session bucketing, premium-plan flagging), and surfaces five daily analytics + a live HTTP API.

**Dataset**: 14 days of June 2022 search events (~1.3M rows/day, ~1.15M post-dedup).

---

## Where to read first

| You want… | Read |
|---|---|
| Workshop overview + architecture | [`docs/5-workshop/5.1-overview/`](sdlf-main-ott/docs/5-workshop/5.1-overview/) |
| Prerequisites (AWS, Bedrock, SDLF stacks) | [`docs/5-workshop/5.2-prerequisites/`](sdlf-main-ott/docs/5-workshop/5.2-prerequisites/) |
| Deploy (CI/CD + local PowerShell) | [`docs/5-workshop/5.3-deploy/`](sdlf-main-ott/docs/5-workshop/5.3-deploy/) |
| Ingest (Stage A/B + Glue + DQ) | [`docs/5-workshop/5.4-ingest/`](sdlf-main-ott/docs/5-workshop/5.4-ingest/) |
| Analyze (trending, content-gap, LUT-refresh) | [`docs/5-workshop/5.5-analyze/`](sdlf-main-ott/docs/5-workshop/5.5-analyze/) |
| Verify (contract test, visuals, dashboard) | [`docs/5-workshop/5.6-verify/`](sdlf-main-ott/docs/5-workshop/5.6-verify/) |
| Live verification report | [`docs/5-workshop/5.7-verification/`](sdlf-main-ott/docs/5-workshop/5.7-verification/) |
| Cleanup / teardown | [`docs/5-workshop/5.8-cleanup/`](sdlf-main-ott/docs/5-workshop/5.8-cleanup/) |

The `docs/` tree is the canonical reference; everything below is just the lay-of-the-land.

---

## Top-level layout

```
ott-sdlf/
├── README.md                # this file
├── ott-pipeline.ps1         # end-to-end local runner (deploy + ingest + analytics + verify)
├── genre_classifier_pkg.zip # classifier zip staged in the Glue bucket (~865 KB)
│
├── dashboard/               # static HTML dashboard prototype (gold-layer KPI view)
│
├── scripts/                 # operational helpers (idempotent; called by ott-pipeline.ps1)
│   ├── package_and_deploy_lambdas.py  # Lambda source → zip → S3 → lambda:UpdateFunctionCode
│   ├── lf_grants.py                   # preemptive LF ALL grants (avoids deploy 403s)
│   ├── contract_test.py               # 16-assertion post-deploy regression test
│   ├── verify_live.py                 # broad live health check (SMs, DLQs, alarms, row counts)
│   ├── audit_visuals.py               # reproducible Athena queries → ASCII visuals
│   └── regenerate_dashboard.py        # rebuild dashboard/index.html from the gold table
│
├── sdlf-cicd/               # CI/CD: CodePipeline + CodeBuild (pushes to main auto-deploy)
│   ├── template-cicd.yaml
│   ├── buildspec-validate.yml
│   └── buildspec-deploy.yml
│
├── sdlf-main/               # SDLF framework setup (foundations + team + dataset, prod)
│   ├── foundations-ott-prod.yaml
│   ├── team-ott-prod.yaml
│   └── dataset-searchevents-prod.yaml
│
├── sdlf-main-ott/           # the OTT pipeline itself — 11 CFN stacks
│   ├── pipeline-ott-glue-job.yaml      # Glue ETL job + raw/curated catalog tables
│   ├── pipeline-ott-mainA.yaml         # Stage A SM (event routing)
│   ├── pipeline-ott-mainB.yaml         # Stage B SM (orchestrates Glue)
│   ├── pipeline-ott-dq-stage.yaml      # DQ SM (used twice: curated + gold)
│   ├── pipeline-ott-lutrefresh.yaml    # Bedrock-driven classifier refresh
│   ├── pipeline-ott-contentgap.yaml    # 5 daily reports + HTML dashboard
│   ├── pipeline-ott-trending.yaml      # WoW growth + gold CTAS
│   ├── pipeline-ott-api.yaml           # HTTP API (x-api-key, X-Data-Freshness)
│   ├── pipeline-ott-lakeformation.yaml # column-level RBAC
│   ├── pipeline-ott-monitoring.yaml    # 17 alarms + dashboard
│   ├── glue/ott-search-glue-job.py     # the enrichment script (17-column curated output)
│   ├── lambda/{api,trending,content-gap,lut-refresh}/src/lambda_function.py
│   └── docs/                            # Hugo site (FCJ 7-section map; workshop = section 5)
│
└── sdlf-framework/          # upstream SDLF source (reference; deploys via SSM-resolved URLs)
```

---

## Two deploy paths

| Path | Use when | Command |
|---|---|---|
| **CI/CD (canonical)** | Pushing to GitHub `main` | `git push origin main` — `sdlf-ott-cicd` CodePipeline runs `buildspec-validate.yml` then `buildspec-deploy.yml`, deploying all 11 stacks |
| **Local PowerShell** | First-time bootstrap, or rapid iteration on Windows | `.\ott-pipeline.ps1` (deploys 11 stacks, copies a raw partition, waits for Stage A→B→DQ, invokes analytics Lambdas, runs the contract test) |

`buildspec-deploy.yml` is the source of truth for deploy ordering and parameters; `ott-pipeline.ps1` mirrors it and additionally runs an ingest + analytics cycle.

---

## Verifying a deployment

```powershell
# 1. Load the API key from SSM
$env:OTT_API_KEY = (aws ssm get-parameter --name "/sdlf/ott/api-key/prod" `
  --region ap-southeast-1 --query "Parameter.Value" --output text)

# 2. Run the 16-assertion contract test
python scripts/contract_test.py

# 3. (optional) Pull live data-evidence visuals
python scripts/audit_visuals.py
```

Both scripts are idempotent and read-only against the live catalog.

---

## Region & framework

- **Region**: `ap-southeast-1` (Singapore)
- **Framework**: AWS SDLF v2
- **Runtime**: Python 3.12 (Lambda) — API Lambda is `arm64`, analytics Lambdas are `x86_64`
- **ETL**: AWS Glue 4.0 / Spark 3.3 — G.1X × 10 workers, ~25 min on the 14-day set
