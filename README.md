# OTT Search Analytics — SDLF Data Lake

Production-grade serverless data lake built on the [AWS Serverless Data Lake Framework (SDLF)](https://github.com/awslabs/aws-serverless-data-lake-framework) that ingests FPT Play OTT search-event Parquet, enriches it (genre classification, Vietnamese timezone, session bucketing, premium-plan flagging), and presents every insight on a **single user-facing CloudFront dashboard** stamped with per-section dt-window source attribution.

**Dataset**: 14 days of June 2022 search events (~1.3M rows/day, ~1.15M post-dedup).

---

## Where to read first

| You want… | Read |
|---|---|
| Workshop overview + architecture | [`docs/5-workshop/5.1-overview/`](sdlf-main-ott/docs/5-workshop/5.1-overview/) |
| Prerequisites (AWS, Bedrock, SDLF stacks) | [`docs/5-workshop/5.2-prerequisites/`](sdlf-main-ott/docs/5-workshop/5.2-prerequisites/) |
| Deploy (CI/CD + local PowerShell) | [`docs/5-workshop/5.3-deploy/`](sdlf-main-ott/docs/5-workshop/5.3-deploy/) |
| Ingest (Stage A/B + Glue + DQ) | [`docs/5-workshop/5.4-ingest/`](sdlf-main-ott/docs/5-workshop/5.4-ingest/) |
| Analyze (dashboard renderer + LUT-refresh) | [`docs/5-workshop/5.5-analyze/`](sdlf-main-ott/docs/5-workshop/5.5-analyze/) |
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
├── dashboard/               # local mirror of the live CloudFront dashboard (regenerate_dashboard.py)
│
├── scripts/                 # operational helpers (idempotent; called by ott-pipeline.ps1)
│   ├── package_and_deploy_lambdas.py  # Lambda source → zip → S3 → lambda:UpdateFunctionCode
│   ├── lf_grants.py                   # preemptive LF ALL grants (avoids deploy 403s)
│   ├── contract_test.py               # post-deploy dashboard contract test
│   ├── verify_live.py                 # broad live health check (SMs, DLQs, alarms, dashboard)
│   ├── audit_visuals.py               # reproducible Athena queries → ASCII visuals
│   └── regenerate_dashboard.py        # pull dashboard/index.html down from the live CloudFront URL
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
├── sdlf-main-ott/           # the OTT pipeline itself — 9 CFN stacks
│   ├── pipeline-ott-glue-job.yaml      # Glue ETL job + raw/curated catalog tables
│   ├── pipeline-ott-mainA.yaml         # Stage A SM (event routing)
│   ├── pipeline-ott-mainB.yaml         # Stage B SM (orchestrates Glue)
│   ├── pipeline-ott-dq-stage.yaml      # Curated-layer DQ SM
│   ├── pipeline-ott-lutrefresh.yaml    # Bedrock-driven classifier refresh
│   ├── pipeline-ott-trending.yaml      # Dashboard renderer (Trending Lambda) + DLQ
│   ├── pipeline-ott-lakeformation.yaml # column-level RBAC
│   ├── pipeline-ott-monitoring.yaml    # 11 alarms + CloudWatch dashboard
│   ├── pipeline-ott-dashboard.yaml     # S3 + CloudFront hosting (OAC) for index.html
│   ├── glue/ott-search-glue-job.py     # the enrichment script (17-column curated output)
│   ├── lambda/{trending,lut-refresh}/src/lambda_function.py
│   └── docs/                            # Hugo site (FCJ 7-section map; workshop = section 5)
│
└── sdlf-framework/          # upstream SDLF source (reference; deploys via SSM-resolved URLs)
```

The analytics HTTP API and the standalone content-gap Lambda were retired — the dashboard now carries the full-depth content-gap and trending tables in HTML (500 rows each, in-page filtering), so there is exactly one user-facing surface.

---

## Two deploy paths

| Path | Use when | Command |
|---|---|---|
| **CI/CD (canonical)** | Pushing to GitHub `main` | `git push origin main` — `sdlf-ott-cicd` CodePipeline runs `buildspec-validate.yml` then `buildspec-deploy.yml`, deploying all 9 stacks |
| **Local PowerShell** | First-time bootstrap, or rapid iteration on Windows | `.\ott-pipeline.ps1` (deploys 9 stacks, copies a raw partition, waits for Stage A→B→DQ, invokes analytics Lambdas, runs the contract test) |

`buildspec-deploy.yml` is the source of truth for deploy ordering and parameters; `ott-pipeline.ps1` mirrors it and additionally runs an ingest + analytics cycle.

---

## Verifying a deployment

```powershell
# 1. Run the dashboard contract test (no env vars required)
python scripts/contract_test.py

# 2. (optional) Pull live data-evidence visuals
python scripts/audit_visuals.py
```

Both scripts are idempotent and read-only against the live catalog.

---

## Region & framework

- **Region**: `ap-southeast-1` (Singapore)
- **Framework**: AWS SDLF v2
- **Runtime**: Python 3.12 (Lambda) — analytics Lambdas are `x86_64`
- **ETL**: AWS Glue 4.0 / Spark 3.3 — G.1X × 10 workers, ~25 min on the 14-day set
- **Serving**: S3 + CloudFront with Origin Access Control — the single user-facing surface
