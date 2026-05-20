# OTT Search Analytics — SDLF Data Lake

Production-grade serverless data lake built on the [AWS Serverless Data Lake Framework (SDLF)](https://github.com/awslabs/aws-serverless-data-lake-framework) that ingests FPT Play OTT search-event Parquet, enriches it (genre classification, Vietnamese timezone, session bucketing, premium-plan flagging), and surfaces five daily analytics + a live HTTP API.

**Dataset**: 14 days of June 2022 search events (~1.3M rows/day, ~1.15M post-dedup).

---

## Where to read first

| You want… | Read |
|---|---|
| Business context + scope | [`sdlf-main-ott/docs/1-introduction/`](sdlf-main-ott/docs/1-introduction/) |
| End-to-end architecture diagram | [`sdlf-main-ott/docs/2-architecture/`](sdlf-main-ott/docs/2-architecture/) |
| ETL design (Stage A/B + Glue) | [`sdlf-main-ott/docs/3-ingestion/`](sdlf-main-ott/docs/3-ingestion/) |
| Data quality | [`sdlf-main-ott/docs/4-quality/`](sdlf-main-ott/docs/4-quality/) |
| Analytics Lambdas | [`sdlf-main-ott/docs/5-analytics/`](sdlf-main-ott/docs/5-analytics/) |
| Gold layer (`keyword_trends`) | [`sdlf-main-ott/docs/6-gold-layer/`](sdlf-main-ott/docs/6-gold-layer/) |
| HTTP API (auth, headers, endpoints) | [`sdlf-main-ott/docs/7-api/`](sdlf-main-ott/docs/7-api/) |
| Security (LF column-level, KMS, IAM) | [`sdlf-main-ott/docs/8-security/`](sdlf-main-ott/docs/8-security/) |
| Monitoring (14 alarms, DLQs, X-Ray) | [`sdlf-main-ott/docs/9-monitoring/`](sdlf-main-ott/docs/9-monitoring/) |
| Deployment (prereqs, CI/CD, smoke test) | [`sdlf-main-ott/docs/10-deployment/`](sdlf-main-ott/docs/10-deployment/) |

The `docs/` tree is the canonical reference; everything below is just the lay-of-the-land.

---

## Top-level layout

```
ott-sdlf/
├── README.md                # this file
├── ott-pipeline.ps1         # end-to-end local runner (deploy + ingest + analytics + verify)
├── deploy.sh                # workshop-style sample-data deploy (Bash entry)
├── run_workshop.sh          # full 14-day reproducible workshop script
├── genre_classifier_pkg.zip # classifier zip uploaded to artifacts bucket (~865 KB)
│
├── data/                    # sample search events + genre taxonomy (for local smoke)
├── dashboard/               # static HTML dashboard prototype (gold-layer KPI view)
│
├── scripts/                 # operational helpers (idempotent; called by ott-pipeline.ps1)
│   ├── package_and_deploy_lambdas.py  # Lambda source → zip → S3 → lambda:UpdateFunctionCode
│   ├── lf_grants.py                   # preemptive LF ALL grants (avoids deploy 403s)
│   ├── contract_test.py               # 16-assertion post-deploy regression test
│   └── audit_visuals.py               # reproducible Athena queries → ASCII visuals
│
├── sdlf-cicd/               # CI/CD: CodePipeline + CodeBuild (pushes to main auto-deploy)
│   ├── template-cicd.yaml
│   ├── buildspec-validate.yml
│   └── buildspec-deploy.yml
│
├── sdlf-main/               # SDLF framework setup (foundations + team + dataset)
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
│   ├── pipeline-ott-monitoring.yaml    # 14 alarms + dashboard
│   ├── glue/ott-search-glue-job.py     # the enrichment script (19 columns out)
│   ├── lambda/{api,trending,content-gap,lut-refresh}/src/lambda_function.py
│   └── docs/                            # Hugo site (markdown, served at sdlf.workshop.aws style)
│
└── sdlf-framework/          # upstream SDLF source (reference; deploys via SSM-resolved URLs)
```

---

## Three deploy paths

| Path | Use when | Command |
|---|---|---|
| **CI/CD (primary)** | Pushing to GitHub `main` | `git push origin main` — `sdlf-ott-cicd` CodePipeline auto-validates and deploys |
| **Local PowerShell** | Iterating on Windows; want full pipeline + verify | `.\ott-pipeline.ps1` (deploys 11 stacks, copies a raw partition, waits for Stage A→B→DQ, invokes analytics Lambdas, runs contract test) |
| **Workshop Bash** | Reproducing the full 14-day dataset run | `bash run_workshop.sh --source-bucket <name>` |

A sample-data-only smoke (no full dataset, no analytics) is `bash deploy.sh -t ott -d searchevents -r ap-southeast-1`.

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
