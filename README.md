# OTT Search Analytics — SDLF Data Lake

This directory adapts the FPT Play OTT search pipeline dataset for the
[AWS Serverless Data Lake Framework (SDLF)](https://github.com/aws-solutions-library-samples/data-lakes-on-aws)
workshop, replacing the legislators demo dataset with the real OTT `log_search` events.

**Source project:** `D:\ott-search-pipeline` (AWS CDK v2 pipeline)
**Workshop reference:** https://sdlf.workshop.aws/

---

## Directory layout

```
ott-sdlf/
├── README.md
├── deploy.sh                        # Bootstrap script (upload data + deploy Glue stack)
│
├── data/                            # OTT sample data (uploaded to SDLF raw bucket)
│   ├── search_events_sample.json    # 18 representative search events (June 2022)
│   └── genre_taxonomy.json          # Genre + platform + network lookup tables
│
├── scripts/
│   ├── ott-search-glue-job.py       # Glue 4.0 enrichment job (18-step pipeline)
│   └── ott-search-glue-job.yaml     # CloudFormation template for the Glue job
│
├── sdlf-main/                       # SDLF infrastructure CloudFormation stacks
│   ├── team-ott-dev.yaml            # OTT team definition
│   └── dataset-searchevents.yaml    # search_events dataset registration
│
└── sdlf-main-ott/                   # OTT team pipeline deployment configs
    ├── datasets.yaml                # Dataset registration (MODULE resource)
    ├── pipeline-main.yaml           # Stage A (event) + Stage B (schedule) pipeline
    └── tags.json                    # Resource tags
```

---

## Dataset

**Source:** FPT Play `log_search` Parquet files — June 2022 (14 days, ~1.15M events)
**Full dataset location:** `s3://ott-search-703668403514-demo/raw-source/log_search/`
**Sample data:** `data/search_events_sample.json` (18 events, all data-quality edge cases represented)

### Schema — raw layer (12 columns)

| Column | Type | Notes |
|---|---|---|
| `eventid` | string | Unique event identifier |
| `datetime` | string | May contain Arabic-Indic numerals or Buddhist Era year (2565→2022) |
| `user_id` | string | Null for ~25% unauthenticated users |
| `keyword` | string | Free-text search query |
| `category` | string | `enter` (result selected) or `quit` (abandoned) |
| `proxy_isp` | string | ISP name from proxy header |
| `platform` | string | 35 distinct raw device strings |
| `networktype` | string | 9 raw values (wifi, 4g, cable, etc.) |
| `action` | string | Event action |
| `userplansmap` | array | Subscription plan list (VIP, HBO GO+, K+, MAX) |

### Schema — curated layer (18 columns, after enrichment)

| Column | Step | Description |
|---|---|---|
| `event_id` | 1 | Renamed eventid |
| `event_ts` | 2 | UTC timestamp after datetime repair |
| `hour_of_day_vn` | 4 | Vietnam timezone hour (UTC+7, 0–23) |
| `user_id_hashed` | 8 | SHA-256(user_id), null-safe |
| `user_is_authenticated` | 7 | user_id IS NOT NULL |
| `session_action` | 5 | `enter` or `quit` |
| `is_search_abandoned` | 6 | category == 'quit' |
| `keyword_norm` | 9 | lower(trim(keyword)) |
| `derived_genre` | 10 | Rule-based: THE_THAO / ANIME / NHAC / TRUYEN_HINH / PHIM_AU_MY / PHIM_TRUNG / PHIM_VIET / UNKNOWN |
| `platform_group` | 11 | OTTBox / SmartTV / Android / iOS / Web / Other |
| `network_type_norm` | 12 | wifi / 4g / cable / 3g / unknown |
| `isp_segment` | 13 | upper(proxy_isp) |
| `has_premium` | 14 | Any of VIP / HBO GO+ / K+ / MAX in userplansmap |
| `subscription_count` | 15 | len(userplansmap) |
| `search_session_id` | 16 | SHA-256(user_id\|30-min bucket) |
| `is_repeat_search` | 17 | Same keyword_norm as previous in same session (LAG window) |
| `is_cross_partition_date` | 18 | Corruption flag: event year != dt partition year |

---

## SDLF mapping

| SDLF concept | OTT value |
|---|---|
| Team name | `ott` |
| Dataset name | `searchevents` |
| Raw S3 prefix | `ott/searchevents/` |
| Stage A trigger | S3 Object Created on raw prefix |
| Stage B trigger | Step Functions Stage A SUCCEEDED + `cron(30 18 * * ? *)` (01:30 UTC+7) |
| Glue job | `sdlf-ott-searchevents-glue-job` |

The pipeline follows the SDLF two-stage pattern:
- **Stage A** — light crawl/catalogue (SDLF stageA Lambda state machine, event-driven)
- **Stage B** — heavy Glue ETL enrichment (18-step pipeline), triggered after Stage A succeeds

---

## Prerequisites

1. SDLF framework deployed in your AWS account/region (see https://sdlf.workshop.aws/)
2. AWS CLI configured with credentials for the target account
3. The OTT team and dataset registered:
   ```bash
   # Deploy team
   aws cloudformation deploy \
     --template-file sdlf-main/team-ott-dev.yaml \
     --stack-name sdlf-ott-team-dev \
     --capabilities CAPABILITY_AUTO_EXPAND \
     --region ap-southeast-1

   # Deploy dataset
   aws cloudformation deploy \
     --template-file sdlf-main/dataset-searchevents.yaml \
     --stack-name sdlf-ott-dataset-searchevents-dev \
     --capabilities CAPABILITY_AUTO_EXPAND \
     --region ap-southeast-1
   ```

4. Pipeline deployed:
   ```bash
   aws cloudformation deploy \
     --template-file sdlf-main-ott/pipeline-main.yaml \
     --stack-name sdlf-ott-pipeline-main-dev \
     --capabilities CAPABILITY_AUTO_EXPAND \
     --region ap-southeast-1
   ```

---

## Reproducible full-dataset run

`run_workshop.sh` is the single entry point. It copies the full 14-day Parquet
dataset from the source S3 bucket, deploys infrastructure, runs the Glue ETL,
and builds the gold keyword_trends table — with a validation checkpoint after
each step.

```bash
bash run_workshop.sh \
  --source-bucket ott-search-703668403514-demo \
  --region ap-southeast-1
```

All options:

| Flag | Default | Description |
|---|---|---|
| `--source-bucket` | `ott-search-703668403514-demo` | S3 bucket holding `raw-source/log_search/YYYYMMDD/` Parquet |
| `--source-prefix` | `raw-source/log_search` | Key prefix inside that bucket |
| `--team` | `ott` | SDLF team name |
| `--dataset` | `searchevents` | SDLF dataset name |
| `--region` | `ap-southeast-1` | AWS region |
| `--profile` | _(none)_ | AWS CLI named profile |
| `--days N` | `all` | Process only the last N days instead of all 14 |
| `--full-backfill` | off | Pass `PUSH_DOWN_PREDICATE=all` to Glue (process every partition) |
| `--skip-upload` | off | Skip S3 copy (data already in SDLF raw bucket) |
| `--skip-deploy` | off | Skip CFN stack deployment (already deployed) |
| `--skip-glue` | off | Skip Glue ETL run (curated data already present) |

### What the script does (9 steps)

```
Step 0  Prerequisites check (aws CLI v2, jq, credentials)
Step 1  Resolve SDLF SSM parameters (raw/stage/artifacts buckets, KMS key)
Step 2  Upload Glue script + genre_classifier_flat.zip to artifacts bucket
Step 3  Package + deploy sdlf-ott-searchevents-glue-job CFN stack
Step 4  aws s3 sync all 14 YYYYMMDD Parquet folders → SDLF raw bucket
Step 5  Wait for SDLF Stage A (EventBridge → Step Functions) to SUCCEED
Step 6  Start Glue ETL job; wait for completion (~200 s on G.1X × 10)
Step 7  Validate curated layer — file count, partitions, sizes
Step 8  Run Athena CTAS → keyword_trends gold table
Step 9  Print summary and sample Athena validation queries
```

### Genre classifier dependency

The Glue job needs `genre_classifier_flat.zip` (the production zip from the CDK
pipeline). The script copies it from `D:\ott-search-pipeline\genre_classifier_flat.zip`
automatically. If it is missing the job runs with a degraded classifier (all genres
= UNKNOWN). To rebuild:

```bash
cd D:\ott-search-pipeline
python -c "
import zipfile, pathlib
z = zipfile.ZipFile('genre_classifier_flat.zip', 'w', zipfile.ZIP_DEFLATED)
for p in pathlib.Path('genre_classifier').rglob('*'):
    if p.is_file() and '__pycache__' not in str(p):
        z.write(p, p.name)
z.close()
print('Built genre_classifier_flat.zip')
"
```

### Sample data only (no AWS account)

To run locally against the 18-row sample:

```bash
bash deploy.sh -t ott -d searchevents -r ap-southeast-1
```

`deploy.sh` uploads only `data/*.json` — useful for smoke-testing SDLF
infrastructure without the full 1.15M-record dataset.

---

## Data quality edge cases (represented in sample data)

| Row | Issue | Expected handling |
|---|---|---|
| e016 | Arabic-Indic numerals in datetime | `clean_datetime_udf` translates to ASCII digits |
| e017 | Buddhist Era year (2565) | Replaced with 2022 |
| e018 | Corrupt year 0004 | Dropped (year < 2015 filter) |
| e002, e004, e008 | Null user_id (unauthenticated) | user_id_hashed = NULL; anon session_id derived from platform |

---

## Lake Formation access (matching source pipeline)

| Role | Databases | Columns excluded |
|---|---|---|
| `ott-data-engineering` | raw, curated | — |
| `ott-analyst` | curated | — |
| `ott-marketing` | curated (keyword_trends view only) | `user_id_hashed`, `unique_users`, `authenticated_rate` |
