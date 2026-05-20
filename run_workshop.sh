#!/usr/bin/env bash
# run_workshop.sh — Reproducible, end-to-end SDLF workshop run for the OTT
# Search Analytics dataset (FPT Play, June 2022, ~1.15M events).
#
# Prerequisites:
#   • AWS CLI ≥ 2.x configured with credentials for account 703668403514 / ap-southeast-1
#   • OttStorage-prod CDK stack deployed (provides ott-search-<account>-prod bucket + KMS key)
#   • Source Parquet files accessible in S3 (--source-bucket flag)
#
# Usage:
#   bash run_workshop.sh [options]
#
# Required:
#   --source-bucket BUCKET    S3 bucket containing raw-source/log_search/ Parquet files
#                             (default: ott-search-703668403514-demo)
# Optional:
#   --source-prefix PREFIX    S3 key prefix for Parquet source (default: raw-source/log_search)
#   --team    NAME            SDLF team name        (default: ott)
#   --dataset NAME            SDLF dataset name     (default: searchevents)
#   --region  REGION          AWS region            (default: ap-southeast-1)
#   --profile PROFILE         AWS CLI profile
#   --days    N               Number of daily folders to process, newest first (default: all=14)
#   --skip-upload             Skip data upload (data already in SDLF raw bucket)
#   --skip-deploy             Skip CFN stack deployment (stack already exists)
#   --skip-glue               Skip Glue ETL run (curated data already present)
#   --full-backfill           Pass PUSH_DOWN_PREDICATE=all so Glue processes every partition
#   -h, --help                Show this help

set -euo pipefail

# ── Defaults ──────────────────────────────────────────────────────────────────
SOURCE_BUCKET="ott-search-703668403514-prod"
SOURCE_PREFIX="raw-source/log_search"
TEAM="ott"
DATASET="searchevents"
REGION="ap-southeast-1"
PROFILE=""
DAYS="all"
SKIP_UPLOAD=false
SKIP_DEPLOY=false
SKIP_GLUE=false
FULL_BACKFILL=false

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# ── Argument parsing ──────────────────────────────────────────────────────────
while [[ $# -gt 0 ]]; do
  case "$1" in
    --source-bucket) SOURCE_BUCKET="$2"; shift 2 ;;
    --source-prefix) SOURCE_PREFIX="$2"; shift 2 ;;
    --team)          TEAM="$2"; shift 2 ;;
    --dataset)       DATASET="$2"; shift 2 ;;
    --region)        REGION="$2"; shift 2 ;;
    --profile)       PROFILE="$2"; shift 2 ;;
    --days)          DAYS="$2"; shift 2 ;;
    --skip-upload)   SKIP_UPLOAD=true; shift ;;
    --skip-deploy)   SKIP_DEPLOY=true; shift ;;
    --skip-glue)     SKIP_GLUE=true; shift ;;
    --full-backfill) FULL_BACKFILL=true; shift ;;
    -h|--help)
      head -30 "$0" | grep "^#" | sed 's/^# \?//'
      exit 0 ;;
    *) echo "Unknown option: $1" >&2; exit 1 ;;
  esac
done

PA=${PROFILE:+--profile "$PROFILE"}

# ── Helpers ───────────────────────────────────────────────────────────────────

log()  { echo "[$(date '+%H:%M:%S')] $*"; }
die()  { echo "[ERROR] $*" >&2; exit 1; }
hr()   { echo "────────────────────────────────────────────────────────────────"; }

# Poll a function until it returns success or timeout (seconds).
wait_for() {
  local label="$1" check_fn="$2" timeout="${3:-600}" interval="${4:-20}"
  local elapsed=0
  log "Waiting for: $label (timeout ${timeout}s)"
  while ! $check_fn 2>/dev/null; do
    sleep "$interval"
    elapsed=$((elapsed + interval))
    [[ $elapsed -ge $timeout ]] && die "Timed out waiting for: $label"
    log "  … still waiting ($elapsed/${timeout}s)"
  done
  log "  ✓ $label"
}

# ── Step 0: Prerequisites check ───────────────────────────────────────────────
hr
log "STEP 0 — Prerequisites"
hr

command -v aws  >/dev/null || die "aws CLI not found"

AWS_VER=$(aws --version 2>&1 | sed 's/aws-cli\/\([0-9]*\).*/\1/')
[[ "$AWS_VER" -ge 2 ]] || die "AWS CLI v2 required (found v$AWS_VER)"

log "Caller identity:"
aws sts get-caller-identity $PA --region "$REGION" --output table

ACCOUNT=$(aws sts get-caller-identity $PA --query "Account" --output text)
log "Account: $ACCOUNT  Region: $REGION"
log "Team: $TEAM  Dataset: $DATASET"
log "Source: s3://$SOURCE_BUCKET/$SOURCE_PREFIX"

# ── Step 1: Resolve bucket/key references (OttStorage-prod CDK stack) ─────────
hr
log "STEP 1 — Resolve bucket and KMS references from OttStorage-prod"
hr

# All data lives in the single OttStorage-prod bucket.
# raw/ and curated/ are separate prefixes to avoid Glue schema-scan conflicts.
ARTIFACTS_BUCKET="ott-search-${ACCOUNT}-prod"
RAW_BUCKET="ott-search-${ACCOUNT}-prod"
STAGE_BUCKET="ott-search-${ACCOUNT}-prod"
KMS_KEY_ARN="arn:aws:kms:${REGION}:${ACCOUNT}:key/5ce12d85-6891-4b70-b225-0471ef278219"
KMS_KEY="5ce12d85-6891-4b70-b225-0471ef278219"

log "Data bucket:  s3://$RAW_BUCKET"
log "KMS key:      $KMS_KEY_ARN"

DEST_PREFIX="$TEAM/$DATASET"
# Raw ingest lands under raw/ so Glue SOURCE_LOCATION doesn't overlap curated output.
RAW_INGEST_PREFIX="raw/$DEST_PREFIX"
CURATED_PATH="s3://$STAGE_BUCKET/$DEST_PREFIX/curated/search_enriched"
GOLD_PATH="s3://$STAGE_BUCKET/$DEST_PREFIX/gold/keyword_trends"
ATHENA_RESULTS="s3://$STAGE_BUCKET/athena-results/"

# ── Step 2: Upload Glue artifacts ─────────────────────────────────────────────
hr
log "STEP 2 — Upload Glue artifacts to s3://$ARTIFACTS_BUCKET/artifacts/"
hr

log "Uploading Glue job script…"
aws s3 cp "$SCRIPT_DIR/scripts/ott-search-glue-job.py" \
  "s3://$ARTIFACTS_BUCKET/artifacts/" $PA

# genre_classifier_flat.zip lives in the CDK project root; copy it here too.
CLASSIFIER_ZIP="$SCRIPT_DIR/../ott-search-pipeline/genre_classifier_flat.zip"
if [[ -f "$CLASSIFIER_ZIP" ]]; then
  log "Uploading genre_classifier_flat.zip…"
  aws s3 cp "$CLASSIFIER_ZIP" "s3://$ARTIFACTS_BUCKET/artifacts/" $PA
else
  log "WARNING: genre_classifier_flat.zip not found at $CLASSIFIER_ZIP"
  log "         The Glue job will fall back to UNKNOWN for all genres."
  log "         To build it: cd D:\\ott-search-pipeline && python -m zipapp genre_classifier -o genre_classifier_flat.zip"
fi

# ── Step 3: Deploy Glue CloudFormation stack ───────────────────────────────────
hr
log "STEP 3 — Deploy Glue job CFN stack"
hr

STACK_NAME="sdlf-${TEAM}-${DATASET}-glue-job"

if $SKIP_DEPLOY; then
  log "  --skip-deploy set; assuming stack $STACK_NAME exists"
else
  mkdir -p "$SCRIPT_DIR/output"

  log "Packaging template…"
  aws cloudformation package \
    --template-file "$SCRIPT_DIR/scripts/ott-search-glue-job.yaml" \
    --s3-bucket "$ARTIFACTS_BUCKET" \
    --output-template-file "$SCRIPT_DIR/output/packaged-template.yaml" \
    $PA

  log "Deploying stack $STACK_NAME…"
  aws cloudformation deploy \
    --s3-bucket "$ARTIFACTS_BUCKET" --s3-prefix sdlf-utils \
    --stack-name "$STACK_NAME" \
    --template-file "$SCRIPT_DIR/output/packaged-template.yaml" \
    --parameter-overrides pTeamName="$TEAM" pDatasetName="$DATASET" \
      pArtifactsBucket="$ARTIFACTS_BUCKET" pKmsKeyArn="$KMS_KEY_ARN" \
    --tags Framework=standalone Team="$TEAM" Dataset="$DATASET" Project=fpt-play-search-analytics \
    --capabilities CAPABILITY_NAMED_IAM CAPABILITY_AUTO_EXPAND \
    --region "$REGION" \
    $PA
  log "  ✓ Stack deployed"
fi

GLUE_JOB_NAME="sdlf-${TEAM}-${DATASET}-glue-job"

# ── Step 4: Upload source Parquet to SDLF raw bucket ──────────────────────────
hr
log "STEP 4 — Upload Parquet data to s3://$RAW_BUCKET/$DEST_PREFIX/"
hr

if $SKIP_UPLOAD; then
  # Point Glue directly at the source location (no copy needed).
  RAW_INGEST_PREFIX="$SOURCE_PREFIX"
  log "  --skip-upload set; Glue SOURCE_LOCATION will use s3://$SOURCE_BUCKET/$RAW_INGEST_PREFIX"
else
  log "Discovering source folders in s3://$SOURCE_BUCKET/$SOURCE_PREFIX/…"

  # List YYYYMMDD subdirectories (raw-source layout).
  FOLDERS=$(aws s3 ls "s3://$SOURCE_BUCKET/$SOURCE_PREFIX/" $PA \
    | awk '{print $2}' | grep -E '^[0-9]{8}/$' | sed 's|/$||' | sort)

  FOLDER_COUNT=$(echo "$FOLDERS" | wc -l | tr -d ' ')
  log "Found $FOLDER_COUNT daily folders"

  if [[ "$DAYS" != "all" ]]; then
    FOLDERS=$(echo "$FOLDERS" | tail -"$DAYS")
    log "Processing last $DAYS days"
  fi

  TOTAL_FILES=0
  for FOLDER in $FOLDERS; do
    log "  Syncing $FOLDER → s3://$RAW_BUCKET/$RAW_INGEST_PREFIX/$FOLDER/"
    aws s3 sync \
      "s3://$SOURCE_BUCKET/$SOURCE_PREFIX/$FOLDER/" \
      "s3://$RAW_BUCKET/$RAW_INGEST_PREFIX/$FOLDER/" \
      --sse aws:kms --sse-kms-key-id "$KMS_KEY" \
      --exclude "*" --include "*.parquet" --include "*.snappy.parquet" \
      $PA
    FILE_COUNT=$(aws s3 ls "s3://$RAW_BUCKET/$RAW_INGEST_PREFIX/$FOLDER/" $PA \
      | grep -c '\.parquet' || true)
    log "    → $FILE_COUNT Parquet file(s)"
    TOTAL_FILES=$((TOTAL_FILES + FILE_COUNT))
  done

  log "Total Parquet files uploaded: $TOTAL_FILES"
  [[ $TOTAL_FILES -gt 0 ]] || die "No Parquet files found in source. Check --source-bucket and --source-prefix."
fi

# ── Step 5: Verify raw data is present in the ingest prefix ───────────────────
# (No SDLF Stage A / Step Functions — data goes straight from S3 to Glue.)
hr
log "STEP 5 — Verify raw Parquet files present in ingest prefix"
hr

RAW_SRC_BUCKET=$( $SKIP_UPLOAD && echo "$SOURCE_BUCKET" || echo "$RAW_BUCKET" )
RAW_CHECK=$(aws s3 ls "s3://$RAW_SRC_BUCKET/$RAW_INGEST_PREFIX/" --recursive $PA \
  | grep -c '\.parquet' || true)
log "Raw Parquet files found: $RAW_CHECK (at s3://$RAW_SRC_BUCKET/$RAW_INGEST_PREFIX/)"
[[ $RAW_CHECK -gt 0 ]] || die "No raw Parquet files found. Check --source-bucket and --source-prefix."
log "  ✓ Raw data verified"

# ── Step 6: Run Glue ETL (Stage B) ────────────────────────────────────────────
hr
log "STEP 6 — Trigger Glue ETL enrichment (Stage B)"
hr

if $SKIP_GLUE; then
  log "  --skip-glue set; skipping Glue run"
else
  # When --skip-upload: read directly from the source location.
  GLUE_SOURCE_BUCKET=$( $SKIP_UPLOAD && echo "$SOURCE_BUCKET" || echo "$RAW_BUCKET" )

  # Build job arguments.
  JOB_ARGS="{
    \"--SOURCE_LOCATION\": \"s3://$GLUE_SOURCE_BUCKET/$RAW_INGEST_PREFIX/\",
    \"--OUTPUT_LOCATION\": \"$CURATED_PATH/\"
  }"

  if $FULL_BACKFILL; then
    log "Full backfill mode — processing all 14 partitions"
    JOB_ARGS="{
      \"--SOURCE_LOCATION\": \"s3://$GLUE_SOURCE_BUCKET/$RAW_INGEST_PREFIX/\",
      \"--OUTPUT_LOCATION\": \"$CURATED_PATH/\",
      \"--PUSH_DOWN_PREDICATE\": \"all\"
    }"
  fi

  log "Starting Glue job: $GLUE_JOB_NAME"
  RUN_ID=$(aws glue start-job-run \
    --job-name "$GLUE_JOB_NAME" \
    --arguments "$JOB_ARGS" \
    --region "$REGION" $PA \
    --query "JobRunId" --output text)
  log "  Job run ID: $RUN_ID"

  check_glue_success() {
    local state
    state=$(aws glue get-job-run \
      --job-name "$GLUE_JOB_NAME" --run-id "$RUN_ID" \
      --region "$REGION" $PA \
      --query "JobRun.JobRunState" --output text)
    case "$state" in
      SUCCEEDED)  return 0 ;;
      FAILED|ERROR|TIMEOUT|STOPPED)
        die "Glue job $RUN_ID ended with state: $state"
        ;;
    esac
    return 1
  }

  wait_for "Glue job $RUN_ID" check_glue_success 2400 30

  log "Glue ETL execution time:"
  aws glue get-job-run \
    --job-name "$GLUE_JOB_NAME" --run-id "$RUN_ID" \
    --region "$REGION" $PA \
    --query "JobRun.{State:JobRunState,Duration:ExecutionTime,DPU:MaxCapacity}" \
    --output table
fi

# ── Step 7: Validate curated output ───────────────────────────────────────────
hr
log "STEP 7 — Validate curated layer"
hr

CURATED_FILE_COUNT=$(aws s3 ls "$CURATED_PATH/" --recursive $PA \
  | grep -c '\.parquet' || true)
log "Curated Parquet files: $CURATED_FILE_COUNT"
[[ $CURATED_FILE_COUNT -gt 0 ]] || die "No curated Parquet files found at $CURATED_PATH"

CURATED_SIZE=$(aws s3 ls "$CURATED_PATH/" --recursive --human-readable $PA \
  | awk '{sum+=$3} END {print sum " " $4}' || echo "unknown")
log "Curated total size: ~$CURATED_SIZE"

# Partition summary
log "Curated dt partitions:"
aws s3 ls "$CURATED_PATH/" $PA | awk '{print "  " $2}'

log "Curated derived_genre partitions (sample from first dt):"
FIRST_DT=$(aws s3 ls "$CURATED_PATH/" $PA | awk 'NR==1{print $2}')
aws s3 ls "$CURATED_PATH/$FIRST_DT" $PA | awk '{print "  " $2}'

# ── Step 7.5: Register Glue catalog databases and curated table ───────────────
hr
log "STEP 7.5 — Register Glue Data Catalog databases and curated table"
hr

CURATED_DB="sdlf_${TEAM}_curated"
GOLD_DB="sdlf_${TEAM}_gold"

for DB in "$CURATED_DB" "$GOLD_DB"; do
  aws glue create-database --database-input "{\"Name\":\"$DB\"}" \
    --region "$REGION" $PA 2>/dev/null \
    && log "  Created Glue database: $DB" \
    || log "  Glue database already exists: $DB"
done

# Register the curated table so Athena CTAS can SELECT from it.
# Schema mirrors the 18 columns written by the Glue job.
CURATED_DDL="
CREATE EXTERNAL TABLE IF NOT EXISTS ${CURATED_DB}.search_enriched (
  event_id                string,
  event_ts                timestamp,
  hour_of_day_vn          int,
  user_id_hashed          string,
  user_is_authenticated   boolean,
  session_action          string,
  is_search_abandoned     boolean,
  keyword_norm            string,
  platform_group          string,
  network_type_norm       string,
  isp_segment             string,
  has_premium             boolean,
  subscription_count      int,
  search_session_id       string,
  is_repeat_search        boolean,
  is_cross_partition_date boolean
)
PARTITIONED BY (dt string, derived_genre string)
STORED AS PARQUET
LOCATION '${CURATED_PATH}/'
TBLPROPERTIES ('parquet.compress'='SNAPPY');
"

log "Creating curated table (Athena DDL)…"
DDL_QE_ID=$(aws athena start-query-execution \
  --query-string "$CURATED_DDL" \
  --work-group "primary" \
  --result-configuration "OutputLocation=${ATHENA_RESULTS}" \
  --region "$REGION" $PA \
  --query "QueryExecutionId" --output text)

check_ddl() {
  local state
  state=$(aws athena get-query-execution \
    --query-execution-id "$DDL_QE_ID" \
    --region "$REGION" $PA \
    --query "QueryExecution.Status.State" --output text)
  case "$state" in
    SUCCEEDED) return 0 ;;
    FAILED|CANCELLED)
      REASON=$(aws athena get-query-execution \
        --query-execution-id "$DDL_QE_ID" \
        --region "$REGION" $PA \
        --query "QueryExecution.Status.StateChangeReason" --output text)
      die "Curated table DDL failed: $REASON" ;;
  esac
  return 1
}
wait_for "Curated table DDL $DDL_QE_ID" check_ddl 120 10

# Repair partitions so Athena sees the dt/derived_genre/platform_group directories.
log "Running MSCK REPAIR TABLE on curated table…"
REPAIR_QE_ID=$(aws athena start-query-execution \
  --query-string "MSCK REPAIR TABLE ${CURATED_DB}.search_enriched;" \
  --work-group "primary" \
  --result-configuration "OutputLocation=${ATHENA_RESULTS}" \
  --region "$REGION" $PA \
  --query "QueryExecutionId" --output text)

check_repair() {
  local state
  state=$(aws athena get-query-execution \
    --query-execution-id "$REPAIR_QE_ID" \
    --region "$REGION" $PA \
    --query "QueryExecution.Status.State" --output text)
  [[ "$state" == "SUCCEEDED" ]]
}
wait_for "MSCK REPAIR $REPAIR_QE_ID" check_repair 300 15
log "  ✓ Curated table partitions registered"

# ── Step 8: Run Athena CTAS gold layer ────────────────────────────────────────
hr
log "STEP 8 — Run Athena CTAS to build keyword_trends gold table"
hr

ATHENA_WG="primary"

SKIP_GOLD=false

if ! $SKIP_GOLD; then
  CTAS_SQL="
CREATE TABLE ${GOLD_DB}.keyword_trends
WITH (
  format='PARQUET',
  parquet_compression='SNAPPY',
  external_location='${GOLD_PATH}/',
  partitioned_by=ARRAY['trend_date']
)
AS
WITH base AS (
  SELECT
    keyword_norm,
    platform_group,
    derived_genre,
    DATE_FORMAT(event_ts, '%Y-%m-%d') AS trend_date,
    COUNT(*)                          AS search_count,
    SUM(CASE WHEN session_action='enter' THEN 1 ELSE 0 END) AS enter_count,
    COUNT(DISTINCT COALESCE(user_id_hashed, search_session_id)) AS unique_users,
    SUM(CASE WHEN is_search_abandoned = true THEN 1 ELSE 0 END) AS abandoned_count
  FROM ${CURATED_DB}.search_enriched
  WHERE keyword_norm IS NOT NULL
    AND is_cross_partition_date = false
  GROUP BY 1,2,3,4
),
ranked AS (
  SELECT *,
    RANK() OVER (PARTITION BY trend_date, derived_genre, platform_group
                 ORDER BY enter_count DESC) AS rank_today
  FROM base
),
with_trend AS (
  SELECT
    r.keyword_norm,
    r.platform_group,
    r.derived_genre,
    r.search_count,
    r.enter_count,
    r.abandoned_count,
    CAST(r.abandoned_count AS double) / NULLIF(r.search_count, 0)  AS abandonment_rate,
    r.unique_users,
    r.rank_today,
    prev.rank_today                                                  AS rank_7d_ago,
    CASE WHEN prev.rank_today IS NULL THEN true ELSE false END       AS is_new_entrant,
    prev.rank_today - r.rank_today                                   AS rank_delta,
    r.trend_date
  FROM ranked r
  LEFT JOIN ranked prev
    ON  r.keyword_norm   = prev.keyword_norm
    AND r.platform_group = prev.platform_group
    AND r.derived_genre  = prev.derived_genre
    AND DATE_DIFF('day', DATE(prev.trend_date), DATE(r.trend_date)) = 7
  WHERE r.rank_today <= 50
)
SELECT * FROM with_trend
ORDER BY trend_date, derived_genre, platform_group, rank_today
;"

  log "Running Athena CTAS for keyword_trends…"
  QE_ID=$(aws athena start-query-execution \
    --query-string "$CTAS_SQL" \
    --work-group "$ATHENA_WG" \
    --result-configuration "OutputLocation=${ATHENA_RESULTS}" \
    --region "$REGION" $PA \
    --query "QueryExecutionId" --output text)
  log "  Athena query execution ID: $QE_ID"

  check_athena() {
    local state
    state=$(aws athena get-query-execution \
      --query-execution-id "$QE_ID" \
      --region "$REGION" $PA \
      --query "QueryExecution.Status.State" --output text)
    case "$state" in
      SUCCEEDED) return 0 ;;
      FAILED|CANCELLED)
        REASON=$(aws athena get-query-execution \
          --query-execution-id "$QE_ID" \
          --region "$REGION" $PA \
          --query "QueryExecution.Status.StateChangeReason" --output text)
        die "Athena query $QE_ID failed: $REASON"
        ;;
    esac
    return 1
  }

  wait_for "Athena CTAS $QE_ID" check_athena 600 15

  GOLD_FILE_COUNT=$(aws s3 ls "$GOLD_PATH/" --recursive $PA \
    | grep -c '\.parquet' || true)
  log "Gold Parquet files: $GOLD_FILE_COUNT"
fi

# ── Step 9: Summary ───────────────────────────────────────────────────────────
hr
log "STEP 9 — Workshop run summary"
hr

log "Source data:     s3://$SOURCE_BUCKET/$SOURCE_PREFIX/"
log "Raw ingest:      s3://$RAW_BUCKET/$RAW_INGEST_PREFIX/"
log "Curated:         $CURATED_PATH/"
log "Gold:            $GOLD_PATH/"
log ""
log "Quick Athena validation queries:"
log ""
log "  -- Row count by date (expect ~992,650 total after ETL)"
log "  SELECT dt, COUNT(*) AS rows"
log "  FROM ${CURATED_DB:-ott_curated}.search_enriched"
log "  GROUP BY dt ORDER BY dt;"
log ""
log "  -- Genre distribution"
log "  SELECT derived_genre, COUNT(*) AS cnt"
log "  FROM ${CURATED_DB:-ott_curated}.search_enriched"
log "  GROUP BY derived_genre ORDER BY cnt DESC;"
log ""
log "  -- Top-10 keywords today"
log "  SELECT keyword_norm, enter_count, abandonment_rate"
log "  FROM ${GOLD_DB:-ott_gold}.keyword_trends"
log "  WHERE trend_date = (SELECT MAX(trend_date) FROM ${GOLD_DB:-ott_gold}.keyword_trends)"
log "  ORDER BY enter_count DESC LIMIT 10;"
hr
log "Done. Workshop run complete."
hr
