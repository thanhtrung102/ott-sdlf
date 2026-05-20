#!/bin/bash
# deploy.sh — Bootstrap the OTT Search Events dataset into an SDLF deployment.
#
# Usage:
#   ./deploy.sh -t <team> -d <dataset> -r <region> [-p <aws-profile>]
#
# Defaults: team=ott  dataset=searchevents  region=ap-southeast-1
#
# What this does:
#   1. Uploads the Glue job script to the SDLF artifacts S3 bucket
#   2. Packages and deploys the Glue job CloudFormation stack
#   3. Uploads sample data to the SDLF raw bucket under <team>/<dataset>/
#   4. Uploads genre taxonomy JSON for reference

set -euo pipefail

pflag=false
tflag=false
dflag=false
rflag=false
TEAM="ott"
DATASET="searchevents"
REGION="ap-southeast-1"

DIRNAME=$(dirname "$0")

usage() {
  echo "
  Usage: ./deploy.sh [options]

  Options:
    -t  Team name (default: ott)
    -d  Dataset name (default: searchevents)
    -r  AWS region (default: ap-southeast-1)
    -p  AWS CLI profile (optional)
    -h  Show this help
"
}

options=':p:t:d:r:h'
while getopts "$options" option; do
  case "$option" in
    p) pflag=true; PROFILE=$OPTARG ;;
    t) tflag=true; TEAM=$OPTARG ;;
    d) dflag=true; DATASET=$OPTARG ;;
    r) rflag=true; REGION=$OPTARG ;;
    h) usage; exit 0 ;;
    \?) echo "Unknown option: -$OPTARG" >&2; usage; exit 1 ;;
    :)  echo "Missing argument for -$OPTARG" >&2; usage; exit 1 ;;
  esac
done

echo "Team:    $TEAM"
echo "Dataset: $DATASET"
echo "Region:  $REGION"

PROFILE_ARG=${PROFILE:+--profile "$PROFILE"}

# ── 1. Resolve SDLF SSM parameters ───────────────────────────────────────────
ARTIFACTS_BUCKET=$(aws --region "$REGION" ssm get-parameter \
  --name "/SDLF2/S3/ArtifactsBucket" \
  --query "Parameter.Value" --output text $PROFILE_ARG)
echo "Artifacts bucket: $ARTIFACTS_BUCKET"

RAW_BUCKET=$(aws --region "$REGION" ssm get-parameter \
  --name "/SDLF2/S3/RawBucket" \
  --query "Parameter.Value" --output text $PROFILE_ARG)
echo "Raw bucket: $RAW_BUCKET"

KMS_KEY=$(aws --region "$REGION" ssm get-parameter \
  --name "/SDLF/KMS/$TEAM/DataKeyId" \
  --query "Parameter.Value" --output text $PROFILE_ARG)

# ── 2. Upload Glue script ─────────────────────────────────────────────────────
echo "Uploading Glue script..."
aws s3 cp "$DIRNAME/scripts/ott-search-glue-job.py" \
  "s3://$ARTIFACTS_BUCKET/artifacts/" $PROFILE_ARG

# ── 3. Package and deploy Glue CloudFormation stack ──────────────────────────
mkdir -p "$DIRNAME/output"

aws cloudformation package \
  --template-file "$DIRNAME/scripts/ott-search-glue-job.yaml" \
  --s3-bucket "$ARTIFACTS_BUCKET" \
  --output-template-file "$DIRNAME/output/packaged-template.yaml" \
  $PROFILE_ARG

STACK_NAME="sdlf-${TEAM}-${DATASET}-glue-job"
echo "Deploying stack: $STACK_NAME"

aws cloudformation deploy \
  --s3-bucket "$ARTIFACTS_BUCKET" --s3-prefix sdlf-utils \
  --stack-name "$STACK_NAME" \
  --template-file "$DIRNAME/output/packaged-template.yaml" \
  --parameter-overrides pTeamName="$TEAM" pDatasetName="$DATASET" \
  --tags Framework=sdlf Team="$TEAM" Dataset="$DATASET" \
  --capabilities CAPABILITY_NAMED_IAM CAPABILITY_AUTO_EXPAND \
  --region "$REGION" \
  $PROFILE_ARG

# ── 4. Upload sample data to SDLF raw bucket ─────────────────────────────────
echo "Uploading sample data to s3://$RAW_BUCKET/$TEAM/$DATASET/..."

for FILE in "$DIRNAME"/data/*.json; do
  aws s3 cp "$FILE" \
    "s3://$RAW_BUCKET/$TEAM/$DATASET/" \
    --sse aws:kms --sse-kms-key-id "$KMS_KEY" \
    $PROFILE_ARG
  echo "  Uploaded: $(basename "$FILE")"
done

echo ""
echo "Done. SDLF Stage A will trigger automatically when the JSON files land in:"
echo "  s3://$RAW_BUCKET/$TEAM/$DATASET/"
echo ""
echo "To run the Glue job manually:"
echo "  aws glue start-job-run --job-name sdlf-${TEAM}-${DATASET}-glue-job \\"
echo "    --arguments '{\"--SOURCE_LOCATION\":\"s3://$RAW_BUCKET/$TEAM/$DATASET/\",\"--OUTPUT_LOCATION\":\"s3://$RAW_BUCKET/curated/search_enriched/\"}'"
