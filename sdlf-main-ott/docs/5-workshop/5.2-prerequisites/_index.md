---
title: "Prerequisites"
date: 2026-05-20
weight: 2
chapter: false
pre: " <b> 5.2 </b> "
---

Everything you need to set up *before* you start deploying. If any of these checks fails, stop and fix it — later chapters assume they pass.

---

## 5.2.1 AWS account + region

You need:

- An AWS account where you can `cloudformation:CreateStack`, `iam:CreateRole`, and `lakeformation:GrantPermissions` (a fresh sandbox account is fine).
- AWS CLI v2 or `boto3` configured for **`ap-southeast-1`** (Singapore). Other regions work but you'll need to substitute every region literal in the templates.

**Verify**:

```powershell
aws sts get-caller-identity --query Account --output text
# Expected: 12-digit account number, e.g. 703668403514

aws ec2 describe-regions --region-names ap-southeast-1 --query "Regions[0].RegionName" --output text
# Expected: ap-southeast-1
```

---

## 5.2.2 IAM principal with Lake Formation admin

The deploying principal must be a Lake Formation administrator. Without this, the `IAM_ALLOWED_PRINCIPALS` revoke step in chapter 5.3 will silently no-op and your column-level RBAC won't activate.

**Verify**:

```powershell
aws lakeformation get-data-lake-settings --region ap-southeast-1 `
  --query "DataLakeSettings.DataLakeAdmins[].DataLakePrincipalIdentifier" --output text
# Expected: at least one ARN that matches your deployer principal
```

> The `list-data-lake-administrators` operation is only available on recent AWS CLI v2 builds and crashes on older ones with `Found invalid choice 'list-data-lake-administrators'`. `get-data-lake-settings` is the older, universally-supported form and returns the same data.

If your principal isn't listed, add it via the Lake Formation console → Administrative roles → Data lake administrators → Add.

---

## 5.2.3 Amazon Bedrock — Claude Haiku enabled

The LUT-Refresh Lambda calls Claude Haiku 4.5. Bedrock model access is per-region and per-account; it's off by default.

**Enable**: Bedrock console → Model access → Enable for Claude Haiku 4.5. Two model IDs surface for this:

- `anthropic.claude-haiku-4-5-20251001-v1:0` — regional model ID (what `list-foundation-models` returns).
- `global.anthropic.claude-haiku-4-5-20251001-v1:0` — global inference profile ID (what the LUT-Refresh Lambda actually calls via the `BEDROCK_MODEL_ID` env var, which transparently routes across regions for higher availability).

Both must be enabled in the Bedrock console; enabling the regional model also makes the global inference profile available.

**Verify**:

```powershell
aws bedrock list-foundation-models --region ap-southeast-1 `
  --query "modelSummaries[?contains(modelId, 'haiku-4-5')].modelId" --output text
```

**Expected** (live output, observed today):

```
anthropic.claude-haiku-4-5-20251001-v1:0
```

---

## 5.2.4 SDLF foundation stacks

The OTT pipeline assumes three SDLF framework stacks are already deployed: foundations, team, dataset. They publish SSM parameters under `/sdlf/...` that the OTT templates resolve at deploy time.

**Deploy them once**:

```powershell
$REGION = "ap-southeast-1"
$TPL    = "D:\ott-sdlf\sdlf-main"

aws cloudformation deploy --template-file "$TPL\foundations-ott-prod.yaml" `
  --stack-name sdlf-foundations-ott-prod --capabilities CAPABILITY_NAMED_IAM CAPABILITY_AUTO_EXPAND `
  --region $REGION

aws cloudformation deploy --template-file "$TPL\team-ott-prod.yaml" `
  --stack-name sdlf-team-ott-prod --capabilities CAPABILITY_NAMED_IAM CAPABILITY_AUTO_EXPAND `
  --region $REGION

aws cloudformation deploy --template-file "$TPL\dataset-searchevents-prod.yaml" `
  --stack-name sdlf-dataset-searchevents-prod --capabilities CAPABILITY_NAMED_IAM CAPABILITY_AUTO_EXPAND `
  --region $REGION
```

**Verify** — every one of these SSM paths must resolve:

```powershell
foreach ($p in @(
  "/sdlf/storage/rRawBucket/prod",
  "/sdlf/storage/rStageBucket/prod",
  "/sdlf/storage/rAnalyticsBucket/prod",
  "/sdlf/storage/rArtifactsBucket/prod",
  "/sdlf/storage/rKMSKey/prod",
  "/sdlf/dataset/rAnalyticsGlueDataCatalog/searchevents"
)) {
  $v = aws ssm get-parameter --name $p --region ap-southeast-1 --query "Parameter.Value" --output text 2>$null
  Write-Host ("  {0,-55} {1}" -f $p, $v)
}
```

Expected output (your bucket names will differ):

```
  /sdlf/storage/rRawBucket/prod                       fpt-ott-ap-southeast-1-703668403514-raw-prod
  /sdlf/storage/rStageBucket/prod                     fpt-ott-ap-southeast-1-703668403514-stage-prod
  /sdlf/storage/rAnalyticsBucket/prod                 fpt-ott-ap-southeast-1-703668403514-analytics-prod
  /sdlf/storage/rArtifactsBucket/prod                 fpt-ott-ap-southeast-1-703668403514-artifacts-prod
  /sdlf/storage/rKMSKey/prod                          arn:aws:kms:ap-southeast-1:...:key/...
  /sdlf/dataset/rAnalyticsGlueDataCatalog/searchevents fpt_ott_searchevents_analytics
```

If any value is empty, the corresponding foundation stack didn't deploy or didn't export.

---

## 5.2.5 The genre-classifier zip and Glue script

The Glue ETL job loads two files from a project-specific bucket — `ott-search-${AWS::AccountId}-prod`, separate from the SDLF artifacts bucket: its `.py` script and a Python `--extra-py-files` classifier zip. (The split exists because the Glue execution role's S3 GetObject permission is scoped to that bucket's `ott/searchevents/` prefix.)

Of the two, **only the classifier zip is a manual prerequisite.** The Glue `.py` script is staged automatically by whichever deploy path you use — the CI/CD `buildspec-deploy.yml` and `ott-pipeline.ps1` both `aws s3 cp` it from the repo before the Glue-job stack deploys. The classifier zip is a binary that is *not* tracked in the repo, so no deploy path can stage it for you — you must upload it once, up front.

**Bootstrap the classifier zip** (one-time):

```powershell
$REGION = "ap-southeast-1"
$ACCT   = aws sts get-caller-identity --query Account --output text
$GLUEBK = "ott-search-$ACCT-prod"

aws s3 cp "D:\ott-sdlf\genre_classifier_pkg.zip" `
  "s3://$GLUEBK/ott/searchevents/genre_classifier_pkg.zip" --region $REGION
```

If the classifier zip is missing, the Glue job fails at launch with `LAUNCH ERROR | Error downloading from S3 ... key does not exist (404)`.

> The LUT-Refresh Lambda also writes the refreshed classifier to the SDLF artifacts bucket (`/sdlf/storage/rArtifactsBucket/prod`) after each successful classification batch. The Glue job does **not** read that copy — it always reads from `ott-search-${ACCT}-prod`. If you replace the classifier manually, copy it to the Glue path above.

**Verify** — only the classifier zip is checked here; the Glue `.py` script will not exist in S3 until the 5.3 deploy stages it:

```powershell
$ACCT = aws sts get-caller-identity --query Account --output text
aws s3api head-object --bucket "ott-search-$ACCT-prod" `
  --key ott/searchevents/genre_classifier_pkg.zip --region ap-southeast-1 `
  --query "{Size:ContentLength, Modified:LastModified}" --output table
```

**Expected** (live, captured 2026-05-20):

```
---------------------------------------
| Size      | Modified                |
|-----------|-------------------------|
| 944698    | 2026-05-20T07:16:29+00 |
---------------------------------------
```

---

## 5.2.6 Source data — 14 days of OTT Parquet

The reference dataset is `fpt-search-events-2022-06-{01..14}.parquet` (~1.3 M events/day). For this workshop, the data is pre-staged in a public-read S3 bucket.

**Copy it into your raw bucket**:

```powershell
$RAW = aws ssm get-parameter --name /sdlf/storage/rRawBucket/prod --query Parameter.Value --output text
foreach ($d in 1..14) {
  $dt = "{0:00}" -f $d
  aws s3 cp "s3://ott-search-703668403514-demo/raw-source/log_search/202206$dt/" `
    "s3://$RAW/ott/searchevents/202206$dt/" `
    --recursive --region ap-southeast-1
  Write-Host "Copied partition 202206$dt"
}
```

**Verify** — at least 14 partitions should be present (the reference set is 20220601..20220614; an existing environment may also have synthetic partitions like 20220617/20220618 from re-ingestion tests):

```powershell
aws s3 ls "s3://$(aws ssm get-parameter --name /sdlf/storage/rRawBucket/prod --query Parameter.Value --output text)/ott/searchevents/" `
  --region ap-southeast-1
```

**Expected** (live output, observed today — the reference + 2 re-ingestion partitions):

```
                           PRE 20220601/
                           PRE 20220602/
                           PRE 20220603/
                           ...
                           PRE 20220614/
                           PRE 20220617/
                           PRE 20220618/
```

---

## Quick prerequisites-completed checklist

Before continuing, confirm all of these:

- [ ] AWS account active, region `ap-southeast-1` reachable.
- [ ] Deploying principal is a Lake Formation administrator.
- [ ] Bedrock Claude Haiku 4.5 model access granted.
- [ ] SDLF foundation/team/dataset stacks deployed; SSM paths resolve.
- [ ] `genre_classifier_pkg.zip` uploaded to the Glue bucket (`ott-search-…-prod`).
- [ ] 14 raw Parquet partitions copied into raw bucket.

When all six check, proceed to [5.3 — Deploy](../5.3-deploy/).
