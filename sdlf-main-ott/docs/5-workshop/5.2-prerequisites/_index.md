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
aws lakeformation list-data-lake-administrators --region ap-southeast-1 `
  --query "DataLakeAdmins[].DataLakePrincipalIdentifier" --output text
# Expected: at least one ARN that matches your deployer principal
```

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

The Glue ETL job loads its `.py` script and a Python `--extra-py-files` zip from a project-specific bucket — `ott-search-${AWS::AccountId}-prod` — separate from the SDLF artifacts bucket. (This split exists because the Glue execution role's S3 GetObject permission is scoped to that bucket and to the team/dataset prefix of the SDLF artifacts bucket; the script + zip live at `ott/searchevents/` directly in the project bucket. Aligning everything onto the SDLF artifacts bucket would require an IAM-policy + bucket-policy change. See workshop bug log for context.)

Both files must exist at the exact paths shown below or the Glue job fails with `LAUNCH ERROR | Error downloading from S3 ... key does not exist (404)`.

**Bootstrap both files** (one-time):

```powershell
$REGION = "ap-southeast-1"
$ACCT   = aws sts get-caller-identity --query Account --output text
$GLUEBK = "ott-search-$ACCT-prod"

aws s3 cp "D:\ott-sdlf\sdlf-main-ott\glue\ott-search-glue-job.py" `
  "s3://$GLUEBK/ott/searchevents/ott-search-glue-job.py" --region $REGION

aws s3 cp "D:\ott-sdlf\genre_classifier_pkg.zip" `
  "s3://$GLUEBK/ott/searchevents/genre_classifier_pkg.zip" --region $REGION
```

> The LUT-Refresh Lambda also writes the refreshed classifier to the SDLF artifacts bucket (`/sdlf/storage/rArtifactsBucket/prod`) after each successful classification batch. The Glue job does **not** read that copy — it always reads from `ott-search-${ACCT}-prod`. If you replace the classifier manually, copy it to the Glue path above.

**Verify**:

```powershell
$ACCT   = aws sts get-caller-identity --query Account --output text
aws s3api head-object --bucket "ott-search-$ACCT-prod" `
  --key ott/searchevents/genre_classifier_pkg.zip --region ap-southeast-1 `
  --query "{Size:ContentLength, Modified:LastModified}" --output table
aws s3api head-object --bucket "ott-search-$ACCT-prod" `
  --key ott/searchevents/ott-search-glue-job.py --region ap-southeast-1 `
  --query "{Size:ContentLength, Modified:LastModified}" --output table
```

**Expected** (live, captured 2026-05-20):

```
---------------------------------------
| Size      | Modified                |
|-----------|-------------------------|
| 944698    | 2026-05-20T07:16:29+00 |
---------------------------------------
---------------------------------------
| Size      | Modified                |
|-----------|-------------------------|
| 17067     | 2026-05-20T08:13:51+00 |
---------------------------------------
```

---

## 5.2.6 Choose + store the API key

The HTTP API is gated by an `x-api-key` header. The key value is a CloudFormation parameter; we store it in SSM at `/sdlf/ott/api-key/prod` so the CI/CD buildspec can fetch it deterministically.

**Generate + store**:

```powershell
$key = -join ((48..57) + (97..122) + (65..90) | Get-Random -Count 32 | ForEach-Object {[char]$_})
aws ssm put-parameter --name "/sdlf/ott/api-key/prod" --value $key --type String --overwrite `
  --region ap-southeast-1 --description "x-api-key value for sdlf-ott-api Lambda"
Write-Host "API key generated. Save somewhere safe — you'll need it to call the API."
Write-Host "Key: $key"
```

**Verify**:

```powershell
aws ssm get-parameter --name "/sdlf/ott/api-key/prod" --region ap-southeast-1 --query "Parameter.Value" --output text
# Expected: the 32-char alphanumeric string you just generated
```

---

## 5.2.7 Source data — 14 days of OTT Parquet

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
- [ ] `genre_classifier_pkg.zip` uploaded to artifacts bucket.
- [ ] API key stored in `/sdlf/ott/api-key/prod`.
- [ ] 14 raw Parquet partitions copied into raw bucket.

When all seven check, proceed to [5.3 — Deploy](../5.3-deploy/).
