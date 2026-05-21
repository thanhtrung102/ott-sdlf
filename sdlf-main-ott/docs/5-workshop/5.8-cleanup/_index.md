---
title: "Cleanup"
date: 2026-05-20
weight: 8
chapter: false
pre: " <b> 5.8 </b> "
---

Tear down everything you built so the AWS account ends up with no remaining OTT-pipeline resources. Order matters: stacks first, then buckets, then framework, then SSM. Doing this out of order leaves orphaned S3 objects you can't delete because Lake Formation still gates them.

> ⚠️ **WARNING:** Every command in this chapter is destructive. Make sure you are on the correct AWS account and region (`ap-southeast-1`) before running anything.

---

## 5.8.1 Stop CI/CD first

If the CI/CD pipeline is set up, disable it before deleting anything — otherwise pushing or re-triggering will re-create what you're about to delete.

```powershell
# Option A: disable the pipeline (rule-of-thumb, reversible)
aws codepipeline disable-stage-transition `
  --pipeline-name sdlf-ott-cicd --stage-name Source --transition-type Inbound `
  --reason "Workshop teardown in progress" --region ap-southeast-1

# Option B: delete the whole CI/CD stack (irreversible without re-bootstrap)
# aws cloudformation delete-stack --stack-name sdlf-ott-cicd --region ap-southeast-1
```

We'll re-enable / re-delete at the end.

---

## 5.8.2 Delete the 9 OTT-managed stacks

Reverse dependency order. The script below deletes in the safest sequence (consumers first, producers last).

```powershell
# The dashboard's S3 bucket must be emptied before CloudFormation will delete it.
$DASH = aws ssm get-parameter --name /sdlf/pipeline/rDashboardBucket/ott `
  --region ap-southeast-1 --query Parameter.Value --output text 2>$null
if ($DASH) { aws s3 rm "s3://$DASH" --recursive --region ap-southeast-1 | Out-Null }

$ORDER = @(
  "sdlf-pipeline-ott-dashboard",
  "sdlf-pipeline-ott-lakeformation",
  "sdlf-pipeline-ott-monitoring",
  "sdlf-pipeline-ott-trending",
  "sdlf-pipeline-ott-lutrefresh",
  "sdlf-pipeline-ott-dataquality",
  "sdlf-pipeline-ott-mainB",   # deletes its 2 nested SDLF stacks too
  "sdlf-pipeline-ott-mainA",   # deletes its 2 nested SDLF stacks too
  "sdlf-ott-searchevents-glue-job"
)
foreach ($s in $ORDER) {
  Write-Host "Deleting $s ..." -NoNewline
  aws cloudformation delete-stack --stack-name $s --region ap-southeast-1
  aws cloudformation wait stack-delete-complete --stack-name $s --region ap-southeast-1
  Write-Host " OK"
}

# Idempotently drop any legacy stacks from earlier workshop revisions
foreach ($legacy in @(
  "sdlf-pipeline-ott-goldquality",
  "sdlf-pipeline-ott-contentgap",
  "sdlf-pipeline-ott-api"
)) {
  $status = (aws cloudformation describe-stacks --stack-name $legacy `
    --region ap-southeast-1 --query "Stacks[0].StackStatus" --output text 2>$null)
  if ($status -and $status -ne "None") {
    Write-Host "Deleting legacy $legacy ($status) ..." -NoNewline
    aws cloudformation delete-stack --stack-name $legacy --region ap-southeast-1
    aws cloudformation wait stack-delete-complete --stack-name $legacy --region ap-southeast-1
    Write-Host " OK"
  }
}
```

**Expected output** (~10 min total — each stack ~30-90 s):

```
Deleting sdlf-pipeline-ott-dashboard ... OK
Deleting sdlf-pipeline-ott-lakeformation ... OK
Deleting sdlf-pipeline-ott-monitoring ... OK
Deleting sdlf-pipeline-ott-trending ... OK
Deleting sdlf-pipeline-ott-lutrefresh ... OK
Deleting sdlf-pipeline-ott-dataquality ... OK
Deleting sdlf-pipeline-ott-mainB ... OK
Deleting sdlf-pipeline-ott-mainA ... OK
Deleting sdlf-ott-searchevents-glue-job ... OK
```

**Verify**:

```powershell
aws cloudformation list-stacks --region ap-southeast-1 `
  --stack-status-filter CREATE_COMPLETE UPDATE_COMPLETE `
  --query "StackSummaries[?starts_with(StackName, 'sdlf-pipeline-ott') || starts_with(StackName, 'sdlf-ott-searchevents')].StackName" `
  --output text
```

**Expected**: empty output (no OTT-pipeline stacks remain).

---

## 5.8.3 Empty the S3 prefixes the pipeline created

CFN stack deletion does NOT delete the data the pipeline wrote — that lives in framework-managed buckets. You have to empty the OTT-specific prefixes manually.

```powershell
$RAW       = aws ssm get-parameter --name /sdlf/storage/rRawBucket/prod       --query Parameter.Value --output text
$STAGE     = aws ssm get-parameter --name /sdlf/storage/rStageBucket/prod     --query Parameter.Value --output text
$ANALYTICS = aws ssm get-parameter --name /sdlf/storage/rAnalyticsBucket/prod --query Parameter.Value --output text
$ARTIFACTS = aws ssm get-parameter --name /sdlf/storage/rArtifactsBucket/prod --query Parameter.Value --output text

foreach ($pair in @(
  @{ Bucket = $RAW;       Prefix = "ott/searchevents/" },
  @{ Bucket = $STAGE;     Prefix = "analytics/" },
  @{ Bucket = $STAGE;     Prefix = "athena-results/" },
  @{ Bucket = $STAGE;     Prefix = "dq-results/" },
  @{ Bucket = $ANALYTICS; Prefix = "ott/searchevents/" },
  @{ Bucket = $ARTIFACTS; Prefix = "ott/searchevents/" },
  @{ Bucket = $ARTIFACTS; Prefix = "lambda/" }
)) {
  Write-Host "Removing s3://$($pair.Bucket)/$($pair.Prefix) ..." -NoNewline
  aws s3 rm "s3://$($pair.Bucket)/$($pair.Prefix)" --recursive --region ap-southeast-1 | Out-Null
  Write-Host " OK"
}
```

**Verify**:

```powershell
foreach ($b in @($RAW, $STAGE, $ANALYTICS, $ARTIFACTS)) {
  $n = (aws s3 ls "s3://$b/ott/" --recursive --region ap-southeast-1 | Measure-Object).Count
  Write-Host "s3://$b/ott/  objects remaining: $n"
}
```

**Expected**: zero objects in each prefix.

> The buckets themselves are framework-owned; leave them. Section 5.8.5 covers the framework teardown if you want to go further.

---

## 5.8.4 Clean SSM parameters this workshop created

```powershell
# The legacy API key (if it still exists from an earlier workshop revision)
aws ssm delete-parameter --name /sdlf/ott/api-key/prod --region ap-southeast-1 2>$null

# Pipeline-specific SSM exports created by stack deletions are removed automatically.
```

**Verify**:

```powershell
aws ssm get-parameters-by-path --path /sdlf/ --recursive --region ap-southeast-1 `
  --query "Parameters[?contains(Name, '/ott/')].Name" --output text
```

**Expected**: empty output (no `/ott/` parameters remain).

---

## 5.8.5 (Optional) Tear down the SDLF framework

Only do this if you're done with SDLF entirely on this account. If you're keeping SDLF for other datasets, **skip this section**.

```powershell
# Delete the three framework stacks in reverse order
foreach ($s in @(
  "sdlf-dataset-searchevents-prod",
  "sdlf-team-ott-prod",
  "sdlf-foundations-ott-prod"
)) {
  Write-Host "Deleting $s ..."
  aws cloudformation delete-stack --stack-name $s --region ap-southeast-1
  aws cloudformation wait stack-delete-complete --stack-name $s --region ap-southeast-1
}
```

If `foundations` deletion fails because S3 buckets aren't empty, empty them and retry:

```powershell
foreach ($b in @($RAW, $STAGE, $ANALYTICS, $ARTIFACTS)) {
  aws s3 rm "s3://$b" --recursive --region ap-southeast-1
}
```

> The KMS key is *scheduled* for deletion (7-day waiting period) — it doesn't disappear immediately. To re-deploy SDLF in this account within 7 days, you'll need to cancel the schedule via `aws kms cancel-key-deletion`.

---

## 5.8.6 Delete the CI/CD pipeline (if previously deployed)

```powershell
aws cloudformation delete-stack --stack-name sdlf-ott-cicd --region ap-southeast-1
aws cloudformation wait stack-delete-complete --stack-name sdlf-ott-cicd --region ap-southeast-1
```

The CodeStar connection to GitHub remains (it's account-level) — delete via the AWS Console if no other repos need it.

---

## 5.8.7 Final verification

```powershell
aws cloudformation list-stacks --region ap-southeast-1 `
  --stack-status-filter CREATE_COMPLETE UPDATE_COMPLETE `
  --query "StackSummaries[?contains(StackName, 'sdlf-')].StackName" --output text
```

**Expected** (if you ran 5.8.5 and 5.8.6): empty output.

```powershell
aws lakeformation list-permissions --region ap-southeast-1 `
  --query "PrincipalResourcePermissions[?contains(Resource.Database.Name, 'fpt_ott')]" --output text
```

**Expected**: empty output (no LF grants on OTT databases).

---

## Estimated time + final cost

| Section | Time | Cost left running |
|---|---|---|
| 5.8.2 Stack deletes | ~10 min | $0 |
| 5.8.3 S3 empty | ~2 min | $0 |
| 5.8.4 SSM delete | <1 min | $0 |
| 5.8.5 Framework teardown (optional) | ~5 min | $0 (after KMS 7-day wait) |
| 5.8.6 CI/CD delete (optional) | ~3 min | $0 |

If you ran 5.8.1 → 5.8.4, your monthly AWS bill from this workshop drops to **$0**. The empty foundation buckets cost ~$0.01/month if you leave them. The KMS key in pending-deletion state costs $0 (no GenerateDataKey calls without anything using it).

---

🎉 **Workshop complete.**

You built, ran, verified, and tore down a production-grade serverless data lake — and saw the business insights it produces along the way.
