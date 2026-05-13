# OTT SDLF - end-to-end pipeline runner
# Deploys all stacks, copies a raw file to a new date partition to trigger ingestion,
# monitors Stage A -> Stage B -> DQ state machines, then invokes analytics Lambdas.
# Usage: .\run-pipeline.ps1
# Usage (skip deploy): .\run-pipeline.ps1 -SkipDeploy
# Usage (analytics only): .\run-pipeline.ps1 -AnalyticsOnly

param(
    [switch]$SkipDeploy,
    [switch]$AnalyticsOnly
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

$Region      = "ap-southeast-1"
$Account     = "703668403514"
$RawBucket   = "fpt-ott-ap-southeast-1-$Account-raw-prod"
$StageBucket = "fpt-ott-ap-southeast-1-$Account-stage-prod"
$Repo        = "D:\ott-sdlf"

$SmA  = "arn:aws:states:${Region}:${Account}:stateMachine:sdlf-ott-mainA-sm"
$SmB  = "arn:aws:states:${Region}:${Account}:stateMachine:sdlf-ott-mainB-sm"
$SmDQ = "arn:aws:states:${Region}:${Account}:stateMachine:sdlf-ott-mainDQ-sm"

function Write-Step { param([string]$Msg) Write-Host "`n-- $Msg" -ForegroundColor Cyan }
function Write-OK   { param([string]$Msg) Write-Host "   OK   $Msg" -ForegroundColor Green }
function Write-Warn { param([string]$Msg) Write-Host "   WARN $Msg" -ForegroundColor Yellow }
function Write-Fail { param([string]$Msg) Write-Host "   FAIL $Msg" -ForegroundColor Red; throw $Msg }

# ---------------------------------------------------------------------------
# Deploy
# ---------------------------------------------------------------------------

function Deploy-Stack {
    param([string]$StackName, [string]$TemplatePath, [string[]]$Overrides = @())
    $a = @(
        "cloudformation", "deploy",
        "--template-file", $TemplatePath,
        "--stack-name", $StackName,
        "--capabilities", "CAPABILITY_NAMED_IAM", "CAPABILITY_AUTO_EXPAND",
        "--no-fail-on-empty-changeset",
        "--region", $Region
    )
    if ($Overrides.Count -gt 0) { $a += "--parameter-overrides"; $a += $Overrides }
    & aws @a
    if ($LASTEXITCODE -ne 0) { Write-Fail "[$StackName] deploy failed" }
    Write-OK $StackName
}

if (-not $SkipDeploy -and -not $AnalyticsOnly) {
    Write-Step "Deploying stacks"
    $KmsKey = (aws ssm get-parameter --name "/sdlf/storage/rKMSKey/prod" --query "Parameter.Value" --output text --region $Region)
    Deploy-Stack "sdlf-ott-searchevents-glue-job" "$Repo\scripts\ott-search-glue-job.yaml" @(
        "pTeamName=ott", "pDatasetName=searchevents",
        "pArtifactsBucket=ott-search-$Account-prod",
        "pKmsKeyArn=$KmsKey", "pPipelineDeploymentInstance=mainB"
    )
    Deploy-Stack "sdlf-pipeline-ott-mainA"       "$Repo\sdlf-main-ott\pipeline-ott-mainA.yaml"
    Deploy-Stack "sdlf-pipeline-ott-mainB"       "$Repo\sdlf-main-ott\pipeline-ott-mainB.yaml"
    Deploy-Stack "sdlf-pipeline-ott-dataquality" "$Repo\sdlf-main-ott\pipeline-ott-dataquality.yaml"
    Deploy-Stack "sdlf-pipeline-ott-lutrefresh"  "$Repo\sdlf-main-ott\pipeline-ott-lutrefresh.yaml" @(
        "pMaxNewKeywords=15000",
        "pBedrockModelId=global.anthropic.claude-haiku-4-5-20251001-v1:0"
    )
    Deploy-Stack "sdlf-pipeline-ott-contentgap"  "$Repo\sdlf-main-ott\pipeline-ott-contentgap.yaml"
    Deploy-Stack "sdlf-pipeline-ott-trending"    "$Repo\sdlf-main-ott\pipeline-ott-trending.yaml"
    Deploy-Stack "sdlf-pipeline-ott-monitoring"  "$Repo\sdlf-main-ott\pipeline-ott-monitoring.yaml"
}

# ---------------------------------------------------------------------------
# Wait for SM execution
# ---------------------------------------------------------------------------

function Wait-SM {
    param([string]$SmArn, [string]$Label, [int]$TimeoutSec = 900)
    $deadline = (Get-Date).AddSeconds($TimeoutSec)
    Start-Sleep 5
    $exec = $null
    while ((Get-Date) -lt $deadline) {
        $running = aws stepfunctions list-executions --state-machine-arn $SmArn `
            --status-filter RUNNING --query "executions[0].executionArn" --output text --region $Region 2>&1
        if ($running -and $running -ne "None") { $exec = $running; break }
        Start-Sleep 5
    }
    if (-not $exec) {
        $last = aws stepfunctions list-executions --state-machine-arn $SmArn `
            --query "executions[0]" --output json --region $Region 2>&1 | ConvertFrom-Json
        if ($last -and $last.status -eq "SUCCEEDED") {
            Write-OK "$Label - already SUCCEEDED"
            return $last.executionArn
        }
        Write-Fail "$Label - no execution found within ${TimeoutSec}s"
    }
    Write-Host "   ... $Label running" -ForegroundColor Yellow
    while ((Get-Date) -lt $deadline) {
        $status = aws stepfunctions describe-execution --execution-arn $exec `
            --query "status" --output text --region $Region 2>&1
        if ($status -eq "SUCCEEDED") { Write-OK "$Label - SUCCEEDED"; return $exec }
        if ($status -in @("FAILED", "ABORTED", "TIMED_OUT")) { Write-Fail "$Label - $status" }
        Write-Host "   ... $Label $status" -ForegroundColor Yellow
        Start-Sleep 15
    }
    Write-Fail "$Label timed out after ${TimeoutSec}s"
}

# ---------------------------------------------------------------------------
# Ingest
# ---------------------------------------------------------------------------

if (-not $AnalyticsOnly) {
    Write-Step "Triggering ingestion (20220614 -> 20220617)"
    $SrcKey  = "ott/searchevents/20220614/part-00000-6e07374d-dd20-4533-83f6-7c32c5bc60c4-c000.snappy.parquet"
    $DestKey = "ott/searchevents/20220617/part-00000-6e07374d-dd20-4533-83f6-7c32c5bc60c4-c000.snappy.parquet"
    aws s3 cp "s3://$RawBucket/$SrcKey" "s3://$RawBucket/$DestKey" --region $Region
    if ($LASTEXITCODE -ne 0) { Write-Fail "Raw file copy failed" }
    Write-OK "s3://$RawBucket/$DestKey"

    Write-Step "Stage A - routing and pre-processing"
    Wait-SM -SmArn $SmA -Label "StageA"

    Write-Step "Stage B - Glue ETL transform"
    Wait-SM -SmArn $SmB -Label "StageB" -TimeoutSec 2400

    Write-Step "Data Quality"
    Wait-SM -SmArn $SmDQ -Label "DQ"
}

# ---------------------------------------------------------------------------
# Analytics
# ---------------------------------------------------------------------------

Write-Step "Content Gap Lambda"
[System.IO.File]::WriteAllText(
    "C:\tmp\cg-payload.json",
    '{"source":"run-pipeline","detail-type":"Test"}',
    [System.Text.Encoding]::UTF8
)
aws lambda invoke --function-name sdlf-ott-mainCG-report --region $Region `
    --payload fileb://C:\tmp\cg-payload.json C:\tmp\cg-response.json | Out-Null
$cg = Get-Content C:\tmp\cg-response.json | ConvertFrom-Json
if ($cg.PSObject.Properties.Name -contains "errorMessage") {
    Write-Fail "Content Gap Lambda error: $($cg.errorMessage)"
}
if ($cg.errors -gt 0) {
    Write-Warn "Content Gap completed with $($cg.errors) query error(s)"
} else {
    Write-OK "Content Gap - $($cg.reports | ConvertTo-Json -Compress)"
}
Write-Host "`n   Dashboard: $($cg.report_url)" -ForegroundColor White

Write-Step "Trending Lambda (reference_date 2022-06-17)"
[System.IO.File]::WriteAllText(
    "C:\tmp\tr-payload.json",
    '{"source":"run-pipeline","detail-type":"Test","reference_date":"2022-06-17"}',
    [System.Text.Encoding]::UTF8
)
aws lambda invoke --function-name sdlf-ott-mainTR-report --region $Region `
    --payload fileb://C:\tmp\tr-payload.json C:\tmp\tr-response.json | Out-Null
$tr = Get-Content C:\tmp\tr-response.json | ConvertFrom-Json
if ($tr.PSObject.Properties.Name -contains "errorMessage") {
    Write-Fail "Trending Lambda error: $($tr.errorMessage)"
}
if ($tr.errors -gt 0) {
    Write-Warn "Trending completed with $($tr.errors) error(s)"
} else {
    Write-OK "Trending - mode:$($tr.mode) unknown:$($tr.reports.trending_unknown)"
}

Write-Step "S3 outputs"
$today = (Get-Date -Format "yyyy-MM-dd")
aws s3 ls "s3://$StageBucket/analytics/content-gap/$today/" --region $Region 2>&1 | ForEach-Object {
    Write-Host "   $_"
}

Write-Host "`nPipeline run complete." -ForegroundColor Green
