# OTT SDLF Pipeline — Full Production Redeploy
# Usage: .\deploy.ps1
# Prerequisites: aws CLI configured with terraform-admin credentials, region ap-southeast-1
# Deploys all 6 application stacks in dependency order. Idempotent — safe to re-run.

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"
$Region = "ap-southeast-1"
$Account = "703668403514"
$Repo = "D:\ott-sdlf"

function Deploy-Stack {
    param(
        [string]$StackName,
        [string]$TemplatePath,
        [string[]]$Overrides = @()
    )
    Write-Host "`n[$StackName] Deploying..." -ForegroundColor Cyan
    $args = @(
        "cloudformation", "deploy",
        "--template-file", $TemplatePath,
        "--stack-name", $StackName,
        "--capabilities", "CAPABILITY_NAMED_IAM", "CAPABILITY_AUTO_EXPAND",
        "--no-fail-on-empty-changeset",
        "--region", $Region
    )
    if ($Overrides.Count -gt 0) {
        $args += "--parameter-overrides"
        $args += $Overrides
    }
    & aws @args
    if ($LASTEXITCODE -ne 0) { throw "[$StackName] Deploy FAILED (exit $LASTEXITCODE)" }
    Write-Host "[$StackName] OK" -ForegroundColor Green
}

# ── 1. Glue ETL job ──────────────────────────────────────────────────────────
$KmsKey = (aws ssm get-parameter --name "/sdlf/storage/rKMSKey/prod" --query "Parameter.Value" --output text --region $Region 2>&1)
Deploy-Stack `
    -StackName "sdlf-ott-searchevents-glue-job" `
    -TemplatePath "$Repo\scripts\ott-search-glue-job.yaml" `
    -Overrides @(
        "pTeamName=ott",
        "pDatasetName=searchevents",
        "pArtifactsBucket=ott-search-$Account-prod",
        "pKmsKeyArn=$KmsKey",
        "pPipelineDeploymentInstance=mainB"
    )

# ── 2. Stage A SM ────────────────────────────────────────────────────────────
Deploy-Stack `
    -StackName "sdlf-pipeline-ott-mainA" `
    -TemplatePath "$Repo\sdlf-main-ott\pipeline-ott-mainA.yaml"

# ── 3. Stage B SM ────────────────────────────────────────────────────────────
Deploy-Stack `
    -StackName "sdlf-pipeline-ott-mainB" `
    -TemplatePath "$Repo\sdlf-main-ott\pipeline-ott-mainB.yaml"

# ── 4. Data Quality SM ───────────────────────────────────────────────────────
Deploy-Stack `
    -StackName "sdlf-pipeline-ott-dataquality" `
    -TemplatePath "$Repo\sdlf-main-ott\pipeline-ott-dataquality.yaml"

# ── 5. LUT Refresh Lambda ────────────────────────────────────────────────────
Deploy-Stack `
    -StackName "sdlf-pipeline-ott-lutrefresh" `
    -TemplatePath "$Repo\sdlf-main-ott\pipeline-ott-lutrefresh.yaml" `
    -Overrides @(
        "pMaxNewKeywords=15000",
        "pBedrockModelId=global.anthropic.claude-haiku-4-5-20251001-v1:0"
    )

# ── 6. Monitoring (dashboards + alarms) ──────────────────────────────────────
Deploy-Stack `
    -StackName "sdlf-pipeline-ott-monitoring" `
    -TemplatePath "$Repo\sdlf-main-ott\pipeline-ott-monitoring.yaml"

Write-Host "`nAll stacks deployed successfully." -ForegroundColor Green
