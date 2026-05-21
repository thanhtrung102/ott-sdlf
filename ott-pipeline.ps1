# ott-pipeline.ps1 — OTT SDLF single end-to-end pipeline script
#
# Handles: deploy → ingest trigger → Stage A/B/DQ wait → analytics Lambdas → summary
#
# Usage:
#   .\ott-pipeline.ps1                   # full run (deploy + ingest + analytics)
#   .\ott-pipeline.ps1 -SkipDeploy       # skip CloudFormation stacks
#   .\ott-pipeline.ps1 -SkipIngest       # skip raw file copy + SM waits
#   .\ott-pipeline.ps1 -AnalyticsOnly    # invoke Lambdas only (no deploy, no ingest)
#   .\ott-pipeline.ps1 -Status           # print current SM / DLQ state and exit

param(
    [switch]$SkipDeploy,
    [switch]$SkipIngest,
    [switch]$AnalyticsOnly,
    [switch]$Status
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

# ── Constants ────────────────────────────────────────────────────────────────
$Region          = "ap-southeast-1"
$Account         = "703668403514"
$Repo            = "D:\ott-sdlf"
$Tpl             = "$Repo\sdlf-main-ott"

$RawBucket       = "fpt-ott-ap-southeast-1-$Account-raw-prod"
$StageBucket     = "fpt-ott-ap-southeast-1-$Account-stage-prod"
$AnalyticsBucket = "fpt-ott-ap-southeast-1-$Account-analytics-prod"
$ArtifactsBucket = "fpt-ott-ap-southeast-1-$Account-artifacts-prod"
$AthenaBucket    = "fpt-ott-ap-southeast-1-$Account-athena-prod"
# Glue script + classifier zip live in a project bucket, NOT the SDLF artifacts
# bucket; the Glue role's S3 grant is scoped to this bucket's ott/searchevents/ prefix.
$GlueBucket      = "ott-search-$Account-prod"

$SmAArn  = "arn:aws:states:${Region}:${Account}:stateMachine:sdlf-ott-mainA-sm"
$SmBArn  = "arn:aws:states:${Region}:${Account}:stateMachine:sdlf-ott-mainB-sm"
$SmDQArn = "arn:aws:states:${Region}:${Account}:stateMachine:sdlf-ott-mainDQ-sm"

# Raw data: 20220601–20220614 are the valid partitions.
# Trigger ingest by copying 20220614 into a new partition (20220615).
$IngestSrcKey  = "ott/searchevents/20220614/part-00000-6e07374d-dd20-4533-83f6-7c32c5bc60c4-c000.snappy.parquet"
$IngestDestKey = "ott/searchevents/20220615/part-00000-6e07374d-dd20-4533-83f6-7c32c5bc60c4-c000.snappy.parquet"

# ── Helpers ───────────────────────────────────────────────────────────────────
function Write-Step  { param([string]$M) Write-Host "`n=== $M ===" -ForegroundColor Cyan }
function Write-OK    { param([string]$M) Write-Host "  [OK]   $M" -ForegroundColor Green }
function Write-Warn  { param([string]$M) Write-Host "  [WARN] $M" -ForegroundColor Yellow }
function Write-Fail  { param([string]$M) Write-Host "  [FAIL] $M" -ForegroundColor Red; throw $M }

function Deploy-Stack {
    param([string]$Name, [string]$Template, [string[]]$Overrides = @())
    Write-Host "  deploying $Name ..." -NoNewline
    $a = @("cloudformation","deploy","--template-file",$Template,
           "--stack-name",$Name,"--capabilities","CAPABILITY_NAMED_IAM","CAPABILITY_AUTO_EXPAND",
           "--no-fail-on-empty-changeset","--region",$Region)
    if ($Overrides.Count -gt 0) { $a += "--parameter-overrides"; $a += $Overrides }
    $out = & aws @a 2>&1
    if ($LASTEXITCODE -ne 0) { Write-Host ""; Write-Fail "[$Name] failed:`n$out" }
    Write-Host " OK" -ForegroundColor Green
}

# Wait for a Step Functions SM execution that started AFTER $AfterTime.
# Returns the execution ARN, or throws on timeout / failure.
function Wait-SM {
    param([string]$Arn, [string]$Label, [DateTime]$AfterTime, [int]$TimeoutSec = 2700)
    $deadline = (Get-Date).AddSeconds($TimeoutSec)
    $exec     = $null

    # Poll until we find a RUNNING or recently-SUCCEEDED execution started after AfterTime
    while ((Get-Date) -lt $deadline) {
        $list = aws stepfunctions list-executions --state-machine-arn $Arn `
            --region $Region --output json 2>&1 | ConvertFrom-Json
        $recent = $list.executions | Where-Object {
            [DateTime]::Parse($_.startDate) -gt $AfterTime
        } | Sort-Object startDate -Descending | Select-Object -First 1

        if ($recent) {
            if ($recent.status -eq "SUCCEEDED") { Write-OK "$Label SUCCEEDED"; return $recent.executionArn }
            if ($recent.status -in @("FAILED","ABORTED","TIMED_OUT")) {
                Write-Fail "$Label $($recent.status): $($recent.executionArn)"
            }
            $exec = $recent.executionArn
            break
        }
        Start-Sleep 10
    }

    if (-not $exec) { Write-Fail "$Label - no execution found after $AfterTime within ${TimeoutSec}s" }

    # Poll until SUCCEEDED
    Write-Host "  waiting $Label" -NoNewline
    while ((Get-Date) -lt $deadline) {
        $status = aws stepfunctions describe-execution --execution-arn $exec `
            --query "status" --output text --region $Region 2>&1
        if ($status -eq "SUCCEEDED") { Write-Host ""; Write-OK "$Label SUCCEEDED"; return $exec }
        if ($status -in @("FAILED","ABORTED","TIMED_OUT")) {
            Write-Host ""
            Write-Fail "$Label ${status}: $exec"
        }
        Write-Host "." -NoNewline
        Start-Sleep 20
    }
    Write-Host ""
    Write-Fail "$Label timed out after ${TimeoutSec}s"
}

# ── -Status mode ──────────────────────────────────────────────────────────────
if ($Status) {
    Write-Step "Pipeline Status"
    foreach ($sm in @(
        @{Name="Stage A"; Arn=$SmAArn},
        @{Name="Stage B"; Arn=$SmBArn},
        @{Name="DQ";      Arn=$SmDQArn}
    )) {
        $e = (aws stepfunctions list-executions --state-machine-arn $sm.Arn `
            --region $Region --output json 2>&1 | ConvertFrom-Json).executions |
            Select-Object -First 1
        if ($e) {
            $ts = [DateTime]::Parse($e.startDate).ToString("yyyy-MM-dd HH:mm:ss")
            Write-Host ("  {0,-10} {1,-12} {2}" -f $sm.Name, $e.status, $ts)
        } else {
            Write-Host ("  {0,-10} no executions found" -f $sm.Name)
        }
    }

    Write-Step "DLQ Counts"
    $queues = aws sqs list-queues --queue-name-prefix "sdlf-ott" `
        --region $Region --output json 2>&1 | ConvertFrom-Json
    foreach ($url in $queues.QueueUrls | Where-Object { $_ -match "dlq" }) {
        $attrs = aws sqs get-queue-attributes --queue-url $url `
            --attribute-names ApproximateNumberOfMessages `
            --region $Region --output json 2>&1 | ConvertFrom-Json
        $n = $attrs.Attributes.ApproximateNumberOfMessages
        $name = $url.Split("/")[-1]
        if ([int]$n -gt 0) { Write-Warn "$name : $n messages" }
        else                { Write-OK   "$name : 0 messages" }
    }

    Write-Step "LUT"
    $fn = aws lambda get-function-configuration --function-name sdlf-ott-mainLUT-refresh `
        --region $Region --output json 2>&1 | ConvertFrom-Json
    Write-Host "  Last modified : $($fn.LastModified)"
    Write-Host "  Code size     : $($fn.CodeSize) bytes"
    exit 0
}

# ── Deploy ─────────────────────────────────────────────────────────────────────
if (-not $SkipDeploy -and -not $AnalyticsOnly) {
    Write-Step "Deploy stacks"

    # These are only needed for deployment
    $KmsKey = (aws ssm get-parameter --name "/sdlf/storage/rKMSKey/prod" `
        --query "Parameter.Value" --output text --region $Region)

    # Crawler role ARN has a random CFN suffix — read from deployed DQ stack on re-runs
    $CrawlerRoleArn = (aws cloudformation describe-stacks `
        --stack-name sdlf-pipeline-ott-dataquality --region $Region `
        --query "Stacks[0].Parameters[?ParameterKey=='pCrawlerRoleArn'].ParameterValue | [0]" `
        --output text 2>$null)
    if ((-not $CrawlerRoleArn) -or $CrawlerRoleArn -eq "None") {
        $CrawlerRoleArn = (aws cloudformation describe-stack-resource `
            --stack-name sdlf-dataset-searchevents-prod `
            --logical-resource-id rDatalakeCrawlerRole `
            --region $Region `
            --query "StackResourceDetail.PhysicalResourceId" --output text 2>$null)
        if ((-not $CrawlerRoleArn) -or $CrawlerRoleArn -eq "None") {
            Write-Fail "Cannot resolve CrawlerRoleArn. Deploy sdlf-dataset-searchevents-prod first."
        }
    }

    # Upload the current Glue script to the Glue bucket before deploying the job stack
    Write-Host "  uploading Glue script to s3://$GlueBucket/ott/searchevents/ ..."
    aws s3 cp "$Tpl\glue\ott-search-glue-job.py" `
        "s3://$GlueBucket/ott/searchevents/ott-search-glue-job.py" `
        --region $Region | Out-Null

    # Package + upload all analytics Lambdas (single source of truth = lambda/*/src/lambda_function.py).
    # Each CFN template references s3://artifacts/lambda/<name>.zip via Code.S3Bucket/S3Key.
    # This must run BEFORE the Lambda stacks deploy.
    Write-Host "  packaging analytics Lambdas to s3://$ArtifactsBucket/lambda/ ..."
    & python "$Repo\scripts\package_and_deploy_lambdas.py"
    if ($LASTEXITCODE -ne 0) { Write-Fail "Lambda packaging failed" }

    # 1. Glue ETL job
    Deploy-Stack "sdlf-ott-searchevents-glue-job" "$Tpl\pipeline-ott-glue-job.yaml" @(
        "pTeamName=ott", "pDatasetName=searchevents",
        "pArtifactsBucket=$GlueBucket", "pKmsKeyArn=$KmsKey",
        "pPipelineDeploymentInstance=mainB"
    )

    # 2–3. Stage A and B SMs
    Deploy-Stack "sdlf-pipeline-ott-mainA" "$Tpl\pipeline-ott-mainA.yaml"
    Deploy-Stack "sdlf-pipeline-ott-mainB" "$Tpl\pipeline-ott-mainB.yaml"

    # 4. Data Quality SM — curated table, triggered by Stage B SUCCEEDED
    Deploy-Stack "sdlf-pipeline-ott-dataquality" "$Tpl\pipeline-ott-dq-stage.yaml" @(
        "pStageName=mainDQ",
        "pDatabaseName=fpt_ott_searchevents_analytics",
        "pTableName=curated",
        "pRulesetName=fpt_ott_searchevents_analytics_curated",
        "pCrawlerName=sdlf-searchevents-analytics-crawler",
        "pCrawlerRoleArn=$CrawlerRoleArn",
        "pStageBucket=$StageBucket",
        "pDataReadBucket=$AnalyticsBucket",
        "pDataReadPrefix=ott/searchevents/",
        "pResultsS3Prefix=dq-results/ott/searchevents",
        "pTriggerType=statesMachine",
        "pStageBSmArn=$SmBArn",
        "pExportRoles=true",
        "pCustomEventSource=",
        "pCustomEventDetailType=",
        "pKmsKeyArn=$KmsKey"
    )

    # 5. LUT Refresh Lambda — triggered by DQ SUCCEEDED
    Deploy-Stack "sdlf-pipeline-ott-lutrefresh" "$Tpl\pipeline-ott-lutrefresh.yaml" @(
        "pMaxNewKeywords=15000",
        "pBedrockModelId=global.anthropic.claude-haiku-4-5-20251001-v1:0",
        "pAthenaResultsBucket=$AthenaBucket",
        "pAthenaWorkgroupKmsKey=$KmsKey"
    )

    # 6–7. Content Gap and Trending Lambdas — triggered by DQ SUCCEEDED
    Deploy-Stack "sdlf-pipeline-ott-contentgap" "$Tpl\pipeline-ott-contentgap.yaml" @(
        "pAthenaResultsBucket=$AthenaBucket",
        "pAthenaWorkgroupKmsKey=$KmsKey"
    )
    Deploy-Stack "sdlf-pipeline-ott-trending" "$Tpl\pipeline-ott-trending.yaml" @(
        "pAthenaResultsBucket=$AthenaBucket",
        "pAthenaWorkgroupKmsKey=$KmsKey"
    )

    # 8. Gold DQ SM — keyword_trends table, triggered by Trending Lambda
    Deploy-Stack "sdlf-pipeline-ott-goldquality" "$Tpl\pipeline-ott-dq-stage.yaml" @(
        "pStageName=mainGoldDQ",
        "pDatabaseName=fpt_ott_searchevents_gold",
        "pTableName=keyword_trends",
        "pRulesetName=fpt_ott_searchevents_gold_keyword_trends",
        "pCrawlerName=sdlf-searchevents-analytics-crawler",
        "pCrawlerRoleArn=$CrawlerRoleArn",
        "pStageBucket=$StageBucket",
        "pDataReadBucket=$AnalyticsBucket",
        "pDataReadPrefix=ott/searchevents/gold/",
        "pResultsS3Prefix=dq-results/ott/gold",
        "pTriggerType=customEvent",
        "pCustomEventSource=sdlf.ott.trending",
        "pCustomEventDetailType=Trending Report Completed",
        "pExportRoles=false",
        "pStageBSmArn=",
        "pKmsKeyArn=$KmsKey"
    )

    # 9. REST API — pApiKey sourced from SSM (generated once with scripts/rotate_api_key.py)
    $ApiKey = (aws ssm get-parameter --name "/sdlf/ott/api-key/prod" `
        --query "Parameter.Value" --output text --region $Region 2>$null)
    if ((-not $ApiKey) -or $ApiKey -eq "None") {
        Write-Fail "API key missing — populate SSM /sdlf/ott/api-key/prod before deploying the API stack"
    }
    Deploy-Stack "sdlf-pipeline-ott-api" "$Tpl\pipeline-ott-api.yaml" @(
        "pApiKey=$ApiKey"
    )

    # 10. Lake Formation column-level RBAC (fully SSM-defaulted)
    Deploy-Stack "sdlf-pipeline-ott-lakeformation" "$Tpl\pipeline-ott-lakeformation.yaml"

    # DATA_LOCATION_ACCESS for the Trending role's CTAS to the gold prefix in the
    # LF-registered analytics bucket. Granted via CLI, not CFN: the
    # AWS::LakeFormation::PrincipalPermissions DataLocation resource has a known
    # stabilization issue. Idempotent — safe to re-run.
    Write-Host "  granting LF DATA_LOCATION_ACCESS to Trending role ..." -NoNewline
    $trendingRole = (aws ssm get-parameter --name "/sdlf/pipeline/rRole/ott-mainTR" `
        --query "Parameter.Value" --output text --region $Region 2>$null)
    if ($trendingRole -and $trendingRole -ne "None") {
        $lfResource = '{"DataLocation":{"CatalogId":"' + $Account + `
            '","ResourceArn":"arn:aws:s3:::' + $AnalyticsBucket + '/ott/searchevents/gold/"}}'
        aws lakeformation grant-permissions `
            --principal "DataLakePrincipalIdentifier=$trendingRole" `
            --resource $lfResource `
            --permissions DATA_LOCATION_ACCESS `
            --region $Region 2>&1 | Out-Null
        Write-Host " OK" -ForegroundColor Green
    } else {
        Write-Host ""
        Write-Warn "Trending role SSM param /sdlf/pipeline/rRole/ott-mainTR not found — skipping LF grant"
    }

    # 11. Monitoring dashboards + alarms (fully SSM-defaulted)
    Deploy-Stack "sdlf-pipeline-ott-monitoring" "$Tpl\pipeline-ott-monitoring.yaml"

    # 12. Dashboard hosting — private S3 bucket + CloudFront (OAC). The Trending
    # Lambda's write_dashboard() publishes the search-analytics dashboard here.
    Deploy-Stack "sdlf-pipeline-ott-dashboard" "$Tpl\pipeline-ott-dashboard.yaml"

    Write-OK "All 12 stacks deployed"
}

# ── Ingest trigger + Stage A → B → DQ wait ────────────────────────────────────
if (-not $SkipIngest -and -not $AnalyticsOnly) {
    Write-Step "Ingest trigger"

    # Verify source file exists
    $srcCheck = aws s3 ls "s3://$RawBucket/$IngestSrcKey" --region $Region 2>&1
    if ($LASTEXITCODE -ne 0 -or -not $srcCheck) {
        Write-Fail "Source file not found: s3://$RawBucket/$IngestSrcKey"
    }

    # Record trigger time before the copy so Wait-SM filters only new executions
    $triggerTime = (Get-Date).AddSeconds(-5)

    Write-Host "  s3://$RawBucket/$IngestSrcKey"
    Write-Host "  -> s3://$RawBucket/$IngestDestKey"
    aws s3 cp "s3://$RawBucket/$IngestSrcKey" "s3://$RawBucket/$IngestDestKey" `
        --region $Region | Out-Null
    Write-OK "Raw file copied → partition 20220615"

    Write-Step "Stage A (routing)"
    $execA = Wait-SM -Arn $SmAArn -Label "Stage A" -AfterTime $triggerTime -TimeoutSec 120

    Write-Step "Stage B (Glue ETL, ~26 min)"
    $execB = Wait-SM -Arn $SmBArn -Label "Stage B" -AfterTime $triggerTime -TimeoutSec 2700

    Write-Step "Data Quality SM"
    $execDQ = Wait-SM -Arn $SmDQArn -Label "DQ" -AfterTime $triggerTime -TimeoutSec 600
}

# ── Analytics Lambdas ─────────────────────────────────────────────────────────
if (-not $SkipIngest -or $AnalyticsOnly) {
    Write-Step "Analytics Lambdas"

    $payloadPath = "C:\tmp\ott-payload.json"
    $responsePath = "C:\tmp\ott-response.json"
    if (-not (Test-Path C:\tmp)) { New-Item -ItemType Directory C:\tmp | Out-Null }
    [System.IO.File]::WriteAllText($payloadPath,
        '{"source":"ott-pipeline","detail-type":"Manual Trigger"}',
        [System.Text.Encoding]::UTF8)

    function Invoke-Lambda {
        param([string]$Name, [string]$Payload, [string]$Response, [switch]$Async)
        if ($Async) {
            $out = aws lambda invoke --function-name $Name --region $Region `
                --invocation-type Event --payload fileb://$Payload $Response 2>&1
        } else {
            # --cli-read-timeout 0 = no CLI-level timeout; Lambda's own timeout applies
            $out = aws lambda invoke --function-name $Name --region $Region `
                --cli-read-timeout 0 --payload fileb://$Payload $Response 2>&1
        }
        if ($LASTEXITCODE -ne 0) { return $null }
        if (-not (Test-Path $Response)) { return $null }
        return Get-Content $Response -Raw | ConvertFrom-Json
    }

    # Content Gap (~3 min, 5 sequential Athena queries)
    Write-Host "  invoking sdlf-ott-mainCG-report ..." -NoNewline
    $cg = Invoke-Lambda -Name sdlf-ott-mainCG-report -Payload $payloadPath -Response $responsePath
    if (-not $cg) {
        Write-Host ""; Write-Warn "Content Gap invocation failed (check CloudWatch logs)"
    } elseif ($cg.PSObject.Properties.Name -contains "errorMessage") {
        Write-Host ""; Write-Warn "Content Gap error: $($cg.errorMessage)"
    } else {
        Write-Host " OK" -ForegroundColor Green
        if ($cg.errors -gt 0) { Write-Warn "  $($cg.errors) query error(s)" }
        else { Write-OK "  reports: $($cg.reports | ConvertTo-Json -Compress)" }
        if ($cg.report_url) { Write-Host "  dashboard: $($cg.report_url)" }
    }

    # Trending (~2 min, week-over-week Athena queries)
    Write-Host "  invoking sdlf-ott-mainTR-report ..." -NoNewline
    $tr = Invoke-Lambda -Name sdlf-ott-mainTR-report -Payload $payloadPath -Response $responsePath
    if (-not $tr) {
        Write-Host ""; Write-Warn "Trending invocation failed (check CloudWatch logs)"
    } elseif ($tr.PSObject.Properties.Name -contains "errorMessage") {
        Write-Host ""; Write-Warn "Trending error: $($tr.errorMessage)"
    } else {
        Write-Host " OK" -ForegroundColor Green
        if ($tr.errors -gt 0) { Write-Warn "  $($tr.errors) error(s)" }
        else { Write-OK "  mode:$($tr.mode)  unknown_targets:$($tr.reports.trending_unknown)" }
    }

    # LUT Refresh: async (15-min Lambda; results visible in next Stage B run)
    Write-Host "  invoking sdlf-ott-mainLUT-refresh (async) ..." -NoNewline
    Invoke-Lambda -Name sdlf-ott-mainLUT-refresh -Payload $payloadPath -Response $responsePath -Async | Out-Null
    Write-Host " fired" -ForegroundColor Green
    Write-OK "  LUT Refresh running in background (check CloudWatch logs for results)"
}

# ── Summary ───────────────────────────────────────────────────────────────────
Write-Step "Summary"

# Genre distribution from curated table
Write-Host "  querying curated genre distribution ..."
$sql = "SELECT derived_genre, COUNT(*) AS cnt FROM fpt_ott_searchevents_analytics.curated GROUP BY derived_genre ORDER BY cnt DESC"
$qid = (aws athena start-query-execution --query-string $sql `
    --result-configuration "OutputLocation=s3://$AthenaBucket/pipeline-summary/" `
    --region $Region --output text --query "QueryExecutionId" 2>&1)

$deadline = (Get-Date).AddSeconds(60)
while ((Get-Date) -lt $deadline) {
    $st = (aws athena get-query-execution --query-execution-id $qid `
        --region $Region --output text --query "QueryExecution.Status.State" 2>&1)
    if ($st -eq "SUCCEEDED") { break }
    if ($st -in @("FAILED","CANCELLED")) { Write-Warn "Athena query $st"; break }
    Start-Sleep 5
}

if ($st -eq "SUCCEEDED") {
    $rows = (aws athena get-query-results --query-execution-id $qid `
        --region $Region --output json 2>&1 | ConvertFrom-Json).ResultSet.Rows |
        Select-Object -Skip 1  # skip header
    $total = ($rows | ForEach-Object { [long]$_.Data[1].VarCharValue } | Measure-Object -Sum).Sum
    Write-Host ""
    Write-Host ("  {0,-16} {1,10}  {2,6}" -f "Genre","Count","%")
    Write-Host ("  {0,-16} {1,10}  {2,6}" -f "-----","-----","---")
    foreach ($r in $rows) {
        $g = $r.Data[0].VarCharValue
        $c = [long]$r.Data[1].VarCharValue
        $p = if ($total -gt 0) { "{0:F1}" -f (100.0 * $c / $total) } else { "0.0" }
        Write-Host ("  {0,-16} {1,10}  {2,5}%" -f $g, $c, $p)
    }
    Write-Host ""
    Write-Host ("  Total: {0:N0} rows" -f $total)
}

Write-Host ""
Write-OK "Pipeline run complete."

# ── Post-deploy LF grants + contract test ─────────────────────────────────────
# Run after every deploy so newly-created tables get codebuild access (next
# CFN deploy won't 403 on `Required Alter`) and end-user contracts are smoke-tested.
if (-not $Status) {
    Write-Step "Post-deploy: LF grants"
    & python "$Repo\scripts\lf_grants.py" --apply
    if ($LASTEXITCODE -ne 0) { Write-Warn "LF grants helper reported failures" }

    Write-Step "Post-deploy: contract test"
    $env:OTT_API_KEY = (aws ssm get-parameter --name "/sdlf/ott/api-key/prod" `
        --query "Parameter.Value" --output text --region $Region)
    & python "$Repo\scripts\contract_test.py"
    if ($LASTEXITCODE -ne 0) { Write-Fail "Contract test FAILED — production behavior regressed" }
    Write-OK "All 16 contract assertions passed."
}
