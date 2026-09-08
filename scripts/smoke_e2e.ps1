# Smoke e2e (Windows): one tenant, short window, re-run for idempotency check.
$ErrorActionPreference = "Stop"
$Root = Split-Path -Parent $PSScriptRoot
Set-Location $Root

$Tenant = if ($env:SMOKE_TENANT) { $env:SMOKE_TENANT } else { "sv" }
$Start = if ($env:SMOKE_START) { $env:SMOKE_START } else { "2025-03-01" }
$End = if ($env:SMOKE_END) { $env:SMOKE_END } else { "2025-03-07" }
$GoldDir = "data/gold/$Tenant/daily_metrics_by_delivery_type"

function Invoke-SmokeRun([string]$Label) {
    Write-Host "==> smoke run ($Label) tenant=$Tenant $Start..$End"
    docker compose --profile smoke run --rm pipeline-smoke `
        python3 -m saas_pipeline `
        --env dev `
        --tenant $Tenant `
        --start-date $Start `
        --end-date $End `
        --layer all
    if ($LASTEXITCODE -ne 0) { throw "pipeline failed ($Label)" }
}

function Get-GoldParquetCount {
    if (-not (Test-Path $GoldDir)) { return 0 }
    return @(Get-ChildItem -Path $GoldDir -Recurse -Filter *.parquet -ErrorAction SilentlyContinue).Count
}

Write-Host "==> smoke build"
docker compose build pipeline
if ($LASTEXITCODE -ne 0) { throw "build failed" }

Invoke-SmokeRun "first"
$first = Get-GoldParquetCount
if ($first -lt 1) { throw "FAIL: expected Gold parquet under $GoldDir" }

Invoke-SmokeRun "second-idempotent"
$second = Get-GoldParquetCount
Write-Host "gold parquet files first=$first second=$second"
if ($second -lt 1) { throw "FAIL: Gold missing after second run" }

Write-Host "OK smoke e2e + idempotent re-run for tenant=$Tenant"
