# Offline Pipeline Demo
# Runs pipeline twice: first for processing, second to verify idempotency

$ErrorActionPreference = "Stop"

$root = Split-Path -Parent $MyInvocation.MyCommand.Path
$python = "python"

Write-Host "=== Offline Pipeline Demo ===" -ForegroundColor Cyan

# Clean up state and output for fresh demo
Write-Host "Cleaning up previous run state..."
$stateDirs = @("state", "locks", "data/work", "data/output")
foreach ($dir in $stateDirs) {
    $path = Join-Path $root $dir
    if (Test-Path $path) {
        Get-ChildItem -Path $path -Force | Remove-Item -Force -Recurse
    }
}

# First run: process pipeline
Write-Host "`n[1] First run (demo1) - process all stages..." -ForegroundColor Yellow
& $python "$root\src\pipeline_runner.py" --pipeline "$root\pipeline.json" --run-id demo1
if ($LASTEXITCODE -ne 0) {
    Write-Host "[FAIL] First run failed" -ForegroundColor Red
    exit 1
}

# Verify output
$result = Join-Path $root "data/output/result.txt"
if (!(Test-Path $result)) {
    Write-Host "[FAIL] Output file not created: $result" -ForegroundColor Red
    exit 1
}
Write-Host "[OK] Output file created: $(Get-Item $result | Select-Object -ExpandProperty FullName)" -ForegroundColor Green
Write-Host "Content preview:" -ForegroundColor Green
Get-Content $result -Head 5

# Second run: verify idempotency
Write-Host "`n[2] Second run (demo2) - verify idempotency..." -ForegroundColor Yellow
& $python "$root\src\pipeline_runner.py" --pipeline "$root\pipeline.json" --run-id demo2
if ($LASTEXITCODE -ne 0) {
    Write-Host "[FAIL] Second run failed" -ForegroundColor Red
    exit 1
}

# Check metrics
Write-Host "`n[Metrics] demo1:" -ForegroundColor Cyan
$metrics1 = Join-Path $root "state/metrics_demo1.json"
Get-Content $metrics1 | ConvertFrom-Json | ForEach-Object {
    Write-Host "  Total Stages: $($_.totalStages)"
    Write-Host "  OK: $($_.okStages), Skipped: $($_.skippedStages), Failed: $($_.failedStages)"
}

Write-Host "`n[Metrics] demo2:" -ForegroundColor Cyan
$metrics2 = Join-Path $root "state/metrics_demo2.json"
Get-Content $metrics2 | ConvertFrom-Json | ForEach-Object {
    Write-Host "  Total Stages: $($_.totalStages)"
    Write-Host "  OK: $($_.okStages), Skipped: $($_.skippedStages), Failed: $($_.failedStages)"
    if ($_.skippedStages -ne $_.totalStages) {
        Write-Host "[WARN] Not all stages were skipped in demo2 (idempotency check failed)" -ForegroundColor Yellow
    }
}

Write-Host "`n=== Demo Complete ===" -ForegroundColor Cyan
exit 0
