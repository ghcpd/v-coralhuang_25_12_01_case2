#!/usr/bin/env pwsh
# Demo script for offline pipeline
# Creates sample input and runs the pipeline

$ErrorActionPreference = "Stop"

Write-Host "=== Offline Pipeline Demo ===" -ForegroundColor Cyan

# Ensure input file exists
$inputDir = "data\input"
$inputFile = "$inputDir\sample.txt"

if (!(Test-Path $inputFile)) {
    Write-Host "Creating sample input file..." -ForegroundColor Yellow
    New-Item -ItemType Directory -Force $inputDir | Out-Null
    1..100 | ForEach-Object { "line $_" } | Out-File $inputFile -Encoding utf8
    Write-Host "Created $inputFile with 100 lines" -ForegroundColor Green
}

Write-Host "`nRunning pipeline (first run)..." -ForegroundColor Yellow
python .\src\pipeline_runner.py --pipeline .\pipeline.json --run-id demo1

Write-Host "`n`nRunning pipeline again (should skip - idempotent)..." -ForegroundColor Yellow
python .\src\pipeline_runner.py --pipeline .\pipeline.json --run-id demo2

Write-Host "`n`n=== Results ===" -ForegroundColor Cyan
Write-Host "Output file:" -ForegroundColor Yellow
Get-Content data\output\result.txt | Select-Object -First 5
Write-Host "..." -ForegroundColor Gray
Write-Host "Total lines:" (Get-Content data\output\result.txt).Count -ForegroundColor Yellow

Write-Host "`nMetrics:" -ForegroundColor Yellow
Get-Content state\metrics_demo1.json | ConvertFrom-Json | Format-List

Write-Host "`n=== Demo Complete ===" -ForegroundColor Green
