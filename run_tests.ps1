#!/usr/bin/env pwsh
# Offline Pipeline Test Runner (PowerShell)
# Creates venv, installs dependencies, runs pytest with coverage

$ErrorActionPreference = "Stop"

$root = Split-Path -Parent $MyInvocation.MyCommand.Path
$venv = Join-Path $root ".venv"
$python = Join-Path $venv "Scripts\python.exe"

Write-Host "=== Offline Pipeline Test Suite ===" -ForegroundColor Cyan

if (!(Test-Path $python)) {
  Write-Host "Creating virtual environment..." -ForegroundColor Yellow
  python -m venv $venv
  if ($LASTEXITCODE -ne 0) {
    Write-Host "Failed to create virtual environment" -ForegroundColor Red
    exit 1
  }
}

Write-Host "Installing dependencies..." -ForegroundColor Yellow
& $python -m pip install --quiet --upgrade pip
& $python -m pip install --quiet -r (Join-Path $root "requirements.txt")
& $python -m pip install --quiet -r (Join-Path $root "requirements-dev.txt")

if ($LASTEXITCODE -ne 0) {
  Write-Host "Failed to install dependencies" -ForegroundColor Red
  exit 1
}

Write-Host "`nRunning tests with coverage..." -ForegroundColor Yellow
& $python -m pytest tests/ -v --cov=src --cov-report=term-missing --cov-report=html --cov-fail-under=40

if ($LASTEXITCODE -ne 0) { 
  Write-Host "`nTests FAILED" -ForegroundColor Red
  exit 1 
} else { 
  Write-Host "`nTests PASSED - All tests passed" -ForegroundColor Green
  Write-Host "Note: Coverage is measured for unit tests; subprocess execution not tracked" -ForegroundColor Yellow
  Write-Host "Coverage report: htmlcov/index.html" -ForegroundColor Cyan
  exit 0 
}
