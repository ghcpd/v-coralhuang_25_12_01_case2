# Ensure venv, install dev deps, run pytest with coverage
$ErrorActionPreference = "Stop"

$root = Split-Path -Parent $MyInvocation.MyCommand.Path
$venv = Join-Path $root ".venv"
$python = Join-Path $venv "Scripts\python.exe"

if (!(Test-Path $python)) {
  Write-Host "Creating virtual environment..."
  python -m venv $venv
}

& $python -m pip install --upgrade pip
& $python -m pip install -r (Join-Path $root "requirements-dev.txt")

Write-Host "Running tests with coverage..."
& $python -m pytest -q --cov=$root/src --cov-report=term-missing
if ($LASTEXITCODE -ne 0) { exit 1 } else { exit 0 }
