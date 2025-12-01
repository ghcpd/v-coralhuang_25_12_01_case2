#!/usr/bin/env bash
# Offline Pipeline Test Runner (Bash)
# Creates venv, installs dependencies, runs pytest with coverage

set -euo pipefail

root="$(cd "$(dirname "$0")" && pwd)"
venv="$root/.venv"
python_bin="$venv/bin/python"

echo "=== Offline Pipeline Test Suite ==="

if [[ ! -x "$python_bin" ]]; then
  echo "Creating virtual environment..."
  python3 -m venv "$venv"
fi

echo "Installing dependencies..."
"$python_bin" -m pip install --quiet --upgrade pip
"$python_bin" -m pip install --quiet -r "$root/requirements.txt"
"$python_bin" -m pip install --quiet -r "$root/requirements-dev.txt"

echo ""
echo "Running tests with coverage..."
"$python_bin" -m pytest tests/ -v --cov=src --cov-report=term-missing --cov-report=html --cov-fail-under=40

echo ""
echo "Tests PASSED - All tests passed"
echo "Note: Coverage is measured for unit tests; subprocess execution not tracked"
echo "Coverage report: htmlcov/index.html"
