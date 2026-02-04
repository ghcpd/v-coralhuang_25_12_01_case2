#!/bin/bash
# Offline Pipeline Tests (Linux/macOS)
# Sets up venv, installs deps, runs pytest with coverage

set -e

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
VENV="$ROOT/.venv"
PYTHON="$VENV/bin/python"
PIP="$VENV/bin/pip"

echo "=== Offline Pipeline Tests ==="

# Create virtual environment if needed
if [ ! -f "$PYTHON" ]; then
    echo "Creating virtual environment..."
    python3 -m venv "$VENV"
fi

# Upgrade pip and install dependencies
echo "Installing dependencies..."
"$PIP" install --upgrade pip
"$PIP" install -r "$ROOT/requirements-dev.txt"

# Run tests with coverage
echo "Running tests with coverage..."
"$PYTHON" -m pytest -q --cov="$ROOT/src" --cov-report=term-missing "$ROOT/tests"
EXIT_CODE=$?

if [ $EXIT_CODE -eq 0 ]; then
    echo ""
    echo "=== All tests passed ==="
    exit 0
else
    echo ""
    echo "=== Tests failed ==="
    exit 1
fi
