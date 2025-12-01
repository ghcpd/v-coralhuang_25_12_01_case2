#!/usr/bin/env bash
set -euo pipefail
root="$(cd "$(dirname "$0")" && pwd)"
venv="$root/.venv"
python_bin="$venv/bin/python"

if [[ ! -x "$python_bin" ]]; then
  python3 -m venv "$venv"
fi

"$python_bin" -m pip install --upgrade pip
"$python_bin" -m pip install -r "$root/requirements-dev.txt"

"$python_bin" -m pytest -q --cov="$root/src" --cov-report=term-missing
