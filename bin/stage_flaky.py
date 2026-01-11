import os
import sys
import json
from pathlib import Path

# A flaky stage: fail first time, succeed second time. Track attempts in state/flaky_attempts.json
out_dir = os.environ.get('PIPELINE_OUTPUT_DIR')
if not out_dir:
    print("Missing PIPELINE_OUTPUT_DIR", file=sys.stderr)
    exit(2)

state_dir = Path('state')
state_dir.mkdir(parents=True, exist_ok=True)
attempts_file = state_dir / 'flaky_attempts.json'
attempts = 0
if attempts_file.exists():
    try:
        with open(attempts_file, 'r', encoding='utf-8') as f:
            attempts = int(json.load(f).get('attempts', 0))
    except Exception:
        attempts = 0

attempts += 1
with open(attempts_file, 'w', encoding='utf-8') as f:
    json.dump({'attempts': attempts}, f)

if attempts < 2:
    print("Simulated transient failure", file=sys.stderr)
    exit(10)

print("flaky stage succeeded")
exit(0)
