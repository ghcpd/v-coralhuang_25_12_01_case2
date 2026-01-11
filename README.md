# Offline Pipeline Demo

This repository implements a single-machine offline batch pipeline with idempotent stages, checkpointing, retry/backoff, local locks, JSON metrics, and audit logs.

Usage
- Run pipeline:
```
python src/pipeline_runner.py --pipeline pipeline.json --run-id demo1
```

- Run tests:
PowerShell:
```
./run_tests.ps1
```
Unix:
```
./run_tests.sh
```

Key Details
- Idempotency: per-stage idempotency keys based on input file hashes and processor mtime
- Checkpointing: support line-based resume via `state/progress_<stage>.json`
- Retry/backoff: stage-level configuration `retry` with `maxAttempts`, `baseDelay`, and `jitter`
- Locks: stage locks created under `locks/<stage>.lock` to avoid concurrent stage executions
- Audit: JSON-lines appended to `state/pipeline.log` for start/stop events
- Atomic writes: state files use tmp files and `os.replace` for atomic replacement

Constraints
- Strictly offline: no HTTP or network libraries are used or imported.

Files
- `src/pipeline_runner.py`: main engine
- `bin/` processors: `stage_copy.py`, `stage_upper.py`, `stage_flaky.py`
- `tests/`: pytest tests
- `pipeline.json`: demo pipeline
