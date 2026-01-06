# Offline Batch Pipeline Demo

This repository demonstrates a single-machine offline, network-isolated pipeline with staged processing, idempotency, checkpointed recovery, retry/backoff, lightweight locks, and local metrics/audit.

## Quick Start
- Run pipeline (first run - produces output):
```powershell
python .\src\pipeline_runner.py --pipeline .\pipeline.json --run-id demo1
```
- Run pipeline again (should skip stages due to idempotency):
```powershell
python .\src\pipeline_runner.py --pipeline .\pipeline.json --run-id demo2
```
- Run tests:
```powershell
./run_tests.ps1
```

## Features
- JSON pipeline declaration: `pipeline.json`
- Idempotency: per-stage idempotency key is written to `state/stage_<name>.json`
- Checkpointed recovery: `stage_upper` supports `lineOffset` checkpoints in `state/`
- Retry/backoff: subprocess failure exit code 10 indicates transient error and triggers retries with exponential backoff
- Locks: simple file-based lock in `locks/` to prevent concurrent stage runs
- Metrics: run and stage metrics are written to `state/` as JSON with atomic writes
- Strict offline guard: pipeline aborts if disallowed networking imports are detected in Python files

## Requirements
- Python 3.10+ (stdlib-only for core pipeline) + `pytest` and `pytest-cov` for running tests

## Maintenance
- All state writes are atomic (temp + `os.replace`) and a tamper-evident chain `stateHash` is maintained in stage JSONs.

## Notes
- Transient errors: processors can emit exit code 10 to request retry; processors may use environment variables: `PIPELINE_OUTPUT_DIR`, `PIPELINE_LINE_OFFSET`, `PIPELINE_STAGE_NAME`, `PIPELINE_CPU_LIMIT`.
