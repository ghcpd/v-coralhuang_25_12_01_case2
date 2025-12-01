# Offline, Network-Isolated Pipeline

Single-machine, staged batch pipeline with idempotent execution, checkpoints, retries with backoff, resource hints, file locks, audit log, atomic state writes, and strict offline guard.

## Features
- **JSON pipeline declaration** (`pipeline.json`)
- **Stage idempotency**: `sha256(inputs + processor mtime + params)` + completion marker
- **Checkpointed recovery**: `state/progress_<stage>.json` with `lineOffset`
- **Retries with backoff**: configurable attempts, delays, jitter, retryable exit codes
- **Resource governance (best-effort)**: CPU/memory hints and IO concurrency env vars
- **File locks**: `locks/<stage>.lock` guards concurrent runs
- **Audit & metrics**: `state/audit_<run>.jsonl` (hash chain), `state/metrics_<run>.json`
- **Offline guard**: static AST scan blocks banned imports (`requests`, `socket`, `urllib`, etc.)
- **Atomic writes**: state/marker files written via tmp + `os.replace`

## Layout
- `src/pipeline_runner.py` — Orchestrator
- `bin/stage_copy.py` — Copies input to work dir (atomic)
- `bin/stage_upper.py` — Uppercases text with checkpointing & transient failure simulation
- `pipeline.json` — Pipeline spec
- `data/` — `input/`, `work/`, `output/`
- `state/` — Run/stage/metrics/progress/audit artifacts
- `locks/` — Stage locks
- `tests/` — Pytest suite
- `run_demo.ps1` — Demo run
- `run_tests.ps1`, `run_tests.sh` — One-click tests

## Quickstart
```powershell
python .\src\pipeline_runner.py --pipeline .\pipeline.json --run-id demo1
python .\src\pipeline_runner.py --pipeline .\pipeline.json --run-id demo2  # idempotent skip
```

## Tests
```powershell
./run_tests.ps1  # Windows
# or
./run_tests.sh   # Linux/Mac
```

## Notes
- Runner injects env vars (`PIPELINE_*`) to processors.
- To simulate a transient error in `stage_upper`, set `params.simulateTransient: true` in pipeline or adjust tests.
- Outputs/state are ignored by git per `.gitignore`.
- No network libraries are allowed; the runner fails fast if they’re detected.
