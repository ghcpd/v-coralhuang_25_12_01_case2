# Task: Offline, Network-Isolated Pipeline (Concise)

Design a single-machine offline batch pipeline with staged processing, idempotent execution, checkpointed recovery, local metrics/audit, resource governance, and zero network I/O.

---
## Goal
Resumable multi-stage processing for text/bulk files; skip repeated work; provide automated validation via pytest and one-click `run_tests`.

---
## Current Problems (from backend_issue_3)
- No unified orchestration; scripts are scattered
- No idempotency; repeated runs duplicate or corrupt outputs
- No checkpoints; interruptions force full reruns
- No retry/backoff for transient local failures (locks/IO)
- No resource quotas; local CPU/memory/IO contention
- Weak observability; lack of structured state/metrics
- No tamper-evident audit; hard to reconstruct actions
- Strict offline constraint: no HTTP/RPC/DB/queues/cloud

---
## New Features
1. JSON pipeline declaration (ordered stages, inputs/outputs)
2. Stage idempotency: `hash(inputs + processorVersion + params)`
3. Checkpointed recovery (line/chunk offsets)
4. Retry with backoff (max attempts, base delay, jitter; retryable vs non-retryable)
5. Resource quotas (CPU cores, memory cap, IO concurrency)
6. Local IPC & file locks to prevent conflicts
7. Audit & observability (structured logs, JSON metrics; optional hash chain)
8. Strict offline guard (stdlib only; optional static validation)
9. Atomic writes (tmp → `os.replace`)

---
## Deliverables
- Core: `pipeline.json`, `src/pipeline_runner.py`, `bin/stage_copy.py`, `bin/stage_upper.py`, data dirs (`input/work/output/state/locks`), `run_demo.ps1`, `.gitignore`, `requirements.txt`, `Prompt.md`
- Tests: `tests/test_offline_pipeline.py`, `requirements-dev.txt`, `run_tests.ps1`, `run_tests.sh` (venv, install, run pytest with coverage)
- Agent must author tests/scripts; `run_tests` exits 0 when features implemented

---
## Implementation Essentials
- Load & validate spec; iterate stages
- Compute idempotency key: per-input SHA256 + processor mtime → combined SHA256
- Skip if key matches and completion marker `outputDir/.<stage>.done` exists
- Execute processors via subprocess (inject `PIPELINE_*` env vars)
- Checkpoint: read/write `state/progress_<stage>.json` with `lineOffset`
- Atomic state: write `*.tmp` then `os.replace`
- On failure: record error and stop remaining stages
- State files: `run_<id>.json`, `stage_<name>.json`, `metrics_<id>.json`, `progress_<stage>.json`
- Offline guard: forbid `requests`, `socket`, `http.client`, `urllib`, networking in `asyncio`

---
## Success Criteria
- First run produces uppercase output
- Second run skips both stages (idempotency)
- Simulated interruption resumes from offset (checkpoint)
- Retry/backoff handles transient local failure deterministically
- No networking imports present
- State JSONs parse and reflect accurate statuses/timing
- `run_tests.ps1` / `.sh` pass with coverage ≥ 70%
- `run_tests` is idempotent (consecutive runs consistent, no side effects)

---
## Usage
```powershell
python .\src\pipeline_runner.py --pipeline .\pipeline.json --run-id demo1
python .\src\pipeline_runner.py --pipeline .\pipeline.json --run-id demo2  # skip

./run_tests.ps1  # one-click tests
```

---
## Maintenance
- Use tmp+replace pattern for all state writes
- Only write completion marker on success
- Keep implementation aligned with this spec; preserve offline guarantees

