# Offline, Network-Isolated Pipeline (Demo)

This repository demonstrates a single-machine offline, staged batch pipeline with idempotency, checkpointed recovery, retry/backoff, local resource governance primitives (locks), and local observability.

The goal is to provide resumable multi-stage processing for text/bulk files with automation tests and a one-click test runner.

---

## Quick Start (Windows PowerShell)

1. Create / activate a virtual environment (this step is optional but recommended):

```powershell
python -m venv .venv
& .\.venv\Scripts\Activate.ps1
```

2. Install dev requirements and run tests:

```powershell
& .\.venv\Scripts\python.exe -m pip install --upgrade pip
& .\.venv\Scripts\python.exe -m pip install -r requirements-dev.txt
./run_tests.ps1
```

3. Run the demo pipeline manually:

```powershell
python .\src\pipeline_runner.py --pipeline .\pipeline.json --run-id demo1
# run again to observe idempotency skip
python .\src\pipeline_runner.py --pipeline .\pipeline.json --run-id demo2
```

(Bash versions of the above commands are available in `run_tests.sh` and are similar.)

---

## What this repository contains

- `pipeline.json`: pipeline spec with stages, processors, inputs, outputs, idempotency, and checkpoint configs.
- `src/pipeline_runner.py`: pipeline runner that validates pipeline, runs stages, supports idempotency, checkpoints, retries, simple locking and metrics/state file outputs.
- `bin/stage_copy.py`, `bin/stage_upper.py`: example processors used by the demo pipeline.
- `data/input`, `data/work`, `data/output`: sample input & work areas for stage outputs.
- `state/`: stores `run_<id>.json`, `stage_<name>.json`, `metrics_<id>.json`, `progress_<stage>.json`, etc.
- `tests/`: pytest based tests for the pipeline.
- `run_tests.ps1`, `run_tests.sh`: convenience scripts to run tests and gather coverage.

---

## Key Concepts & Features

- Idempotency
  - The runner computes an idempotency key using the SHA256 of input files + processor modification time. If the key matches and a `.stage_name.done` marker exists, the stage is skipped.

- Checkpoints
  - If enabled, processors can write `state/progress_<stage>.json` with `lineOffset` to allow resuming from where processing left off.
  - Processors are expected to use `PIPELINE_LINE_OFFSET` environment variable to resume processing.

- Retry & Backoff
  - Stages can configure a `retry` policy in the pipeline JSON. The runner performs exponential backoff and deterministic jitter based on config values.
  - Processors may support a simulated transient failure via `PIPELINE_SIMULATED_FAIL` for testing.

- Locks & Resource Governance
  - A simple per-stage lock directory (under `locks/`) is used to avoid concurrent runs from conflicting with each other.
  - This is intentionally lightweight so the solution remains standard-library-only and cross-platform.

- Audit & Observability
  - On completion/error, the runner writes `state/run_<id>.json` and per-stage `state/stage_<name>.json` JSONs, and `state/metrics_<id>.json` for metrics.
  - A `runHash` (SHA256) is included for minimal tamper evidence of run metadata.

- Offline Guard
  - The pipeline runner scans `src` and `bin` for known network-related imports (requests, socket, urllib, http.client, asyncio, etc.) and fails early if any are found to enforce offline-only operations.

- Atomic Writes
  - All state and checkpoint files use the `tmp` + `os.replace` pattern for atomicity.
  - Processors also write outputs (progress/targets) atomically where feasible.

---

## How to Verify Critical Scenarios

- First run should produce uppercase `data/output/result.txt` from `data/input/sample.txt`.
- Second run should skip both stages if pipeline is unchanged (idempotency).
- Checkpoint resume: create `state/progress_stage_upper.json` with a `lineOffset` value (e.g., `{"lineOffset": 1}`), remove `data/output/.stage_upper.done` and re-run the pipeline — the stage should resume processing from the offset.
- Retry & backoff: `pipeline.json` used in this repo includes `retry` config and `simulateFailAttempts` to simulate a transient failure and demonstrate retry behavior. Tests exercise that behavior deterministically.

---

## Engineering Notes

- Processors are executed as subprocesses and receive the following environment variables:
  - `PIPELINE_STAGE_NAME` — stage name
  - `PIPELINE_OUTPUT_DIR` — absolute output directory
  - `PIPELINE_LINE_OFFSET` — line offset for checkpoint/resume
  - `PIPELINE_SIMULATED_FAIL` — optional flag to simulate transient failure (test-only, controlled by `pipeline.json`)

- State files produced by the runner live under `state/` to keep the pipeline auditable:
  - `run_<id>.json` — run metadata
  - `stage_<name>.json` — per-stage metadata
  - `metrics_<id>.json` — aggregated metrics
  - `progress_<stage>.json` — progression from processors (if using checkpointing)

- To add new stages, update `pipeline.json` with `name`, `processor`, `inputs`, `outputDir`, and optional `idempotency`, `checkpoint`, `retry` keys.

---

## Contributing / Checklist for New Stage

1. Implement your processor in `bin/` and keep it standard-library-only (no network imports).
2. Add a `name`, `processor`, `inputs`, `outputDir` to `pipeline.json`.
3. If the stage must be resumable, set `checkpoint.enabled` to `true` and ensure your processor writes `state/progress_<stage>.json` periodically.
4. For idempotency, rely on the default SHA256 idempotency mechanism; be sure that any deterministic parameters are included in the processor behavior.
5. Add or update tests under `tests/` to ensure new stage behaves as expected.

---

## Support & Troubleshooting

- If tests fail and report network imports, inspect your code and any 3rd-party libs for unallowed imports (run `pip freeze` to review dependencies).
- To debug the pipeline runner, run it in verbose mode (wrap run call to see stdout/stderr from processors).
- Clean state by removing `state/*` and `data/output/*` before re-running tests or demos.

---

## License & Author

Demonstration / educational repo; feel free to adapt for your offline processing needs.

---

If you want an extended README with API reference for `pipeline.json` or a diagram, I can add it next.  
