# Offline Batch Pipeline

A single-machine, network-isolated batch pipeline with staged processing, idempotent execution, checkpointed recovery, and comprehensive observability.

## Features

- **Staged Processing**: Orchestrate multiple processing stages via `pipeline.json`
- **Idempotency**: Skip repeated stages using SHA256-based input/processor hashing
- **Checkpointed Recovery**: Resume interrupted stages from saved line offsets
- **Retry & Backoff**: Automatic exponential backoff with jitter for transient failures
- **File Locking**: Prevent concurrent stage execution via OS-level locks
- **Atomic State Writes**: Use tmp+replace pattern for crash-safe state updates
- **Structured Metrics**: JSON-based audit trail with per-run and per-stage metrics
- **Offline Guarantee**: Forbids network imports (`requests`, `socket`, `http.*`, etc.)
- **Zero Dependencies**: Uses Python standard library only

## Architecture

```
pipeline.json              - Stage definition and parameters
├── src/
│   └── pipeline_runner.py - Core orchestration engine
├── bin/
│   ├── stage_copy.py      - Example: copy input files
│   └── stage_upper.py     - Example: convert to uppercase with checkpoints
├── data/
│   ├── input/             - Read-only input files
│   ├── work/              - Intermediate outputs
│   └── output/            - Final results
├── state/                 - Persisted state (JSON)
│   ├── run_<id>.json      - Per-run execution state
│   ├── stage_<name>.json  - Per-stage metadata & idempotency key
│   ├── metrics_<id>.json  - Per-run aggregated metrics
│   ├── checkpoint_<name>.json - Checkpoint (line offset)
│   └── progress_<name>.json   - In-flight progress
└── locks/                 - File-based locks (created during execution)
```

## Setup

### Prerequisites
- Python 3.7+
- PowerShell 5.1+ (for Windows) or Bash (for Linux/macOS)

### Installation

```bash
# Clone and navigate
cd /path/to/offline-pipeline

# Create virtual environment
python -m venv .venv

# Activate venv
# On Windows:
.\.venv\Scripts\Activate.ps1
# On Linux/macOS:
source .venv/bin/activate

# Install dev dependencies (for testing)
pip install -r requirements-dev.txt
```

## Usage

### Run Demo

**Windows (PowerShell)**:
```powershell
.\run_demo.ps1
```

**Linux/macOS (Bash)**:
```bash
bash run_tests.sh demo
```

This executes the pipeline twice:
1. **First run** (`demo1`): Processes all stages, produces output
2. **Second run** (`demo2`): Skips all stages (idempotency)

### Run Tests

**Windows (PowerShell)**:
```powershell
.\run_tests.ps1
```

**Linux/macOS (Bash)**:
```bash
bash run_tests.sh
```

Tests verify:
- ✓ First run produces output
- ✓ Second run skips stages (idempotency)
- ✓ Checkpoint resume works
- ✓ No network imports present
- ✓ State files are valid JSON
- ✓ Coverage ≥ 70%

### Manual Execution

```bash
python src/pipeline_runner.py --pipeline pipeline.json --run-id my_run
```

Outputs:
- `state/run_my_run.json` - Execution state
- `state/metrics_my_run.json` - Aggregated metrics
- `data/output/result.txt` - Final results (if successful)

## Configuration

Edit `pipeline.json` to define stages:

```json
{
  "name": "my_pipeline",
  "version": "1.0.0",
  "stages": [
    {
      "name": "stage_copy",
      "processor": "bin/stage_copy.py",
      "inputs": ["data/input/sample.txt"],
      "outputDir": "data/work",
      "idempotency": { "enabled": true },
      "checkpoint": { "enabled": false }
    },
    {
      "name": "stage_upper",
      "processor": "bin/stage_upper.py",
      "inputs": ["data/work/sample.txt"],
      "outputDir": "data/output",
      "idempotency": { "enabled": true },
      "checkpoint": { "enabled": true, "lineInterval": 50 }
    }
  ]
}
```

### Stage Configuration

- **name**: Unique stage identifier
- **processor**: Path to Python script to execute
- **inputs**: List of input file paths
- **outputDir**: Directory for stage outputs
- **idempotency.enabled**: Skip if inputs/processor unchanged
- **checkpoint.enabled**: Enable line-offset recovery
- **checkpoint.lineInterval**: Checkpoint frequency (lines)

## State Files

### `state/run_<id>.json`
```json
{
  "runId": "demo1",
  "pipeline": "offline_pipeline_demo",
  "version": "1.0.0",
  "startedAt": "2025-12-01T10:00:00.000000",
  "endedAt": "2025-12-01T10:00:05.123456",
  "state": "completed"
}
```

### `state/metrics_<id>.json`
```json
{
  "runId": "demo1",
  "timestamp": "2025-12-01T10:00:05.123456",
  "stages": [
    { "stage": "stage_copy", "status": "ok" },
    { "stage": "stage_upper", "status": "ok" }
  ],
  "totalStages": 2,
  "okStages": 2,
  "skippedStages": 0,
  "failedStages": 0
}
```

### `state/stage_<name>.json`
```json
{
  "lastStatus": "ok",
  "lastDurationSec": 0.123,
  "idempotencyKey": "abc123def456...",
  "lastError": null
}
```

### `state/checkpoint_<name>.json`
```json
{
  "lineOffset": 150
}
```

## Offline Guarantee

The pipeline strictly forbids network-related imports:
- `requests`
- `socket`, `http.client`, `urllib.*`
- `asyncio` (networking context)
- `aiohttp`, `paramiko`, `ftplib`, `smtplib`, etc.

**Enforcement**: Before executing each processor, `pipeline_runner.py` scans for forbidden imports and fails if found.

## Idempotency

Stages skip execution if:
1. Idempotency is enabled in `pipeline.json`
2. All inputs are unchanged (SHA256 hash matches)
3. Processor script is unchanged (mtime-based version)
4. Completion marker (`.stage_name.done`) exists in output directory

### Idempotency Key Calculation
```
SHA256( SHA256(input1) | SHA256(input2) | ... | processor_version )
```

## Checkpointing

If a stage is interrupted, it can resume from the last checkpoint:
- Processor writes `state/progress_<stage>.json` with `lineOffset`
- Next run reads `PIPELINE_LINE_OFFSET` env var and skips already-processed lines
- Completion marker must be removed to trigger re-execution

## Error Handling

On stage failure:
1. Error recorded in `state/stage_<name>.json`
2. Completion marker **not** written
3. Next run will retry the stage
4. Pipeline stops; remaining stages skipped

## File Locking

File-based locks prevent concurrent execution of the same stage:
- Lock acquired at `locks/<stage>.lock`
- Timeout: 10 seconds
- Lock released on completion (success or failure)

## Retry & Backoff

Failed stages are retried with exponential backoff:
- Max attempts: 3
- Base delay: 0.5 seconds
- Jitter: ±10% random variance
- Formula: `delay = base * (2 ^ (attempt - 1)) + jitter`

## Atomic Writes

All state updates use tmp+replace pattern:
```python
tmp = path + ".tmp"
with open(tmp, 'w') as f:
    json.dump(data, f)
os.replace(tmp, path)  # Atomic on POSIX & Windows
```

This ensures:
- No partial/corrupted JSON on crash
- Safe recovery from interruptions

## Monitoring

Check metrics after each run:
```bash
cat state/metrics_demo1.json | python -m json.tool
```

Check stage state:
```bash
cat state/stage_stage_upper.json | python -m json.tool
```

## Cleanup

To reset state and outputs:
```bash
# Remove state and output files (keep input)
rm -r state/* locks/* data/work/* data/output/*

# Alternatively, use run_tests.ps1 cleanup logic
```

## Troubleshooting

### Pipeline hangs or takes long time
- Check `locks/` for stale lock files
- Verify processor scripts don't have infinite loops
- Use `PIPELINE_LINE_OFFSET` to resume from checkpoint

### "Offline violation: forbidden import"
- Check processor script for `import requests`, `import socket`, etc.
- Use only stdlib modules

### State files are corrupted
- This should not happen with atomic writes
- Delete `.tmp` files manually if process crashed mid-write
- Retry run with `--run-id <new_id>` to use fresh state

### Checkpoint not working
- Ensure `checkpoint.enabled: true` in `pipeline.json`
- Verify processor writes `state/progress_<stage>.json`
- Check `PIPELINE_LINE_OFFSET` env var is received by processor

## Contributing

When adding new stages:
1. Accept inputs via `sys.argv[1:]`
2. Read output directory from `PIPELINE_OUTPUT_DIR` env var
3. Read line offset from `PIPELINE_LINE_OFFSET` (if resuming)
4. Write progress to `state/progress_<stage>.json` (if checkpointing)
5. Use only stdlib; no network I/O
6. Exit with 0 on success, non-zero on failure
7. Use atomic writes (tmp+replace)

## License

Internal project.
