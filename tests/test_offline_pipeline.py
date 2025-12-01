import os
import json
import subprocess
import runpy
import sys
import io
from contextlib import redirect_stdout, redirect_stderr
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src" / "pipeline_runner.py"
PIPELINE = ROOT / "pipeline.json"
STATE = ROOT / "state"
OUTPUT = ROOT / "data" / "output"
WORK = ROOT / "data" / "work"


def run(cmd: list[str]):
    # Run pipeline_runner in-process to gather coverage
    assert cmd[0] == "python"
    script = cmd[1]
    argv = [script] + cmd[2:]
    old_argv = sys.argv
    sys.argv = argv
    fstdout = io.StringIO()
    fstderr = io.StringIO()
    try:
        with redirect_stdout(fstdout), redirect_stderr(fstderr):
            runpy.run_path(script, run_name="__main__")
    finally:
        sys.argv = old_argv
    out = fstdout.getvalue()
    err = fstderr.getvalue()
    if err:
        out += err
    return out


def test_first_run_produces_output(tmp_path):
    # Clean output/state for a fresh run
    for p in [STATE, OUTPUT, WORK]:
        p.mkdir(parents=True, exist_ok=True)
        # remove files inside
        for child in p.glob("**/*"):
            try:
                if child.is_file():
                    child.unlink()
            except Exception:
                pass

    stdout = run(["python", str(SRC), "--pipeline", str(PIPELINE), "--run-id", "t1"])
    # Uppercase stage produces result.txt
    result = OUTPUT / "result.txt"
    assert result.exists(), "result.txt should exist after first run"

    # State files exist and parse
    run_state = STATE / "run_t1.json"
    metrics = STATE / "metrics_t1.json"
    assert run_state.exists() and metrics.exists()
    for f in [run_state, metrics]:
        with open(f, "r", encoding="utf-8") as fp:
            json.load(fp)


def test_second_run_skips_stages():
    stdout = run(["python", str(SRC), "--pipeline", str(PIPELINE), "--run-id", "t2"])
    # Expect SKIP messages in stdout for both stages
    assert "[SKIP] stage_copy" in stdout
    assert "[SKIP] stage_upper" in stdout


def test_checkpoint_resume():
    # Simulate interruption mid-way by writing a progress file with offset
    progress = STATE / "progress_stage_upper.json"
    with open(progress, "w", encoding="utf-8") as fp:
        json.dump({"lineOffset": 1}, fp)
    # Remove completion marker to force execution
    marker = OUTPUT / ".stage_upper.done"
    if marker.exists():
        marker.unlink()
    # Run and verify it completes from offset (no assertion on content, but ensure completion marker exists)
    stdout = run(["python", str(SRC), "--pipeline", str(PIPELINE), "--run-id", "t3"])
    assert "[DONE] stage_upper" in stdout
    assert marker.exists(), "Completion marker should be written on success"


def test_retry_backoff(tmp_path):
    # Prepare a temporary pipeline that uses a flaky stage
    flaky_pipeline = tmp_path / 'flaky_pipeline.json'
    pipeline_spec = {
        "name": "flaky_demo",
        "version": "1.0.0",
        "stages": [
            {
                "name": "stage_flaky",
                "processor": str(ROOT / 'bin' / 'stage_flaky.py'),
                "inputs": [],
                "outputDir": str(ROOT / 'data' / 'work'),
                "idempotency": {"enabled": False},
                "checkpoint": {"enabled": False},
                "retry": {"maxAttempts": 3, "baseDelay": 0.01, "jitter": 0.0}
            }
        ]
    }
    with open(flaky_pipeline, 'w', encoding='utf-8') as fp:
        json.dump(pipeline_spec, fp)

    # Ensure attempts file is reset
    flaky_attempts = ROOT / 'state' / 'flaky_attempts.json'
    if flaky_attempts.exists():
        flaky_attempts.unlink()

    stdout = run(["python", str(SRC), "--pipeline", str(flaky_pipeline), "--run-id", "t_retry"]) 
    # One successful run, and the attempts file should reflect retries (attempts >= 2)
    assert flaky_attempts.exists()
    with open(flaky_attempts, 'r', encoding='utf-8') as fp:
        data = json.load(fp)
    assert int(data.get('attempts', 0)) >= 2, "Stage should have retried at least once"


def test_stage_lock_failure(tmp_path):
    # Simulate a lock already present to ensure runner detects and fails
    locks_dir = ROOT / 'locks'
    locks_dir.mkdir(parents=True, exist_ok=True)
    lock = locks_dir / 'stage_copy.lock'
    # create lock dir
    lock.mkdir(parents=True, exist_ok=True)
    try:
        # Create a temporary pipeline spec that sets idempotency disabled so the stage will run
        pipeline_path = tmp_path / 'lock_pipeline.json'
        spec = {
            "name": "lock_demo",
            "version": "1.0",
            "stages": [
                {"name": "stage_copy", "processor": str(ROOT / 'bin' / 'stage_copy.py'), "inputs": [str(ROOT / 'data' / 'input' / 'sample.txt')], "outputDir": str(ROOT / 'data' / 'work'), "idempotency": {"enabled": False}, "checkpoint": {"enabled": False}}
            ]
        }
        with open(pipeline_path, 'w', encoding='utf-8') as fp:
            json.dump(spec, fp)
        stdout = run(["python", str(SRC), "--pipeline", str(pipeline_path), "--run-id", "t_lock"]) 
        assert "[LOCK_FAIL] stage_copy" in stdout or "[FAIL] stage_copy" in stdout
    finally:
        # cleanup
        try:
            lock.rmdir()
        except Exception:
            pass
