import os
import json
import subprocess
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src" / "pipeline_runner.py"
PIPELINE = ROOT / "pipeline.json"
STATE = ROOT / "state"
OUTPUT = ROOT / "data" / "output"
WORK = ROOT / "data" / "work"


def run(cmd: list[str]):
    proc = subprocess.run(cmd, capture_output=True, text=True)
    assert proc.returncode == 0, f"Command failed: {cmd}\nstdout: {proc.stdout}\nstderr: {proc.stderr}"
    return proc.stdout


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
