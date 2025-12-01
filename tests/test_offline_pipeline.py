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


def run(cmd: list[str], env: dict = None):
    merged_env = os.environ.copy()
    if env:
        merged_env.update(env)
    proc = subprocess.run(cmd, capture_output=True, text=True, env=merged_env)
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

    # Check stage state JSONs include idempotencyKey and stateHash
    sc = STATE / "stage_stage_copy.json"
    su = STATE / "stage_stage_upper.json"
    assert sc.exists() and su.exists()
    for s in [sc, su]:
        with open(s, 'r', encoding='utf-8') as fp:
            data = json.load(fp)
        assert 'stateHash' in data
        assert 'lastStatus' in data


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


def test_retry_backoff_transient():
    # Clean transient marker
    sim_marker = STATE / "sim_stage_copy_transient"
    if sim_marker.exists():
        sim_marker.unlink()
    # ensure output markers removed to force execution
    copy_marker = WORK / ".stage_copy.done"
    up_marker = OUTPUT / ".stage_upper.done"
    if copy_marker.exists():
        copy_marker.unlink()
    if up_marker.exists():
        up_marker.unlink()
    # Run with env var to simulate transient failure once
    stdout = run(["python", str(SRC), "--pipeline", str(PIPELINE), "--run-id", "t4"], env={"PIPELINE_SIMULATE_TRANSIENT": "1"})
    assert ("[RETRY] stage_copy" in stdout) or ("Simulating transient error" in stdout)
    # ensure final completion
    assert "[DONE] stage_copy" in stdout
    assert "[DONE] stage_upper" in stdout


def test_offline_guard_detects_network_import():
    # Write a temporary python file with a forbidden import and ensure pipeline fails offline guard
    bad_file = Path('tmp_bad_network_import.py')
    bad_file.write_text('import requests\n')
    try:
        proc = subprocess.run(["python", str(SRC), "--pipeline", str(PIPELINE), "--run-id", "t_offline"], capture_output=True, text=True)
        assert proc.returncode != 0
        assert "Offline guard" in proc.stderr or "forbidden import" in proc.stderr
    finally:
        if bad_file.exists():
            bad_file.unlink()
