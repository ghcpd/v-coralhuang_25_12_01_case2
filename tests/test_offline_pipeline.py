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


def test_retry_backoff(tmp_path):
    # Clean state/output/work
    for p in [STATE, OUTPUT, WORK]:
        p.mkdir(parents=True, exist_ok=True)
        for child in p.glob("**/*"):
            try:
                if child.is_file():
                    child.unlink()
            except Exception:
                pass

    # Run and expect retry message for stage_copy
    proc = subprocess.run(["python", str(SRC), "--pipeline", str(PIPELINE), "--run-id", "t_retry"], capture_output=True, text=True)
    assert proc.returncode == 0, f"Retry run failed: {proc.stderr}"
    assert "[RETRY] stage_copy" in proc.stdout or "Simulated transient failure" in proc.stderr


def test_no_network_imports():
    # Ensures we don't accidentally import network libraries
    banned = ['socket', 'requests', 'http.client', 'urllib', 'asyncio', 'aiohttp', 'urllib3']
    repo_root = ROOT
    found = []
    for p in repo_root.rglob('*.py'):
        # ignore virtualenv files
        if '.venv' in str(p):
            continue
        try:
            text = p.read_text(encoding='utf-8')
        except Exception:
            continue
        for b in banned:
            if f"import {b}" in text or f"from {b} import" in text:
                found.append((p, b))
    assert not found, f"Network imports found: {found}"


def test_inprocess_utils(monkeypatch, tmp_path):
    import importlib
    pr = importlib.import_module('src.pipeline_runner')

    # compute idempotency key changes when file content changes
    sample = ROOT / 'data' / 'input' / 'sample.txt'
    k1 = pr.compute_idempotency_key([str(sample)], 'bin/stage_copy.py')
    # write a small marker to input and recompute key
    sample.write_text(sample.read_text() + '\nmarker')
    k2 = pr.compute_idempotency_key([str(sample)], 'bin/stage_copy.py')
    assert k1 != k2

    # Test run state hashing
    rs = {
        'runId': 'x', 'pipeline': 'p', 'version': 'v', 'startedAt': 't', 'state': 'running'
    }
    import hashlib, json
    h = hashlib.sha256(json.dumps(rs, sort_keys=True).encode('utf-8')).hexdigest()
    rs['runHash'] = h
    tmp_run_file = tmp_path / 'run_x.json'
    pr.save_run_state('x', rs)
    assert os.path.exists(os.path.join('state', 'run_x.json'))


def test_inprocess_pipeline_run(monkeypatch, tmp_path):
    import importlib, sys
    pr = importlib.import_module('src.pipeline_runner')
    # monkeypatch subprocess.run to simulate success
    class Dummy:
        def __init__(self, returncode=0, stdout='', stderr=''):
            self.returncode = returncode
            self.stdout = stdout
            self.stderr = stderr

    def fake_run(cmd, env=None, capture_output=False, text=False):
        # simulate processors creating output via touching marker indirectly
        return Dummy(returncode=0, stdout='ok', stderr='')

    import subprocess, sys
    monkeypatch.setattr(subprocess, 'run', fake_run)
    # prepare argv
    monkeypatch.setattr(sys, 'argv', [sys.argv[0], '--pipeline', str(PIPELINE), '--run-id', 'inproc'])
    pr.main()
    assert os.path.exists(os.path.join('state', 'run_inproc.json'))
    # metrics should be created
    assert os.path.exists(os.path.join('state', 'metrics_inproc.json'))
