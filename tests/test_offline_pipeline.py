import json
import os
import shutil
import subprocess
import sys
import tempfile
import io
import traceback
import importlib
from contextlib import redirect_stdout, redirect_stderr
from types import SimpleNamespace
from typing import Optional
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src" / "pipeline_runner.py"
PIPELINE = ROOT / "pipeline.json"
STATE = ROOT / "state"
OUTPUT = ROOT / "data" / "output"
WORK = ROOT / "data" / "work"
INPUT = ROOT / "data" / "input" / "sample.txt"


def run(cmd: list[str], env: Optional[dict] = None, expect_fail: bool = False):
    env = env.copy() if env else os.environ.copy()
    env.setdefault("COVERAGE_PROCESS_START", str(ROOT / ".coveragerc"))

    # Run pipeline_runner in-process for accurate coverage
    if cmd and cmd[0] == str(SRC):
        args = cmd[1:]
        try:
            pipeline_idx = args.index("--pipeline")
            pipeline_path = args[pipeline_idx + 1]
        except ValueError:
            pipeline_path = str(PIPELINE)
        try:
            runid_idx = args.index("--run-id")
            run_id = args[runid_idx + 1]
        except ValueError:
            run_id = "test"

        pr = importlib.import_module("src.pipeline_runner".replace("/", "."))
        out_buf, err_buf = io.StringIO(), io.StringIO()
        rc = 0
        # Temporarily inject env vars into process env for runner
        old_env = os.environ.copy()
        os.environ.update(env)
        with redirect_stdout(out_buf), redirect_stderr(err_buf):
            try:
                pr.run_pipeline(pipeline_path, run_id)
            except SystemExit as e:
                rc = e.code if isinstance(e.code, int) else 1
            except Exception:
                rc = 1
                err_buf.write(traceback.format_exc())
        # Restore environment
        os.environ.clear()
        os.environ.update(old_env)

        if expect_fail:
            assert rc != 0, f"Expected failure but succeeded: {cmd}\nstdout: {out_buf.getvalue()}\nstderr: {err_buf.getvalue()}"
        else:
            assert rc == 0, f"Command failed: {cmd}\nstdout: {out_buf.getvalue()}\nstderr: {err_buf.getvalue()}"
        return SimpleNamespace(returncode=rc, stdout=out_buf.getvalue(), stderr=err_buf.getvalue())

    # fallback to subprocess
    proc = subprocess.run(cmd, capture_output=True, text=True, env=env)
    if expect_fail:
        assert proc.returncode != 0, f"Expected failure but succeeded: {cmd}\nstdout: {proc.stdout}\nstderr: {proc.stderr}"
    else:
        assert proc.returncode == 0, f"Command failed: {cmd}\nstdout: {proc.stdout}\nstderr: {proc.stderr}"
    return proc


def clean_dirs():
    for p in [STATE, OUTPUT, WORK]:
        if p.exists():
            shutil.rmtree(p)
        p.mkdir(parents=True, exist_ok=True)


def read_file(path: Path) -> str:
    with open(path, "r", encoding="utf-8") as f:
        return f.read()


def test_first_run_produces_output_and_state():
    clean_dirs()
    proc = run([str(SRC), "--pipeline", str(PIPELINE), "--run-id", "t1"])
    # Uppercase stage produces result.txt
    result = OUTPUT / "result.txt"
    assert result.exists(), "result.txt should exist after first run"
    expected = read_file(INPUT).upper()
    assert read_file(result) == expected

    # State files exist and parse
    run_state = STATE / "run_t1.json"
    metrics = STATE / "metrics_t1.json"
    assert run_state.exists() and metrics.exists()
    for f in [run_state, metrics]:
        with open(f, "r", encoding="utf-8") as fp:
            json.load(fp)

    # Completion markers
    assert (WORK / ".stage_copy.done").exists()
    assert (OUTPUT / ".stage_upper.done").exists()


def test_second_run_skips_stages():
    clean_dirs()
    run([str(SRC), "--pipeline", str(PIPELINE), "--run-id", "skip1"])
    proc = run([str(SRC), "--pipeline", str(PIPELINE), "--run-id", "skip2"])
    stdout = proc.stdout
    assert "[SKIP] stage_copy" in stdout
    assert "[SKIP] stage_upper" in stdout


def test_checkpoint_resume():
    clean_dirs()
    # run once to create work file
    run([str(SRC), "--pipeline", str(PIPELINE), "--run-id", "cp1"])
    # simulate partial progress
    progress = STATE / "progress_stage_upper.json"
    with open(progress, "w", encoding="utf-8") as fp:
        json.dump({"lineOffset": 1}, fp)
    # remove completion marker and truncate output to first line
    marker = OUTPUT / ".stage_upper.done"
    if marker.exists():
        marker.unlink()
    result_file = OUTPUT / "result.txt"
    if result_file.exists():
        # keep only first line
        lines = read_file(result_file).splitlines(keepends=True)
        with open(result_file, "w", encoding="utf-8") as f:
            if lines:
                f.write(lines[0])
    # rerun
    proc = run([str(SRC), "--pipeline", str(PIPELINE), "--run-id", "cp2"])
    assert "[DONE] stage_upper" in proc.stdout
    assert marker.exists()
    # ensure output matches expected uppercase (no duplication)
    expected = read_file(INPUT).upper()
    assert read_file(result_file) == expected


def test_retry_transient_failure():
    clean_dirs()
    # Create temp pipeline with transient failure simulation
    fd, tmp_path = tempfile.mkstemp(suffix=".json", prefix="pipeline_")
    os.close(fd)
    tmp_pipeline = Path(tmp_path)
    with open(PIPELINE, "r", encoding="utf-8") as f:
        spec = json.load(f)
    for st in spec["stages"]:
        if st["name"] == "stage_upper":
            st["params"] = {"simulateTransient": True, "transientExitCode": 75}
            st["retry"]["maxAttempts"] = 3
            st["retry"]["retryableExitCodes"] = [75]
    with open(tmp_pipeline, "w", encoding="utf-8") as f:
        json.dump(spec, f)

    run([str(SRC), "--pipeline", str(tmp_pipeline), "--run-id", "retry1"])
    # Stage state should record >=2 attempts
    stage_state = STATE / "stage_stage_upper.json"
    with open(stage_state, "r", encoding="utf-8") as f:
        data = json.load(f)
    assert data.get("attempts", 0) >= 2
    assert data.get("lastStatus") == "ok"


def test_offline_guard_blocks_banned_imports():
    clean_dirs()
    # Create a temporary processor that imports socket
    fd_proc, tmp_proc_path = tempfile.mkstemp(suffix=".py", prefix="proc_banned_")
    os.close(fd_proc)
    proc_path = Path(tmp_proc_path)
    proc_path.write_text("import socket\nprint('hi')\n", encoding="utf-8")
    fd_pipe, tmp_pipe_path = tempfile.mkstemp(suffix=".json", prefix="pipeline_guard_")
    os.close(fd_pipe)
    tmp_pipeline = Path(tmp_pipe_path)
    spec = {
        "name": "guard_test",
        "version": "0",
        "stages": [
            {
                "name": "banned",
                "processor": str(proc_path),
                "inputs": [],
                "outputDir": str(OUTPUT),
                "idempotency": {"enabled": False},
                "offlineGuard": True
            }
        ]
    }
    tmp_pipeline.write_text(json.dumps(spec), encoding="utf-8")
    proc = run([str(SRC), "--pipeline", str(tmp_pipeline), "--run-id", "guard1"], expect_fail=True)
    assert "Offline guard violation" in proc.stderr


def test_resource_env_propagated():
    clean_dirs()
    env = os.environ.copy()
    env["PIPELINE_DEBUG_ENV"] = "1"
    run([str(SRC), "--pipeline", str(PIPELINE), "--run-id", "res1"], env=env)
    env_dump = STATE / "env_stage_stage_upper.json"
    with open(env_dump, "r", encoding="utf-8") as f:
        data = json.load(f)
    assert data.get("PIPELINE_RESOURCES_CPU_CORES") == "1"
    assert data.get("PIPELINE_RESOURCES_MEMORY_MB") == "128"
    assert data.get("PIPELINE_RESOURCES_IO_CONCURRENCY") == "1"
