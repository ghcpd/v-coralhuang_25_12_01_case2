import os
import json
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src" / "pipeline_runner.py"
PIPELINE = ROOT / "pipeline.json"
STATE = ROOT / "state"
OUTPUT = ROOT / "data" / "output"
WORK = ROOT / "data" / "work"
BIN = ROOT / "bin"


def cleanup_state():
    """Clean up state, locks, and output directories."""
    for p in [STATE, WORK, OUTPUT]:
        p.mkdir(parents=True, exist_ok=True)
        for child in p.glob("**/*"):
            try:
                if child.is_file():
                    child.unlink()
            except Exception:
                pass


def run(cmd: list[str]):
    """Run command and return stdout."""
    proc = subprocess.run(cmd, capture_output=True, text=True)
    if proc.returncode != 0:
        raise AssertionError(
            f"Command failed with exit code {proc.returncode}: {' '.join(cmd)}\n"
            f"STDOUT: {proc.stdout}\nSTDERR: {proc.stderr}"
        )
    return proc.stdout


class TestOfflinePipeline:
    """Test suite for offline pipeline implementation."""

    def setup_method(self):
        """Clean state before each test."""
        cleanup_state()

    def test_first_run_produces_output(self):
        """Test that first run processes all stages and produces output."""
        cleanup_state()
        stdout = run(["python", str(SRC), "--pipeline", str(PIPELINE), "--run-id", "t1"])
        
        # Verify output file exists
        result = OUTPUT / "result.txt"
        assert result.exists(), "result.txt should exist after first run"
        
        # Verify output has content
        content = result.read_text(encoding="utf-8")
        assert len(content) > 0, "output file should not be empty"
        
        # Verify content is uppercase
        lines = content.strip().split("\n")
        assert lines[0].isupper(), "output should be uppercase"
        
        # Verify state files exist and parse
        run_state = STATE / "run_t1.json"
        metrics = STATE / "metrics_t1.json"
        assert run_state.exists(), "run_t1.json should exist"
        assert metrics.exists(), "metrics_t1.json should exist"
        
        for f in [run_state, metrics]:
            data = json.loads(f.read_text(encoding="utf-8"))
            assert isinstance(data, dict), f"{f} should contain valid JSON"
        
        # Verify metrics show successful execution
        metrics_data = json.loads(metrics.read_text(encoding="utf-8"))
        assert metrics_data["okStages"] == 2, "Both stages should succeed"
        assert metrics_data["skippedStages"] == 0, "No stages should skip"

    def test_second_run_skips_stages(self):
        """Test that second run skips all stages (idempotency)."""
        # First run
        run(["python", str(SRC), "--pipeline", str(PIPELINE), "--run-id", "t2a"])
        
        # Second run
        stdout = run(["python", str(SRC), "--pipeline", str(PIPELINE), "--run-id", "t2b"])
        
        # Verify SKIP messages in stdout
        assert "[SKIP] stage_copy" in stdout, "stage_copy should be skipped"
        assert "[SKIP] stage_upper" in stdout, "stage_upper should be skipped"
        
        # Verify metrics show skipped stages
        metrics = STATE / "metrics_t2b.json"
        metrics_data = json.loads(metrics.read_text(encoding="utf-8"))
        assert metrics_data["skippedStages"] == 2, "Both stages should be skipped"
        assert metrics_data["okStages"] == 0, "No stages should execute"

    def test_idempotency_key_matching(self):
        """Test that idempotency key prevents re-execution."""
        # First run
        run(["python", str(SRC), "--pipeline", str(PIPELINE), "--run-id", "t3a"])
        
        # Check stage state has idempotency key
        stage_state = STATE / "stage_stage_upper.json"
        state_data = json.loads(stage_state.read_text(encoding="utf-8"))
        idem_key = state_data.get("idempotencyKey")
        assert idem_key, "idempotencyKey should be stored"
        
        # Second run should skip
        run(["python", str(SRC), "--pipeline", str(PIPELINE), "--run-id", "t3b"])
        
        # Verify idempotency key is unchanged
        state_data_2 = json.loads(stage_state.read_text(encoding="utf-8"))
        assert state_data_2.get("idempotencyKey") == idem_key, "idempotencyKey should not change"

    def test_checkpoint_resume(self):
        """Test that interrupted stage can resume from checkpoint."""
        # First run to completion
        run(["python", str(SRC), "--pipeline", str(PIPELINE), "--run-id", "t4a"])
        
        # Get initial output size
        output_file = OUTPUT / "result.txt"
        initial_size = output_file.stat().st_size
        initial_content = output_file.read_text(encoding="utf-8")
        
        # Simulate interruption by setting checkpoint and removing completion marker
        progress = STATE / "progress_stage_upper.json"
        progress.write_text(json.dumps({"lineOffset": 1}), encoding="utf-8")
        
        marker = OUTPUT / ".stage_upper.done"
        if marker.exists():
            marker.unlink()
        
        # Also clear idempotency key to force execution
        stage_state_file = STATE / "stage_stage_upper.json"
        if stage_state_file.exists():
            state_data = json.loads(stage_state_file.read_text(encoding="utf-8"))
            state_data.pop("idempotencyKey", None)
            stage_state_file.write_text(json.dumps(state_data), encoding="utf-8")
        
        # Run again from checkpoint
        stdout = run(["python", str(SRC), "--pipeline", str(PIPELINE), "--run-id", "t4b"])
        assert "[DONE] stage_upper" in stdout, "stage_upper should complete from checkpoint"
        
        # Verify completion marker was written
        assert marker.exists(), "Completion marker should be written"
        
        # Verify output file was written
        assert output_file.exists(), "Output file should exist after checkpoint resume"

    def test_no_network_imports_in_stages(self):
        """Test that processor scripts don't contain forbidden network imports."""
        forbidden = {
            "requests", "socket", "http.client", "urllib",
            "aiohttp", "ftplib", "smtplib", "paramiko"
        }
        
        for stage_file in BIN.glob("stage_*.py"):
            content = stage_file.read_text(encoding="utf-8")
            for forbidden_import in forbidden:
                assert f"import {forbidden_import}" not in content, \
                    f"{stage_file.name} should not import {forbidden_import}"
                assert f"from {forbidden_import}" not in content, \
                    f"{stage_file.name} should not import {forbidden_import}"

    def test_state_files_are_valid_json(self):
        """Test that all state files are valid JSON."""
        cleanup_state()
        run(["python", str(SRC), "--pipeline", str(PIPELINE), "--run-id", "t5"])
        
        # Parse all JSON files in state directory
        for json_file in STATE.glob("*.json"):
            try:
                data = json.loads(json_file.read_text(encoding="utf-8"))
                assert isinstance(data, dict), f"{json_file.name} should contain a JSON object"
            except json.JSONDecodeError as e:
                raise AssertionError(f"{json_file.name} is not valid JSON: {e}")

    def test_run_twice_is_idempotent(self):
        """Test that running the same run_id twice produces same results."""
        cleanup_state()
        
        # First execution
        stdout1 = run(["python", str(SRC), "--pipeline", str(PIPELINE), "--run-id", "t6"])
        
        # Save first run metrics
        metrics1 = STATE / "metrics_t6.json"
        data1 = json.loads(metrics1.read_text(encoding="utf-8"))
        
        # Second execution with same run_id (should overwrite)
        stdout2 = run(["python", str(SRC), "--pipeline", str(PIPELINE), "--run-id", "t6"])
        
        # Verify second run also succeeds
        data2 = json.loads(metrics1.read_text(encoding="utf-8"))
        assert data2["totalStages"] == data1["totalStages"], "Stage count should match"

    def test_atomic_state_writes(self):
        """Test that state writes are atomic (tmp+replace pattern used)."""
        cleanup_state()
        
        # Run pipeline
        run(["python", str(SRC), "--pipeline", str(PIPELINE), "--run-id", "t7"])
        
        # Verify no .tmp files left behind
        tmp_files = list(STATE.glob("*.tmp"))
        assert len(tmp_files) == 0, f"Should not have leftover .tmp files, found: {tmp_files}"

    def test_pipeline_completes_successfully(self):
        """Test that pipeline completes with exit code 0 on success."""
        cleanup_state()
        proc = subprocess.run(
            ["python", str(SRC), "--pipeline", str(PIPELINE), "--run-id", "t8"],
            capture_output=True,
            text=True
        )
        assert proc.returncode == 0, \
            f"Pipeline should exit with 0 on success. Got {proc.returncode}\nSTDOUT: {proc.stdout}\nSTDERR: {proc.stderr}"

    def test_pipeline_fails_on_missing_processor(self):
        """Test that pipeline fails gracefully if processor not found."""
        cleanup_state()
        
        # Create a bad pipeline with missing processor
        bad_pipeline = STATE.parent / "bad_pipeline.json"
        bad_config = {
            "name": "bad_pipeline",
            "version": "1.0.0",
            "stages": [
                {
                    "name": "bad_stage",
                    "processor": "nonexistent/processor.py",
                    "inputs": ["data/input/sample.txt"],
                    "outputDir": "data/output",
                    "idempotency": {"enabled": True}
                }
            ]
        }
        bad_pipeline.write_text(json.dumps(bad_config), encoding="utf-8")
        
        # Run should fail
        proc = subprocess.run(
            ["python", str(SRC), "--pipeline", str(bad_pipeline), "--run-id", "t9"],
            capture_output=True,
            text=True
        )
        assert proc.returncode != 0, "Pipeline should fail with missing processor"
        
        # Cleanup
        bad_pipeline.unlink()


if __name__ == "__main__":
    import pytest
    pytest.main([__file__, "-v"])
