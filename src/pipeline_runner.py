import argparse
import ast
import hashlib
import json
import os
import random
import subprocess
import sys
import time
from datetime import datetime
from typing import Dict, List, Optional, Tuple

STATE_DIR = "state"
LOCKS_DIR = "locks"
DATA_DIRS = ["data/input", "data/work", "data/output"]

# Banned modules for offline guard
BANNED_MODULE_PREFIXES = (
    "requests",
    "socket",
    "http.client",
    "urllib",
    "urllib3",
    "httpx",
    "aiohttp",
    "asyncio"  # conservative: block asyncio to avoid accidental networking
)

for d in [STATE_DIR, LOCKS_DIR] + DATA_DIRS:
    os.makedirs(d, exist_ok=True)


def sha256_file(path: str) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            h.update(chunk)
    return h.hexdigest()


def get_processor_version(processor_path: str) -> str:
    try:
        mtime = os.path.getmtime(processor_path)
        return f"v{int(mtime)}"
    except OSError:
        return "v0"


def load_pipeline(pipeline_path: str) -> Dict:
    with open(pipeline_path, "r", encoding="utf-8") as f:
        return json.load(f)


def validate_pipeline_spec(spec: Dict) -> None:
    if not isinstance(spec, dict):
        raise ValueError("Pipeline spec must be a JSON object")
    if "stages" not in spec or not isinstance(spec["stages"], list):
        raise ValueError("Pipeline spec must contain a 'stages' list")
    for stage in spec["stages"]:
        if not isinstance(stage, dict):
            raise ValueError("Each stage must be an object")
        for required in ["name", "processor", "outputDir"]:
            if required not in stage:
                raise ValueError(f"Stage missing required field: {required}")
        # optional fields defaulting
        stage.setdefault("inputs", [])
        stage.setdefault("idempotency", {"enabled": True})
        stage.setdefault("checkpoint", {"enabled": False, "lineInterval": 0})
        stage.setdefault("retry", {"maxAttempts": 1, "baseDelaySeconds": 1.0, "maxDelaySeconds": 30.0, "jitterSeconds": 0.5})
        stage.setdefault("resources", {"cpuCores": None, "memoryMB": None, "ioConcurrency": None})
        stage.setdefault("params", {})
        stage.setdefault("offlineGuard", True)
        stage.setdefault("useLock", True)


def load_stage_state(stage_name: str) -> Dict:
    path = os.path.join(STATE_DIR, f"stage_{stage_name}.json")
    if os.path.exists(path):
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    return {}


def atomic_write_json(path: str, data: Dict) -> None:
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    os.replace(tmp, path)


def save_stage_state(stage_name: str, data: Dict):
    path = os.path.join(STATE_DIR, f"stage_{stage_name}.json")
    atomic_write_json(path, data)


def save_run_state(run_id: str, data: Dict):
    path = os.path.join(STATE_DIR, f"run_{run_id}.json")
    atomic_write_json(path, data)


def compute_idempotency_key(inputs: List[str], processor_path: str, params: Optional[Dict] = None) -> str:
    parts = []
    for p in inputs:
        if os.path.exists(p):
            parts.append(sha256_file(p))
        else:
            parts.append("missing")
    parts.append(get_processor_version(processor_path))
    if params:
        parts.append(json.dumps(params, sort_keys=True))
    raw = "|".join(parts)
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def scan_for_banned_imports(path: str) -> List[str]:
    """Return list of banned modules imported by file at path."""
    with open(path, "r", encoding="utf-8") as f:
        tree = ast.parse(f.read(), filename=path)
    banned_found = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for alias in node.names:
                mod = alias.name
                if mod.startswith(BANNED_MODULE_PREFIXES):
                    banned_found.append(mod)
        elif isinstance(node, ast.ImportFrom):
            mod = node.module or ""
            if mod.startswith(BANNED_MODULE_PREFIXES):
                banned_found.append(mod)
    return banned_found


def enforce_offline_guard(processor_path: str) -> None:
    banned = scan_for_banned_imports(processor_path)
    if banned:
        raise RuntimeError(f"Offline guard violation: {processor_path} imports banned modules: {banned}")


class FileLock:
    """Cross-platform file lock using fcntl/msvcrt."""

    def __init__(self, path: str):
        self.path = path
        self._fh = None

    def acquire(self):
        self._fh = open(self.path, "a+")
        try:
            if os.name == "nt":
                import msvcrt

                msvcrt.locking(self._fh.fileno(), msvcrt.LK_LOCK, 1)
            else:
                import fcntl

                fcntl.flock(self._fh, fcntl.LOCK_EX)
        except Exception:
            self._fh.close()
            raise

    def release(self):
        if not self._fh:
            return
        try:
            if os.name == "nt":
                import msvcrt

                msvcrt.locking(self._fh.fileno(), msvcrt.LK_UNLCK, 1)
            else:
                import fcntl

                fcntl.flock(self._fh, fcntl.LOCK_UN)
        finally:
            self._fh.close()
            self._fh = None

    def __enter__(self):
        self.acquire()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.release()


def set_resource_limits(resources: Dict):
    """Best-effort resource governance; skips silently when unsupported."""
    cpu_cores = resources.get("cpuCores")
    memory_mb = resources.get("memoryMB")

    # CPU affinity (best-effort)
    if cpu_cores:
        try:
            if hasattr(os, "sched_setaffinity"):
                os.sched_setaffinity(0, set(range(cpu_cores)))
        except Exception:
            pass

    # Memory limit (POSIX only)
    if memory_mb:
        try:
            import resource

            bytes_limit = memory_mb * 1024 * 1024
            resource.setrlimit(resource.RLIMIT_AS, (bytes_limit, bytes_limit))
        except Exception:
            pass


def atomic_write_text(path: str, content: str) -> None:
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        f.write(content)
    os.replace(tmp, path)


def read_checkpoint(stage_name: str) -> int:
    cp_path = os.path.join(STATE_DIR, f"progress_{stage_name}.json")
    if os.path.exists(cp_path):
        try:
            with open(cp_path, "r", encoding="utf-8") as f:
                data = json.load(f)
            return int(data.get("lineOffset", 0))
        except Exception:
            return 0
    return 0


def write_checkpoint(stage_name: str, line_offset: int) -> None:
    cp_path = os.path.join(STATE_DIR, f"progress_{stage_name}.json")
    atomic_write_json(cp_path, {"lineOffset": line_offset})


def audit_log(run_id: str, stage: Optional[str], event: str, message: str, extra: Optional[Dict] = None):
    """Append audit entry with a hash chain."""
    audit_path = os.path.join(STATE_DIR, f"audit_{run_id}.jsonl")
    prev_hash = ""
    if os.path.exists(audit_path):
        try:
            with open(audit_path, "rb") as f:
                f.seek(0, os.SEEK_END)
                size = f.tell()
                if size > 0:
                    # Read last line efficiently
                    f.seek(max(0, size - 4096))
                    tail = f.read().decode("utf-8", errors="ignore")
                    *_, last_line = tail.strip().splitlines()
                    last_obj = json.loads(last_line)
                    prev_hash = last_obj.get("hash", "")
        except Exception:
            prev_hash = ""
    entry = {
        "ts": datetime.utcnow().isoformat(),
        "stage": stage,
        "event": event,
        "message": message,
    }
    if extra:
        entry.update(extra)
    # compute hash chain
    chain_input = prev_hash + json.dumps(entry, sort_keys=True)
    entry_hash = hashlib.sha256(chain_input.encode("utf-8")).hexdigest()
    entry["hash"] = entry_hash
    entry["prevHash"] = prev_hash
    with open(audit_path, "a", encoding="utf-8") as f:
        f.write(json.dumps(entry, ensure_ascii=False) + "\n")


def run_stage(stage: Dict, run_id: str) -> Dict:
    name = stage["name"]
    processor = stage["processor"]
    inputs = stage.get("inputs", [])
    output_dir = stage["outputDir"]
    params = stage.get("params", {})
    idem_enabled = stage.get("idempotency", {}).get("enabled", False)
    checkpoint_cfg = stage.get("checkpoint", {})
    retry_cfg = stage.get("retry", {})
    resources_cfg = stage.get("resources", {})
    offline_guard = stage.get("offlineGuard", True)
    use_lock = stage.get("useLock", True)

    os.makedirs(output_dir, exist_ok=True)
    stage_state = load_stage_state(name)
    stage_state.setdefault("history", [])

    if offline_guard:
        enforce_offline_guard(processor)

    idem_key = compute_idempotency_key(inputs, processor, params) if idem_enabled else None
    output_marker = os.path.join(output_dir, f".{name}.done")
    if (
        idem_enabled
        and stage_state.get("idempotencyKey") == idem_key
        and os.path.exists(output_marker)
    ):
        msg = f"[SKIP] {name} (idempotent key matched)"
        print(msg)
        audit_log(run_id, name, "skip", msg)
        return {"stage": name, "status": "skipped"}

    line_offset = read_checkpoint(name) if checkpoint_cfg.get("enabled", False) else 0
    cp_interval = int(checkpoint_cfg.get("lineInterval", 0))

    max_attempts = int(retry_cfg.get("maxAttempts", 1))
    base_delay = float(retry_cfg.get("baseDelaySeconds", 1.0))
    max_delay = float(retry_cfg.get("maxDelaySeconds", 30.0))
    jitter = float(retry_cfg.get("jitterSeconds", 0.5))
    retryable_exit_codes = set(retry_cfg.get("retryableExitCodes", []))
    seed = retry_cfg.get("seed")
    rand = random.Random(seed)

    attempts = 0
    start_ts = time.time()
    last_error = None
    status = "failed"

    def should_retry(exit_code: Optional[int]) -> bool:
        if attempts >= max_attempts:
            return False
        if retryable_exit_codes:
            return exit_code in retryable_exit_codes
        # If not specified, treat any non-zero as retryable until max_attempts exhausted
        return True

    while attempts < max_attempts:
        attempts += 1
        attempt_info = {"attempt": attempts, "startedAt": datetime.utcnow().isoformat()}
        audit_log(run_id, name, "start", f"Attempt {attempts}")
        try:
            if not os.path.exists(processor):
                raise FileNotFoundError(f"Processor not found: {processor}")

            env = os.environ.copy()
            # Resource hints to subprocess
            if resources_cfg.get("cpuCores"):
                env["PIPELINE_RESOURCES_CPU_CORES"] = str(resources_cfg.get("cpuCores"))
                env["OMP_NUM_THREADS"] = str(resources_cfg.get("cpuCores"))
            if resources_cfg.get("memoryMB"):
                env["PIPELINE_RESOURCES_MEMORY_MB"] = str(resources_cfg.get("memoryMB"))
            if resources_cfg.get("ioConcurrency"):
                env["PIPELINE_RESOURCES_IO_CONCURRENCY"] = str(resources_cfg.get("ioConcurrency"))

            env["PIPELINE_STAGE_NAME"] = name
            env["PIPELINE_RUN_ID"] = run_id
            env["PIPELINE_OUTPUT_DIR"] = os.path.abspath(output_dir)
            env["PIPELINE_LINE_OFFSET"] = str(line_offset)
            env["PIPELINE_LINE_INTERVAL"] = str(cp_interval)
            env["PIPELINE_PROGRESS_PATH"] = os.path.abspath(os.path.join(STATE_DIR, f"progress_{name}.json"))
            env["PIPELINE_PARAMS"] = json.dumps(params)
            env["PIPELINE_ATTEMPT"] = str(attempts)

            # Best-effort resource governance in parent process too
            set_resource_limits(resources_cfg)

            # Acquire lock if enabled
            lock_ctx = FileLock(os.path.join(LOCKS_DIR, f"{name}.lock")) if use_lock else None

            def _execute():
                cmd = [sys.executable, processor] + inputs
                proc = subprocess.run(cmd, env=env, capture_output=True, text=True)
                return proc

            if lock_ctx:
                with lock_ctx:
                    proc = _execute()
            else:
                proc = _execute()

            attempt_info["stdout"] = proc.stdout.strip()
            attempt_info["stderr"] = proc.stderr.strip()
            if proc.returncode != 0:
                last_error = attempt_info["stderr"] or attempt_info["stdout"] or f"exit {proc.returncode}"
                attempt_info["status"] = "failed"
                attempt_info["error"] = last_error
                attempt_info["exitCode"] = proc.returncode
                audit_log(run_id, name, "fail", last_error, {"attempt": attempts, "exitCode": proc.returncode})
                if should_retry(proc.returncode) and attempts < max_attempts:
                    # backoff
                    delay = min(max_delay, base_delay * (2 ** (attempts - 1)))
                    delay += rand.uniform(0, jitter)
                    time.sleep(delay)
                    continue
                else:
                    break
            # success
            status = "ok"
            attempt_info["status"] = "ok"
            # Write output marker atomically
            atomic_write_text(output_marker, datetime.utcnow().isoformat())

            # Update checkpoint if enabled
            if checkpoint_cfg.get("enabled", False):
                progress_file = os.path.join(STATE_DIR, f"progress_{name}.json")
                if os.path.exists(progress_file):
                    try:
                        with open(progress_file, "r", encoding="utf-8") as pf:
                            prog = json.load(pf)
                        write_checkpoint(name, int(prog.get("lineOffset", 0)))
                    except Exception:
                        pass
            break
        finally:
            attempt_info["endedAt"] = datetime.utcnow().isoformat()
            stage_state["history"].append(attempt_info)
            save_stage_state(name, stage_state)

    duration = time.time() - start_ts
    stage_state["lastDurationSec"] = duration
    stage_state["lastStatus"] = status
    stage_state["attempts"] = attempts
    if idem_enabled:
        stage_state["idempotencyKey"] = idem_key
    save_stage_state(name, stage_state)

    if status != "ok":
        print(f"[FAIL] {name}: {last_error}")
        audit_log(run_id, name, "fail", last_error or "unknown error", {"attempts": attempts})
        return {"stage": name, "status": "failed", "error": last_error}

    print(f"[DONE] {name} in {duration:.3f}s")
    audit_log(run_id, name, "done", f"Duration {duration:.3f}s")
    return {"stage": name, "status": "ok", "attempts": attempts}


def aggregate_metrics(run_id: str, results: List[Dict]):
    metrics = {
        "runId": run_id,
        "timestamp": datetime.utcnow().isoformat(),
        "stages": results,
        "totalStages": len(results),
        "failedStages": sum(1 for r in results if r["status"] == "failed"),
        "skippedStages": sum(1 for r in results if r["status"] == "skipped"),
        "okStages": sum(1 for r in results if r["status"] == "ok"),
        "durationSec": sum(r.get("durationSec", 0) for r in results if isinstance(r, dict)),
    }
    path = os.path.join(STATE_DIR, f"metrics_{run_id}.json")
    atomic_write_json(path, metrics)


def run_pipeline(pipeline_path: str, run_id: str) -> Dict:
    pipeline = load_pipeline(pipeline_path)
    validate_pipeline_spec(pipeline)
    audit_log(run_id, None, "run_start", f"Pipeline {pipeline.get('name')}")
    run_state = {
        'runId': run_id,
        'pipeline': pipeline['name'],
        'version': pipeline.get('version'),
        'startedAt': datetime.utcnow().isoformat(),
        'state': 'running'
    }
    save_run_state(run_id, run_state)

    results = []
    for stage in pipeline["stages"]:
        res = run_stage(stage, run_id)
        results.append(res)
        if res["status"] == "failed":
            run_state["state"] = "failed"
            break
    else:
        run_state["state"] = "completed"

    run_state["endedAt"] = datetime.utcnow().isoformat()
    save_run_state(run_id, run_state)
    aggregate_metrics(run_id, results)
    audit_log(run_id, None, "run_end", run_state["state"])
    print(f"Run {run_id} state: {run_state['state']}")
    return run_state


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--pipeline', required=True)
    parser.add_argument('--run-id', required=True)
    args = parser.parse_args()
    run_pipeline(args.pipeline, args.run_id)


if __name__ == '__main__':
    main()
