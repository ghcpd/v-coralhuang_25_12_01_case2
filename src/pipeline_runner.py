import argparse
import json
import hashlib
import os
import time
import random
import sys
from datetime import datetime, timezone
from typing import List, Dict
import subprocess

# Offline guard: forbid network imports
FORBIDDEN_IMPORTS = {
    'requests',
    'socket',
    'http.client',
    'urllib',
    'urllib.request',
    'urllib.parse',
    'urllib.error',
    'http',
    'asyncio',
    'aiohttp',
    'paramiko',
    'ftplib',
    'smtplib',
    'poplib',
    'imaplib',
    'telnetlib',
    'xmlrpc',
    'xmlrpc.client',
}

STATE_DIR = "state"
LOCKS_DIR = "locks"

os.makedirs(STATE_DIR, exist_ok=True)
os.makedirs(LOCKS_DIR, exist_ok=True)
os.makedirs("data/input", exist_ok=True)
os.makedirs("data/work", exist_ok=True)
os.makedirs("data/output", exist_ok=True)


def check_offline_compliance(processor_path: str):
    """Scan processor script for forbidden imports."""
    try:
        with open(processor_path, 'r', encoding='utf-8') as f:
            content = f.read()
        for imp in FORBIDDEN_IMPORTS:
            if f"import {imp}" in content or f"from {imp}" in content:
                raise RuntimeError(f"Offline violation: forbidden import '{imp}' in {processor_path}")
    except Exception as e:
        if "Offline violation" in str(e):
            raise
        # Silently skip if file doesn't exist or can't be read (may not be a Python file)
        pass


def sha256_file(path: str) -> str:
    h = hashlib.sha256()
    with open(path, 'rb') as f:
        while True:
            chunk = f.read(8192)
            if not chunk:
                break
            h.update(chunk)
    return h.hexdigest()


def get_processor_version(processor_path: str) -> str:
    try:
        mtime = os.path.getmtime(processor_path)
        return f"v{int(mtime)}"
    except OSError:
        return "v0"


def load_pipeline(pipeline_path: str) -> Dict:
    with open(pipeline_path, 'r', encoding='utf-8') as f:
        return json.load(f)


def load_stage_state(stage_name: str) -> Dict:
    path = os.path.join(STATE_DIR, f"stage_{stage_name}.json")
    if os.path.exists(path):
        with open(path, 'r', encoding='utf-8') as f:
            return json.load(f)
    return {}


def save_stage_state(stage_name: str, data: Dict):
    path = os.path.join(STATE_DIR, f"stage_{stage_name}.json")
    tmp = path + ".tmp"
    with open(tmp, 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    os.replace(tmp, path)


def save_run_state(run_id: str, data: Dict):
    path = os.path.join(STATE_DIR, f"run_{run_id}.json")
    tmp = path + ".tmp"
    with open(tmp, 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    os.replace(tmp, path)


def compute_idempotency_key(inputs: List[str], processor_path: str) -> str:
    parts = []
    for p in inputs:
        if os.path.exists(p):
            parts.append(sha256_file(p))
        else:
            parts.append("missing")
    parts.append(get_processor_version(processor_path))
    raw = "|".join(parts)
    return hashlib.sha256(raw.encode('utf-8')).hexdigest()


def acquire_lock(stage_name: str, timeout: int = 5) -> str:
    """Acquire a file lock for stage execution. Returns lock file path."""
    lock_file = os.path.join(LOCKS_DIR, f"{stage_name}.lock")
    start = time.time()
    while time.time() - start < timeout:
        try:
            # Try to create lock file exclusively
            fd = os.open(lock_file, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
            os.write(fd, f"{os.getpid()}\n".encode())
            os.close(fd)
            return lock_file
        except FileExistsError:
            time.sleep(0.1)
    raise RuntimeError(f"Could not acquire lock for {stage_name} within {timeout}s")


def release_lock(lock_file: str):
    """Release a file lock."""
    try:
        os.remove(lock_file)
    except Exception:
        pass


def retry_with_backoff(fn, max_attempts: int = 3, base_delay: float = 0.5, jitter: bool = True):
    """Retry function with exponential backoff."""
    last_error = None
    for attempt in range(1, max_attempts + 1):
        try:
            return fn()
        except Exception as e:
            last_error = e
            if attempt < max_attempts:
                delay = base_delay * (2 ** (attempt - 1))
                if jitter:
                    delay += random.uniform(0, delay * 0.1)
                print(f"[RETRY] Attempt {attempt}/{max_attempts} failed. Retrying in {delay:.2f}s...")
                time.sleep(delay)
            else:
                print(f"[RETRY] All {max_attempts} attempts exhausted.")
    if last_error:
        raise last_error


def run_stage(stage: Dict, run_id: str):
    name = stage['name']
    processor = stage['processor']
    inputs = stage.get('inputs', [])
    output_dir = stage['outputDir']
    os.makedirs(output_dir, exist_ok=True)
    stage_state = load_stage_state(name)

    # Offline guard
    check_offline_compliance(processor)

    idem_enabled = stage.get('idempotency', {}).get('enabled', False)
    idem_key = compute_idempotency_key(inputs, processor) if idem_enabled else None

    output_marker = os.path.join(output_dir, f".{name}.done")
    if idem_enabled and 'idempotencyKey' in stage_state and stage_state['idempotencyKey'] == idem_key and os.path.exists(output_marker):
        print(f"[SKIP] {name} (idempotent key matched)")
        return {'stage': name, 'status': 'skipped'}

    checkpoint_cfg = stage.get('checkpoint', {})
    cp_enabled = checkpoint_cfg.get('enabled', False)
    cp_line_interval = int(checkpoint_cfg.get('lineInterval', 0))
    cp_path = os.path.join(STATE_DIR, f"checkpoint_{name}.json")
    line_offset = 0
    if cp_enabled and os.path.exists(cp_path):
        with open(cp_path, 'r', encoding='utf-8') as f:
            try:
                cp_data = json.load(f)
                line_offset = int(cp_data.get('lineOffset', 0))
            except Exception:
                line_offset = 0

    start_ts = time.time()
    result = {'stage': name, 'status': 'ok'}
    lock_file = None

    try:
        # Acquire lock for stage execution
        lock_file = acquire_lock(name, timeout=10)

        # Execute processor script via Python with retry/backoff
        if not os.path.exists(processor):
            raise FileNotFoundError(f"Processor not found: {processor}")

        def execute_processor():
            env = os.environ.copy()
            env['PIPELINE_STAGE_NAME'] = name
            env['PIPELINE_OUTPUT_DIR'] = os.path.abspath(output_dir)
            env['PIPELINE_LINE_OFFSET'] = str(line_offset)
            cmd = ['python', processor] + inputs
            proc = subprocess.run(cmd, env=env, capture_output=True, text=True, timeout=300)
            if proc.returncode != 0:
                result['status'] = 'failed'
                result['error'] = proc.stderr.strip() or proc.stdout.strip()
                raise RuntimeError(result['error'])
            return proc

        retry_with_backoff(execute_processor, max_attempts=3, base_delay=0.5)

        # Write output marker atomically (tmp → replace)
        marker_tmp = output_marker + ".tmp"
        with open(marker_tmp, 'w') as f:
            f.write(datetime.now(timezone.utc).isoformat())
        os.replace(marker_tmp, output_marker)

    except Exception as e:
        # checkpoint not advanced on failure
        print(f"[FAIL] {name}: {e}")
        result['status'] = 'failed'
        result['error'] = str(e)
        stage_state['lastError'] = str(e)
        stage_state['lastStatus'] = 'failed'
        # Save state atomically
        save_stage_state(name, stage_state)
        return result
    finally:
        if lock_file:
            release_lock(lock_file)

    # Update checkpoint if enabled (processor may emit progress file)
    if cp_enabled:
        progress_file = os.path.join(STATE_DIR, f"progress_{name}.json")
        if os.path.exists(progress_file):
            try:
                with open(progress_file, 'r', encoding='utf-8') as pf:
                    prog = json.load(pf)
                # Write checkpoint atomically
                cp_tmp = cp_path + ".tmp"
                with open(cp_tmp, 'w', encoding='utf-8') as cf:
                    json.dump({'lineOffset': prog.get('lineOffset', 0)}, cf)
                os.replace(cp_tmp, cp_path)
            except Exception:
                pass

    duration = time.time() - start_ts
    stage_state['lastDurationSec'] = duration
    stage_state['lastStatus'] = result['status']
    if idem_enabled:
        stage_state['idempotencyKey'] = idem_key
    # Save state atomically
    save_stage_state(name, stage_state)
    print(f"[DONE] {name} in {duration:.3f}s")
    return result


def aggregate_metrics(run_id: str, results: List[Dict]):
    metrics = {
        'runId': run_id,
        'timestamp': datetime.now(timezone.utc).isoformat(),
        'stages': results,
        'totalStages': len(results),
        'failedStages': sum(1 for r in results if r['status'] == 'failed'),
        'skippedStages': sum(1 for r in results if r['status'] == 'skipped'),
        'okStages': sum(1 for r in results if r['status'] == 'ok')
    }
    path = os.path.join(STATE_DIR, f"metrics_{run_id}.json")
    tmp = path + ".tmp"
    with open(tmp, 'w', encoding='utf-8') as f:
        json.dump(metrics, f, ensure_ascii=False, indent=2)
    os.replace(tmp, path)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--pipeline', required=True)
    parser.add_argument('--run-id', required=True)
    args = parser.parse_args()

    try:
        pipeline = load_pipeline(args.pipeline)
    except Exception as e:
        print(f"[FATAL] Failed to load pipeline: {e}", file=sys.stderr)
        sys.exit(1)

    run_state = {
        'runId': args.run_id,
        'pipeline': pipeline['name'],
        'version': pipeline.get('version'),
        'startedAt': datetime.now(timezone.utc).isoformat(),
        'state': 'running'
    }
    save_run_state(args.run_id, run_state)

    results = []
    try:
        for stage in pipeline['stages']:
            res = run_stage(stage, args.run_id)
            results.append(res)
            if res['status'] == 'failed':
                run_state['state'] = 'failed'
                break
        else:
            run_state['state'] = 'completed'
    except Exception as e:
        print(f"[FATAL] Pipeline execution failed: {e}", file=sys.stderr)
        run_state['state'] = 'failed'
        sys.exit(1)

    run_state['endedAt'] = datetime.now(timezone.utc).isoformat()
    save_run_state(args.run_id, run_state)
    aggregate_metrics(args.run_id, results)
    print(f"Run {args.run_id} state: {run_state['state']}")

    # Exit with appropriate code
    if run_state['state'] == 'completed':
        sys.exit(0)
    else:
        sys.exit(1)


if __name__ == '__main__':
    main()
