import argparse
import json
import hashlib
import os
import time
from datetime import datetime
from typing import List, Dict
import random
import subprocess

STATE_DIR = "state"
LOCKS_DIR = "locks"

os.makedirs(STATE_DIR, exist_ok=True)
os.makedirs(LOCKS_DIR, exist_ok=True)
os.makedirs("data/input", exist_ok=True)
os.makedirs("data/work", exist_ok=True)
os.makedirs("data/output", exist_ok=True)


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


def scan_for_network_imports(root_dirs):
    banned = ['socket', 'requests', 'http.client', 'urllib', 'asyncio', 'aiohttp', 'urllib3']
    import_lines = []
    for root in root_dirs:
        for dirpath, _, filenames in os.walk(root):
            for fn in filenames:
                if fn.endswith('.py'):
                    p = os.path.join(dirpath, fn)
                    try:
                        with open(p, 'r', encoding='utf-8') as f:
                            txt = f.read()
                        for b in banned:
                            if f"import {b}" in txt or f"from {b} import" in txt:
                                import_lines.append((p, b))
                    except Exception:
                        pass
    return import_lines


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


def run_stage(stage: Dict, run_id: str):
    name = stage['name']
    processor = stage['processor']
    inputs = stage.get('inputs', [])
    output_dir = stage['outputDir']
    os.makedirs(output_dir, exist_ok=True)
    stage_state = load_stage_state(name)

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

    # Acquire a simple lock per-stage (lock dir)
    lock_path = os.path.join(LOCKS_DIR, f"{name}.lock")
    lock_acquired = False
    lock_wait_seconds = stage.get('lock', {}).get('waitSeconds', 5)
    lock_tries = int(lock_wait_seconds) * 2
    for i in range(lock_tries):
        try:
            os.mkdir(lock_path)
            lock_acquired = True
            break
        except FileExistsError:
            time.sleep(0.5)
    if not lock_acquired:
        raise RuntimeError(f"Failed to acquire lock for stage {name}")

    try:
        # Retry/backoff loop
        retry_cfg = stage.get('retry', {})
        max_attempts = int(retry_cfg.get('attempts', 1))
        base_delay_ms = int(retry_cfg.get('baseDelayMs', 100))
        jitter_ms = int(retry_cfg.get('jitterMs', 0))
        simulate_fail_attempts = int(stage.get('simulateFailAttempts', 0))

        attempt = int(stage_state.get('attempts', 0))
        last_error = None
        proc = None
        while attempt < max_attempts:

            # Prepare environment - optionally instruct processor to simulate transient failure
            env = os.environ.copy()
            env['PIPELINE_STAGE_NAME'] = name
            env['PIPELINE_OUTPUT_DIR'] = os.path.abspath(output_dir)
            env['PIPELINE_LINE_OFFSET'] = str(line_offset)
            if simulate_fail_attempts > attempt:
                env['PIPELINE_SIMULATED_FAIL'] = '1'
            else:
                env.pop('PIPELINE_SIMULATED_FAIL', None)

            # Execute processor script via Python
            if not os.path.exists(processor):
                raise FileNotFoundError(f"Processor not found: {processor}")

            cmd = ['python', processor] + inputs
            proc = subprocess.run(cmd, env=env, capture_output=True, text=True)
            attempt += 1
            stage_state['attempts'] = attempt
            # On success, break
            if proc.returncode == 0:
                last_error = None
                break
            # On failure, decide if retry
            last_error = proc.stderr.strip() or proc.stdout.strip() or 'unknown'
            # If no retries configured, break with failure
            if attempt >= max_attempts:
                break
            # Print retry info and backoff deterministically
            print(f"[RETRY] {name} attempt {attempt}/{max_attempts}: {last_error}")
            backoff_ms = base_delay_ms * (2 ** (attempt - 1))
            # deterministic jitter (avoid randomness in tests)
            jitter = (attempt * (jitter_ms // 2))
            time.sleep((backoff_ms + jitter) / 1000.0)

        # If still failed after attempts, raise
        if proc is not None and proc.returncode != 0:
            result['status'] = 'failed'
            result['error'] = last_error
            raise RuntimeError(result['error'])

        # Write output marker
        with open(output_marker, 'w') as f:
            f.write(datetime.utcnow().isoformat())

    except Exception as e:
        # checkpoint not advanced on failure
        print(f"[FAIL] {name}: {e}")
        stage_state['lastError'] = str(e)
        stage_state['lastStatus'] = 'failed'
        save_stage_state(name, stage_state)
        return {'stage': name, 'status': 'failed', 'error': str(e)}
    finally:
        # release lock
        try:
            if lock_acquired:
                os.rmdir(lock_path)
        except Exception:
            pass

    # Update checkpoint if enabled (processor may emit progress file)
    if cp_enabled:
        progress_file = os.path.join(STATE_DIR, f"progress_{name}.json")
        if os.path.exists(progress_file):
            try:
                with open(progress_file, 'r', encoding='utf-8') as pf:
                    prog = json.load(pf)
                with open(cp_path, 'w', encoding='utf-8') as cf:
                    json.dump({'lineOffset': prog.get('lineOffset', 0)}, cf)
            except Exception:
                pass

    duration = time.time() - start_ts
    stage_state['lastDurationSec'] = duration
    stage_state['lastStatus'] = result['status']
    if idem_enabled:
        stage_state['idempotencyKey'] = idem_key
    save_stage_state(name, stage_state)
    print(f"[DONE] {name} in {duration:.3f}s")
    return result


def aggregate_metrics(run_id: str, results: List[Dict]):
    metrics = {
        'runId': run_id,
        'timestamp': datetime.utcnow().isoformat(),
        'stages': results,
        'totalStages': len(results),
        'failedStages': sum(1 for r in results if r['status'] == 'failed'),
        'skippedStages': sum(1 for r in results if r['status'] == 'skipped'),
        'okStages': sum(1 for r in results if r['status'] == 'ok')
    }
    path = os.path.join(STATE_DIR, f"metrics_{run_id}.json")
    with open(path, 'w', encoding='utf-8') as f:
        json.dump(metrics, f, ensure_ascii=False, indent=2)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--pipeline', required=True)
    parser.add_argument('--run-id', required=True)
    args = parser.parse_args()

    pipeline = load_pipeline(args.pipeline)
    # Offline guard - scan for banned network-related imports in our code
    banned_usages = scan_for_network_imports(["src", "bin"]) 
    if banned_usages:
        msg = ", ".join([f"{p}:{b}" for p, b in banned_usages])
        raise RuntimeError(f"Network imports found (offline guard): {msg}")
    run_state = {
        'runId': args.run_id,
        'pipeline': pipeline['name'],
        'version': pipeline.get('version'),
        'startedAt': datetime.utcnow().isoformat(),
        'state': 'running'
    }
    # compute a simple run hash and chain (tamper-evident marker)
    run_hash = hashlib.sha256(json.dumps(run_state, sort_keys=True).encode('utf-8')).hexdigest()
    run_state['runHash'] = run_hash
    save_run_state(args.run_id, run_state)

    results = []
    for stage in pipeline['stages']:
        res = run_stage(stage, args.run_id)
        results.append(res)
        if res['status'] == 'failed':
            run_state['state'] = 'failed'
            break
    else:
        run_state['state'] = 'completed'

    run_state['endedAt'] = datetime.utcnow().isoformat()
    save_run_state(args.run_id, run_state)
    aggregate_metrics(args.run_id, results)
    print(f"Run {args.run_id} state: {run_state['state']}")


if __name__ == '__main__':
    main()
