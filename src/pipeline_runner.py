import argparse
import json
import hashlib
import os
import re
import time
import random
from datetime import datetime
from typing import List, Dict
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


DISALLOWED_IMPORTS = ['requests', 'socket', 'http.client', 'urllib', 'asyncio']


def check_offline_guard(root_dir: str) -> None:
    # scan python files for any disallowed imports; raise if found
    for folder, _, files in os.walk(root_dir):
        for fn in files:
            if not fn.endswith('.py'):
                continue
            path = os.path.join(folder, fn)
            try:
                with open(path, 'r', encoding='utf-8') as f:
                    content = f.read()
                for token in DISALLOWED_IMPORTS:
                    # Match 'import token' or 'from token import' as an actual import statement (start of line with optional whitespace)
                    p1 = re.compile(rf"^[\s]*import\s+{re.escape(token)}\b", re.MULTILINE)
                    p2 = re.compile(rf"^[\s]*from\s+{re.escape(token)}\s+import\b", re.MULTILINE)
                    if p1.search(content) or p2.search(content):
                        raise RuntimeError(f"Offline guard: forbidden import '{token}' found in {path}")
            except UnicodeDecodeError:
                continue


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
    lock_path = os.path.join(LOCKS_DIR, f"{name}.lock")

    def acquire_lock(max_wait_s: int = 5):
        waited = 0
        backoff = 0.05
        while True:
            try:
                fd = os.open(lock_path, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
                os.write(fd, str(os.getpid()).encode('utf-8'))
                os.close(fd)
                return True
            except FileExistsError:
                if waited >= max_wait_s:
                    return False
                time.sleep(backoff)
                waited += backoff
                backoff = min(1.0, backoff * 2)

    def release_lock():
        try:
            if os.path.exists(lock_path):
                os.remove(lock_path)
        except Exception:
            pass

    try:
        if not acquire_lock():
            raise RuntimeError(f"Could not acquire lock for stage {name}")
        # Execute processor script via Python with retry/backoff for transient local failures
        if not os.path.exists(processor):
            raise FileNotFoundError(f"Processor not found: {processor}")

        env = os.environ.copy()
        env['PIPELINE_STAGE_NAME'] = name
        env['PIPELINE_OUTPUT_DIR'] = os.path.abspath(output_dir)
        env['PIPELINE_LINE_OFFSET'] = str(line_offset)
        # Optional resource hints; processors may read these and apply limits
        resources = stage.get('resources', {})
        if 'cpu' in resources:
            env['PIPELINE_CPU_LIMIT'] = str(resources.get('cpu'))
        if 'memoryMB' in resources:
            env['PIPELINE_MEMORY_MB'] = str(resources.get('memoryMB'))

        cmd = ['python', processor] + inputs

        max_attempts = int(stage.get('retry', {}).get('maxAttempts', 3))
        base_delay = float(stage.get('retry', {}).get('baseDelaySeconds', 0.1))
        attempt = 0
        while True:
            attempt += 1
            proc = subprocess.run(cmd, env=env, capture_output=True, text=True)
            # Convention: if return code == 10 -> transient failure (retryable)
            if proc.returncode == 0:
                break
            elif proc.returncode == 10 and attempt < max_attempts:
                # transient error, backoff and retry
                delay = base_delay * (2 ** (attempt - 1))
                jitter = random.uniform(0, base_delay)
                delay = delay + jitter
                print(f"[RETRY] {name} attempt {attempt}/{max_attempts} after transient error: {proc.stderr.strip() or proc.stdout.strip()} (sleep {delay:.2f}s)")
                time.sleep(delay)
                continue
            else:
                # non-retryable or out of attempts
                result['status'] = 'failed'
                result['error'] = proc.stderr.strip() or proc.stdout.strip()
                raise RuntimeError(result['error'])

        # Write output marker atomically
        tmp_marker = output_marker + ".tmp"
        with open(tmp_marker, 'w', encoding='utf-8') as f:
            f.write(datetime.utcnow().isoformat())
        os.replace(tmp_marker, output_marker)

    except Exception as e:
        # checkpoint not advanced on failure
        print(f"[FAIL] {name}: {e}")
        stage_state['lastError'] = str(e)
        stage_state['lastStatus'] = 'failed'
        save_stage_state(name, stage_state)
        release_lock()
        return {'stage': name, 'status': 'failed', 'error': str(e)}

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
    # Save stage state atomically along with running tamper-evident hash chain
    prev_hash = stage_state.get('stateHash')
    # Update state fields before computing hash
    stage_state['lastDurationSec'] = duration
    stage_state['lastStatus'] = result['status']
    if idem_enabled:
        stage_state['idempotencyKey'] = idem_key
    # compute new state hash
    content = json.dumps(stage_state, sort_keys=True, ensure_ascii=False)
    state_hash = hashlib.sha256(content.encode('utf-8')).hexdigest()
    stage_state['stateHash'] = hashlib.sha256((state_hash + (prev_hash or '')).encode('utf-8')).hexdigest()
    save_stage_state(name, stage_state)
    release_lock()
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
    tmp = path + '.tmp'
    with open(tmp, 'w', encoding='utf-8') as f:
        json.dump(metrics, f, ensure_ascii=False, indent=2)
    os.replace(tmp, path)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--pipeline', required=True)
    parser.add_argument('--run-id', required=True)
    args = parser.parse_args()

    # Run offline guard
    check_offline_guard('.')
    pipeline = load_pipeline(args.pipeline)
    run_state = {
        'runId': args.run_id,
        'pipeline': pipeline['name'],
        'version': pipeline.get('version'),
        'startedAt': datetime.utcnow().isoformat(),
        'state': 'running'
    }
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
