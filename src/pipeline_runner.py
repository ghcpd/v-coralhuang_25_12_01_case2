import argparse
import json
import hashlib
import os
import time
import random
import platform
from datetime import datetime, timezone
from typing import List, Dict, Optional
import subprocess
import sys

STATE_DIR = "state"
LOCKS_DIR = "locks"

os.makedirs(STATE_DIR, exist_ok=True)
os.makedirs(LOCKS_DIR, exist_ok=True)
os.makedirs("data/input", exist_ok=True)
os.makedirs("data/work", exist_ok=True)
os.makedirs("data/output", exist_ok=True)

# Offline guard: prevent networking imports
FORBIDDEN_MODULES = ['requests', 'urllib.request', 'urllib3', 'http.client', 'socket', 'aiohttp']

def validate_offline_imports():
    """Ensure no networking modules are imported"""
    for mod in FORBIDDEN_MODULES:
        if mod in sys.modules:
            raise RuntimeError(f"Forbidden network module detected: {mod}")

# Resource governance defaults
DEFAULT_MAX_MEMORY_MB = 1024
DEFAULT_MAX_CPU_CORES = 2
DEFAULT_MAX_IO_CONCURRENCY = 4

class FileLock:
    """Cross-platform file locking"""
    def __init__(self, lock_path: str):
        self.lock_path = lock_path
        self.lock_file = None
        
    def __enter__(self):
        os.makedirs(os.path.dirname(self.lock_path), exist_ok=True)
        # Simple lock file approach - create file atomically
        max_attempts = 10
        for attempt in range(max_attempts):
            try:
                self.lock_file = open(self.lock_path, 'x')
                return self
            except FileExistsError:
                if attempt < max_attempts - 1:
                    time.sleep(0.1 * (attempt + 1))
                else:
                    raise RuntimeError(f"Could not acquire lock: {self.lock_path}")
        
    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.lock_file:
            self.lock_file.close()
            try:
                os.remove(self.lock_path)
            except Exception:
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
    return hashlib.sha256(raw.encode('utf-8')).hexdigest()


def apply_resource_limits(limits: Dict):
    """Apply resource governance limits (CPU, memory)"""
    max_memory = limits.get('maxMemoryMB', DEFAULT_MAX_MEMORY_MB)
    max_cpu_cores = limits.get('maxCpuCores', DEFAULT_MAX_CPU_CORES)
    
    # Note: Setting hard limits on Windows requires different approaches
    # Here we provide soft guidance via environment variables
    os.environ['PIPELINE_MAX_MEMORY_MB'] = str(max_memory)
    os.environ['PIPELINE_MAX_CPU_CORES'] = str(max_cpu_cores)


def retry_with_backoff(func, max_attempts: int = 3, base_delay: float = 1.0, jitter: float = 0.5):
    """Retry function with exponential backoff and jitter"""
    for attempt in range(max_attempts):
        try:
            return func()
        except Exception as e:
            if attempt == max_attempts - 1:
                raise
            # Check if error is retryable (IO errors, temporary failures)
            error_str = str(e).lower()
            non_retryable = ['filenotfound', 'permission denied', 'invalid']
            if any(nr in error_str for nr in non_retryable):
                raise
            
            delay = base_delay * (2 ** attempt) + random.uniform(0, jitter)
            print(f"[RETRY] Attempt {attempt + 1}/{max_attempts} failed: {e}. Retrying in {delay:.2f}s...")
            time.sleep(delay)


def run_stage(stage: Dict, run_id: str):
    name = stage['name']
    processor = stage['processor']
    inputs = stage.get('inputs', [])
    output_dir = stage['outputDir']
    params = stage.get('params', {})
    os.makedirs(output_dir, exist_ok=True)
    
    # Acquire file lock to prevent concurrent execution
    lock_path = os.path.join(LOCKS_DIR, f"{name}.lock")
    with FileLock(lock_path):
        stage_state = load_stage_state(name)

        idem_enabled = stage.get('idempotency', {}).get('enabled', False)
        idem_key = compute_idempotency_key(inputs, processor, params) if idem_enabled else None

        output_marker = os.path.join(output_dir, f".{name}.done")
        if idem_enabled and 'idempotencyKey' in stage_state and stage_state['idempotencyKey'] == idem_key and os.path.exists(output_marker):
            print(f"[SKIP] {name} (idempotent key matched)")
            return {'stage': name, 'status': 'skipped', 'reason': 'idempotent'}

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
                    if line_offset > 0:
                        print(f"[RESUME] {name} from line {line_offset}")
                except Exception:
                    line_offset = 0

        # Apply resource limits
        resource_limits = stage.get('resources', {})
        if resource_limits:
            apply_resource_limits(resource_limits)

        start_ts = time.time()
        result = {'stage': name, 'status': 'ok'}

        # Retry configuration
        retry_cfg = stage.get('retry', {})
        max_attempts = retry_cfg.get('maxAttempts', 3)
        base_delay = retry_cfg.get('baseDelay', 1.0)
        jitter = retry_cfg.get('jitter', 0.5)

        def execute_processor():
            # Execute processor script via Python
            if not os.path.exists(processor):
                raise FileNotFoundError(f"Processor not found: {processor}")

            env = os.environ.copy()
            env['PIPELINE_STAGE_NAME'] = name
            env['PIPELINE_OUTPUT_DIR'] = os.path.abspath(output_dir)
            env['PIPELINE_LINE_OFFSET'] = str(line_offset)
            env['PIPELINE_RUN_ID'] = run_id
            
            # Pass params as JSON
            if params:
                env['PIPELINE_PARAMS'] = json.dumps(params)
            
            cmd = ['python', processor] + inputs
            proc = subprocess.run(cmd, env=env, capture_output=True, text=True, timeout=300)
            if proc.returncode != 0:
                error_msg = proc.stderr.strip() or proc.stdout.strip()
                raise RuntimeError(error_msg)
            return proc

        try:
            # Execute with retry logic
            proc = retry_with_backoff(execute_processor, max_attempts, base_delay, jitter)

            # Write output marker atomically
            marker_tmp = output_marker + ".tmp"
            with open(marker_tmp, 'w') as f:
                f.write(datetime.now(timezone.utc).isoformat())
            os.replace(marker_tmp, output_marker)

        except Exception as e:
            # checkpoint not advanced on failure
            print(f"[FAIL] {name}: {e}")
            stage_state['lastError'] = str(e)
            stage_state['lastStatus'] = 'failed'
            stage_state['lastFailedAt'] = datetime.now(timezone.utc).isoformat()
            save_stage_state(name, stage_state)
            return {'stage': name, 'status': 'failed', 'error': str(e)}

        # Update checkpoint if enabled (processor may emit progress file)
        if cp_enabled:
            progress_file = os.path.join(STATE_DIR, f"progress_{name}.json")
            if os.path.exists(progress_file):
                try:
                    with open(progress_file, 'r', encoding='utf-8') as pf:
                        prog = json.load(pf)
                    cp_tmp = cp_path + ".tmp"
                    with open(cp_tmp, 'w', encoding='utf-8') as cf:
                        json.dump({'lineOffset': prog.get('lineOffset', 0)}, cf)
                    os.replace(cp_tmp, cp_path)
                except Exception as ex:
                    print(f"[WARN] Failed to update checkpoint: {ex}")

        duration = time.time() - start_ts
        stage_state['lastDurationSec'] = duration
        stage_state['lastStatus'] = result['status']
        stage_state['lastCompletedAt'] = datetime.now(timezone.utc).isoformat()
        if idem_enabled:
            stage_state['idempotencyKey'] = idem_key
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
    with open(path, 'w', encoding='utf-8') as f:
        json.dump(metrics, f, ensure_ascii=False, indent=2)


def main():
    parser = argparse.ArgumentParser(description='Offline Pipeline Runner')
    parser.add_argument('--pipeline', required=True, help='Path to pipeline.json')
    parser.add_argument('--run-id', required=True, help='Unique run identifier')
    parser.add_argument('--validate-offline', action='store_true', help='Validate no network imports')
    args = parser.parse_args()

    # Validate offline constraint if requested
    if args.validate_offline:
        validate_offline_imports()
        print("[OFFLINE] Validation passed: no forbidden network modules detected")

    pipeline = load_pipeline(args.pipeline)
    
    # Validate pipeline structure
    if 'stages' not in pipeline or not pipeline['stages']:
        raise ValueError("Pipeline must contain at least one stage")
    
    run_state = {
        'runId': args.run_id,
        'pipeline': pipeline['name'],
        'version': pipeline.get('version'),
        'startedAt': datetime.now(timezone.utc).isoformat(),
        'state': 'running',
        'hostname': platform.node(),
        'platform': platform.system()
    }
    save_run_state(args.run_id, run_state)

    results = []
    try:
        for stage in pipeline['stages']:
            print(f"\n[START] Stage: {stage['name']}")
            res = run_stage(stage, args.run_id)
            results.append(res)
            if res['status'] == 'failed':
                run_state['state'] = 'failed'
                run_state['failedStage'] = stage['name']
                break
        else:
            run_state['state'] = 'completed'
    except KeyboardInterrupt:
        print("\n[INTERRUPTED] Pipeline execution interrupted by user")
        run_state['state'] = 'interrupted'
        results.append({'stage': 'unknown', 'status': 'interrupted', 'error': 'KeyboardInterrupt'})
    except Exception as e:
        print(f"\n[ERROR] Pipeline execution failed: {e}")
        run_state['state'] = 'error'
        run_state['error'] = str(e)
        results.append({'stage': 'pipeline', 'status': 'error', 'error': str(e)})

    run_state['endedAt'] = datetime.now(timezone.utc).isoformat()
    save_run_state(args.run_id, run_state)
    aggregate_metrics(args.run_id, results)
    
    print(f"\n{'='*60}")
    print(f"Run {args.run_id} state: {run_state['state']}")
    print(f"Completed: {sum(1 for r in results if r['status'] == 'ok')}/{len(pipeline['stages'])} stages")
    print(f"{'='*60}")
    
    # Exit with non-zero if failed
    if run_state['state'] in ['failed', 'error']:
        sys.exit(1)


if __name__ == '__main__':
    main()
