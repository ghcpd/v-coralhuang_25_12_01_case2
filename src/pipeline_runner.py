import argparse
import json
import hashlib
import os
import time
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

    try:
        # Execute processor script via Python
        if not os.path.exists(processor):
            raise FileNotFoundError(f"Processor not found: {processor}")

        env = os.environ.copy()
        env['PIPELINE_STAGE_NAME'] = name
        env['PIPELINE_OUTPUT_DIR'] = os.path.abspath(output_dir)
        env['PIPELINE_LINE_OFFSET'] = str(line_offset)
        cmd = ['python', processor] + inputs
        proc = subprocess.run(cmd, env=env, capture_output=True, text=True)
        if proc.returncode != 0:
            result['status'] = 'failed'
            result['error'] = proc.stderr.strip() or proc.stdout.strip()
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
