import json
import os
import sys
from pathlib import Path

BANNED_MODULES = ("requests", "socket", "http.client", "urllib", "urllib3", "httpx", "asyncio", "aiohttp")


def offline_guard():
    for name in list(sys.modules.keys()):
        for banned in BANNED_MODULES:
            if name.startswith(banned):
                raise RuntimeError(f"Offline guard: module {name} should not be loaded")


offline_guard()


def atomic_write_json(path: str, data: dict):
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(data, f)
    os.replace(tmp, path)


def main():
    inputs = sys.argv[1:]
    if not inputs:
        print("No input provided", file=sys.stderr)
        exit(1)

    out_dir = os.environ.get('PIPELINE_OUTPUT_DIR')
    if not out_dir:
        print("Missing PIPELINE_OUTPUT_DIR", file=sys.stderr)
        exit(2)

    Path(out_dir).mkdir(parents=True, exist_ok=True)

    line_offset = int(os.environ.get('PIPELINE_LINE_OFFSET', '0'))
    progress_path = os.environ.get('PIPELINE_PROGRESS_PATH') or os.path.join('state', f"progress_stage_upper.json")
    line_interval = int(os.environ.get('PIPELINE_LINE_INTERVAL', '50') or '50')
    params = json.loads(os.environ.get('PIPELINE_PARAMS') or '{}')
    attempt = int(os.environ.get('PIPELINE_ATTEMPT', '1'))

    # Simulate transient failure on first attempt if requested
    if params.get('simulateTransient') and attempt == 1:
        print("Simulated transient failure", file=sys.stderr)
        sys.exit(int(params.get('transientExitCode', 75)))

    input_file = inputs[0]
    output_file = os.path.join(out_dir, 'result.txt')

    processed = 0
    with open(input_file, 'r', encoding='utf-8') as fin, open(output_file, 'a', encoding='utf-8') as fout:
        for idx, line in enumerate(fin):
            if idx < line_offset:
                continue
            fout.write(line.upper())
            processed += 1
            if line_interval and processed % line_interval == 0:
                atomic_write_json(progress_path, {'lineOffset': idx + 1})

    # Final progress write
    atomic_write_json(progress_path, {'lineOffset': line_offset + processed})

    # Optional debug: persist env vars for tests
    if os.environ.get("PIPELINE_DEBUG_ENV") == "1":
        env_path = Path("state") / "env_stage_stage_upper.json"
        env_path.parent.mkdir(parents=True, exist_ok=True)
        with open(env_path, "w", encoding="utf-8") as f:
            json.dump({
                "PIPELINE_RESOURCES_CPU_CORES": os.environ.get("PIPELINE_RESOURCES_CPU_CORES"),
                "PIPELINE_RESOURCES_MEMORY_MB": os.environ.get("PIPELINE_RESOURCES_MEMORY_MB"),
                "PIPELINE_RESOURCES_IO_CONCURRENCY": os.environ.get("PIPELINE_RESOURCES_IO_CONCURRENCY"),
            }, f, indent=2)

    print("stage_upper completed")


if __name__ == "__main__":
    main()
