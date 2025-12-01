import json
import os
import shutil
import sys
from pathlib import Path

BANNED_MODULES = ("requests", "socket", "http.client", "urllib", "urllib3", "httpx", "asyncio", "aiohttp")


def offline_guard():
    # Conservative runtime guard against network modules
    for name in list(sys.modules.keys()):
        for banned in BANNED_MODULES:
            if name.startswith(banned):
                raise RuntimeError(f"Offline guard: module {name} should not be loaded")


offline_guard()


def atomic_copy(src: str, dst: str):
    tmp = dst + ".tmp"
    shutil.copyfile(src, tmp)
    os.replace(tmp, dst)


def main():
    inputs = sys.argv[1:]
    if not inputs:
        print("No input provided", file=sys.stderr)
        exit(1)

    out_dir = os.environ.get("PIPELINE_OUTPUT_DIR")
    if not out_dir:
        print("Missing PIPELINE_OUTPUT_DIR", file=sys.stderr)
        exit(2)

    Path(out_dir).mkdir(parents=True, exist_ok=True)

    for p in inputs:
        if not os.path.exists(p):
            print(f"Input missing: {p}", file=sys.stderr)
            exit(3)
        target = os.path.join(out_dir, os.path.basename(p))
        atomic_copy(p, target)

    # Optional debug: persist env to state for tests
    if os.environ.get("PIPELINE_DEBUG_ENV") == "1":
        env_path = Path("state") / "env_stage_stage_copy.json"
        env_path.parent.mkdir(parents=True, exist_ok=True)
        with open(env_path, "w", encoding="utf-8") as f:
            json.dump({
                "PIPELINE_RESOURCES_CPU_CORES": os.environ.get("PIPELINE_RESOURCES_CPU_CORES"),
                "PIPELINE_RESOURCES_MEMORY_MB": os.environ.get("PIPELINE_RESOURCES_MEMORY_MB"),
                "PIPELINE_RESOURCES_IO_CONCURRENCY": os.environ.get("PIPELINE_RESOURCES_IO_CONCURRENCY"),
            }, f, indent=2)

    print("stage_copy completed")


if __name__ == "__main__":
    main()
