import os, sys, shutil, json
from pathlib import Path

# Copy first input file to output directory retaining name
inputs = sys.argv[1:]
if not inputs:
    print("No input provided", file=sys.stderr)
    exit(1)

out_dir = os.environ.get('PIPELINE_OUTPUT_DIR')
if not out_dir:
    print("Missing PIPELINE_OUTPUT_DIR", file=sys.stderr)
    exit(2)

Path(out_dir).mkdir(parents=True, exist_ok=True)

for p in inputs:
    if not os.path.exists(p):
        print(f"Input missing: {p}", file=sys.stderr)
        exit(3)
    target = os.path.join(out_dir, os.path.basename(p))
    shutil.copyfile(p, target)

print("stage_copy completed")
