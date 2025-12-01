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
    # Support simulated transient failure for testing
    if os.environ.get('PIPELINE_SIMULATED_FAIL') == '1':
        print('Simulated transient failure', file=sys.stderr)
        exit(5)
    basename = os.path.basename(p)
    target = os.path.join(out_dir, basename)
    tmp_target = target + '.tmp'
    shutil.copyfile(p, tmp_target)
    os.replace(tmp_target, target)

print("stage_copy completed")
