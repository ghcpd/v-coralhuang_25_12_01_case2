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
# Optionally simulate a transient local failure once
transient_flag = os.environ.get('PIPELINE_SIMULATE_TRANSIENT', '0')
state_dir = os.path.join('state')
Path(state_dir).mkdir(parents=True, exist_ok=True)
sim_marker = os.path.join(state_dir, 'sim_stage_copy_transient')
if transient_flag == '1' and not os.path.exists(sim_marker):
    # write marker and fail transiently
    with open(sim_marker, 'w') as f:
        f.write('transient')
    print('Simulating transient error', file=sys.stderr)
    # exit code 10 means transient
    sys.exit(10)

print("stage_copy completed")
