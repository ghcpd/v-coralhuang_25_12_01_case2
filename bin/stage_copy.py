#!/usr/bin/env python3
"""
stage_copy.py: Copy input files to output directory.
Offline-compliant: uses only stdlib, no network I/O.
Atomic writes: uses tmp+replace pattern.
"""

import os
import sys
import shutil
from pathlib import Path

# Copy first input file to output directory retaining name
inputs = sys.argv[1:]
if not inputs:
    print("No input provided", file=sys.stderr)
    sys.exit(1)

out_dir = os.environ.get('PIPELINE_OUTPUT_DIR')
if not out_dir:
    print("Missing PIPELINE_OUTPUT_DIR", file=sys.stderr)
    sys.exit(2)

Path(out_dir).mkdir(parents=True, exist_ok=True)

try:
    for p in inputs:
        if not os.path.exists(p):
            print(f"Input missing: {p}", file=sys.stderr)
            sys.exit(3)
        target = os.path.join(out_dir, os.path.basename(p))
        # Atomic copy: use tmp + replace
        target_tmp = target + ".tmp"
        shutil.copyfile(p, target_tmp)
        os.replace(target_tmp, target)
    print("stage_copy completed")
    sys.exit(0)
except Exception as e:
    print(f"stage_copy failed: {e}", file=sys.stderr)
    sys.exit(1)
