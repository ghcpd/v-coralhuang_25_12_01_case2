#!/usr/bin/env python3
"""
Stage Copy Processor
Copies input files to output directory with atomic writes.
"""
import os
import sys
import shutil
import json
from pathlib import Path

def main():
    # Read pipeline environment variables
    inputs = sys.argv[1:]
    if not inputs:
        print("No input provided", file=sys.stderr)
        sys.exit(1)

    out_dir = os.environ.get('PIPELINE_OUTPUT_DIR')
    if not out_dir:
        print("Missing PIPELINE_OUTPUT_DIR", file=sys.stderr)
        sys.exit(2)

    stage_name = os.environ.get('PIPELINE_STAGE_NAME', 'stage_copy')
    
    Path(out_dir).mkdir(parents=True, exist_ok=True)

    # Copy each input file atomically
    for p in inputs:
        if not os.path.exists(p):
            print(f"Input missing: {p}", file=sys.stderr)
            sys.exit(3)
        
        target = os.path.join(out_dir, os.path.basename(p))
        target_tmp = target + ".tmp"
        
        try:
            # Copy to temp location first
            shutil.copyfile(p, target_tmp)
            # Atomic rename
            os.replace(target_tmp, target)
            print(f"Copied: {os.path.basename(p)}")
        except Exception as e:
            print(f"Failed to copy {p}: {e}", file=sys.stderr)
            sys.exit(4)

    print(f"{stage_name} completed successfully")

if __name__ == '__main__':
    main()
