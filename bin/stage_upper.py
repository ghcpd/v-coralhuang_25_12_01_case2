#!/usr/bin/env python3
"""
stage_upper.py: Convert text to uppercase with checkpoint support.
Offline-compliant: uses only stdlib, no network I/O.
Checkpoint-aware: resumes from lineOffset, writes progress atomically.
"""

import os
import sys
import json
from pathlib import Path

inputs = sys.argv[1:]
if not inputs:
    print("No input provided", file=sys.stderr)
    sys.exit(1)

out_dir = os.environ.get('PIPELINE_OUTPUT_DIR')
if not out_dir:
    print("Missing PIPELINE_OUTPUT_DIR", file=sys.stderr)
    sys.exit(2)

Path(out_dir).mkdir(parents=True, exist_ok=True)

line_offset = int(os.environ.get('PIPELINE_LINE_OFFSET', '0'))
progress_path = os.path.join('state', f"progress_stage_upper.json")

input_file = inputs[0]
output_file = os.path.join(out_dir, 'result.txt')

try:
    processed = 0
    with open(input_file, 'r', encoding='utf-8') as fin:
        with open(output_file, 'a', encoding='utf-8') as fout:
            for idx, line in enumerate(fin):
                if idx < line_offset:
                    continue
                fout.write(line.upper())
                processed += 1
                # Write progress checkpoint every 50 lines (atomic tmp+replace)
                if processed % 50 == 0:
                    progress_tmp = progress_path + ".tmp"
                    with open(progress_tmp, 'w', encoding='utf-8') as pf:
                        json.dump({'lineOffset': idx + 1}, pf)
                    os.replace(progress_tmp, progress_path)

    # Final progress write (atomic tmp+replace)
    final_offset = line_offset + processed
    progress_tmp = progress_path + ".tmp"
    os.makedirs(os.path.dirname(progress_path), exist_ok=True)
    with open(progress_tmp, 'w', encoding='utf-8') as pf:
        json.dump({'lineOffset': final_offset}, pf)
    os.replace(progress_tmp, progress_path)

    print("stage_upper completed")
    sys.exit(0)
except Exception as e:
    print(f"stage_upper failed: {e}", file=sys.stderr)
    sys.exit(1)
