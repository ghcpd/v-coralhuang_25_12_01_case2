#!/usr/bin/env python3
"""
Stage Upper Processor
Converts text to uppercase with checkpoint support for resumable processing.
"""
import os
import sys
import json
from pathlib import Path

def save_progress(progress_path: str, line_offset: int):
    """Atomically save progress checkpoint"""
    tmp_path = progress_path + ".tmp"
    with open(tmp_path, 'w', encoding='utf-8') as pf:
        json.dump({'lineOffset': line_offset}, pf, indent=2)
    os.replace(tmp_path, progress_path)

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

    stage_name = os.environ.get('PIPELINE_STAGE_NAME', 'stage_upper')
    line_offset = int(os.environ.get('PIPELINE_LINE_OFFSET', '0'))
    
    Path(out_dir).mkdir(parents=True, exist_ok=True)
    
    progress_path = os.path.join('state', f"progress_{stage_name}.json")

    input_file = inputs[0]
    if not os.path.exists(input_file):
        print(f"Input file not found: {input_file}", file=sys.stderr)
        sys.exit(3)
    
    output_file = os.path.join(out_dir, 'result.txt')
    output_tmp = output_file + ".tmp"

    # Determine mode based on checkpoint
    if line_offset > 0:
        print(f"Resuming from line {line_offset}")
        # When resuming, read existing output and append
        if os.path.exists(output_file):
            shutil.copy2(output_file, output_tmp)
            mode = 'a'
        else:
            mode = 'w'
    else:
        mode = 'w'

    processed = 0
    total_lines = 0
    
    try:
        with open(input_file, 'r', encoding='utf-8') as fin:
            with open(output_tmp, mode, encoding='utf-8') as fout:
                for idx, line in enumerate(fin):
                    total_lines = idx + 1
                    if idx < line_offset:
                        continue
                    
                    fout.write(line.upper())
                    processed += 1
                    
                    # Save checkpoint every 50 lines
                    if processed % 50 == 0:
                        save_progress(progress_path, idx + 1)
                        print(f"Processed {processed} lines (checkpoint at {idx + 1})")
        
        # Final progress write
        save_progress(progress_path, line_offset + processed)
        
        # Atomic rename of output
        os.replace(output_tmp, output_file)
        
        print(f"{stage_name} completed: processed {processed} lines (total {total_lines})")
        
    except Exception as e:
        print(f"Error processing: {e}", file=sys.stderr)
        # Clean up temp file
        if os.path.exists(output_tmp):
            os.remove(output_tmp)
        sys.exit(4)

if __name__ == '__main__':
    import shutil
    main()
