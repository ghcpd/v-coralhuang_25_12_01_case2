import os, sys, json
from pathlib import Path

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
progress_path = os.path.join('state', f"progress_stage_upper.json")

input_file = inputs[0]
output_file = os.path.join(out_dir, 'result.txt')

# If resuming, read existing output lines
existing_lines = []
if line_offset > 0 and os.path.exists(output_file):
    with open(output_file, 'r', encoding='utf-8') as f:
        existing_lines = f.readlines()

processed = 0
with open(input_file, 'r', encoding='utf-8') as fin, open(output_file, 'a', encoding='utf-8') as fout:
    for idx, line in enumerate(fin):
        if idx < line_offset:
            continue
        fout.write(line.upper())
        processed += 1
        if processed % 50 == 0:
            # atomic progress write
            tmp = progress_path + '.tmp'
            with open(tmp, 'w', encoding='utf-8') as pf:
                json.dump({'lineOffset': idx + 1}, pf)
            os.replace(tmp, progress_path)

# Final progress write
tmp = progress_path + '.tmp'
with open(tmp, 'w', encoding='utf-8') as pf:
    json.dump({'lineOffset': line_offset + processed}, pf)
os.replace(tmp, progress_path)

print("stage_upper completed")
