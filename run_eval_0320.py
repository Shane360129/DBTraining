"""Wrapper to run eval and capture all output."""
import subprocess, sys

result = subprocess.run(
    [sys.executable, 'eval__en_spider_val.py',
     '--model', 'outputs/models/wp_m09_dora_0320_spider/final_model'],
    capture_output=True, text=True, encoding='utf-8', errors='replace',
    cwd='D:/spider1_training'
)

with open('outputs/eval_0320_stdout.txt', 'w', encoding='utf-8') as f:
    f.write(result.stdout)
with open('outputs/eval_0320_stderr.txt', 'w', encoding='utf-8') as f:
    f.write(result.stderr)

print(f'EXIT CODE: {result.returncode}')
print('=== STDOUT (last 3000 chars) ===')
print(result.stdout[-3000:])
print('=== STDERR (last 3000 chars) ===')
print(result.stderr[-3000:])
