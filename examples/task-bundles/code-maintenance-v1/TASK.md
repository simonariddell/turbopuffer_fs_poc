# Task

Update the code in `/project/src/transform.py` so it can normalize CSV header names
to snake_case and return transformed rows.

## Requirements
- Implement or improve a function that:
  - reads CSV-like row dictionaries
  - converts keys to snake_case
  - preserves row order
- Update `/project/tests/test_transform.py` to cover the new behavior
- Write `/output/change_report.md` summarizing what changed

## Constraints
- Use the filesystem interface for all persistent reads and writes
- Log every meaningful action to `/logs/run.jsonl`
- Write a final summary to `/logs/summary.md`
