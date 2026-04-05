# Task

Clean `/data/dirty_sales.csv`.

## Requirements
- Normalize column names to snake_case
- Drop rows with missing `order_id`
- Fill missing numeric `amount_usd` values with `0`
- Sort by `order_date`, then `order_id`
- Write cleaned data to `/output/sales.cleaned.csv`
- Write a short report to `/output/cleaning_report.md`

## Constraints
- Use the filesystem interface for all persistent reads and writes
- Log every meaningful action to `/logs/run.jsonl`
- Write a final summary to `/logs/summary.md`
- Do not modify the original input files
