# Project plans and notes

## Ledger
| Date  | Done  |  Notes | 
|---|---|---|
|   |   |   |
|   |   |   | 
|   |   |   | 

## Notes 

*Project steps:*
- Generate datasets (S/M/L), save as Parquet file (use PyArrow) -> mimic logs/activity of a web app
- Upload data to object storage (S3 through boto client)
  - When doing so, there will be put/get/list commands, should be counted somehow
- Benchmark harness = script measuring the performance of our pipeline (measures how much data is moved, how fast, etc)

- Compare how data engineering choices impact pricing (2 per category)
  - Format (other than parquet?)
  - Layout(no partitioning/by date?)
  - Compression methods 
  - File sizing (S/M/L)
    
- Function taking tracked metrics and applying the given price sheet
