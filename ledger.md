# Project plans and notes

## Ledger
| Date  | Done  |  Notes | 
|---|---|---|
| 23/04  | Dataset_gen  | Origninally had issues because I couldn't create them directly in the git, had create them locally. |
| 23/04  | csv_to_parquet | Done but files are local, not on github (probably fine) | 
| 27/04  | Dataset_gen | Added seed and modulable path  | 
| 01/05  | Fixed Boto being visible in repo, changed keys for security. Added Upload,download and bench for raw and parquet |
| 10/05  | Added the upload_parquet_small(), upload_parquet_partion() and the corresponding downloads and the global function in bench.py |
| 11/05 | bench.py works so implementation is done, analysis of results started, README started |

*ROADMAP TO THE END:*
- analysis.ipynb -- what do we keep what do we get rid of 
- Add nano data and manipulation <<<<<<<<<<<
- check overall code -> weird prints in download and upload --> pimp bench.py
- Finish README
- Finish report
