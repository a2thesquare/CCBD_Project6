# CCBD_Project6

This project analyzes how data engineering choices impact object storage costs. I built an estimator to calculate the financial trade-offs between different file formats and layouts. The goal is to provide concrete recommendations for reducing cloud spend while maintaining acceptable performance, backed by a clear distinction between measured data and approximations.

We compared at two design choices for each dataset size:

* Compression: Snappy vs. ZSTD.
* File Sizing: Small (100k lines) vs. Compact files.
* Layout: Partitioned (Month-Year) vs. Flat.

## Project structure

```text
CCBD_PROJECT6/
├── dataset_gen.py
├── data/                  # Generated data in 3 sizes
├── s3_client.py
├── upload.py      
├── download.py
├── bench.py
├── analysis.ipynb         # Graphs and plots                  
├── requirements.txt       # Python libraries used in the project
└── README.md
```

## Files

`dataset_gen.py`

Generates the Small, Medium, and Large datasets in various formats (CSV, Parquet) and compression types.

`s3_client.py`

Handles the Boto3 connection to AWS and manages bucket infrastructure.

`upload.py`

Uploads data to S3 while measuring upload speed and counting PUT/LIST requests.

`download.py`

Downloads data from S3 to measure retrieval performance and count GET requests.

`analysis.ipynb`

Calculates the final cloud bill in CHF and generates charts for the comparison report.

## Setup 


