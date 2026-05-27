# CCBD Project 6 — Cloud Bill Estimator (Variant 6)

**Authors:** Angeliki Andreadi, Keenan Hardy  
**Course:** Cloud Computing and Big Data — SP26  

[Demo video by swisstransfer](https://www.swisstransfer.com/d/41303bda-f7d4-4eeb-837d-3a3dae809d70)

## Overview
This project analyzes how data engineering choices impact object storage costs. I built an estimator to calculate the financial trade-offs between different file formats and layouts. The goal is to provide concrete recommendations for reducing cloud spend while maintaining acceptable performance, backed by a clear distinction between measured data and approximations.

We compared 3 sets of two design choices for each dataset size:

* Compression: Snappy vs. ZSTD.
* File Sizing: Small (100k lines) vs. Compact files.
* Layout: Partitioned (Month-Year) vs. Flat.

---

## Dependencies

- Python 3.13+
- See `requirements.txt` for full list

## Configuration

```bash
python3 -m venv .venv
source .venv/bin/activate # if on MAC
source .venv/Scripts/activate # if on WINDOWS
pip install -r requirements.txt
```

Copy `.env.example` to `.env` and fill in your credentials:

```bash
cp .env.example .env
```

`.env` must contain:
```bash
AWS_ACCESS_KEY_ID=your_key
AWS_SECRET_ACCESS_KEY=your_secret
AWS_DEFAULT_REGION=your_region # e.g. eu-west-1
AWS_BUCKET_NAME=your_bucket_name
```
To maintain platform security, API keys are restricted. 
Please contact keenan.hardy@unine.ch to request access.

---

## How to Reproduce Results

### Step 1 — Generate datasets

```bash
python3 dataset_gen.py
```

Output is saved under `data/raw/`.  
Target sizes: "S": 5_000_000, "M": 25_000_000, "L": 100_000_000


### Step 2 — Run the benchmark

```bash
python3 bench.py
```

For each data size (<label> -> S, M, L), this will : 
1. Upload raw CSV to S3 under `raw/<label>/`
2. Convert and upload Parquet variants (none, snappy, zstd) to `curated/<label>/`
3. Upload small-file and partitioned layouts
4. Download each variant back from S3
5. Run the fixed analytics query locally using `pyarrow.dataset`
6. Record all metrics in `results.csv`


### Step 3 — Analyse results

```bash
jupyter notebook analysis.ipynb
```

Run all cells. The notebook reads `results.csv` and produces:
- Cost breakdown plots (storage vs requests vs transfer)
- Query time comparison across variants 

---

## Pricing Model

| Category | Unit | Price (CHF) |
|---|---|---|
| Storage | per GB-month | 0.020 |
| PUT/COPY/POST/LIST | per 1,000 requests | 0.010 |
| GET/HEAD | per 1,000 requests | 0.001 |
| Data egress (download) | per GB | 0.090 |
| Data ingress (upload) | per GB | 0.000 |

---

## Fixed Analytics Query

All variants are benchmarked against the same query:

- **Filter:** `region = "Europe"` AND `ts` between `2022-01-01` and `2023-01-01`
- **Aggregation:** `COUNT` and `AVG(value)` grouped by `event_type`
- **Engine:** `pyarrow.dataset` with predicate pushdown

---

## Variants Benchmarked

| Variant | S3 Prefix | Description |
|---|---|---|
| `raw` | `raw/<label>/` | Raw CSV baseline |
| `parquet/none` | `curated/<label>/parquet/` | Parquet, no compression |
| `parquet/snappy` | `curated/<label>/parquet/` | Parquet, Snappy |
| `parquet/zstd` | `curated/<label>/parquet/` | Parquet, Zstd |
| `parquet_small` | `curated/<label>/parquet_small/` | 50 × ~10 MB files |
| `parquet_partitioned` | `curated/<label>/parquet_partitioned/` | Partitioned by year-month |

> Label was either S, M, or L
---

## Team Contributions

See git commit history for per-commit attribution.

