# Main benchmark script: upload variants, download them, query them, and save results.
import boto3
import os
import time
from pathlib import Path
from dotenv import load_dotenv
import pandas as pd
import pyarrow as pa
import pyarrow.dataset as ds
import pyarrow.compute as pc
from datetime import datetime, timezone

# Reuse upload/download functions so the benchmark stays focused on measurement.
from upload import upload_raw, upload_parquet, upload_parquet_small, upload_parquet_partitioned
from download import download_raw, download_parquet, download_parquet_small, download_parquet_partitioned

# Load AWS credentials and bucket name from .env.
load_dotenv()


BUCKET = os.getenv("AWS_BUCKET_NAME")
s3     = boto3.client("s3")


# Simple pricing model used to estimate the S3 bill for each variant.
STORAGE_PER_GB_MONTH = 0.020  # per GB stored per month
PUT_PER_1000         = 0.010  # per 1000 PUT/LIST requests
GET_PER_1000         = 0.001  # per 1000 GET requests
EGRESS_PER_GB        = 0.090  # per GB downloaded (egress)


def dir_size(path):
    # Works for both one file and folders with many files.
    p = Path(path)
    if p.is_dir():
        return sum(f.stat().st_size for f in p.rglob("*") if f.is_file())
    return p.stat().st_size


def get_stored_gb(prefix):
    # Ask S3 what is stored under a prefix and also time the listing request.
    t0 = time.time()
    response = s3.list_objects_v2(Bucket=BUCKET, Prefix=prefix)
    listing_time = time.time() - t0
    objects = response.get("Contents", [])
    return sum(o["Size"] for o in objects) / 1e9, len(objects), round(listing_time, 4)

def run_query(label, variant, compression=None):

    # Pick the local downloaded dataset that matches the variant being tested.
    if variant == "raw":
        path = f"data/download/{label}_raw.csv"
        dataset = ds.dataset(path, format="csv")
    elif variant == "parquet":
        path = f"data/download/{label}.parquet"
        dataset = ds.dataset(path, format="parquet")
    elif variant == "parquet_small":
        path = f"data/download/parquet_small/{label}"
        dataset = ds.dataset(path, format="parquet")
    elif variant == "parquet_partitioned":
        path = f"data/download/parquet_partitioned/{label}"
        dataset = ds.dataset(path, format="parquet", partitioning="hive")  # reads year_month=... folders

    # Same filter for every variant, so the timing comparison is fair.
    REGION = "Europe"
    TIME_START = "2022-01-01"
    TIME_END   = "2023-01-01"

    # Pyarrow can apply this filter before loading all rows into memory.
    filt = (
        (pc.field("region") == REGION) &
        (pc.field("ts").cast(pa.string()) >= TIME_START) &
        (pc.field("ts").cast(pa.string()) <  TIME_END)
    )

    t0 = time.time()
    table = dataset.to_table(filter=filt, columns=["event_type", "value"])
    elapsed = time.time() - t0

    # After filtering we calculate the requested count and average per event type.
    df = table.to_pandas()
    df["value"] = pd.to_numeric(df["value"], errors="coerce")
    result = df.groupby("event_type")["value"].agg(count="count", avg="mean")

    print(f"\n[{label}/{variant}/{compression}] Query in {elapsed:.2f}s")
    print(result.to_string())

    return round(elapsed, 3)  # returned to caller so it can be written into results.csv


def append_query_time(label, variant, compression, query_time_s):
    # run_exp creates the row first, this fills in the query time afterwards.
    df = pd.read_csv(RESULTS_PATH)
    mask = (
        (df["label"] == label) &
        (df["variant"] == variant) &
        (df["compression"].fillna("None").astype(str) == str(compression))
    )
    df.loc[mask, "query_time_s"] = query_time_s
    df.to_csv(RESULTS_PATH, index=False)


def compute_cost(stored_gb, puts, gets, lists, egress_gb):
    # Split the estimate into storage, requests, and download transfer.
    storage  = stored_gb * STORAGE_PER_GB_MONTH
    requests = ((puts + lists) / 1000) * PUT_PER_1000
    requests += (gets / 1000) * GET_PER_1000
    transfer = egress_gb * EGRESS_PER_GB
    return storage, requests, transfer, storage + requests + transfer

RESULTS_PATH = Path("results.csv")


def run_exp(label, variant, compression=None):
    # Set the local download path and S3 prefix for the chosen layout.
    if variant == "raw":
        download_path = Path(f"data/download/{label}_raw.csv")
        s3_prefix     = f"raw/{label}/"

    elif variant == "parquet":
        download_path = Path(f"data/download/{label}.parquet")
        s3_prefix     = f"curated/{label}/parquet/"

    elif variant == "parquet_small":
        download_path = Path(f"data/download/parquet_small/{label}")
        s3_prefix     = f"curated/{label}/parquet_small/"

    elif variant == "parquet_partitioned":
        download_path = Path(f"data/download/parquet_partitioned/{label}")
        s3_prefix     = f"curated/{label}/parquet_partitioned/"

    # Upload the chosen layout and count the approximate S3 requests it causes.
    t0 = time.time()

    if variant == "raw":
        upload_raw(label)
        upload_bytes = dir_size(Path(f"data/raw/{label}.csv"))
        # One uploaded file: 1 PUT, 1 download GET, 1 query GET, and 1 LIST.
        puts, gets, lists = 1, 2, 1

    elif variant == "parquet":
        upload_parquet(label, compression)
        upload_bytes = dir_size(Path(f"data/parquet/{label}.parquet"))
        # Same request estimate as raw because it is also one object.
        puts, gets, lists = 1, 2, 1

    elif variant == "parquet_small":
        n_files = upload_parquet_small(label)
        upload_bytes = dir_size(Path(f"data/parquet_small/{label}"))
        # Many objects increase request cost: one PUT and two GETs per file.
        puts, gets, lists = n_files, n_files * 2, 1

    elif variant == "parquet_partitioned":
        n_files = upload_parquet_partitioned(label, compression)
        upload_bytes = dir_size(Path(f"data/parquet_partitionned/{label}"))
        # Partition folders are counted as extra LIST work in this simple estimate.
        puts, gets, lists = n_files, n_files * 2, n_files

    upload_time = time.time() - t0

    # Download the same variant back locally so the query uses the measured file layout.
    t0 = time.time()

    if variant == "raw":
        download_raw(label)
    elif variant == "parquet":
        download_parquet(label)
    elif variant == "parquet_small":
        download_parquet_small(label)
    elif variant == "parquet_partitioned":
        download_parquet_partitioned(label)

    download_time  = time.time() - t0
    download_bytes = dir_size(download_path)

    # Measure what is stored on S3 after upload and convert downloaded bytes to egress GB.
    stored_gb, n_files, listing_time_s = get_stored_gb(s3_prefix)
    egress_gb = download_bytes / 1e9

    # Compute the estimated bill and save one row in results.csv.
    storage, requests, transfer, total = compute_cost(stored_gb, puts, gets, lists, egress_gb)

    row = {
    "label":          label,
    "variant":        variant,
    "compression":    compression,
    "n_files":        n_files,
    "stored_mb":      round(stored_gb * 1000, 2),
    "up_mbps":        round(upload_bytes / 1e6 / upload_time, 2),
    "dl_mbps":        round(download_bytes / 1e6 / download_time, 2),
    "puts":           puts,
    "gets":           gets,
    "lists":          lists,
    "listing_time_s": listing_time_s,
    "storage_chf":    round(storage, 6),
    "requests_chf":   round(requests, 6),
    "transfer_chf":   round(transfer, 6),
    "total_chf":      round(total, 6),
    "query_time_s":   None,
}
    df_row = pd.DataFrame([row])
    df_row.to_csv(RESULTS_PATH, mode="a", header=not RESULTS_PATH.exists(), index=False)

if __name__ == "__main__":
    for label in ["S","M","L"]:
        # 1) Baseline: raw CSV, then Parquet without compression.
        run_exp(label, "raw")
        qt = run_query(label, "raw")
        append_query_time(label, "raw", None, qt)

        run_exp(label, "parquet", compression="none")
        qt = run_query(label, "parquet")
        append_query_time(label, "parquet", "none", qt)

        # 2) Compression comparison: Snappy vs ZSTD.
        run_exp(label, "parquet", compression="snappy")
        qt = run_query(label, "parquet", compression="snappy")
        append_query_time(label, "parquet", "snappy", qt)

        run_exp(label, "parquet", compression="zstd")
        qt = run_query(label, "parquet", compression="zstd")
        append_query_time(label, "parquet", "zstd", qt)

        # 3) File-size comparison: many small Parquet files.
        run_exp(label, "parquet_small")
        qt = run_query(label, "parquet_small")
        append_query_time(label, "parquet_small", None, qt)

        # 4) Layout comparison: Parquet partitioned by month.
        run_exp(label, "parquet_partitioned")
        qt = run_query(label, "parquet_partitioned")
        append_query_time(label, "parquet_partitioned", None, qt)
