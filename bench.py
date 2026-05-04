import boto3
import os
import time
from pathlib import Path
from dotenv import load_dotenv

# reuse upload/download functions from our existing scripts
from upload import upload_raw, upload_parquet
from download import download_raw, download_parquet

load_dotenv()

BUCKET = os.getenv("AWS_BUCKET_NAME")
s3     = boto3.client("s3")

# AWS S3 pricing 
STORAGE_PER_GB_MONTH = 0.020  # per GB stored per month
PUT_PER_1000         = 0.010  # per 1000 PUT/LIST requests
GET_PER_1000         = 0.001  # per 1000 GET requests
EGRESS_PER_GB        = 0.090  # per GB downloaded (egress)


def get_stored_gb(prefix):
    # list_objects_v2 returns all objects under a given prefix - used to measure actual storage footprint
    response = s3.list_objects_v2(Bucket=BUCKET, Prefix=prefix)
    objects  = response.get("Contents", [])
    return sum(o["Size"] for o in objects) / 1e9, len(objects)


def compute_cost(stored_gb, puts, gets, lists, egress_gb):
    storage  = stored_gb * STORAGE_PER_GB_MONTH
    requests = ((puts + lists) / 1000) * PUT_PER_1000
    requests += (gets / 1000) * GET_PER_1000
    transfer = egress_gb * EGRESS_PER_GB
    return storage, requests, transfer, storage + requests + transfer


def run_bench(label, variant, puts, gets, lists, up_bytes, dl_bytes, up_time, dl_time, prefix):
    stored_gb, n_files = get_stored_gb(prefix)
    egress_gb = dl_bytes / 1e9  # bytes downloaded converted to GB for cost calculation

    storage, requests, transfer, total = compute_cost(stored_gb, puts, gets, lists, egress_gb)

    print(f"\n{'='*40}")
    print(f"  {label} / {variant}")
    print(f"{'='*40}")
    print(f"  files in S3:       {n_files}")
    print(f"  stored:            {stored_gb*1000:.0f} MB")
    print(f"  upload speed:      {up_bytes/1e6/up_time:.1f} MB/s")
    print(f"  download speed:    {dl_bytes/1e6/dl_time:.1f} MB/s")
    print(f"  PUT requests:      {puts}")
    print(f"  GET requests:      {gets}")
    print(f"  LIST requests:     {lists}")
    print(f"  --- cost breakdown ---")
    print(f"  storage:           CHF {storage:.6f}")
    print(f"  requests:          CHF {requests:.6f}")
    print(f"  transfer (egress): CHF {transfer:.6f}")
    print(f"  TOTAL:             CHF {total:.6f}")


# -- run for S -----

label = "S"

# --- raw ---
t0 = time.time(); upload_raw(label); up_time = time.time() - t0
up_bytes = os.path.getsize(f"data/raw/{label}.csv")
t0 = time.time(); download_raw(label); dl_time = time.time() - t0
dl_bytes = os.path.getsize(f"data/download/{label}_raw.csv")
run_bench(label, "raw", puts=1, gets=1, lists=1,
          up_bytes=up_bytes, dl_bytes=dl_bytes, up_time=up_time, dl_time=dl_time,
          prefix=f"raw/{label}/")

# --- parquet (all compression types) ---
for compression in ["snappy", "zstd", "gzip"]:
    t0 = time.time(); upload_parquet(label, compression); up_time = time.time() - t0
    up_bytes = os.path.getsize(f"data/parquet/{label}.parquet")
    t0 = time.time(); download_parquet(label, compression); dl_time = time.time() - t0
    dl_bytes = os.path.getsize(f"data/download/{label}.parquet")
    run_bench(label, f"parquet/{compression}", puts=1, gets=1, lists=1,
              up_bytes=up_bytes, dl_bytes=dl_bytes, up_time=up_time, dl_time=dl_time,
              prefix=f"curated/{label}/parquet/")