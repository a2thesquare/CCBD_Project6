import boto3
import os
import time
from pathlib import Path
from dotenv import load_dotenv
import pandas as pd

# reuse upload/download functions from our existing scripts
from upload import upload_raw, upload_parquet, upload_parquet_small, upload_parquet_partitioned
from download import download_raw, download_parquet, download_parquet_small, download_parquet_partitioned

load_dotenv()

BUCKET = os.getenv("AWS_BUCKET_NAME")
s3     = boto3.client("s3")

# AWS S3 pricing 
STORAGE_PER_GB_MONTH = 0.020  # per GB stored per month
PUT_PER_1000         = 0.010  # per 1000 PUT/LIST requests
GET_PER_1000         = 0.001  # per 1000 GET requests
EGRESS_PER_GB        = 0.090  # per GB downloaded (egress)

def dir_size(path):
    # Getting correct path, it was returning 0 mbs up and down speed
    p = Path(path)
    if p.is_dir():
        return sum(f.stat().st_size for f in p.rglob("*") if f.is_file())
    return p.stat().st_size

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


# -- run for S ----------------

# label = "S"

# # --- raw ---
# # time for upload
# t0 = time.time()
# upload_raw(label)
# up_time = time.time() - t0

# # time for download
# up_bytes = os.path.getsize(f"data/raw/{label}.csv")
# t0 = time.time()
# download_raw(label)
# dl_time = time.time() - t0

# dl_bytes = os.path.getsize(f"data/download/{label}_raw.csv")
# run_bench(label, "raw", puts=1, gets=1, lists=1,
#           up_bytes=up_bytes, dl_bytes=dl_bytes, up_time=up_time, dl_time=dl_time,
#           prefix=f"raw/{label}/")

# # --- parquet (all compression types) ---
# for compression in ["snappy", "zstd", "gzip"]:
#     # time for download
#     t0 = time.time()
#     upload_parquet(label, compression)
#     up_time = time.time() - t0

#     # time for upload 
#     up_bytes = os.path.getsize(f"data/parquet/{label}.parquet")
#     t0 = time.time()
#     download_parquet(label, compression)
#     dl_time = time.time() - t0

#     dl_bytes = os.path.getsize(f"data/download/{label}.parquet")
#     run_bench(label, f"parquet/{compression}", puts=1, gets=1, lists=1,
#               up_bytes=up_bytes, dl_bytes=dl_bytes, up_time=up_time, dl_time=dl_time,
#               prefix=f"curated/{label}/parquet/")
    
#----------------------------------------------------------------------------------------------------
# not touching the code on top yet, just trying it out as another way

    # Alternative as a main funct and loop
RESULTS_PATH = Path("results.csv")

def run_exp(label, variant, compression=None):
    if variant == "raw": # just a csv
        upload_path = Path(f"data/raw/{label}.csv")
        download_path = Path(f"data/download/{label}_raw.csv")
        s3_prefix = f"raw/{label}/"
        
    elif variant == "parquet":
        upload_path = Path(f"data/parquet/{label}.parquet")
        download_path = Path(f"data/download/{label}.parquet")
        s3_prefix = f"curated/{label}/parquet/"

    elif variant == "parquet_small":
        upload_path = Path(f"data/parquet_small/{label}")
        download_path = Path(f"data/download/parquet_small/{label}")
        s3_prefix = f"curated/{label}/parquet_small/"

    
    elif variant == "parquet_partitioned":
        upload_path = Path(f"data/parquet_partitionned/{label}")
        download_path = Path(f"data/download/parquet_partitioned/{label}")
        s3_prefix = f"curated/{label}/parquet_partitioned/"
        
    # time to upload
    t0 = time.time() # return current time in sec

    if variant == "raw":
        upload_raw(label) #upload happens
        puts, gets, lists = 1,1,1

    elif variant == "parquet":
        upload_parquet(label, compression)
        puts, gets, lists = 1,1,1

    elif variant == "parquet_small":
        n_files = upload_parquet_small(label)
        puts, gets, lists = n_files, n_files, 1

    elif variant == "parquet_partitioned":
        n_files = upload_parquet_partitioned(label, compression)
        puts, gets, lists = n_files, n_files, n_files

    upload_time = time.time() - t0 # time now - t0
    upload_bytes = dir_size(upload_path)

    # time to download
    t0 = time.time()

    if variant == "raw":
        download_raw(label)
    elif variant == "parquet":
        download_parquet(label)
    elif variant == "parquet_small":
        download_parquet_small(label)
    elif variant == "parquet_partitioned":
        download_parquet_partitioned(label)

    download_time = time.time() - t0
    download_bytes = dir_size(download_path)


    # measure S3 storage
    stored_gb, n_files = get_stored_gb(s3_prefix)
    egress_gb = download_bytes / 1e9

    # compute the cost
    storage, requests, transfer, total = compute_cost(stored_gb, puts, gets, lists, egress_gb)

    row = {
    "label": label,
    "variant": variant,
    "compression":compression,
    "n_files": n_files,
    "stored_mb": round(stored_gb * 1000, 2),
    "up_mbps": round(upload_bytes / 1e6 / upload_time, 2),
    "dl_mbps": round(download_bytes / 1e6 / download_time, 2),
    "puts": puts,
    "gets": gets,
    "lists":lists,
    "storage_chf": round(storage, 6),
    "requests_chf": round(requests, 6),
    "transfer_chf": round(transfer, 6),
    "total_chf": round(total, 6),
}
    df_row = pd.DataFrame([row])
    df_row.to_csv(RESULTS_PATH, mode="a", header=not RESULTS_PATH.exists(), index=False)


if __name__ == "__main__":
    for label in ["S"]:
        # 1) raw CSV and raw parquet
        run_exp(label, "raw")
        run_exp(label, "parquet", compression="none")

        # # 2) parquet with snappy vs zstd vs gzip
        run_exp(label, "parquet", compression="snappy")
        run_exp(label, "parquet", compression="zstd")

        # # 3) parquet small vs parquet compact
        run_exp(label, "parquet_small") 

        # # 4) parquet sectionned by date
        run_exp(label, "parquet_partitioned")
