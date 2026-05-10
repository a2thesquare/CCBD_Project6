import boto3
import os
import time
import argparse
from pathlib import Path
from dotenv import load_dotenv


# Load AWS credentials and bucket name from the .env file
load_dotenv()


BUCKET = os.getenv("AWS_BUCKET_NAME")
s3     = boto3.client("s3")


def download_raw(label):
    out_path = Path(f"data/download/{label}_raw.csv")
    out_path.parent.mkdir(parents=True, exist_ok=True)  # create data/download/ if it doesn't exist
    s3_key = f"raw/{label}/{label}.csv"

    # head_object fetches metadata only (no data transferred) — used to get file size before downloading
    size_mb = s3.head_object(Bucket=BUCKET, Key=s3_key)["ContentLength"] / 1e6
    print(f"Downloading raw CSV ({size_mb:.0f} MB)...")

    t0 = time.time()
    s3.download_file(BUCKET, s3_key, str(out_path))
    elapsed = time.time() - t0

    print(f"Done in {elapsed:.1f}s -> {size_mb / elapsed:.1f} MB/s")


def download_parquet(label, compression="snappy"):
    out_path = Path(f"data/download/{label}.parquet")
    out_path.parent.mkdir(parents=True, exist_ok=True)  # create data/download/ if it doesn't exist
    # S3 key format: curated/<label>/parquet/<label>.parquet
    s3_key = f"curated/{label}/parquet/{label}.parquet"

    # head_object fetches metadata only (no data transferred) — used to get file size before downloading
    size_mb = s3.head_object(Bucket=BUCKET, Key=s3_key)["ContentLength"] / 1e6
    print(f"Downloading Parquet ({compression}) ({size_mb:.0f} MB)...")  # just to check if it's the right size or if we have to cancel the action

    t0 = time.time()
    s3.download_file(BUCKET, s3_key, str(out_path))
    elapsed = time.time() - t0

    print(f"Done in {elapsed:.1f}s -> {size_mb / elapsed:.1f} MB/s")

def download_parquet_small(label, compression=None):
    out_dir = Path(f"data/download/parquet_small/{label}")
    out_dir.mkdir(parents=True, exist_ok=True)
    s3_prefix = f"curated/{label}/parquet_small/" #since its going to be many files, its a file not a .parquet

    response = s3.list_objects_v2(Bucket=BUCKET, Prefix=s3_prefix)
    objects = response.get("Contents", [])

    if not objects:
        print(f"No files found under {s3_prefix}")

    total_mb = sum(o["Size"] for o in objects)/1e6

    t0 = time.time()
    for obj in objects:
        filename = Path(obj["Key"]).name # extract just the filenames from the s3 key
        out_path = out_dir/filename
        s3.download_file(BUCKET, obj["Key"], str(out_path))
    time_passed = time.time() - t0

def download_parquet_partitioned(label, compression=None):
    out_dir = Path(f"data/download/parquet_partitioned/{label}")
    out_dir.mkdir(parents=True, exist_ok=True)
    s3_prefix = f"curated/{label}/parquet_partitioned/"

    response = s3.list_objects_v2(Bucket=BUCKET, prefix=s3_prefix)
    objects = response.get("Contents", [])

    if not objects:
        print(f"No files found under {s3_prefix}")
    

    total_mb = sum(o["Size"] for o in objects) / 1e6
    print(f"Downloading {len(objects)} partitioned Parquet files ({total_mb:.0f} MB)...")

    t0 = time.time()
    for obj in objects:
        # rebuild the subfolder structure locally
        relative_path = obj["Key"].replace(s3_prefix, "")
        out_path      = out_dir / relative_path
        out_path.parent.mkdir(parents=True, exist_ok=True)  # create date=.../ subfolders
        s3.download_file(BUCKET, obj["Key"], str(out_path))
    elapsed = time.time() - t0

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("dataset", choices=["S", "M", "L"])
    parser.add_argument("variant", choices=["raw", "parquet"])
    parser.add_argument(
        "-compression",
        default="snappy",
        choices=["snappy", "zstd", "gzip"],
        help="Parquet compression codec (default: snappy)"
    )
    args = parser.parse_args()

    if args.variant == "raw":
        download_raw(args.dataset)
    elif args.variant == "parquet":
        download_parquet(args.dataset, args.compression)


# Commands to run this:

# python download.py S raw
# python download.py S parquet
# python download.py S parquet -compression zstd
# python download.py S parquet -compression gzip

# just run from bench, this will be useless 
