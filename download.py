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
