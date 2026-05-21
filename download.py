import boto3
import os
import time
import argparse
from pathlib import Path
from dotenv import load_dotenv


# Read the AWS settings from .env so the script knows which S3 bucket to use.
load_dotenv()


BUCKET = os.getenv("AWS_BUCKET_NAME")
s3     = boto3.client("s3")

def download_raw(label):
    out_path = Path(f"data/download/{label}_raw.csv")
    out_path.parent.mkdir(parents=True, exist_ok=True)  # make the local download folder if needed
    s3_key = f"raw/{label}/{label}.csv"

    # Check the object metadata first so we can print the size before downloading it.
    size_mb = s3.head_object(Bucket=BUCKET, Key=s3_key)["ContentLength"] / 1e6
    print(f"Downloading raw CSV ({size_mb:.0f} MB)...")

    s3.download_file(BUCKET, s3_key, str(out_path))

def download_parquet(label, compression="snappy"):
    out_path = Path(f"data/download/{label}.parquet")
    out_path.parent.mkdir(parents=True, exist_ok=True)  # make the local download folder if needed
    # Single-file Parquet variants are stored under this curated S3 prefix.
    s3_key = f"curated/{label}/parquet/{label}.parquet"

    # Metadata is enough to get the file size; the real download happens below.
    size_mb = s3.head_object(Bucket=BUCKET, Key=s3_key)["ContentLength"] / 1e6
    print(f"Downloading Parquet ({compression}) ({size_mb:.0f} MB)...")  # useful sanity check for large files

    s3.download_file(BUCKET, s3_key, str(out_path))

def download_parquet_small(label, compression=None):
    out_dir = Path(f"data/download/parquet_small/{label}")
    out_dir.mkdir(parents=True, exist_ok=True)
    # Small-file layout has many Parquet files, so we download everything under the prefix.
    s3_prefix = f"curated/{label}/parquet_small/"

    response = s3.list_objects_v2(Bucket=BUCKET, Prefix=s3_prefix)
    objects = response.get("Contents", [])

    if not objects:
        print(f"No files found under {s3_prefix}")

    total_mb = sum(o["Size"] for o in objects)/1e6
    print(f"Downloading {len(objects)} raw ({total_mb:.0f} MB)...")

    for obj in objects:
        # Keep only the filename because this layout does not need nested folders locally.
        filename = Path(obj["Key"]).name
        out_path = out_dir/filename
        s3.download_file(BUCKET, obj["Key"], str(out_path))

def download_parquet_partitioned(label, compression=None):
    out_dir = Path(f"data/download/parquet_partitioned/{label}")
    out_dir.mkdir(parents=True, exist_ok=True)
    # Partitioned files are stored inside year_month=... folders on S3.
    s3_prefix = f"curated/{label}/parquet_partitioned/"

    response = s3.list_objects_v2(Bucket=BUCKET, Prefix=s3_prefix)
    objects = response.get("Contents", [])

    if not objects:
        print(f"No files found under {s3_prefix}")
    

    total_mb = sum(o["Size"] for o in objects) / 1e6
    print(f"Downloading {len(objects)} partitioned Parquet files ({total_mb:.0f} MB)...")

    for obj in objects:
        # Rebuild the same partition folders locally so pyarrow can read them later.
        relative_path = obj["Key"].replace(s3_prefix, "")
        out_path      = out_dir / relative_path
        out_path.parent.mkdir(parents=True, exist_ok=True)
        s3.download_file(BUCKET, obj["Key"], str(out_path))
