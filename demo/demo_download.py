import boto3
import os
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()

BUCKET = os.getenv("AWS_BUCKET_NAME")
s3     = boto3.client("s3")


def download_raw(label):
    out_path = Path(f"demo/demo_data/download/{label}_raw.csv")
    out_path.parent.mkdir(parents=True, exist_ok=True)
    s3_key  = f"raw/{label}/{label}.csv"
    size_mb = s3.head_object(Bucket=BUCKET, Key=s3_key)["ContentLength"] / 1e6
    print(f"Downloading raw CSV ({size_mb:.0f} MB)...")
    s3.download_file(BUCKET, s3_key, str(out_path))


def download_parquet(label):
    out_path = Path(f"demo/demo_data/download/{label}.parquet")
    out_path.parent.mkdir(parents=True, exist_ok=True)
    s3_key  = f"curated/{label}/parquet/{label}.parquet"
    size_mb = s3.head_object(Bucket=BUCKET, Key=s3_key)["ContentLength"] / 1e6
    print(f"Downloading Parquet ({size_mb:.0f} MB)...")
    s3.download_file(BUCKET, s3_key, str(out_path))


def download_parquet_small(label):
    out_dir   = Path(f"demo/demo_data/download/parquet_small/{label}")
    out_dir.mkdir(parents=True, exist_ok=True)
    s3_prefix = f"curated/{label}/parquet_small/"

    objects = s3.list_objects_v2(Bucket=BUCKET, Prefix=s3_prefix).get("Contents", [])
    if not objects:
        print(f"No files found under {s3_prefix}")
        return

    total_mb = sum(o["Size"] for o in objects) / 1e6
    print(f"Downloading {len(objects)} small files ({total_mb:.0f} MB)...")

    for obj in objects:
        out_path = out_dir / Path(obj["Key"]).name
        s3.download_file(BUCKET, obj["Key"], str(out_path))


def download_parquet_partitioned(label):
    out_dir   = Path(f"demo/demo_data/download/parquet_partitioned/{label}")
    out_dir.mkdir(parents=True, exist_ok=True)
    s3_prefix = f"curated/{label}/parquet_partitioned/"

    objects = s3.list_objects_v2(Bucket=BUCKET, Prefix=s3_prefix).get("Contents", [])
    if not objects:
        print(f"No files found under {s3_prefix}")
        return

    total_mb = sum(o["Size"] for o in objects) / 1e6
    print(f"Downloading {len(objects)} partitioned files ({total_mb:.0f} MB)...")

    for obj in objects:
        relative_path = obj["Key"].replace(s3_prefix, "")
        out_path      = out_dir / relative_path
        out_path.parent.mkdir(parents=True, exist_ok=True)
        s3.download_file(BUCKET, obj["Key"], str(out_path))