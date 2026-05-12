import boto3
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import os
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()

BUCKET     = os.getenv("AWS_BUCKET_NAME")
s3         = boto3.client("s3")
CHUNK_SIZE = 500_000


def csv_to_parquet(csv_path, parquet_path, compression="snappy"):
    reader = pd.read_csv(csv_path, chunksize=CHUNK_SIZE)
    writer = None

    for chunk in reader:
        table = pa.Table.from_pandas(chunk)
        if writer is None:
            writer = pq.ParquetWriter(parquet_path, table.schema, compression=compression)
        writer.write_table(table)

    if writer:
        writer.close()


def upload_raw(label):
    path = Path(f"demo/demo_data/raw/{label}.csv")
    size_mb = os.path.getsize(path) / 1e6
    print(f"Uploading raw CSV ({size_mb:.0f} MB)...")
    s3.upload_file(str(path), BUCKET, f"raw/{label}/{label}.csv")


def upload_parquet(label, compression="none"):
    csv_path = Path(f"demo/demo_data/raw/{label}.csv")
    out_path = Path(f"demo/demo_data/parquet/{label}.parquet")
    out_path.parent.mkdir(parents=True, exist_ok=True)

    print(f"Converting to Parquet ({compression})...")
    csv_to_parquet(csv_path, out_path, compression)

    size_mb = os.path.getsize(out_path) / 1e6
    print(f"Uploading Parquet ({size_mb:.0f} MB)...")
    s3.upload_file(str(out_path), BUCKET, f"curated/{label}/parquet/{label}.parquet")


def upload_parquet_small(label, compression="snappy"):
    csv_path   = Path(f"demo/demo_data/raw/{label}.csv")
    output_dir = Path(f"demo/demo_data/parquet_small/{label}")
    output_dir.mkdir(parents=True, exist_ok=True)

    s3_prefix = f"curated/{label}/parquet_small/"
    n_files   = 0

    for i, chunk in enumerate(pd.read_csv(csv_path, chunksize=CHUNK_SIZE)):
        table       = pa.Table.from_pandas(chunk)
        output_path = output_dir / f"part_{i:05d}.parquet"
        pq.write_table(table, output_path, compression=compression)
        s3.upload_file(str(output_path), BUCKET, f"{s3_prefix}part_{i:05d}.parquet")
        n_files += 1

    print(f"Uploaded {n_files} small files to {s3_prefix}")
    return n_files


def upload_parquet_partitioned(label, compression=None):
    csv_path   = Path(f"demo/demo_data/raw/{label}.csv")
    output_dir = Path(f"demo/demo_data/parquet_partitionned/{label}")
    output_dir.mkdir(parents=True, exist_ok=True)

    df = pd.read_csv(csv_path)
    df['year_month'] = pd.to_datetime(df['ts']).dt.to_period('M').astype(str)

    pq.write_to_dataset(
        pa.Table.from_pandas(df),
        root_path=str(output_dir),
        partition_cols=["year_month"],
        compression=compression
    )

    n_files = 0
    for f in output_dir.rglob("*.parquet"):
        s3_key = f"curated/{label}/parquet_partitioned/{f.relative_to(output_dir)}"
        s3.upload_file(str(f), BUCKET, s3_key)
        n_files += 1

    return n_files