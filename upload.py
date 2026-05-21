# Angeliki Andreadi, Keenan Hardy
import boto3
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import os
import time
import argparse
from pathlib import Path
from dotenv import load_dotenv


# Read the AWS settings from .env so the script can connect to the S3 bucket.
load_dotenv()


BUCKET     = os.getenv("AWS_BUCKET_NAME")
s3         = boto3.client("s3")
CHUNK_SIZE = 1_000_000  # rows per chunk, so large CSV files do not all sit in memory


def csv_to_parquet(csv_path, parquet_path, compression="snappy"):
    # Convert the raw CSV to one Parquet file, reading it piece by piece.
    reader = pd.read_csv(csv_path, chunksize=CHUNK_SIZE)
    writer = None

    for chunk in reader:
        table = pa.Table.from_pandas(chunk)

        # The first chunk decides the Parquet schema. The rest of the chunks use it too.
        if writer is None:
            writer = pq.ParquetWriter(parquet_path, table.schema, compression=compression)

        writer.write_table(table)

    if writer:
        writer.close()

# Upload the original CSV baseline to S3.

def upload_raw(label):
    path = Path(f"data/raw/{label}.csv")
    size_mb = os.path.getsize(path) / 1e6  # convert bytes to MB for readable logging
    print(f"Uploading raw CSV ({size_mb:.0f} MB)...")
    # Raw files stay under raw/<label>/ so bench.py can compare them with curated files.
    s3.upload_file(str(path), BUCKET, f"raw/{label}/{label}.csv")

# Convert the CSV to one Parquet file and upload it as a compact curated version.

def upload_parquet(label, compression="None"):
    csv_path = Path(f"data/raw/{label}.csv")
    out_path = Path(f"data/parquet/{label}.parquet")
    out_path.parent.mkdir(parents=True, exist_ok=True)  # create data/parquet/ if needed

    print(f"Converting to Parquet ({compression})...") 
    csv_to_parquet(csv_path, out_path, compression) 

    size_mb = os.path.getsize(out_path) / 1e6
    print(f"Uploading Parquet ({size_mb:.0f} MB)...")  # sanity check before sending it to S3
    # All single-file Parquet compression variants use this same curated path.
    s3.upload_file(str(out_path), BUCKET, f"curated/{label}/parquet/{label}.parquet")

# Split the CSV into many smaller Parquet files and upload each part.

def upload_parquet_small(size, compression="snappy"):
    csv_path = Path(f"data/raw/{size}.csv")  # input CSV for this dataset size
    output_dir = Path(f"data/parquet_small/{size}")  # local folder for the generated parts
    output_dir.mkdir(parents=True, exist_ok=True)

    # Each chunk becomes one separate Parquet file. This tests the cost of many objects.
    reader = pd.read_csv(csv_path, chunksize=100_000)
    s3_prefix = f"curated/{size}/parquet_small/"
    n_files = 0

    for i, chunk in enumerate(reader):
        table = pa.Table.from_pandas(chunk)
        output_path = output_dir/f"part_{i:05d}.parquet"
        pq.write_table(table, output_path, compression=compression)
        s3.upload_file(str(output_path), BUCKET, f"{s3_prefix}part_{i:05d}.parquet")
        n_files += 1
    

    print(f"Uploaded {n_files} small files to {s3_prefix}")
    return n_files

# Write Parquet files partitioned by month, then upload the folder structure to S3.

def upload_parquet_partitioned(size, compression=None):
    csv_path = Path(f"data/raw/{size}.csv")
    output_dir = Path(f"data/parquet_partitionned/{size}")
    output_dir.mkdir(parents=True, exist_ok=True)

    df = pd.read_csv(csv_path)
    # Add a month column from the timestamp. Pyarrow will turn this into folders.
    df['year_month'] = pd.to_datetime(df['ts']).dt.to_period('M').astype(str)  # example: "2021-03"

    table = pa.Table.from_pandas(df)

    pq.write_to_dataset(
        table,
        root_path=str(output_dir),
        partition_cols=["year_month"],
        compression=compression
    )

    n_files = 0
    for f in output_dir.rglob("*.parquet"):
        # Keep the same partition path on S3, for example year_month=2021-03/file.parquet.
        s3_key = f"curated/{size}/parquet_partitioned/{f.relative_to(output_dir)}"
        s3.upload_file(str(f), BUCKET, s3_key)
        n_files += 1

    return n_files
