import boto3
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import os
import time
import argparse
from pathlib import Path
from dotenv import load_dotenv


# Load AWS credentials and bucket name from the .env file
load_dotenv()


BUCKET     = os.getenv("AWS_BUCKET_NAME")
s3         = boto3.client("s3")
CHUNK_SIZE = 1_000_000  # rows per chunk — avoids loading the full CSV into memory at once


def csv_to_parquet(csv_path, parquet_path, compression="snappy"):
    # Read the CSV in chunks to handle large files without running out of RAM
    reader = pd.read_csv(csv_path, chunksize=CHUNK_SIZE)
    writer = None

    for chunk in reader:
        table = pa.Table.from_pandas(chunk)

        # Initialize the writer on the first chunk using the schema inferred from it
        # All subsequent chunks must match this schema
        if writer is None:
            writer = pq.ParquetWriter(parquet_path, table.schema, compression=compression)

        writer.write_table(table)

    if writer:
        writer.close()

# Upload of csv to s3

def upload_raw(label):
    path = Path(f"data/raw/{label}.csv")
    size_mb = os.path.getsize(path) / 1e6  # getsize returns bytes, divide by 1e6 to get MB
    print(f"Uploading raw CSV ({size_mb:.0f} MB)...")
    # S3 key format: raw/<label>/<label>.csv
    s3.upload_file(str(path), BUCKET, f"raw/{label}/{label}.csv")

# Upload of parquet to s3

def upload_parquet(label, compression="None"): # changed the compression here
    csv_path = Path(f"data/raw/{label}.csv")
    out_path = Path(f"data/parquet/{label}.parquet")
    out_path.parent.mkdir(parents=True, exist_ok=True)  # create data/parquet/ if it doesn't exist

    print(f"Converting to Parquet ({compression})...") 
    csv_to_parquet(csv_path, out_path, compression) 

    size_mb = os.path.getsize(out_path) / 1e6
    print(f"Uploading Parquet ({size_mb:.0f} MB)...") # just to check if it's the right size or if we have to cancel the action
    # S3 key format: curated/<label>/parquet/<label>.parquet
    s3.upload_file(str(out_path), BUCKET, f"curated/{label}/parquet/{label}.parquet")

# Upload segmented parquet
# Havents put print statements yet

def upload_parquet_small(size, compression="snappy"):
    csv_path = Path(f"data/raw/{size}.csv") # where we read from 
    output_dir = Path(f"data/parquet_small/{size}") # where the new files will be stored
    output_dir.mkdir(parents=True, exist_ok=True)

    reader = pd.read_csv(csv_path, chunksize=100_000) # loads 1000000 rows at a time
    s3_prefix = f"curated/{size}/parquet_small/" # where the files will be upload on s3
    n_files = 0

    for i, chunk in enumerate(reader): # loops trough the csv chunk by chunk
        table = pa.Table.from_pandas(chunk) # convert the pd df in a pyarrow table
        output_path = output_dir/f"part_{i:05d}.parquet" # create file path for that chunk
        pq.write_table(table, output_path, compression=compression) # write the chunk 
        s3.upload_file(str(output_path), BUCKET, f"{s3_prefix}part_{i:05d}.parquet") # uploads chunk to s3
        n_files += 1
    

    print(f"Uploaded {n_files} small files to {s3_prefix}")
    return n_files

# Upload of partitioned by date files

def upload_parquet_partitioned(size, compression=None):
    csv_path = Path(f"data/raw/{size}.csv")
    output_dir = Path(f"data/parquet_partitionned/{size}")
    output_dir.mkdir(parents=True, exist_ok=True)

    df = pd.read_csv(csv_path)
    df['year_month'] = pd.to_datetime(df['ts']).dt.to_period('M').astype(str)  # "2021-03"

    table = pa.Table.from_pandas(df)

    pq.write_to_dataset(
        table,
        root_path=str(output_dir),
        partition_cols=["year_month"],
        compression=compression
    )

    n_files = 0
    for f in output_dir.rglob("*.parquet"):
        s3_key = f"curated/{size}/parquet_partitioned/{f.relative_to(output_dir)}"
        s3.upload_file(str(f), BUCKET, s3_key)
        n_files += 1

    return n_files