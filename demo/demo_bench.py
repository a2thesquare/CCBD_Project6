import boto3
import os
import time
from pathlib import Path
from dotenv import load_dotenv
import pandas as pd
import pyarrow as pa
import pyarrow.dataset as ds
import pyarrow.compute as pc

from demo_upload import upload_raw, upload_parquet, upload_parquet_small, upload_parquet_partitioned
from demo_download import download_raw, download_parquet, download_parquet_small, download_parquet_partitioned

load_dotenv()

BUCKET = os.getenv("AWS_BUCKET_NAME")
s3     = boto3.client("s3")

STORAGE_PER_GB_MONTH = 0.020
PUT_PER_1000         = 0.010
GET_PER_1000         = 0.001
EGRESS_PER_GB        = 0.090

RESULTS_PATH = Path("demo/demo_results.csv")


def dir_size(path):
    p = Path(path)
    if p.is_dir():
        return sum(f.stat().st_size for f in p.rglob("*") if f.is_file())
    return p.stat().st_size


def get_stored_gb(prefix):
    t0           = time.time()
    response     = s3.list_objects_v2(Bucket=BUCKET, Prefix=prefix)
    listing_time = time.time() - t0
    objects      = response.get("Contents", [])
    return sum(o["Size"] for o in objects) / 1e9, len(objects), round(listing_time, 4)


def run_query(label, variant, compression=None):
    paths = {
        "raw":                f"demo/demo_data/download/{label}_raw.csv",
        "parquet":            f"demo/demo_data/download/{label}.parquet",
        "parquet_small":      f"demo/demo_data/download/parquet_small/{label}",
        "parquet_partitioned":f"demo/demo_data/download/parquet_partitioned/{label}",
    }
    formats = {
        "raw":                "csv",
        "parquet":            "parquet",
        "parquet_small":      "parquet",
        "parquet_partitioned":"parquet",
    }

    kwargs = {"format": formats[variant]}
    if variant == "parquet_partitioned":
        kwargs["partitioning"] = "hive"

    dataset = ds.dataset(paths[variant], **kwargs)

    filt = (
        (pc.field("region") == "Europe") &
        (pc.field("ts").cast(pa.string()) >= "2022-01-01") &
        (pc.field("ts").cast(pa.string()) <  "2023-01-01")
    )

    t0      = time.time()
    table   = dataset.to_table(filter=filt, columns=["event_type", "value"])
    elapsed = time.time() - t0

    df = table.to_pandas()
    df["value"] = pd.to_numeric(df["value"], errors="coerce")
    result = df.groupby("event_type")["value"].agg(count="count", avg="mean")

    print(f"\n[{label}/{variant}/{compression}] Query in {elapsed:.2f}s")
    print(result.to_string())

    return round(elapsed, 3)


def append_query_time(label, variant, compression, query_time_s):
    df   = pd.read_csv(RESULTS_PATH)
    mask = (
        (df["label"] == label) &
        (df["variant"] == variant) &
        (df["compression"].fillna("None").astype(str) == str(compression))
    )
    df.loc[mask, "query_time_s"] = query_time_s
    df.to_csv(RESULTS_PATH, index=False)


def compute_cost(stored_gb, puts, gets, lists, egress_gb):
    storage  = stored_gb * STORAGE_PER_GB_MONTH
    requests = ((puts + lists) / 1000) * PUT_PER_1000 + (gets / 1000) * GET_PER_1000
    transfer = egress_gb * EGRESS_PER_GB
    return storage, requests, transfer, storage + requests + transfer


def run_exp(label, variant, compression=None):
    download_paths = {
        "raw":                Path(f"demo/demo_data/download/{label}_raw.csv"),
        "parquet":            Path(f"demo/demo_data/download/{label}.parquet"),
        "parquet_small":      Path(f"demo/demo_data/download/parquet_small/{label}"),
        "parquet_partitioned":Path(f"demo/demo_data/download/parquet_partitioned/{label}"),
    }
    s3_prefixes = {
        "raw":                f"raw/{label}/",
        "parquet":            f"curated/{label}/parquet/",
        "parquet_small":      f"curated/{label}/parquet_small/",
        "parquet_partitioned":f"curated/{label}/parquet_partitioned/",
    }
    upload_source_paths = {
        "raw":                Path(f"demo/demo_data/raw/{label}.csv"),
        "parquet":            Path(f"demo/demo_data/parquet/{label}.parquet"),
        "parquet_small":      Path(f"demo/demo_data/parquet_small/{label}"),
        "parquet_partitioned":Path(f"demo/demo_data/parquet_partitionned/{label}"),
    }

    t0 = time.time()

    if variant == "raw":
        upload_raw(label)
        puts, gets, lists = 1, 2, 1
    elif variant == "parquet":
        upload_parquet(label, compression)
        puts, gets, lists = 1, 2, 1
    elif variant == "parquet_small":
        n_files = upload_parquet_small(label)
        puts, gets, lists = n_files, n_files * 2, 1
    elif variant == "parquet_partitioned":
        n_files = upload_parquet_partitioned(label, compression)
        puts, gets, lists = n_files, n_files * 2, n_files

    upload_time  = time.time() - t0        
    upload_bytes = dir_size(upload_source_paths[variant])  

    t0 = time.time()
    {"raw": download_raw, "parquet": download_parquet,
     "parquet_small": download_parquet_small,
     "parquet_partitioned": download_parquet_partitioned}[variant](label)
    download_time  = time.time() - t0
    download_bytes = dir_size(download_paths[variant])

    stored_gb, n_files, listing_time_s = get_stored_gb(s3_prefixes[variant])
    storage, requests, transfer, total = compute_cost(
        stored_gb, puts, gets, lists, download_bytes / 1e9
    )

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

    RESULTS_PATH.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame([row]).to_csv(RESULTS_PATH, mode="a", header=not RESULTS_PATH.exists(), index=False)


if __name__ == "__main__":
    for label in ["N"]:
        run_exp(label, "raw")
        append_query_time(label, "raw", None, run_query(label, "raw"))

        run_exp(label, "parquet", compression="none")
        append_query_time(label, "parquet", "none", run_query(label, "parquet"))

        run_exp(label, "parquet", compression="snappy")
        append_query_time(label, "parquet", "snappy", run_query(label, "parquet", compression="snappy"))

        run_exp(label, "parquet", compression="zstd")
        append_query_time(label, "parquet", "zstd", run_query(label, "parquet", compression="zstd"))

        run_exp(label, "parquet_small")
        append_query_time(label, "parquet_small", None, run_query(label, "parquet_small"))

        run_exp(label, "parquet_partitioned")
        append_query_time(label, "parquet_partitioned", None, run_query(label, "parquet_partitioned"))