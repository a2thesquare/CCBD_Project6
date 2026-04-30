import boto3
import os
import time
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()

BUCKET   = os.getenv("AWS_BUCKET_NAME")
csv_path = Path("data/raw/S.csv")
s3_key   = "raw/S/S.csv"

s3 = boto3.client("s3")

size_mb = os.path.getsize(csv_path) / 1_000_000 # approximate, I think it should be / 1024*1024.0 according to this : https://stackoverflow.com/questions/6080477/how-to-get-the-size-of-tar-gz-in-mb-file-in-python

t0 = time.time()
s3.upload_file(str(csv_path), BUCKET, s3_key)
elapsed = time.time() - t0

print(f"done in {elapsed:.1f}s  -> {size_mb / elapsed:.1f} MB/s") # i wanted to know how long M and L is going to take, IT'S GONNA BE LONG 
