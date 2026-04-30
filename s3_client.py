import boto3
from dotenv import load_dotenv
import os

load_dotenv()

s3 = boto3.client("s3")

try:
    s3.head_bucket(Bucket=os.getenv("AWS_BUCKET_NAME"))
    print("Connected to S3 bucket successfully.")
except Exception as e:
    print(f"Connection failed: {e}")