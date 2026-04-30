# config.py
import os
from dotenv import load_dotenv

load_dotenv()

S3_CONFIG = {
    "aws_access_key_id": os.getenv("AWS_ACCESS_KEY_ID"),
    "aws_secret_access_key": os.getenv("AWS_SECRET_ACCESS_KEY"),
    "region_name": os.getenv("AWS_REGION", "eu-central-1"),
}

BUCKET_NAME = "ccbd-project6-keenan"