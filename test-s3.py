import boto3
from config import S3_CONFIG, BUCKET_NAME

def test_s3():
    s3 = boto3.client("s3", **S3_CONFIG)

    # 1. Create bucket
    print("Creating bucket...")
    try:
        s3.create_bucket(
            Bucket=BUCKET_NAME,
            CreateBucketConfiguration={"LocationConstraint": S3_CONFIG["region_name"]}
        )
        print(f"  Bucket '{BUCKET_NAME}' created")
    except s3.exceptions.BucketAlreadyOwnedByYou:
        print(f"  Bucket '{BUCKET_NAME}' already exists")

    # 2. Upload a small test object
    print("Uploading test object...")
    s3.put_object(Bucket=BUCKET_NAME, Key="test/hello.txt", Body=b"Hello CCBD!")
    print("  Upload successful")

    # 3. Download it back
    print("Downloading test object...")
    response = s3.get_object(Bucket=BUCKET_NAME, Key="test/hello.txt")
    content = response["Body"].read()
    assert content == b"Hello CCBD!", "Content mismatch!"
    print(f"   Download successful: {content.decode()}")

    # 4. List objects
    print("Listing objects...")
    result = s3.list_objects_v2(Bucket=BUCKET_NAME, Prefix="test/")
    for obj in result.get("Contents", []):
        print(f"   Found: {obj['Key']} ({obj['Size']} bytes)")

    # # 5. Cleanup
    # print("Cleaning up...")
    # s3.delete_object(Bucket=BUCKET_NAME, Key="test/hello.txt")
    # print("   Test object deleted")

    print("\n All tests passed! S3 is ready.")

if __name__ == "__main__":
    test_s3()