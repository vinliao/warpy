import os

import boto3
from botocore.exceptions import NoCredentialsError
from tqdm import tqdm


def upload_to_s3(file_name: str, bucket_name: str, object_name: str):
    s3 = boto3.resource(
        "s3",
        endpoint_url="https://159cf33b100c2bee8783ee5604f0f62e.r2.cloudflarestorage.com",
        aws_access_key_id=os.getenv("WARPY_R2_ACCESS_KEY"),
        aws_secret_access_key=os.getenv("WARPY_R2_SECRET_KEY"),
    )
    try:
        # Use tqdm to add a progress bar to the upload process
        with tqdm(total=os.path.getsize(file_name)) as pbar:
            s3.Object(bucket_name, object_name).upload_file(
                file_name, Callback=pbar.update
            )
        print(f"{file_name} uploaded successfully to {bucket_name} as {object_name}")
        return True
    except FileNotFoundError:
        print(f"The file {file_name} was not found")
        return False
    except NoCredentialsError:
        print("Credentials not available")
        return False


def main():
    tar_gz_file_path = "1681979704072.tar.gz"
    tar_gz_file_name = os.path.basename(tar_gz_file_path)

    # Set the S3 bucket name
    bucket_name = "warpy-datasets"

    # Set the S3 object name
    upload_to_s3(tar_gz_file_path, bucket_name, tar_gz_file_name)
