import boto3
from dotenv import load_dotenv
import os

load_dotenv()

# if data.db doens't exist, download it with boto3 s3
r2_access_key = os.getenv('WARPY_R2_ACCESS_KEY')
r2_access_secret_key = os.getenv('WARPY_R2_SECRET_KEY')
r2_endpoint_url = 'https://159cf33b100c2bee8783ee5604f0f62e.r2.cloudflarestorage.com'

s3 = boto3.resource('s3',
                    endpoint_url=r2_endpoint_url,
                    aws_access_key_id=r2_access_key,
                    aws_secret_access_key=r2_access_secret_key
                    )

# Upload a file to S3
with open('data.db', 'rb') as f:
    s3.Bucket('warpy').put_object(Key='data.db', Body=f)
