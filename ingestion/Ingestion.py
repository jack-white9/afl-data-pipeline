import os
from dotenv import load_dotenv
import boto3
import requests


class Ingestion:
    def __init__(self, access_key, secret_key):
        self.client = boto3.client(
            "s3",
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key,
        )

    def get_data(self, format):
        res = requests.get(f"https://api.squiggle.com.au/?q=tips;format={format}")
        print(res.status_code)
        return res.text

    def upload_to_s3(self, bucket, key, file):
        self.client.put_object(Body=file, Bucket=bucket, Key=key)


if __name__ == "__main__":
    load_dotenv()
    AWS_ACCESS_KEY_ID = os.getenv("ACCESS_KEY_ID")
    AWS_SECRET_ACCESS_KEY = os.getenv("SECRET_ACCESS_KEY")

    bucket_name = "afl-data-raw"
    file_name = "afl_data.csv"

    ingestion = Ingestion(AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY)
    data = ingestion.get_data("csv")
    ingestion.upload_to_s3(bucket_name, file_name, data)
