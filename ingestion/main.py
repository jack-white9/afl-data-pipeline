import os
from dotenv import load_dotenv
from Ingestion import Ingestion

if __name__ == "__main__":
    load_dotenv()
    AWS_ACCESS_KEY_ID = os.getenv("ACCESS_KEY_ID")
    AWS_SECRET_ACCESS_KEY = os.getenv("SECRET_ACCESS_KEY")

    bucket_name = "afl-data-raw"
    file_name = "afl_data.csv"

    ingestion = Ingestion(AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY)
    data = ingestion.get_data("csv")
    ingestion.upload_to_s3(bucket_name, file_name, data)
