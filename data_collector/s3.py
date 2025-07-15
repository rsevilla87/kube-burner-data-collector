import os
import csv
import boto3
import logging
import tempfile

logger = logging.getLogger(__name__)

def upload_csv_to_s3(chunk_rows, fieldnames, bucket, foldername, filename):
    """Uploads a CSV file to S3"""
    tmp = tempfile.NamedTemporaryFile(mode='w+', newline='', delete=False)
    try:
        writer = csv.DictWriter(tmp, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(chunk_rows)
        tmp.flush()

        # Upload to S3
        s3 = boto3.client("s3")
        s3_key = f"{foldername.rstrip('/')}/{filename}"
        s3.upload_file(tmp.name, bucket, s3_key)
        logger.info(f"✅ Uploaded chunk to s3://{bucket}/{s3_key}")
    finally:
        tmp.close()
        os.remove(tmp.name)
        logger.info(f"🧹 Temporary file {tmp.name} deleted")
