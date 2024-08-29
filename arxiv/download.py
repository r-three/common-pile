import os

import boto3
from boto3.s3.transfer import TransferConfig

from licensed_pile import logs



def s3_download(endpoint_url, bucket_name, file_path, output_path, aws_profile=None, overwrite=False):
    logger = logs.get_logger("arxiv-papers")

    if not overwrite and os.path.exists(output_path):
        logger.info(f"Skipping download since {output_path} already exists")
        return

    session = boto3.Session(profile_name=aws_profile)
    s3 = session.resource("s3", endpoint_url=endpoint_url)
    bucket = s3.Bucket(bucket_name)
    config = TransferConfig(max_bandwidth=1024*1024)

    logger.info(f"Downloading from {os.path.join(endpoint_url, bucket_name, file_path)} to {output_path}")
    bucket.download_file(file_path, output_path, Config=config)
