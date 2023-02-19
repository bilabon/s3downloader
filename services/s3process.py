import logging
import time
from concurrent import futures
from concurrent.futures import ProcessPoolExecutor

import boto3.session
from config import (AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY,
                    LOCAL_DOWNLOAD_FOLDER, S3_BUCKET, S3_FOLDER)

MAX_WORKERS = 25
logger_info = logging.getLogger('log_info')
logger_error = logging.getLogger('log_error')
logger_debug = logging.getLogger('log_debug')


def download_object(file_name: str) -> str:
    """Downloads an object from S3 to local with multithreading"""
    download_path = f"{S3_FOLDER}/{file_name}"
    local_path = f"{LOCAL_DOWNLOAD_FOLDER}/{file_name}"
    # print(f"Downloading {file_name} to {download_path}")
    # for multiprocessing we create client
    s3_client = boto3.client("s3", aws_access_key_id=AWS_ACCESS_KEY_ID, aws_secret_access_key=AWS_SECRET_ACCESS_KEY)
    s3_client.download_file(
        S3_BUCKET,
        download_path,
        local_path,
    )
    return "Success"


def download_parallel_multiprocessing(keys_to_download: set) -> tuple[str, str]:
    with ProcessPoolExecutor(max_workers=MAX_WORKERS) as executor:
        future_to_key = {executor.submit(download_object, key): key for key in keys_to_download}
        for future in futures.as_completed(future_to_key):
            key = future_to_key[future]
            exception = future.exception()
            if not exception:
                yield key, future.result()
            else:
                yield key, exception


def run_parallel_multiprocessing(filenames_to_download: str):
    """run in parallel multiprocessing"""
    len_filenames = len(filenames_to_download)
    start_time = time.monotonic()
    success_counter = 0
    for key, result in download_parallel_multiprocessing(filenames_to_download):
        if result == "Success":
            success_counter += 1
            if success_counter % 100 == 0:
                diff = time.monotonic() - start_time
                logger_debug.warning(f'> {success_counter} of {len_filenames} succeeded, time: {diff}')
            logger_info.info(f"{key} result: {result}")
        else:
            logger_error.error(f"{key} result: {result}")
    diff = time.monotonic() - start_time
    logger_debug.warning(f'Finish in {diff} min')
