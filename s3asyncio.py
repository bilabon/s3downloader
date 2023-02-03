import asyncio
import logging
import time

from aiobotocore.session import get_session
from config import (AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY,
                    LOCAL_DOWNLOAD_FOLDER, S3_BUCKET, S3_FOLDER)

logger_info = logging.getLogger("log_info")
logger_error = logging.getLogger("log_error")
logger_debug = logging.getLogger("log_debug")
sem = asyncio.Semaphore(250)


async def safe_download_object(s3_client, file_name: str):
    async with sem:  # semaphore limits num of simultaneous downloads
        return await download_object(s3_client, file_name)


async def download_object(s3_client, file_name: str):
    """send request and retrieve the obj from S3"""
    download_path = f"{S3_FOLDER}/{file_name}"
    local_path = f"{LOCAL_DOWNLOAD_FOLDER}{file_name}"
    # print(f"Downloading {file_name} to {download_path}")
    response = await s3_client.get_object(
        Bucket=S3_BUCKET,
        Key=download_path,
    )
    async with response["Body"] as stream:
        obj = await stream.read()
        with open(local_path, "wb") as binary_file:
            binary_file.write(obj)
    return "Success"


async def go(download_list):
    """launch the process to download multiple files"""
    session = get_session()
    async with session.create_client(
        "s3",
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
        aws_access_key_id=AWS_ACCESS_KEY_ID,
    ) as s3_client:
        tasks = [
            safe_download_object(s3_client, file_name) for file_name in download_list
        ]
        await asyncio.gather(*tasks)


def run_parallel_asyncio(filenames_to_download: str):
    """run in parallel multiprocessing"""
    start_time = time.monotonic()
    asyncio.run(go(filenames_to_download))
    diff = time.monotonic() - start_time
    logger_debug.debug(f"Finish in {diff} min")
