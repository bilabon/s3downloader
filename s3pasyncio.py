import asyncio
import logging
import multiprocessing
import time

from aiobotocore.session import get_session
from config import (AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY,
                    LOCAL_DOWNLOAD_FOLDER, S3_BUCKET, S3_FOLDER)

logger_info = logging.getLogger("log_info")
logger_error = logging.getLogger("log_error")
logger_debug = logging.getLogger("log_debug")
SEM = asyncio.Semaphore(20)
# NUMBER_OF_PROCCESS = multiprocessing.cpu_count()
# NUMBER_OF_TASKS = multiprocessing.cpu_count() * 4
NUMBER_OF_PROCCESS = 8
NUMBER_OF_TASKS = 16


async def safe_download_object(s3_client, file_name: str):
    async with SEM:  # semaphore limits num of simultaneous downloads
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
        tasks = [download_object(s3_client, file_name) for file_name in download_list]
        await asyncio.gather(*tasks)


def singleProcess(download_list_):
    """mission for single process"""
    asyncio.run(go(download_list_))
    # loop = asyncio.new_event_loop()
    # loop.run_until_complete(go(download_list_))


def run_parallel_process_asyncio(filenames_to_download: set):
    """run in parallel process and asyncio"""
    filenames_to_download = list(filenames_to_download)
    start_time = time.monotonic()
    download_list_chunk = [
        filenames_to_download[i::NUMBER_OF_TASKS] for i in range(NUMBER_OF_TASKS)
    ]
    with multiprocessing.Pool(NUMBER_OF_PROCCESS) as p:
        p.map(singleProcess, download_list_chunk)
    diff = time.monotonic() - start_time
    logger_debug.warning(f"Finish in {diff} min")
