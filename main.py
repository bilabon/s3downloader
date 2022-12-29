import logging
import os
import time
from concurrent import futures
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor

import boto3.session
import mysql.connector
from config import (AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, DB_DATABASE,
                    DB_HOST, DB_PASSWORD, DB_USER, S3_BUCKET, S3_FOLDER)

LOCAL_DOWNLOAD_FOLDER = "files/"
MAX_WORKERS = 25

logger_info = logging.getLogger('log_info')
logger_error = logging.getLogger('log_error')
logger_debug = logging.getLogger('log_debug')


def setup_logger(logger_name, log_file, level=logging.DEBUG):
    l = logging.getLogger(logger_name)
    formatter = logging.Formatter('%(message)s')
    fileHandler = logging.FileHandler(log_file, mode='w')
    fileHandler.setFormatter(formatter)
    streamHandler = logging.StreamHandler()
    streamHandler.setFormatter(formatter)
    l.setLevel(level)
    l.addHandler(fileHandler)
    l.addHandler(streamHandler)


def download_object(file_name: str, s3_client=None) -> str:
    """Downloads an object from S3 to local with multithreading"""
    download_path = f"{S3_FOLDER}/{file_name}"
    local_path = f"{LOCAL_DOWNLOAD_FOLDER}/{file_name}"
    logger_debug.debug(f"Downloading {file_name} to {download_path}")
    if not s3_client:
        # for multiprocessing we create client
        s3_client = boto3.client("s3", aws_access_key_id=AWS_ACCESS_KEY_ID, aws_secret_access_key=AWS_SECRET_ACCESS_KEY)
    s3_client.download_file(
        S3_BUCKET,
        download_path,
        local_path,
    )
    return "Success"


def download_parallel_multithreading(keys_to_download: set) -> tuple[str, str]:
    # Create a session and use it to make our client
    session = boto3.session.Session(
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY
    )
    s3_client = session.client("s3")

    # Dispatch work tasks with our s3_client
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        future_to_key = {executor.submit(download_object, key, s3_client): key for key in keys_to_download}

        for future in futures.as_completed(future_to_key):
            key = future_to_key[future]
            exception = future.exception()

            if not exception:
                yield key, future.result()
            else:
                yield key, exception


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


def get_all_s3filenames() -> set:
    """Get and return set of file names for downloading"""
    try:
        logger_debug.debug("MySQL connection is opening")
        connection = mysql.connector.connect(
            host=DB_HOST,
            database=DB_DATABASE,
            user=DB_USER,
            password=DB_PASSWORD,
        )
        logger_debug.debug("MySQL connection is opened")
        sql = "select hash_file_name from web_products_order_files where file_status_id=6 limit 100;"
        cursor = connection.cursor()
        cursor.execute(sql)
        # get all records
        records = cursor.fetchall()
        records = set(item[0] for item in records)
    except mysql.connector.Error as e:
        logger_debug.error("Error reading data from MySQL table", e)
    finally:
        if connection.is_connected():
            connection.close()
            cursor.close()
            logger_debug.debug("MySQL connection is closed")
    return (records)


if __name__ == "__main__":
    # setup loggers
    setup_logger('log_info', "info.log.txt")
    setup_logger('log_error', "error.log.txt")
    setup_logger('log_debug', "debug.log.txt")

    # created local directory for files
    if not os.path.exists(LOCAL_DOWNLOAD_FOLDER):
        os.makedirs(LOCAL_DOWNLOAD_FOLDER)

    _keys_to_download = get_all_s3filenames()
    len_keys = len(_keys_to_download)

    start_time = time.monotonic()
    success_counter = 0
    for key, result in download_parallel_multithreading(_keys_to_download):
        if result == "Success":
            success_counter += 1
            if success_counter % 100 == 0:
                diff = time.monotonic() - start_time
                logger_debug.debug(f'> {success_counter} of {len_keys} succeeded, time: {diff}')
            logger_info.info(f"{key} result: {result}")
        else:
            logger_error.error(f"{key} result: {result}")
    diff = time.monotonic() - start_time
    logger_debug.debug(f'Finish in {diff} min')
