import os

from config import LOCAL_DOWNLOAD_FOLDER
# from services.s3asyncio import run_parallel_asyncio
# from services.s3pasyncio import run_parallel_process_asyncio
# from services.s3process import run_parallel_multiprocessing
from services.s3thread import run_parallel_multithreading
from tools.log import setup_logger
from tools.db import get_s3filenames_from_db


if __name__ == "__main__":
    setup_logger("log_info", "info.log.txt")
    setup_logger("log_error", "error.log.txt")
    setup_logger("log_debug", "debug.log.txt")

    # delete local directory for files
    if os.path.exists(LOCAL_DOWNLOAD_FOLDER):
        import shutil
        shutil.rmtree(LOCAL_DOWNLOAD_FOLDER)

    # created local directory for files
    if not os.path.exists(LOCAL_DOWNLOAD_FOLDER):
        os.makedirs(LOCAL_DOWNLOAD_FOLDER)

    filenames_to_download = get_s3filenames_from_db(limit=100, offset=0)

    # 1) uncomment to run with multithreading
    run_parallel_multithreading(filenames_to_download)

    # 2) uncomment to run with multiprocessing
    # run_parallel_multiprocessing(list(filenames_to_download))

    # 3) uncomment to run with asyncio
    # run_parallel_asyncio(filenames_to_download)

    # 4) uncomment to run with asyncio and multiprocessing
    # run_parallel_process_asyncio(filenames_to_download)
