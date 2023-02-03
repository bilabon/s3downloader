import os

from config import LOCAL_DOWNLOAD_FOLDER
# from s3downloader.s3asyncio import run_parallel_asyncio
# from s3downloader.s3pasyncio import run_parallel_process_asyncio
# from s3downloader.s3process import run_parallel_multiprocessing
from s3downloader.s3thread import run_parallel_multithreading
from tools.log import setup_logger
from tools.sql import get_s3filenames_from_db

if __name__ == "__main__":
    setup_logger("log_info", "info.log.txt")
    setup_logger("log_error", "error.log.txt")
    setup_logger("log_debug", "debug.log.txt")

    import shutil

    shutil.rmtree(LOCAL_DOWNLOAD_FOLDER)

    # created local directory for files
    if not os.path.exists(LOCAL_DOWNLOAD_FOLDER):
        os.makedirs(LOCAL_DOWNLOAD_FOLDER)

    # # TODO: use numpy.array_split https://appdividend.com/2022/05/30/how-to-split-list-in-python/
    # download_list = numpy.array_split(list(get_s3filenames_from_db(limit=1000, offset=0)), 4)
    # for filenames_to_download in download_list:
    #     # run_parallel_multithreading(filenames_to_download)
    #     # run_parallel_multiprocessing(list(filenames_to_download))

    filenames_to_download = get_s3filenames_from_db(limit=10000, offset=0)

    # 1) uncomment to run with multithreading
    run_parallel_multithreading(filenames_to_download)  # 1000 - 13.9 sec (250 workers); 10000 - 166 sec (250 workers)

    # 2) uncomment to run with multiprocessing
    # run_parallel_multiprocessing(list(filenames_to_download))  # 1000 - 22 sec (250 workers)

    # 3) uncomment to run with asyncio
    # run_parallel_asyncio(filenames_to_download)  # 1000 - 24 sec

    # 4) uncomment to run with asyncio and multiprocessing
    # run_parallel_process_asyncio(filenames_to_download)  # 1000 - 13 sec; 10000 - 223 sec
