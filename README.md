# s3downloader

Parallel downloader from S3.

Just use (check in main.py):
- `run_parallel_multithreading()` for threads
- `run_parallel_multiprocessing()` for processes
- `run_parallel_asyncio()` for asyncio
- `run_parallel_process_asyncio()` for processes + asyncio

Setup:
- use python3, tested with python3.11
- install requirements
- create file config.py and specify there: AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, S3_BUCKET, S3_FOLDER, DB_HOST, DB_DATABASE, DB_USER, DB_PASSWORD
