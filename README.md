# s3downloader

Parallel downloading from S3 in threads or processes.

Just use `download_parallel_multithreading()` for threads or `download_parallel_multiprocessing()` for processes.

Setup:
- use python3, tested with python3.11
- install requirements
- create file config.py and specify there: AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, S3_BUCKET, S3_FOLDER, DB_HOST, DB_DATABASE, DB_USER, DB_PASSWORD
