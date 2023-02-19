import logging

import mysql.connector
from config import DB_DATABASE, DB_HOST, DB_PASSWORD, DB_USER

# COUNT_FILES_TO_DOWNLOAD_LIMIT = 0
# COUNT_FILES_TO_DOWNLOAD_OFFSET = 0
logger_info = logging.getLogger('log_info')
logger_error = logging.getLogger('log_error')
logger_debug = logging.getLogger('log_debug')


def get_s3filenames_from_db(limit: int, offset: int) -> set:
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
        # sql = f"SELECT hash_file_name FROM web_products_order_files WHERE file_status_id=6 LIMIT {limit} OFFSET {offset};"
        sql = f"SELECT hash_file_name FROM web_products_order_files WHERE file_status_id=6 and file_time >= 1640995200 limit 1000;"
        cursor = connection.cursor()
        cursor.execute(sql)
        # get all records
        records = cursor.fetchall()
        records = set(item[0] for item in records)
    except mysql.connector.Error as e:
        logger_error.error("Error reading data from MySQL table", e)
    finally:
        if connection.is_connected():
            connection.close()
            cursor.close()
            logger_debug.debug("MySQL connection is closed")
    return (records)
