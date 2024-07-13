import redis
import shlex
from jproperties import Properties
import logging
import traceback
import sys
import os
sys.path.append(os.path.abspath('redis_connection'))
from connection import RedisConnection

sys.path.append(os.path.abspath('uncompressed'))
from generate import DataloaderBasic


logger = logging.getLogger("DB Metrics")
logging.basicConfig(encoding='utf-8', level=logging.INFO)
configs = Properties()
with open('config/app-config.properties', 'rb') as config_file:
    configs.load(config_file)


def calculate_usage():
    # Check total memory usage before generating data
    response = conn.info("memory")
    return (float(response['used_memory'])) / (1024 * 1024)


if __name__ == '__main__':
    conn = RedisConnection().get_connection()
    # Check total memory usage before generating data
    before = calculate_usage()
    logger.info(f'Memory usage before generating data: {before:.2f} MB')

    # Generate uncompressed data
    DataloaderBasic(conn).generate()

    # Check total memory usage after generating data
    after = calculate_usage()
    logger.info(f'Memory usage after generating data: {after:.2f} MB')
    logger.info(f'Actual dataset usage: {after-before:.2f} MB')


