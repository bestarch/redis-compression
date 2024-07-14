import redis
import shlex
from jproperties import Properties
import logging
import traceback
import sys
import os
sys.path.append(os.path.abspath('redis_connection'))
from connection import RedisConnection

sys.path.append(os.path.abspath('generator'))
from generate import Dataloader


logger = logging.getLogger("DB Metrics")
logging.basicConfig(encoding='utf-8', level=logging.INFO)
configs = Properties()
with open('config/app-config.properties', 'rb') as config_file:
    configs.load(config_file)


def calculate_memory_usage():
    # Check total memory usage before generating data
    response = conn.info("memory")
    return (float(response['used_memory'])) / (1024 * 1024)


def generate_metrics():
    conn.flushdb()
    Dataloader(conn).generateBasic(pattern="_SAM_:")

    # Check total memory usage before compression
    before = calculate_memory_usage()
    logger.info(f'Memory usage before compression: {before:.2f} MB')
    logger.info('*********************************************************************')

    Dataloader(conn).compress(pattern="_SAM_:")
    # Check total memory usage after data compression
    after = calculate_memory_usage()
    logger.info(f'Memory usage after compression: {after:.2f} MB')
    logger.info(f'Actual dataset usage with compressed data: {before-after:.2f} MB')
    logger.info(f'Optimization: {(before - after)*100/before:.2f}%')


if __name__ == '__main__':
    conn = RedisConnection().get_connection()
    generate_metrics()



