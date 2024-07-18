from jproperties import Properties
import logging

import sys
import os
import time

sys.path.append(os.path.abspath('redis_connection'))
from connection import RedisConnection

sys.path.append(os.path.abspath('generator'))
from generate import Dataloader

sys.path.append(os.path.abspath('reader'))
from read import DataReader


logger = logging.getLogger("DB Metrics")
logging.basicConfig(encoding='utf-8', level=logging.INFO)
configs = Properties()
with open('config/app-config.properties', 'rb') as config_file:
    configs.load(config_file)


def calculate_memory_usage():
    # Check total memory usage before generating data
    response = conn.info("memory")
    return (float(response['used_memory'])) / (1024 * 1024)


def initialise():
    conn.flushdb()
    path = 'config/master_data.txt'
    with open(path, 'r') as file:
        for line in file:
            command = line.strip()
            if command:
                commands.append(command)


def getOperationSet():
    operationSet1 = {
        '1': 'Load uncompressed data in Redis [Metrics: Application CPU, Redis memory]',
        '2': 'Get uncompressed data from Redis. [Metrics: Application CPU]',
        '3': 'Load compressed data in Redis. [Metrics: Application CPU, Redis memory]',
        '4': 'Get compressed data from Redis & decompress it. [Metrics: Application CPU]'
    }
    operationSet2 = {
        '1': 'Load compressed data in Redis. [Metrics: Application CPU, Redis memory]',
        '2': 'Get compressed data from Redis & decompress it. [Metrics: Application CPU]'
    }

    print('\nProvide one of the following options -->')
    print("********* Option 1 *********")
    for key, value in operationSet1.items():
        print(f"{key}: {value}")
    print("")
    print("********* Option 2 *********")
    for key, value in operationSet2.items():
        print(f"{key}: {value}")
    print("")
    print("********* Option 3 *********")
    print("Flush the DB: 5")
    print("")

    while True:
        prompt = input("Choose an option (1 or 2 or 5(Data will be deleted)): ")
        if str(prompt) == '1' or str(prompt) == '2' or str(prompt) == '5':
            return prompt
        else:
            logger.error("\n")
            logger.error("Invalid option!!!")


def main():
    print("The application measures the Redis memory optimisation")
    time.sleep(1)
    selectedOps = getOperationSet()

    if int(selectedOps) == 1:
        Dataloader(conn).generate(pattern="_SAM_:", commands=commands)
        # Check total memory usage after the operation
        before = calculate_memory_usage()
        logger.info(f'Memory usage after loading uncompressed data: {before:.2f} MB')
        DataReader(conn).read(pattern="_SAM_:")
        time.sleep(0.5)
        prompt = input("Do you want to measure the memory for compressed data? (y/n)")
        if prompt == 'y':
            conn.flushdb()
            Dataloader(conn).generateAndCompress(pattern="_SAM_:", commands=commands)
            # Check total memory usage after the operation
            after = calculate_memory_usage()
            logger.info(f'Memory usage after compression: {after:.2f} MB')

            logger.info(f'Actual dataset usage of compressed data: {before - after:.2f} MB')
            logger.info(f'Optimization achieved: {(before - after) * 100 / before:.2f}%')
            DataReader(conn).readAndDecompress(pattern="_SAM_:")
    elif int(selectedOps) == 2:
        Dataloader(conn).generateAndCompress(pattern="_SAM_:", commands=commands)
        # Check total memory usage after the operation
        after = calculate_memory_usage()
        logger.info(f'Memory usage with compression: {after:.2f} MB')
        DataReader(conn).readAndDecompress(pattern="_SAM_:")
    elif int(selectedOps) == 5:
        conn.flushdb()

    print("Done!!!")


if __name__ == '__main__':
    commands = []
    conn = RedisConnection().get_connection()
    initialise()
    main()



