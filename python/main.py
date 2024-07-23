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
    operationSet = {
        "operationSet1": {
            '1': ['Load raw data in Redis & records memory consumption & time taken',
                  'Flush the DB',
                  'Load compressed data in Redis & records memory consumption & time taken']
        },
        "operationSet2": {
            '2': ['Load raw data in Redis & records memory consumption & time taken']
        },
        "operationSet3": {
            '3': ['Load compressed data in Redis & records memory consumption & time taken']
        },
        "operationSet4": {
            '3': ['Reads raw data (First it\'ll generate sample data) and records the time taken']
        },
        "operationSet5": {
            '3': ['Reads compressed data, decodes it (First it\'ll generate sample data) and records the time taken']
        }
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
        prompt = input("Choose an option between 1-6 ('5' means Data will be deleted)): ")
        if str(prompt) == '1' or str(prompt) == '2' or str(prompt) == '3' or str(prompt) == '4' or str(
                prompt) == '5' or str(prompt) == '6':
            return prompt
        else:
            logger.error("\n")
            logger.error("Invalid option!!!")


def main():
    output = []
    print("The application measures the Redis memory optimisation")
    time.sleep(1)
    selectedOps = getOperationSet()

    # First loads raw data
    # Calculates memory & Time taken
    # Then loads compressed data
    # Calculates memory & Time taken
    # Calculates the difference
    if int(selectedOps) == 1:
        startTime = time.time()
        Dataloader(conn).generateV2(pattern="_SAM_:", commands=commands)
        endTime = time.time()
        output.append(f"Time taken to ingest raw data {(endTime - startTime):.3f} seconds")
        print(f"Time taken to ingest raw data {(endTime - startTime):.3f} seconds")
        before = calculate_memory_usage()
        output.append(f'Memory usage after loading raw data: {before:.2f} MB')
        print(f'Memory usage after loading raw data: {before:.2f} MB')

        time.sleep(1)

        conn.flushdb()
        startTime2 = time.time()
        Dataloader(conn).generateAndCompressV2(pattern="_SAM_:", commands=commands)
        endTime2 = time.time()
        output.append(f"Time taken to ingest compressed data {(endTime2 - startTime2):.3f} seconds")
        print(f"Time taken to ingest compressed data {(endTime2 - startTime2):.3f} seconds")
        after = calculate_memory_usage()
        output.append(f'Memory usage after compression: {after:.2f} MB')
        print(f'Memory usage after compression: {after:.2f} MB')

        output.append(f'Actual dataset usage of compressed data: {before - after:.2f} MB')
        output.append(f'Optimization achieved: {(before - after) * 100 / before:.2f}%')
        print(f'Actual dataset usage of compressed data: {before - after:.2f} MB')
        print(f'Optimization achieved: {(before - after) * 100 / before:.2f}%')

    # Only loads raw data
    elif int(selectedOps) == 2:
        startTime = time.time()
        Dataloader(conn).generateV2(pattern="_SAM_:", commands=commands)
        endTime = time.time()
        output.append(f"Time taken to ingest raw data {(endTime - startTime):.3f} seconds")
        print(f"Time taken to ingest raw data {(endTime - startTime):.3f} seconds")
        before = calculate_memory_usage()
        output.append(f'Memory usage after loading raw data: {before:.2f} MB')
        print(f'Memory usage after loading raw data: {before:.2f} MB')

    # Only loads compressed data
    elif int(selectedOps) == 3:
        startTime = time.time()
        Dataloader(conn).generateAndCompressV2(pattern="_SAM_:", commands=commands)
        endTime = time.time()
        output.append(f"Time taken to ingest compressed data {(endTime - startTime):.3f} seconds")
        print(f"Time taken to ingest compressed data {(endTime - startTime):.3f} seconds")
        after = calculate_memory_usage()
        output.append(f'Memory usage after compression: {after:.2f} MB')
        print(f'Memory usage after compression: {after:.2f} MB')

    # Reads raw data (This first generates sample raw data) and records the Time taken
    elif int(selectedOps) == 4:
        Dataloader(conn).generateV2(pattern="_SAM_:", commands=commands)
        startTime = time.time()
        DataReader(conn).read(pattern="_SAM_:")
        endTime = time.time()
        output.append(f"Time taken to read raw data {(endTime - startTime):.3f} seconds")
        print(f"Time taken to read raw data {(endTime - startTime):.3f} seconds")

    # Reads compressed data, decodes it (This first generates sample compressed data) and records the Time taken
    elif int(selectedOps) == 5:
        Dataloader(conn).generateAndCompressV2(pattern="_SAM_:", commands=commands)
        startTime = time.time()
        DataReader(conn).readAndDecompress(pattern="_SAM_:")
        endTime = time.time()
        output.append(f"Time taken to read & decode compressed data {(endTime - startTime):.3f} seconds")
        print(f"Time taken to read & decode compressed data {(endTime - startTime):.3f} seconds")

    with open('output.txt', 'w') as file:
        for line in output:
            file.write(line + '\n')
    print("Done!!!")


if __name__ == '__main__':
    commands = []
    conn = RedisConnection().get_connection()
    initialise()
    main()
