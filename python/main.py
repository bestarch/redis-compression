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
        '1': ['Load raw data in Redis & records memory consumption & time taken',
              'Flush the DB',
              'Load compressed data in Redis & records memory consumption & time taken'],
        '2': ['Load raw data in Redis & records memory consumption & time taken'],
        '3': ['Load compressed data in Redis & records memory consumption & time taken'],
        '4': ['Reads raw data (First it\'ll generate sample data) and records the time taken'],
        '5': ['Reads compressed data, decodes it (First it\'ll generate sample data) and records the time taken'],
        '6': ['Flush the DB']
    }

    print('\nProvide one of the following options -->')
    print("********* Options *********")

    for key, value in operationSet.items():
        print(f"\nEnter '{key}' for:")
        for options in value:
            print(f"\t--> {options}")

    record_num = int(configs.get("KEY_TYPE_COUNT").data)
    print(f"\n**** Sampling will be done for {record_num * 10} records ****\n")

    while True:
        prompt = input("Choose an option between 1-6 ('6' means Data will be deleted)): ")
        if '1' <= prompt <= '6':
            return prompt
        else:
            print("\n")
            logger.error("Invalid option!!!")


def main():
    output = []
    print("The application measures the Redis memory optimisation")
    time.sleep(1)
    selectedOps = getOperationSet()
    print(f"The selected option is {selectedOps}")

    # First loads raw data
    # Calculates memory & Time taken
    # Then loads compressed data
    # Calculates memory & Time taken
    # Calculates the difference
    if int(selectedOps) == 1:
        startTime = time.time()
        Dataloader(conn).generate(pattern="_SAM_:", commands=commands)
        endTime = time.time()
        output.append(f"Time taken to ingest raw data {(endTime - startTime):.3f} seconds")
        print(f"Time taken to ingest raw data {(endTime - startTime):.3f} seconds")
        before = calculate_memory_usage()
        output.append(f'Memory usage after loading raw data: {before:.2f} MB')
        print(f'Memory usage after loading raw data: {before:.2f} MB')

        time.sleep(1)

        conn.flushdb()
        startTime2 = time.time()
        Dataloader(conn).generateAndCompress(pattern="_SAM_:", commands=commands)
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
        Dataloader(conn).generate(pattern="_SAM_:", commands=commands)
        endTime = time.time()
        output.append(f"Time taken to ingest raw data {(endTime - startTime):.3f} seconds")
        print(f"Time taken to ingest raw data {(endTime - startTime):.3f} seconds")
        before = calculate_memory_usage()
        output.append(f'Memory usage after loading raw data: {before:.2f} MB')
        print(f'Memory usage after loading raw data: {before:.2f} MB')

    # Only loads compressed data
    elif int(selectedOps) == 3:
        startTime = time.time()
        Dataloader(conn).generateAndCompress(pattern="_SAM_:", commands=commands)
        endTime = time.time()
        output.append(f"Time taken to ingest compressed data {(endTime - startTime):.3f} seconds")
        print(f"Time taken to ingest compressed data {(endTime - startTime):.3f} seconds")
        after = calculate_memory_usage()
        output.append(f'Memory usage after compression: {after:.2f} MB')
        print(f'Memory usage after compression: {after:.2f} MB')

    # Reads raw data (This first generates sample raw data) and records the Time taken
    elif int(selectedOps) == 4:
        Dataloader(conn).generate(pattern="_SAM_:", commands=commands)
        startTime = time.time()
        DataReader(conn).read(pattern="_SAM_:")
        endTime = time.time()
        output.append(f"Time taken to read raw data {(endTime - startTime):.3f} seconds")
        print(f"Time taken to read raw data {(endTime - startTime):.3f} seconds")

    # Reads compressed data, decodes it (This first generates sample compressed data) and records the Time taken
    elif int(selectedOps) == 5:
        Dataloader(conn).generateAndCompress(pattern="_SAM_:", commands=commands)
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
