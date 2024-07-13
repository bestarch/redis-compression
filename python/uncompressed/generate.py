import sys
import os

import shlex
from jproperties import Properties
import logging
import traceback

logger = logging.getLogger("DB Metrics")
logging.basicConfig(encoding='utf-8', level=logging.INFO)
configs = Properties()
with open('config/app-config.properties', 'rb') as config_file:
    configs.load(config_file)


class DataloaderBasic:
    def __init__(self, connection):
        try:
            self.conn = connection
            self.path = 'config/master_data.txt'
        except Exception as e:
            traceback.print_exc()
            raise Exception('An error occurred while reading the master config data')

    def generate(self):
        # logger.info(f'Generating data without compression')
        record_num = int(configs.get("KEY_TYPE_COUNT").data)
        master_record_count = 0
        with open(self.path, 'r') as file:
            for line in file:
                command = line.strip()
                if command:
                    master_record_count += 1
                    for i in range(record_num):
                        try:
                            parts = shlex.split(command)
                            parts[1] = parts[1] + ":" + str(i)
                            self.conn.execute_command(*parts)
                        except Exception as e:
                            print(f"Failed to execute command: {command}\nError: {str(e)}\n")
        logger.info(f'Generated {record_num * master_record_count} samples')
