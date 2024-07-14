import sys
import os

import shlex
from jproperties import Properties
import logging
import traceback
import lz4.frame

logger = logging.getLogger("DB Metrics")
logging.basicConfig(encoding='utf-8', level=logging.INFO)
configs = Properties()
with open('config/app-config.properties', 'rb') as config_file:
    configs.load(config_file)


class Dataloader:
    def __init__(self, connection):
        try:
            self.conn = connection
            self.path = 'config/master_data.txt'
            self.total_memory = None
        except Exception as e:
            traceback.print_exc()
            raise Exception('An error occurred while reading the master config data')

    def generateBasic(self, pattern):
        logger.info(f'Generating sample data for usage')
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
                            parts[1] = pattern + parts[1] + ":" + str(i)
                            self.conn.execute_command(*parts)
                        except Exception as e:
                            print(f"Failed to execute command: {command}\nError: {str(e)}\n")

    def serializeAndCompress(self, bytess):
        # bytess = raw_string.encode('utf-8')
        compressed_val = lz4.frame.compress(bytess)
        return compressed_val

    def compress(self, pattern):
        # logger.info(f'Generating data without compression')
        try:
            # Use SCAN to iterate through keys
            cursor = '0'
            while cursor != 0:
                cursor, keys = self.conn.scan(cursor=cursor, match=pattern+'*', count=500)
                for key in keys:
                    key_type = self.conn.type(key).decode('utf-8')
                    # print(f"Key: {key}, Type: {key_type}")
                    if key_type == 'hash':
                        entries = self.conn.hgetall(key)
                        for hashElement, value in entries.items():
                            self.conn.hset(key, hashElement.decode('utf-8'), self.serializeAndCompress(value))
                            # print(f"Element: {hashElement}, Value: {self.serializeAndCompress(value)}")
                    elif key_type == 'string':
                        value = self.conn.get(key)
                        self.conn.set(key, self.serializeAndCompress(value))
                    elif key_type == 'zset':
                        tuples = self.conn.zrange(key, 0, -1, withscores=True)
                        for member, score in tuples:
                            self.conn.zrem(key, member)
                            self.conn.zadd(key, {self.serializeAndCompress(member): score})
                    else:
                        pass
        except Exception as e:
            print(f"Error while iterating through keys\nError: {str(e)}")


    # This method is not in use
    def update_memory_usage(self, key):
        memory_usage = self.conn.memory_usage(key)
        if memory_usage:
            self.total_memory += memory_usage
