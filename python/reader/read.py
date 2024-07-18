import sys
import os

import shlex
from jproperties import Properties
import logging
import traceback
import lz4.frame

logger = logging.getLogger("DB Metrics")
logging.basicConfig(encoding='utf-8', level=logging.INFO)


class DataReader:
    def __init__(self, connection):
        self.conn = connection
        self.total_memory = None

    def read(self, pattern):
        logger.info(f'Reading uncompressed data from Redis')
        try:
            # Use SCAN to iterate through keys
            cursor = '0'
            while cursor != 0:
                cursor, keys = self.conn.scan(cursor=cursor, match=pattern + '*', count=500)
                for key in keys:
                    key_type = self.conn.type(key).decode('utf-8')
                    if key_type == 'hash':
                        self.conn.hgetall(key)
                    elif key_type == 'string':
                        self.conn.get(key).decode('utf-8')
                    elif key_type == 'zset':
                        self.conn.zrange(key, 0, -1, withscores=True)
                    else:
                        pass
        except Exception as e:
            print(f"Error while iterating through keys\nError: {str(e)}")

    def deSerializeAndDecompress(self, compressed):
        decompressed_bytes = lz4.frame.decompress(compressed)
        return decompressed_bytes

    def readAndDecompress(self, pattern):
        try:
            # Use SCAN to iterate through keys
            cursor = '0'
            while cursor != 0:
                cursor, keys = self.conn.scan(cursor=cursor, match=pattern+'*', count=500)
                for key in keys:
                    key_type = self.conn.type(key).decode('utf-8')
                    if key_type == 'hash':
                        entries = self.conn.hgetall(key)
                        for hashElement, value in entries.items():
                            self.deSerializeAndDecompress(value)
                    elif key_type == 'string':
                        value = self.conn.get(key)
                        self.deSerializeAndDecompress(value)
                    elif key_type == 'zset':
                        tuples = self.conn.zrange(key, 0, -1, withscores=True)
                        for member, score in tuples:
                            self.deSerializeAndDecompress(member)
                    else:
                        pass
        except Exception as e:
            print(f"Error while iterating through keys\nError: {str(e)}")
