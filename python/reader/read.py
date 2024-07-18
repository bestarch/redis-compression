import logging
import lz4.frame
from util.dsType import DSType

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
                    key_type = self.conn.type(key).decode('utf-8').upper()
                    if key_type == DSType.HASH.name:
                        self.conn.hgetall(key)
                    elif key_type == DSType.STRING.name:
                        self.conn.get(key).decode('utf-8')
                    elif key_type == DSType.ZSET.name:
                        self.conn.zrange(key, 0, -1, withscores=True)
                    else:
                        pass
        except Exception as e:
            print(f"Error while iterating through keys\nError: {str(e)}")
        logger.info(f'Finished reading uncompressed data from Redis')

    def deSerializeAndDecompress(self, compressed):
        decompressed_bytes = lz4.frame.decompress(compressed)
        return decompressed_bytes

    def readAndDecompress(self, pattern):
        logger.info(f'Reading data from Redis and decompressing it')
        try:
            # Use SCAN to iterate through keys
            cursor = '0'
            while cursor != 0:
                cursor, keys = self.conn.scan(cursor=cursor, match=pattern+'*', count=500)
                for key in keys:
                    key_type = self.conn.type(key).decode('utf-8').upper()
                    if key_type == DSType.HASH.name:
                        entries = self.conn.hgetall(key)
                        for hashElement, value in entries.items():
                            self.deSerializeAndDecompress(value)
                    elif key_type == DSType.STRING.name:
                        value = self.conn.get(key)
                        self.deSerializeAndDecompress(value)
                    elif key_type == DSType.ZSET.name:
                        tuples = self.conn.zrange(key, 0, -1, withscores=True)
                        for member, score in tuples:
                            self.deSerializeAndDecompress(member)
                    else:
                        pass
        except Exception as e:
            print(f"Error while iterating through keys\nError: {str(e)}")
        logger.info(f'Finished reading & decompressing the data from Redis')