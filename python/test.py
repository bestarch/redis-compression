import sys
import os

import shlex
from jproperties import Properties
import logging
import traceback
import lz4.frame
sys.path.append(os.path.abspath('redis_connection'))
from connection import RedisConnection

logger = logging.getLogger("Test")
logging.basicConfig(encoding='utf-8', level=logging.INFO)

if __name__ == '__main__':
    conn = RedisConnection().get_connection()
    compressed = conn.get('XYZ#{123}+789:0')
    decompressed_data = lz4.frame.decompress(compressed)
    print(decompressed_data.decode('utf-8'))

    entries = conn.hgetall('sh_0000000000000000000_000000000:100')
    for hashElement, value in entries.items():
        decompressed_data = lz4.frame.decompress(value)
        print(f"Element: {hashElement}, Value: {decompressed_data.decode('utf-8')}")



