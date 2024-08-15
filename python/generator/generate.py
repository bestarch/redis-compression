import shlex
from jproperties import Properties
import logging
import traceback
import lz4.frame
from util.dsType import DSType

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


    def generate(self, pattern, commands):
        self.initialise(commands, pattern)
        record_num = int(configs.get("KEY_TYPE_COUNT").data)
        try:
            # Use SCAN to iterate through keys
            cursor = '0'
            while cursor != 0:
                cursor, keys = self.conn.scan(cursor=cursor, match=pattern + '*', count=500)
                for key in keys:
                    key_type = self.conn.type(key).decode('utf-8').upper()
                    if key_type == DSType.HASH.name:
                        entries = self.conn.hgetall(key)
                        self.conn.delete(key)
                        key = key.decode('utf-8')
                        for i in range(record_num):
                            for hashElement, value in entries.items():
                                self.conn.hset(key + str(i), hashElement.decode('utf-8'), value)
                    elif key_type == DSType.STRING.name:
                        value = self.conn.get(key)
                        self.conn.delete(key)
                        key = key.decode('utf-8')
                        for i in range(record_num):
                            self.conn.set(key + str(i), value)
                    elif key_type == DSType.ZSET.name:
                        tuples = self.conn.zrange(key, 0, -1, withscores=True)
                        self.conn.delete(key)
                        key = key.decode('utf-8')
                        for i in range(record_num):
                            for member, score in tuples:
                                self.conn.zadd(key + str(i), {member: score})
                    else:
                        pass
        except Exception as e:
            print(f"Error while iterating through keys\nError: {str(e)}")


    def serializeAndCompress(self, bytess):
        # bytess = raw_string.encode('utf-8')
        compressed_val = lz4.frame.compress(bytess)
        return compressed_val


    def generateAndCompress(self, pattern, commands):
        self.initialise(commands, pattern)
        record_num = int(configs.get("KEY_TYPE_COUNT").data)
        try:
            # Use SCAN to iterate through keys
            cursor = '0'
            while cursor != 0:
                cursor, keys = self.conn.scan(cursor=cursor, match=pattern + '*', count=500)
                for key in keys:
                    key_type = self.conn.type(key).decode('utf-8').upper()
                    if key_type == DSType.HASH.name:
                        entries = self.conn.hgetall(key)
                        self.conn.delete(key)
                        key = key.decode('utf-8')
                        for i in range(record_num):
                            for hashElement, value in entries.items():
                                self.conn.hset(key + str(i), hashElement.decode('utf-8'),
                                               self.serializeAndCompress(value))
                    elif key_type == DSType.STRING.name:
                        value = self.conn.get(key)
                        self.conn.delete(key)
                        key = key.decode('utf-8')
                        for i in range(record_num):
                            self.conn.set(key + str(i), self.serializeAndCompress(value))
                    elif key_type == DSType.ZSET.name:
                        tuples = self.conn.zrange(key, 0, -1, withscores=True)
                        self.conn.delete(key)
                        key = key.decode('utf-8')
                        for i in range(record_num):
                            for member, score in tuples:
                                self.conn.zadd(key + str(i), {self.serializeAndCompress(member): score})
                    else:
                        pass
        except Exception as e:
            print(f"Error while iterating through keys\nError: {str(e)}")

    def initialise(self, commands, pattern):
        for line in commands:
            command = line.strip()
            if command:
                try:
                    parts = shlex.split(command)
                    parts[1] = pattern + parts[1]
                    self.conn.execute_command(*parts)
                except Exception as e:
                    print(f"Failed to execute command: {command}\nError: {str(e)}\n")


    def init(self, commands, pattern):
        for line in commands:
            command = line.strip()
            if command:
                try:
                    parts = shlex.split(command)
                    parts[1] = pattern + parts[1]
                    self.conn.execute_command(*parts)
                except Exception as e:
                    print(f"Failed to execute command: {command}\nError: {str(e)}\n")


    # This method is not in use
    def update_memory_usage(self, key):
        memory_usage = self.conn.memory_usage(key)
        if memory_usage:
            self.total_memory += memory_usage
