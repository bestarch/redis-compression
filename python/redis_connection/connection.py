import redis
import os
import traceback
from redis.exceptions import RedisError
import ssl


class RedisConnection:
    def __init__(self):

        try:
            password = os.getenv('PASSWORD')
            if not (password and password.strip()):
                self.client = redis.Redis(
                    host=os.getenv('HOST', "localhost"),
                    port=os.getenv('PORT', 6379),
                    ssl=True,
                    ssl_cert_reqs=None,
                    decode_responses=False)
            else:
                self.client = redis.Redis(
                    host=os.getenv('HOST', "localhost"),
                    port=os.getenv('PORT', 6379),
                   # password=password,
                    ssl=True,
                    ssl_cert_reqs=None,  # Disable SSL certificate verification
                    decode_responses=False)
            self.client.ping()
        except RedisError as e:
            traceback.print_exc()
            raise Exception('Redis unavailable')

    def get_connection(self):
        return self.client


if __name__ == "__main__":
    redis_conn = RedisConnection()
    conn = redis_conn.get_connection()
    conn.s
    print(conn.ping())

