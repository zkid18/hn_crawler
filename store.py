import functools
import asyncio
import aioredis
from datetime import timedelta, datetime

def try_connection(attemps):
    def decorator(method):
        @functools.wraps(method)
        def connect(*args, **kwargs):
            for _ in range(attemps):
                while True:
                    try:
                        return method(*args, **kwargs)
                    except redis.ConnectionError:
                        continue
                    break
        return connect
    return decorator


class RedisAsyncStore:
    ''' Async Redis connector
    '''
    def __init__(self, host, port, connect_now = True):
        self.host = host
        self.port = port
        if connect_now:
            self.connect
    
    async def connect(self):
        self.client = await aioredis.create_redis_pool(host=self.host, port=self.port)
    
    async def disconnect(self):
        self.client.close()
        await self.client.wait_closed()

    async def ping(self):
        try:
            return await self.client.ping()
        except redis.ConnectionError:
            raise ConnectionError

    async def get(self, link_id, key):
        try:
            value = await self.client.hget((link_id, key))
            return value.decode('utf-8') if value else value
        except redis.RedisError:
            raise ConnectionError

    async def set(self, link_id, key, value, seconds):
        try:
            return await self.client.hmset(link_id, {key:value}, ex=seconds)
        except redis.RedisError:
            raise ConnectionError  

