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



'''
Re-write for async
'''

class Store:
    MAX_ATTEMPS = 10

    def __init__(self, store):
        self.cache_storage = store
        self.key_value_storage = store

    def ping_cache(self):
        return self.cache_storage.ping()

    def connect_cache(self):
        return self.cache_storage.connect()
    
    @try_connection(MAX_ATTEMPS)
    def cache_get(self, link_id, key, value):
        '''
        Communication with client-server cache stoarage (i.e. memcache, tarantool, redis)
        '''
        return self.cache_storage.get(link_id, key)
    
    @try_connection(MAX_ATTEMPS)
    def get(self, link_id, key):
        '''
        Communication with separate key-value stoarage (i.e nosql)
        '''
        return self.key_value_storage.get(link_id, key)

    @try_connection(MAX_ATTEMPS)
    def set(self, link_id, key, value, seconds=60*60):
        '''
        Communication with separate key-value stoarage (i.e nosql)
        '''
        return self.cache_storage.set(link_id, key, value, seconds)

    @try_connection(MAX_ATTEMPS)
    def cache_set(self, link_id, key, value, seconds=60*60):
        '''
        Communication with client-server cache stoarage (i.e. memcache, tarantool, redis)
        '''
        try:
            return self.storage.set(link_id, key, value, seconds)
        except:
            raise ConnectionError
