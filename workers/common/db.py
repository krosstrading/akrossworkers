from enum import Enum
from typing import List
from urllib.parse import quote_plus
import motor.motor_asyncio
import pymongo
import logging
from akross.common import env


LOGGER = logging.getLogger(__name__)


MONGO_URI = f"mongodb://{quote_plus('akross')}:{quote_plus('Akross@q')}" \
            "@" + env.get_rmq_url()


class DBEnum(str, Enum):
    KRX_TASK_DB = 'krx_task'
    BINANCE_DB = 'binance'
    KRX_BROKER_DB = 'krx_broker'
    KRX_PERFORMANCE_DB = 'krx_performance'
    KRX_QUOTE_DB = 'krx_quote'
    BINANCE_QUOTE_DB = 'binance_quote'


class Database:
    def __init__(self):
        self.client = motor.motor_asyncio.AsyncIOMotorClient(
            MONGO_URI, serverSelectionTimeoutMS=3000)
        self.check_connected = False
        self._is_connected = False
    
    async def connected(self) -> bool:
        if not self.check_connected:
            try:
                await self.client.server_info()
                LOGGER.warning('mongodb connected')
                self._is_connected = True
            except pymongo.errors.ServerSelectionTimeoutError as e:
                LOGGER.error('mongodb Server timeout %s', str(e))
                self._is_connected = False
            self.check_connected = True
        else:
            return self._is_connected
        return self.check_connected and self._is_connected

    async def get_data(
        self,
        db_name: str,
        collection_name: str,
        query: dict = {}
    ) -> list:
        # assume that database has a data from beginning
        if await self.connected():
            db = self.client[db_name]
            cursor = db[collection_name].find(
                query, projection={'_id': False})
            return await cursor.to_list(None)
        return []

    async def drop_collection(self, db_name: str, collection_name: str) -> None:
        if await self.connected():
            db = self.client[db_name]
            await db[collection_name].drop()

    async def find_one(self, db_name: str, collection_name: str, query: dict = {}):
        if await self.connected():
            db = self.client[db_name]
            return await db[collection_name].find_one(query)
        return None

    async def delete_many(self, db_name: str, collection_name: str, query: dict = {}):
        if await self.connected():
            db = self.client[db_name]
            return await db[collection_name].delete_many(query)
        return None

    async def insert_one(self, db_name: str, collection_name: str, data) -> None:
        if await self.connected():
            db = self.client[db_name]
            await db[collection_name].insert_one(data)

    async def insert_many(self, db_name: str, collection_name: str, data: List[dict]) -> None:
        if await self.connected():
            db = self.client[db_name]
            await db[collection_name].insert_many(data)

    async def find_latest(self, db_name: str, collection_name: str, count: int):
        if await self.connected():
            db = self.client[db_name]
            cursor = db[collection_name].find().limit(count).sort([('$natural', -1)])
            return await cursor.to_list(None)
        return []

    async def find_first(self, db_name: str, collection_name: str, count: int):
        if await self.connected():
            db = self.client[db_name]
            cursor = db[collection_name].find().limit(count).sort([('$natural', 1)])
            return await cursor.to_list(None)
        return []


async def test_main():
    from datetime import datetime
    db = Database()
    await db.connected()
    result = await db.find_latest(DBEnum.BINANCE_QUOTE_DB, 'btcusdt_1d', 1)
    print(datetime.fromtimestamp(result[0]['endTime'] / 1000))


if __name__ == '__main__':
    import asyncio
    import logging
    LOG_FORMAT = ('%(levelname) -10s %(asctime)s %(name) -30s %(funcName) '
                  '-35s %(lineno) -5d: %(message)s')
    logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)
    asyncio.run(test_main())
