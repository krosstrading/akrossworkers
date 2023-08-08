import motor.motor_asyncio
import asyncio
import logging
from urllib.parse import quote_plus

from akross.common import env
from akross.common import aktime

from akrossworker.common.db import DBEnum
from akrossworker.cybos.krxinfo.common import KrxTaskEnum

MONGO_URI = f"mongodb://{quote_plus(env.get_mongo_user())}:{quote_plus(env.get_mongo_password())}" \
            "@" + env.get_mongo_url()

LOGGER = logging.getLogger(__name__)


class TaskDatabase:
    def __init__(self):
        client = motor.motor_asyncio.AsyncIOMotorClient(MONGO_URI)
        self.db = client[DBEnum.KRX_TASK_DB]

    async def has_task_log(self, task_name: str, intdate: str, target: str):
        document = await self.db[KrxTaskEnum.TASK_LOG].find_one({
            'task_name': task_name,
            'intdate': intdate,
            'target': target.lower()
        })
        return document is not None

    async def get_task_by_date(self, intdate: str):
        cursor = self.db[KrxTaskEnum.TASK_LOG].find({
            'intdate': intdate,
        }).sort('intdate')
        return await cursor.to_list(None)

    async def update_task_log(self, task_name: str, intdate: str, target: str):
        LOGGER.debug('update_task_log %s, %s, %s', task_name, intdate, target)
        await self.db[KrxTaskEnum.TASK_LOG].update_one(
            {
                'task_name': task_name,
                'intdate': intdate,
                'target': target.lower()
            },
            {
                '$set': {
                    'task_name': task_name,
                    'intdate': intdate,
                    'target': target.lower(),
                    'time': aktime.get_msec()
                }
            },
            upsert=True
        )
    
    async def delete_out_date(self, today: str):
        query = {'intdate': {"$ne": today}}
        d = await self.db[KrxTaskEnum.TASK_LOG].delete_many(query)
        LOGGER.warning('deleted task logs: %d', d.deleted_count)


class DailyStatDatabase:
    def __init__(self):
        client = motor.motor_asyncio.AsyncIOMotorClient(MONGO_URI)
        self.db = client[DBEnum.KRX_TASK_DB]

    async def get_data(self, collection, symbol_name):
        cursor = self.db[collection].find({
            'symbol': symbol_name
        }, projection={'_id': False, 'symbol': False})
        return await cursor.to_list(None)

    async def upsert_data(self, collection, data):
        LOGGER.debug('upsert_data %s, %s', collection, data)

        await self.db[collection].update_one(
            {
                'symbol': data['symbol'],
                'yyyymmdd': data['yyyymmdd']
            },
            {'$set': data}, upsert=True)

    async def insert_data(self, collection, data):
        LOGGER.debug('upsert_data %s, %s', collection, data)

        await self.db[collection].insert_one(data)


async def test_run():
    from datetime import datetime
    task_db = TaskDatabase()
    result = await task_db.get_task_by_date('20230214')
    if len(result) > 0:
        print('matched', len(result))
        for row in result:
            print(datetime.fromtimestamp(int(row['time'] / 1000)),
                  row['task_name'],
                  row['target'])
    else:
        print('no result')
    
    # await task_db.update_task_log(KrxTaskEnum.INVESTOR_STAT,
    #                               '20230211', 'a005930')
    # print(await task_db.has_task_log(KrxTaskEnum.INVESTOR_STAT,
    #                                  '20230211', 'a005930'))
    # await task_db.delete_out_date('20230213')

if __name__ == '__main__':
    asyncio.run(test_run())
