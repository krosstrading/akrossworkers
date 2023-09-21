from typing import List
from akrossworker.common.db import DBEnum, Database


class DBQuoteQuery:
    def __init__(self, quote_db_name, mongo_url: str = ''):
        self.db = Database(mongo_url)
        self.quote_db_name = quote_db_name

    async def find_first_ms(self, symbol: str, interval_type: str) -> int:
        row = await self.db.find_first(
            self.quote_db_name, symbol.lower() + '_1' + interval_type, 1)
        if len(row) == 1:
            return row[0]['startTime']
        return -1

    async def insert_one(self, db_name: str, collection_name: str, data) -> None:
        await self.db.insert_one(db_name, collection_name, data)
    
    async def insert_many(self, db_name: str, collection_name: str, data: List[dict]) -> None:
        await self.db.insert_many(db_name, collection_name, data)

    async def find_latest_ms(self, symbol: str, interval_type: str) -> int:
        row = await self.db.find_latest(
            self.quote_db_name, symbol.lower() + '_1' + interval_type, 1)
        if len(row) == 1:
            return row[0]['endTime']
        return -1
    
    async def get_data(self, symbol: str, interval_type: str, start_time: int, end_time: int):
        return await self.db.get_data(
            self.quote_db_name,
            symbol.lower() + '_1' + interval_type,
            {
                'startTime': {'$gte': start_time},
                'endTime': {'$lte': end_time}
            }
        )
    
    async def get_data_all(self, symbol: str, interval_type: str):
        return await self.db.get_data(
            self.quote_db_name,
            symbol.lower() + '_1' + interval_type
        )

    async def get_price_stream_data(self, symbol: str, start_time: int, end_time: int):
        return await self.db.get_data(
            'throwback',
            'p_' + symbol.lower(),
            {
                'time': {'$gte': start_time, '$lte': end_time}
            }
        )
    
    async def get_orderbook_stream_data(self, symbol: str, start_time: int, end_time: int):
        return await self.db.get_data(
            'throwback',
            'o_' + symbol.lower(),
            {
                'time': {'$gte': start_time, '$lte': end_time}
            }
        )

    async def get_program_stream_data(self, symbol: str, start_time: int, end_time: int):
        return await self.db.get_data(
            'throwback',
            'r_' + symbol.lower(),
            {
                'eventTime': {'$gte': start_time, '$lte': end_time}
            }
        )


async def main():
    from datetime import datetime
    symbol = 'a005930'
    dbquery = DBQuoteQuery(DBEnum.KRX_QUOTE_DB)
    time = await dbquery.find_first_ms(symbol, 'm')
    if time > 0:
        print('first data for', symbol, datetime.fromtimestamp(time / 1000))
    else:
        print('no data')
    time = await dbquery.find_latest_ms(symbol, 'm')
    if time > 0:
        print('latest data for', symbol, datetime.fromtimestamp(time / 1000))
    else:
        print('no data')


if __name__ == '__main__':
    import asyncio
    asyncio.run(main())