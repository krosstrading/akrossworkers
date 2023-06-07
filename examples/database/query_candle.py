import asyncio
from datetime import datetime
from akrossworkers.common.db import Database


async def read(db_name, collection, query):
    db = Database()
    """
    check when extended time and normal time mixed
    """
    stored = await db.get_data(db_name, collection, query)
    return stored


async def main():
    data = await read('krx_quote', 'a005930_1d', {})
    start_times = {}
    for d in data:
        if d['startTime'] not in start_times:
            start_times[d['startTime']] = True
        else:
            print('duplicate', d['startTime'], datetime.fromtimestamp(int(d['startTime'] / 1000)))

if __name__ == '__main__':
    import logging
    LOG_FORMAT = ('%(levelname) -10s %(asctime)s %(name) -30s %(funcName) '
                  '-35s %(lineno) -5d: %(message)s')
    logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)

    asyncio.run(main())
