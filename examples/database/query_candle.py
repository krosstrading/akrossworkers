import asyncio
from datetime import datetime
from akrossworker.common.db import Database


async def read(db_name, collection, query):
    db = Database()
    """
    check when extended time and normal time mixed
    """
    stored = await db.get_data(db_name, collection, query)
    return stored


async def main():
    data = await read('krx_quote', 'a168360_1d', {})
    start_times = {}
    prev_time = 0
    print('data len', len(data),
          'from', datetime.fromtimestamp(data[0]['startTime'] / 1000),
          'until', datetime.fromtimestamp(data[-1]['endTime'] / 1000))
    for d in data:
        if prev_time == 0:
            prev_time = d['startTime']

        if d['startTime'] not in start_times:
            start_times[d['startTime']] = True
        else:
            print('duplicate', d['startTime'], datetime.fromtimestamp(d['startTime'] / 1000))

        if prev_time > d['startTime']:
            print('found not asc time')
        
        prev_time = d['startTime']


if __name__ == '__main__':
    import logging
    LOG_FORMAT = ('%(levelname) -10s %(asctime)s %(name) -30s %(funcName) '
                  '-35s %(lineno) -5d: %(message)s')
    logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)

    asyncio.run(main())
