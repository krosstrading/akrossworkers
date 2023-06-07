from datetime import datetime
import sys
import asyncio
from akrossworker.common.db import Database
from akross.common import aktime
from akrossworker.common.args_constants import (
    CandleLimitDays
)
from akrossworker.common.protocol import (
    PriceCandleProtocol
)


def get_db_start_search(interval_type) -> int:
    return aktime.get_msec_before_day(
        CandleLimitDays.get_limit_days(interval_type))


async def read(db_name, collection):
    db = Database()
    """
    check when extended time and normal time mixed
    """
    now = aktime.get_msec()
    interval_type = collection[-1]
    start_time = get_db_start_search(interval_type)
    print('interval_type', collection[-1], 'now', now, 'start_time', start_time)
    stored = await db.get_data(
        db_name, collection, {'startTime': {'$gte': start_time}})
    
    print('read', db_name, collection, datetime.fromtimestamp(int(start_time / 1000)))
    now = aktime.get_msec()
    result = []
    for data in stored:
        candle = PriceCandleProtocol.ParseDatabase(data)
        if candle.end_time < now:
            result.append(candle)
    db_start = result[0].to_network()[4]
    db_end = result[-1].to_network()[5]
    print('db data', datetime.fromtimestamp(int(db_start / 1000)),
          datetime.fromtimestamp(int(db_end / 1000)))
    print('last data', result[-1].to_network())


async def main():
    if len(sys.argv) < 3:
        print(sys.argv[0], 'db_name', 'collection')
        sys.exit(0)
    print('read', sys.argv[1], sys.argv[2])
    await read(sys.argv[1], sys.argv[2])


if __name__ == '__main__':
    import logging
    LOG_FORMAT = ('%(levelname) -10s %(asctime)s %(name) -30s %(funcName) '
                  '-35s %(lineno) -5d: %(message)s')
    logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)
    
    asyncio.run(main())    