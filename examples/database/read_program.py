from datetime import datetime
import asyncio

from akrossworker.common.db import DBEnum
from akross.common import aktime
from akrossworker.common.db_quote_query import DBQuoteQuery
from akrossworker.common.db import Database


async def get_program_on_day(db: DBQuoteQuery, symbol: str, ms: int):
    search_ms = aktime.get_start_time(ms, 'd', 'KRX')
    data = await db.get_program_stream_data(
        symbol.lower(),
        search_ms,
        search_ms + aktime.interval_type_to_msec('d') - 1
    )
    return data


async def read():
    db = DBQuoteQuery(DBEnum.KRX_QUOTE_DB)
    """
    check when extended time and normal time mixed
    """
    data = await get_program_on_day(db, 'a274400', datetime(2023, 8, 16).timestamp() * 1000)
    print('data len', len(data))
    if len(data) > 0:
        for row in data[-30:]:
            print(datetime.fromtimestamp(row['eventTime'] / 1000), row)
        
        # if data['timeType'] != current_type:
        #     print(datetime.fromtimestamp(data['time'] / 1000), 'type', data['timeType'])
        #     current_type = data['timeType']


async def main():
    await read()


if __name__ == '__main__':
    import logging
    LOG_FORMAT = ('%(levelname) -10s %(asctime)s %(name) -30s %(funcName) '
                  '-35s %(lineno) -5d: %(message)s')
    logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)

    asyncio.run(main())