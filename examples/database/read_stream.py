from datetime import datetime
import asyncio
from typing import List
from akrossworker.common.db import DBEnum
from akross.common import aktime
from akrossworker.common.args_constants import (
    CandleLimitDays
)
from akrossworker.common.db_quote_query import DBQuoteQuery
from akrossworker.common.protocol import PriceStreamProtocol


def get_db_start_search(interval_type) -> int:
    return aktime.get_msec_before_day(
        CandleLimitDays.get_limit_days(interval_type))


async def get_rprice_on_day(db: DBQuoteQuery, symbol: str, ms: int) -> List[PriceStreamProtocol]:
    search_ms = aktime.get_start_time(ms, 'd', 'KRX')
    data = await db.get_price_stream_data(
        symbol.lower(),
        search_ms,
        search_ms + aktime.interval_type_to_msec('d') - 1
    )
    results: List[PriceStreamProtocol] = []
    for row in data:
        results.append(PriceStreamProtocol.ParseDatabase(symbol, row))
    return results


async def read():
    db = DBQuoteQuery(DBEnum.KRX_QUOTE_DB)
    """
    check when extended time and normal time mixed
    """
    data = await get_rprice_on_day(db, 'a000430', datetime(2023, 7, 18).timestamp() * 1000)
    current_type = ''
    for row in data:
        data = row.to_database()
        if data['timeType'] != current_type:
            print(datetime.fromtimestamp(data['time'] / 1000), 'type', data['timeType'])
            current_type = data['timeType']


async def main():
    await read()


if __name__ == '__main__':
    import logging
    LOG_FORMAT = ('%(levelname) -10s %(asctime)s %(name) -30s %(funcName) '
                  '-35s %(lineno) -5d: %(message)s')
    logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)

    asyncio.run(main())