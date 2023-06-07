import asyncio
from datetime import datetime
import logging
from typing import List
import aiohttp
import sys
from urllib.parse import urlencode

from akrossworker.binance.api.spot_rest import BinanceSpotRest
from akross.common import aktime
from akrossworker.common.args_constants import TradingStatus
from akrossworker.common.db import DBEnum, Database
from akrossworker.common.protocol import (
    PriceCandleProtocol,
    SymbolInfo
)
from akrossworker.binance.api.utils import create_symbol_info


LOGGER = logging.getLogger(__name__)
MARKET_NAME = 'binance.spot'


async def _get_data_begin(symbol):
    url = 'https://api.binance.com/api/v1/klines?'
    params = {
        'symbol': symbol.upper(),
        'interval': '1m',
        'startTime': 0,
        'limit': 1
    }
    async with aiohttp.ClientSession() as session:
        async with session.get(url + urlencode(params)) as response:
            if response.status == 200:
                payload = await response.json()
                if len(payload) == 1 and isinstance(payload[0], list):
                    return payload[0][0]
                else:
                    LOGGER.error('unknown response type %s', payload)
            else:
                LOGGER.error('failed to get response')

    return 0


async def main() -> None:
    # 1 year is not supported on binance
    intervals = [
        '1m', '1h', '1d', '1w', '1M'
    ]
    spot = BinanceSpotRest()
    exchange_info = spot.exchange_info()
    db = Database()
    symbol_infos: List[SymbolInfo] = []
    now = aktime.get_msec()
    if 'symbols' in exchange_info:
        for d in exchange_info['symbols']:
            symbol_infos.append(await create_symbol_info('binance.spot', db, d))

    for i, symbol_info in enumerate(symbol_infos):
        if len(sys.argv) > 1 and sys.argv[1].lower() != symbol_info.symbol.lower():
            continue
        elif symbol_info.status != TradingStatus.Trading:
            LOGGER.warning('not trading status %s', symbol_info.symbol.upper())
            continue

        LOGGER.warning('start %d/%d: %s', i+1, len(symbol_infos), symbol_info.symbol.upper())
        start_time = await _get_data_begin(symbol_info.symbol)

        LOGGER.warning('start time(%d): %s',
                       start_time,
                       datetime.fromtimestamp(int(start_time / 1000)))
        for interval in intervals:
            col = symbol_info.symbol.lower() + '_' + interval
            interval_start_time = start_time
            # await db.drop_collection(DBEnum.BINANCE_QUOTE_DB, col)
            latest_data = await db.find_latest(DBEnum.BINANCE_QUOTE_DB, col, 1)
            if len(latest_data) > 0:
                interval_start_time = latest_data[-1]['endTime'] + 1

            query = {
                'startTime': interval_start_time,
                'endTime': now,
            }
            res = []
            record_data = []
            try:
                res = await spot.klines(symbol_info.symbol.upper(), interval, limit=1000, **query)
            except Exception as e:
                LOGGER.error(f'binance service raise error {str(e)}')

            for data in res:
                adqp = PriceCandleProtocol.CreatePriceCandle(
                    data[1], data[2], data[3], data[4],
                    data[0], data[6], data[5], data[7]
                )
                if adqp.end_time > now:
                    continue

                record_data.append(adqp.to_database())

            if len(record_data) > 0:
                LOGGER.warning('write to db(%s)', interval)
                await db.insert_many(DBEnum.BINANCE_QUOTE_DB, col, record_data)
                LOGGER.warning('write to db done')
        LOGGER.warning('done %s', symbol_info.symbol.upper())


if __name__ == '__main__':
    import logging
    LOG_FORMAT = ('%(levelname) -10s %(asctime)s %(name) -30s %(funcName) '
                  '-35s %(lineno) -5d: %(message)s')
    logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)
    asyncio.run(main())
