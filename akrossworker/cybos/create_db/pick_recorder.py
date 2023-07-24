import asyncio
import logging
from datetime import datetime
from typing import List
from akross.connection.aio.quote_channel import QuoteChannel, Market
from akrossworker.common.args_constants import TradingStatus
from akrossworker.common.command import ApiCommand
from akrossworker.common.db_quote_query import DBQuoteQuery
from akrossworker.common.util import date_str
from akrossworker.cybos.create_db.picker import past_ranked_list

from akrossworker.common.db import Database, DBEnum
from akross.common import aktime
from akrossworker.common.protocol import (
    PriceCandleProtocol,
    SymbolInfo
)


LOGGER = logging.getLogger(__name__)


async def get_yesterday_start_time(quote: QuoteChannel, market: Market, today_start: int):
    cmd, resp = await quote.api_call(
        market,
        ApiCommand.Candle,
        symbol='a005930',
        interval='1d',
        startTime=today_start - aktime.interval_type_to_msec('d') * 7,
        endTime=today_start - 1,
        cache=False
    )
    if isinstance(resp, list) and len(resp) > 0:
        candle = PriceCandleProtocol.ParseNetwork(resp[-1])
        return (candle.start_time, candle.end_time)
    return 0, 0


async def main():
    today_start = aktime.get_start_time(aktime.get_msec(), 'd', 'KRX')
    if datetime.fromtimestamp(today_start / 1000).weekday() > 4:
        LOGGER.warning('stop, today is holiday')
        return
    
    db = Database()
    quote = QuoteChannel('krx.spot')
    await quote.connect()
    await quote.market_discovery()
    await quote.wait_for_market('krx.spot')
    krx_spot = quote.get_markets('krx.spot')[0]
    quote_db = DBQuoteQuery(DBEnum.KRX_QUOTE_DB)
    krx_symbols: List[SymbolInfo] = []
    _, resp = await quote.api_call(krx_spot, ApiCommand.SymbolInfo, cache=False)

    for symbol_info_raw in resp:
        symbol_info = SymbolInfo.CreateSymbolInfo(symbol_info_raw)
        if symbol_info.status == TradingStatus.Trading:
            krx_symbols.append(SymbolInfo.CreateSymbolInfo(symbol_info_raw))

    yesterday_start, _ = await get_yesterday_start_time(quote, krx_spot, today_start)
    LOGGER.warning('search ranked yesterday: %s', date_str(yesterday_start))
    past_top = await past_ranked_list(quote_db, krx_symbols, yesterday_start)

    latest_rank = await db.find_latest(DBEnum.KRX_HAS_PROFIT_DB, 'ranks', 1)
    if len(latest_rank) == 0 or latest_rank[0]['startTime'] != today_start:
        await db.insert_one(DBEnum.KRX_HAS_PROFIT_DB, 'ranks', {
            'startTime': today_start,
            'endTime': today_start + aktime.interval_type_to_msec('d') - 1,
            'symbols': [candidate.symbol.lower() for candidate in past_top]
        })


if __name__ == '__main__':
    import logging
    LOG_FORMAT = ('%(levelname) -10s %(asctime)s %(name) -30s %(funcName) '
                  '-35s %(lineno) -5d: %(message)s')
    logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)
    asyncio.run(main())
