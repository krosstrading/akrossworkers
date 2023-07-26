import asyncio
from datetime import datetime
from akrossworker.common.db import Database, DBEnum
from akrossworker.common.command import ApiCommand
from akrossworker.common.protocol import SymbolInfo
from akross.connection.aio.quote_channel import QuoteChannel
import sys


MARKET_NAME = 'krx.spot'


async def main():
    conn = QuoteChannel(MARKET_NAME)
    await conn.connect()
    await conn.market_discovery()
    await conn.wait_for_market(MARKET_NAME)
    krx = conn.get_markets(MARKET_NAME)[0]
    db = Database()
    intervals = ['1m', '1d', '1w', '1M']
    ret, symbols = await conn.api_call(krx, ApiCommand.SymbolInfo, cache=False)
    if not isinstance(symbols, list) or len(symbols) == 0:
        print('no symbols')
        sys.exit(1)

    for symbol in symbols:
        symbol_info = SymbolInfo.CreateSymbolInfo(symbol)
        if 'index' in symbol_info.sectors or 'etf' in symbol_info.sectors:
            for interval in intervals:
                await db.drop_collection(DBEnum.KRX_QUOTE_DB, symbol_info.symbol.lower() + '_' + interval)


if __name__ == '__main__':
    import logging
    LOG_FORMAT = ('%(levelname) -10s %(asctime)s %(name) -30s %(funcName) '
                  '-35s %(lineno) -5d: %(message)s')
    logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)

    asyncio.run(main())
