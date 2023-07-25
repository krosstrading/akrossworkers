import asyncio
from akross.connection.aio.quote_channel import QuoteChannel
from akrossworker.common.command import ApiCommand


MARKET = 'krx.spot'


async def main():
    await conn.connect()
    await conn.market_discovery()
    await conn.wait_for_market(MARKET)
    krx = conn.get_markets(MARKET)[0]
    cmd, resp = await conn.api_call(krx, ApiCommand.SymbolInfo)
    for symbol_info in resp:
        if symbol_info.symbol.lower() == 'a252670':
            print('found')
    

if __name__ == '__main__':
    import logging
    LOG_FORMAT = ('%(levelname) -10s %(asctime)s %(name) -30s %(funcName) '
                  '-35s %(lineno) -5d: %(message)s')
    logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)
    conn = QuoteChannel('binance.spot')
    asyncio.run(main())
