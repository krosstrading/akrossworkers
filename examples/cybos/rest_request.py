import asyncio
from datetime import datetime

from akross.connection.aio.quote_channel import QuoteChannel


MARKET = 'krx.spot'


async def main():
    await conn.connect()
    await conn.market_discovery()
    await conn.wait_for_market(MARKET)
    hellos = conn.get_markets(MARKET)
    cmd, resp = await conn.api_call(hellos[0], 'candle', symbol='a000910', interval='1m')
    # print('response',
    #       resp,
    #       datetime.fromtimestamp(int(resp[-1][4] / 1000)),
    #       datetime.fromtimestamp(int(resp[-1][5] / 1000)))
    for data in resp:
        print(datetime.fromtimestamp(int(data[4] / 1000)),
              datetime.fromtimestamp(int(data[5] / 1000)))
    await asyncio.get_running_loop().create_future()


if __name__ == '__main__':
    import logging
    LOG_FORMAT = ('%(levelname) -10s %(asctime)s %(name) -30s %(funcName) '
                  '-35s %(lineno) -5d: %(message)s')
    logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)
    conn = QuoteChannel('binance.spot')
    asyncio.run(main())
