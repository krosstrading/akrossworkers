import asyncio
from datetime import datetime
from typing import List

from akross.connection.aio.quote_channel import QuoteChannel
from akrossworker.common.grouping import get_candle
from akrossworker.common.protocol import PriceCandleProtocol

MARKET = 'krx.spot'


async def main():
    await conn.connect()
    await conn.market_discovery()
    await conn.wait_for_market(MARKET)
    hellos = conn.get_markets(MARKET)
    cmd, resp = await conn.api_call(hellos[0], 'candle', symbol='a383310', interval='1m')
    # print('response',
    #       resp,
    #       datetime.fromtimestamp(int(resp[-1][4] / 1000)),
    #       datetime.fromtimestamp(int(resp[-1][5] / 1000)))
    candles: List[PriceCandleProtocol] = []
    for data in resp:
        candles.append(PriceCandleProtocol.ParseNetwork(data))

    candles = get_candle(candles, 'm', 1)
    for candle in candles:
        result = PriceCandleProtocol.ParseNetwork(candle)
        print(datetime.fromtimestamp(int(result.start_time / 1000)), result.time_type)
    # for data in candles[:-300]:
    #     print(datetime.fromtimestamp(int(data.start_time / 1000)),
    #           datetime.fromtimestamp(int(data.end_time / 1000)))
    await asyncio.get_running_loop().create_future()


if __name__ == '__main__':
    import logging
    LOG_FORMAT = ('%(levelname) -10s %(asctime)s %(name) -30s %(funcName) '
                  '-35s %(lineno) -5d: %(message)s')
    logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)
    conn = QuoteChannel('binance.spot')
    asyncio.run(main())
