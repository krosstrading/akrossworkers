import asyncio
from datetime import datetime

from akross.connection.aio.quote_channel import QuoteChannel
from akross.common import enums
from workers.common.command import ApiCommand


async def main():
    await conn.connect()
    await conn.market_discovery()
    await conn.wait_for_market('binance.spot')
    hellos = conn.get_markets('binance.spot')
    cmd, resp = await conn.api_call(
        hellos[0],
        enums.Command.CreateBacktest,
        targets=['BTCUSDT'], timeFrame='d', testId='hello',
        startTime=1680361200000, endTime=1680879600000)
    print(cmd, resp)
    cmd, resp = await conn.api_call(
        hellos[0],
        ApiCommand.Candle,
        symbol='BTCUSDT', interval='1d', backtest='hello')
    print('response',
          'from',
          datetime.fromtimestamp(int(resp[0][4] / 1000)),
          datetime.fromtimestamp(int(resp[0][5] / 1000)),
          'until',
          datetime.fromtimestamp(int(resp[-1][4] / 1000)),
          datetime.fromtimestamp(int(resp[-1][5] / 1000)))
    # print('response', resp)
    await asyncio.get_running_loop().create_future()


if __name__ == '__main__':
    import logging
    LOG_FORMAT = ('%(levelname) -10s %(asctime)s %(name) -30s %(funcName) '
                  '-35s %(lineno) -5d: %(message)s')
    logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)
    conn = QuoteChannel('binance.spot')
    asyncio.run(main())