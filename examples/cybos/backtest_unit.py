import asyncio

from datetime import datetime
from akross.connection.aio.quote_channel import QuoteChannel
from akrossworker.common.command import ApiCommand
from akross.common import env


async def stream_arrived(msg):
    print('stream msg', msg)


async def main():
    await conn.connect()
    await conn.market_discovery()
    await conn.wait_for_market('krx.spot')
    krx = conn.get_markets('krx.spot')
    backtest_info = {
        'startTime': datetime(2023, 6, 10).timestamp() * 1000,
        'endTime': datetime(2023, 6, 13).timestamp() * 1000,
        'targets': ['a005930'],
        'timeFrame': 'm',
        'backtest': 'helloWorld'
    }
    resp = await conn.api_call(krx[0], ApiCommand.CreateBacktest, **backtest_info)
    print(resp)
    await conn.subscribe_stream(
        krx[0], ApiCommand.BacktestStream, stream_arrived, backtest='helloWorld')
    resp = await conn.api_call(krx[0], ApiCommand.Next, backtest='helloWorld')
    
    # await conn.subscribe_stream(krx[0], ApiCommand.OrderbookStream, stream_arrived, target='A024900')
    await asyncio.get_running_loop().create_future()


if __name__ == '__main__':
    import logging
    LOG_FORMAT = ('%(levelname) -10s %(asctime)s %(name) -30s %(funcName) '
                  '-35s %(lineno) -5d: %(message)s')
    logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)
    conn = QuoteChannel('krx.spot')
    asyncio.run(main())