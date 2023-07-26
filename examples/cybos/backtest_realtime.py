import asyncio

from akross.connection.aio.quote_channel import QuoteChannel
from akrossworker.common.command import ApiCommand


async def stream_arrived(msg):
    print('stream msg', msg)


async def main():
    await conn.connect()
    await conn.market_discovery()
    await conn.wait_for_market('krx.spot')
    krx = conn.get_markets('krx.spot')
    backtest_info = {
        'startTime': 1686612600000,
        'endTime': 1686623400000,
        'targets': ['a145990'],
        'timeFrame': 'r',
        'backtest': 'helloWorld'
    }
    resp = await conn.api_call(krx[0], ApiCommand.CreateBacktest, **backtest_info)
    print(resp)
    await conn.subscribe_stream(
        krx[0], ApiCommand.BacktestStream, stream_arrived, backtest='helloWorld')
    resp = await conn.api_call(krx[0], ApiCommand.Play, backtest='helloWorld')
    print('play resp', resp)
    # await conn.subscribe_stream(krx[0], ApiCommand.OrderbookStream, stream_arrived, target='A024900')
    await asyncio.get_running_loop().create_future()


if __name__ == '__main__':
    import logging
    LOG_FORMAT = ('%(levelname) -10s %(asctime)s %(name) -30s %(funcName) '
                  '-35s %(lineno) -5d: %(message)s')
    logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)
    conn = QuoteChannel('krx.spot')
    asyncio.run(main())