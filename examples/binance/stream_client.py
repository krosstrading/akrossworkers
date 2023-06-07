import asyncio

from akross.connection.aio.quote_channel import QuoteChannel
from akrossworker.common.command import ApiCommand
from akross.common import env


async def stream_arrived(msg):
    print('stream msg', msg)


async def main():
    await conn.connect()
    await conn.market_discovery()
    await conn.wait_for_market('binance.spot')
    binance = conn.get_markets('binance.spot')
    await conn.subscribe_stream(binance[0], ApiCommand.PriceStream, stream_arrived, target='BTCUSDT')
    await asyncio.get_running_loop().create_future()


if __name__ == '__main__':
    import logging
    LOG_FORMAT = ('%(levelname) -10s %(asctime)s %(name) -30s %(funcName) '
                  '-35s %(lineno) -5d: %(message)s')
    logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)
    conn = QuoteChannel('binance.spot', env.get_rmq_url())
    asyncio.run(main())