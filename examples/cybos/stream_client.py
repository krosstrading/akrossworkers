import asyncio

from akross.connection.aio.quote_channel import QuoteChannel
from akrossworker.common.command import ApiCommand
from akross.common import env


async def stream_arrived(msg):
    print('stream msg', msg)


async def orderbook_arrived(msg):
    print('orderbook msg', msg)


async def main():
    await conn.connect()
    await conn.market_discovery()
    await conn.wait_for_market('krx.spot')
    krx = conn.get_markets('krx.spot')
    await conn.subscribe_stream(krx[0], ApiCommand.OrderbookStream, stream_arrived, target='a083790')
    # await conn.subscribe_stream(krx[0], ApiCommand.OrderbookStream, stream_arrived, target='A024900')
    await asyncio.get_running_loop().create_future()


if __name__ == '__main__':
    import logging
    LOG_FORMAT = ('%(levelname) -10s %(asctime)s %(name) -30s %(funcName) '
                  '-35s %(lineno) -5d: %(message)s')
    logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)
    conn = QuoteChannel('krx.spot')
    asyncio.run(main())