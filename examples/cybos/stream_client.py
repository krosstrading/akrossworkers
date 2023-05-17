import asyncio

from akross.connection.aio.quote_channel import QuoteChannel
from workers.common.command import ApiCommand


async def stream_arrived(msg):
    print('stream msg', msg)


async def main():
    await conn.connect()
    await conn.market_discovery()
    await conn.wait_for_market('krx.spot')
    krx = conn.get_markets('krx.spot')
    await conn.subscribe_stream(krx[0], ApiCommand.PriceStream, stream_arrived, target='A005930')
    await asyncio.get_running_loop().create_future()


if __name__ == '__main__':
    import logging
    LOG_FORMAT = ('%(levelname) -10s %(asctime)s %(name) -30s %(funcName) '
                  '-35s %(lineno) -5d: %(message)s')
    logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)
    conn = QuoteChannel('krx.spot', '172.17.64.1')
    asyncio.run(main())