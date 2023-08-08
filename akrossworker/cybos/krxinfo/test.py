import asyncio
from datetime import datetime

import akross
from akross.rabbitmq.connection import QuoteConnection
from akross.akross_env import get_password, get_url, get_user


async def fetch_test(conn):
    now = datetime.now()
    # 대략 0.14초 걸림
    cmd, payload = await conn.request(
        'cybos.spot',
        akross.DAILY_INVESTOR_BY_GROUP, symbol='A005930')
    print(len(payload), payload[-1])
    cmd, payload = await conn.request(
        'cybos.spot',
        akross.DAILY_SHORT_SELL, symbol='A005930')
    print(len(payload), payload[-1])
    cmd, payload = await conn.request(
        'cybos.spot',
        akross.DAILY_CREDIT, symbol='A005930')
    print(len(payload), payload[-1])
    cmd, payload = await conn.request(
        'cybos.spot',
        akross.DAILY_PROGRAM_TRADE, symbol='A005930')
    print(len(payload), payload[-1])
    print('done took',
          (datetime.now() - now).total_seconds(),
          'secs')


async def queue_test(conn):
    cmd, payload = await conn.request(
        akross.KRX_SERVICE_QUEUE,
        akross.DAILY_KRX_INFO, symbol='A005930')

    for row in payload:
        print(row)
    cmd, payload = await conn.request(
        akross.KRX_SERVICE_QUEUE,
        akross.BROKER_TRADE, symbol='A005930', interval='m')
    print('-' * 100)
    for row in payload:
        print(row)
    print('-' * 100)


async def main():
    conn = QuoteConnection(get_url(), get_user(), get_password(), '/')
    await conn.connect()

    # await fetch_test(conn)
    await queue_test(conn)


if __name__ == '__main__':
    import logging
    LOG_FORMAT = ('%(levelname) -10s %(asctime)s %(name) -30s %(funcName) '
                  '-35s %(lineno) -5d: %(message)s')
    LOGGER = logging.getLogger(__name__)
    logging.basicConfig(level=logging.WARNING, format=LOG_FORMAT)
    asyncio.run(main())
    
    
