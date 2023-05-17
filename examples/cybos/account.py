import asyncio
from akross.connection.aio.account_channel import AccountChannel

from workers.common.command import AccountApiCommand


async def account_event(cmd, payload):
    print('account_event', cmd, payload)


async def main():
    await conn.connect()
    await conn.account_discovery()
    await conn.wait_for_market('krx.spot')
    account = conn.get_accounts('krx.spot')[0]
    
    await conn.subscribe_account(account, account_event)
    open_orders = await conn.api_call(account, AccountApiCommand.OpenOrder)
    print(await conn.api_call(account, 'assetList'))
    print('open orders', open_orders)
    await conn.api_call(account, AccountApiCommand.CreateOrder, **{
        'side': 'buy',
        'symbol': 'a005930',
        'quantity': 1,
        'price': 65000
    })
    await asyncio.get_running_loop().create_future()


if __name__ == '__main__':
    import logging
    LOG_FORMAT = ('%(levelname) -10s %(asctime)s %(name) -30s %(funcName) '
                  '-35s %(lineno) -5d: %(message)s')
    logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)
    conn = AccountChannel('krx.spot')
    asyncio.run(main())