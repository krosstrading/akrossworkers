import asyncio

from akross.connection.aio.planner_channel import PlannerChannel
from akrossworker.common.command import PlannerApiCommand
from akross.common import env
from akrossworker.common.protocol import ActivationType, PlanItemStatus
from akross.common import aktime


async def stream_arrived(cmd, msg):
    print('stream msg', msg)


async def main():
    test_data = {
        'symbolId': 'krx.spot.a005930',
        'precondition': {
            'linkId': '',
            'when': ''
        },
        'activation': {
            'when': ActivationType.OpenPrice,
            'condition': {
                'first': '8900',
                'second': '9100'
            },
            'direction': 'down'
        },
        'startTime': aktime.get_msec(),
        'endTime': aktime.get_msec() + 1000000,
        'stopPrice': '8700',
        'amount': '1000000',
        'buy': {
            'strategy': [],
            'items': [
                {'price': '8000', 'qty': '100'},
                {'price': '7900', 'qty': '100'}
            ]
        },
        'sell': {
            'strategy': [],
            'items': [
                {'price': '9000', 'qty': '100'},
                {'price': '9100', 'qty': '100'}
            ]
        },
        'status': PlanItemStatus.Wait,
        'logs': [],
    }
    await conn.connect()
    cmd, res = await conn.api_call(PlannerApiCommand.Planner)
    await conn.subscribe_planner(stream_arrived)
    print(cmd, res)
    cmd, res = await conn.api_call(PlannerApiCommand.CreatePlan, plan=test_data)
    plan_id = res['planId']
    print(cmd, res)
    test_data['planId'] = plan_id
    test_data['stopPrice'] = '7000'
    cmd, res = await conn.api_call(
        PlannerApiCommand.UpdatePlan, plan=test_data)
    await asyncio.get_running_loop().create_future()


if __name__ == '__main__':
    import logging
    LOG_FORMAT = ('%(levelname) -10s %(asctime)s %(name) -30s %(funcName) '
                  '-35s %(lineno) -5d: %(message)s')
    logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)
    conn = PlannerChannel(env.get_rmq_url())
    asyncio.run(main())