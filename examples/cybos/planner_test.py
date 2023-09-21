import asyncio
from typing import List

from akrossworker.common.planner.plan_container import PlanContainer, PlanItemListener
from akrossworker.common.planner.plan_object import PlanObject
from akrossworker.common.protocol import ActivationType, PlanItem, PlanItemStatus
from akross.common import aktime


class ItemListener(PlanItemListener):
    def __init__(self):
        pass

    async def on_item_changed(self, event_type: str, columns: List[str], plan_object: PlanObject):
        print('event_type', event_type, 'columns', columns, 'plan_id', plan_object.plan_item.planId)


async def main():
    test_data = {
        'symbolId': 'krx.spot.a005930',
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
                {'price': '8000', 'weight': '50'},  # order_price, order_qty
                {'price': '7900', 'weight': '50'}
            ]
        },
        'sell': {
            'strategy': [],
            'items': [
                {'price': '9000', 'weight': '50'},
                {'price': '9100', 'weight': '100'}
            ]
        },
        'status': PlanItemStatus.Wait,
        'profit': '',
        'logs': [],
    }

    listener = ItemListener()
    container: PlanContainer = PlanContainer()
    container.set_listener(listener)
    await container.add_plan(PlanItem.ParseNetwork(test_data, True))



if __name__ == '__main__':
    import logging
    LOG_FORMAT = ('%(levelname) -10s %(asctime)s %(name) -30s %(funcName) '
                  '-35s %(lineno) -5d: %(message)s')
    logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)
    asyncio.run(main())