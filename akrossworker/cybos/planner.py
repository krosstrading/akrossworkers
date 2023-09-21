from argparse import ArgumentError
import logging
import asyncio

from akross.common import env
from akross.rpc.base import RpcBase
from akross.connection.aio.quote_channel import QuoteChannel, Market
from akross.connection.aio.planner_channel import PlannerChannel

from akross.common import util
from akrossworker.common.planner.order.order_manager import OrderManager
from akrossworker.common.planner.plan_container import PlanContainer
from akrossworker.common.planner.plan_executor import PlanExecutor

from akrossworker.common.protocol import PlanItem


LOGGER = logging.getLogger(__name__)
MARKET_NAME = 'krx.spot'


class Planner(RpcBase):
    def __init__(self, planner_channel: PlannerChannel):
        super().__init__()
        self._conn = QuoteChannel(MARKET_NAME)
        self._market: Market = None
        self._planner_channel = planner_channel
        self._container: PlanContainer = PlanContainer(self._planner_channel)
        self._order: OrderManager = OrderManager(MARKET_NAME)
        self._executor: PlanExecutor = None

        self.planner = self.on_planner
        self.createPlan = self.on_create_plan
        self.updatePlan = self.on_update_plan
        self.cancelPlan = self.on_cancel_plan

    async def preload(self):
        await self._conn.connect()
        await self._conn.market_discovery()
        await self._conn.wait_for_market(MARKET_NAME)
        self._market = self._conn.get_markets(MARKET_NAME)[0]

        await self._order.connect()
        self._executor = PlanExecutor(
            self._conn,
            self._market,
            self._order)
        self._container.set_listener(self._executor)
        await self._container.load()
        await self._executor.propagate_history()

    async def on_create_plan(self, **kwargs):
        util.check_required_parameters(kwargs, 'plan')
        plan_item = PlanItem.ParseNetwork(kwargs['plan'], True)
        await self._container.add_plan(plan_item)
        return {'planId': plan_item.planId}

    async def on_planner(self, **kwargs):
        return self._container.get_all()

    async def on_update_plan(self, **kwargs):
        util.check_required_parameters(kwargs, 'plan')
        plan_item = PlanItem.ParseNetwork(kwargs['plan'])
        if plan_item is not None:
            await self._container.update_plan(plan_item.planId, plan_item)
        else:
            raise ArgumentError('wrong plan format')
        return {}

    async def on_cancel_plan(self, **kwargs):
        util.check_required_parameters(kwargs, 'planId')
        await self._container.cancel_plan(kwargs['planId'])
        return {}


async def main() -> None:
    LOGGER.warning('run planner')
    conn = PlannerChannel(env.get_rmq_url())

    planner = Planner(conn)
    await planner.preload()
    await conn.connect()
    await conn.run_with_bus_queue(planner)
    await asyncio.get_running_loop().create_future()


if __name__ == '__main__':
    import logging
    LOG_FORMAT = ('%(levelname) -10s %(asctime)s %(name) -30s %(funcName) '
                  '-35s %(lineno) -5d: %(message)s')
    logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)
    asyncio.run(main())
