from typing import Dict, List

from akross.connection.aio.quote_channel import QuoteChannel, Market
from akrossworker.common.planner.order.order_manager import OrderManager
from akrossworker.common.planner.plan_container import PlanItemListener
from akrossworker.common.planner.plan_object import PlanObject
from akrossworker.common.planner.plan_runner import PlanRunner
from akrossworker.common.planner.symbol_subscriber import SymbolSubscriber
from akrossworker.common.protocol import PlanMutableColumn


class PlanExecutor(PlanItemListener):
    def __init__(
        self,
        quote: QuoteChannel,
        market: Market,
        order_manager: OrderManager
    ):
        self.quote = quote
        self.market = market
        self.order_manager = order_manager

        self.runner: Dict[str, PlanRunner] = {}  # key: plan id
        self.subscriber: Dict[str, SymbolSubscriber] = {}  # key: symbol id
        self.plan_status: Dict[str, str] = {}

    async def propagate_history(self):
        for runner in self.runner.values():
            for key, value in self.plan_status.items():
                await runner.on_other_plan_status(key, value)

    async def add_subscribe(self, plan_object: PlanObject, runner: PlanRunner):
        symbol_id = plan_object.plan_item.symbolId.lower()
        if symbol_id in self.subscriber:
            self.subscriber[symbol_id].add_runner(runner)
        else:
            self.subscriber[symbol_id] = SymbolSubscriber(
                self.quote, self.market, symbol_id, self.order_manager)
            self.subscriber[symbol_id].add_runner(runner)
            await self.subscriber[symbol_id].start_subscribe()

    async def on_item_changed(
        self,
        event_type: str,
        columns: List[str],
        plan_object: PlanObject
    ):
        plan_id = plan_object.plan_item.planId
        if event_type == PlanObject.Add:
            self.plan_status[plan_id] = plan_object.plan_item.status
            if plan_object.is_alive() and plan_id not in self.runner:
                runner = PlanRunner(plan_object, self.order_manager)
                self.runner[plan_id] = runner
                await self.add_subscribe(plan_object, runner)
        elif event_type == PlanObject.Update:
            if plan_id in self.runner:
                await self.runner[plan_id].update(columns, plan_object)

            if len(columns) == 1 and columns[0] == PlanMutableColumn.Status:
                status = plan_object.plan_item.planId
                for runner in self.runner.values():
                    if runner.get_plan_id() != plan_id:
                        await runner.on_other_plan_status(plan_id, status)
