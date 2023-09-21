from typing import List
from akrossworker.common.planner.actions.action_factory import ActionFactory
from akrossworker.common.planner.order.order_manager import OrderManager
from akrossworker.common.planner.plan_object import PlanObject
from akrossworker.common.protocol import OrderbookStreamProtocol, PriceStreamProtocol


class PlanRunner:
    def __init__(
        self,
        plan_object: PlanObject,
        order_manger: OrderManager
    ):
        self.plan_object = plan_object
        self.order_manager = order_manger
        self.head = ActionFactory.CreateActions(
            plan_object, self.order_manager)

    def get_plan_id(self) -> str:
        return self.plan_object.plan_item.planId

    async def update(self, columns: List[str], plan_object: PlanObject):
        await self.head.update_column(columns, plan_object)

    async def on_price_stream(self, stream: PriceStreamProtocol):
        await self.head.on_price_stream(stream)

    async def on_orderbook_stream(self, stream: OrderbookStreamProtocol):
        await self.head.on_orderbook_stream(stream)

    async def on_other_plan_status(self, plan_id: str, plan_status) -> None:
        await self.head.on_other_action_event(plan_id, plan_status)
