from typing import List
from typing_extensions import Self
from akrossworker.common.planner.order.order_manager import OrderManager
from akrossworker.common.protocol import OrderbookStreamProtocol, PlanItemActivation, PriceStreamProtocol
from akrossworker.common.planner.plan_object import PlanObject
from akross.common import aktime


class ActionInterface:
    def __init__(
        self,
        plan_object: PlanObject,
        order_manager: OrderManager = None
    ):
        self.next: Self = None
        self.order_manager = order_manager
        self.plan_object = plan_object
        self.start_propagate = False

    def plan_status(self) -> str:
        return self.plan_object.plan_item.status

    def get_plan(self) -> PlanObject:
        return self.plan_object()

    def get_symbol_id(self) -> str:
        return self.plan_object.plan_item.symbolId

    def get_order_manager(self) -> OrderManager:
        return self.order_manager

    def get_activation(self) -> PlanItemActivation:
        return self.get_plan().plan_item.activation

    def is_in_time(self) -> bool:
        return (self.plan_object.plan_item.startTime <=
                aktime.get_msec() <=
                self.plan_object.plan_item.startTime)

    async def init(self):
        pass

    async def on_price_stream(self, stream: PriceStreamProtocol) -> None:
        if not self.start_propagate:
            self.start_propagate = True
            if self.next is not None:
                await self.next.init()
        
        if self.next is not None:
            await self.next.on_price_stream(stream)

    async def on_orderbook_stream(self, stream: OrderbookStreamProtocol) -> None:
        if self.next is not None:
            await self.next.on_orderbook_stream(stream)

    async def update_column(self, columns: List[str], plan_object: PlanObject) -> None:
        if self.next is not None:
            self.next.update_column(columns, plan_object)

    async def on_other_action_event(self, plan_id: str, status: str) -> None:
        pass

    def set_next(self, action: Self):
        self.next = action
