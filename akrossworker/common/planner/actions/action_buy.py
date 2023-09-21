from typing import List
from akrossworker.common.args_constants import TickTimeType

from akrossworker.common.planner.actions.action_interface import ActionInterface
from akrossworker.common.planner.order.order import Order
from akrossworker.common.planner.order.order_manager import OrderManager
from akrossworker.common.planner.plan_object import PlanObject
from akrossworker.common.protocol import (
    OrderbookStreamProtocol,
    PlanItemOrderItem,
    PlanItemStatus,
    PriceStreamProtocol
)
from akross.connection.aio.account_channel import AccountChannel, Account


class BuyOrderItem(Order):
    def __init__(
        self,
        plan_object: PlanObject,
        order_item: PlanItemOrderItem,
        channel: AccountChannel,
        account: Account,
    ):
        super().__init__(
            plan_object.plan_item.symbolId,
            channel,
            account,
            float(order_item.price),
            float(order_item.weight),
            float(plan_object.plan_item.amount)
        )
        self.plan_object = plan_object

    async def order(self):
        return await self.buy()

    def is_triggered(self, price: str) -> bool:
        if self.get_price() >= float(price):
            return True
        return False

    async def status_callback(self) -> None:
        pass

    async def trade_qty_callback(self) -> None:
        # record trade qty to plan object
        pass

    async def order_info_callback(self) -> None:
        # record order info to plan object
        pass


class ActionBuy(ActionInterface):
    def __init__(
        self,
        plan_object: PlanObject,
        order_manager: OrderManager
    ):
        super().__init__(plan_object, order_manager)
        self.items: List[BuyOrderItem] = []

    def has_trade_qty(self) -> bool:
        qty = 0
        for item in self.items:
            if item.order is not None:
                qty += item.get_trade_qty()
        return qty > 0 or len(self.items) == 0

    async def init(self):
        self.get_plan().change_status(PlanItemStatus.Progress)
        for item in self.get_plan().plan_item.buy.items:
            self.items.append(BuyOrderItem(
                self.get_symbol_id(),
                self.get_plan(),
                item,
                self.get_order_manager().get_channel(),
                self.get_order_manager().get_account(),
                float(self.get_plan().plan_item.amount)
            ))

    async def on_price_stream(self, stream: PriceStreamProtocol) -> None:
        if self.is_in_time() and stream.time_type == TickTimeType.Normal:
            if self.plan_status() == PlanItemStatus.Progress:
                for item in self.items:
                    if item.is_waiting() and item.is_triggered(stream.price):
                        await self.get_order_manager().execute_order(item)

        if self.has_trade_qty():
            await super().on_price_stream(stream)

    async def on_orderbook_stream(self, stream: OrderbookStreamProtocol) -> None:
        await super().on_orderbook_stream(stream)

    async def update_column(self, columns: List[str], plan_object: PlanObject) -> None:
        await super().update_column(columns, plan_object)
