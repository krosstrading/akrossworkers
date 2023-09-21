from typing import List
from akrossworker.common.args_constants import TickTimeType
from akrossworker.common.planner.actions.action_interface import ActionInterface
from akrossworker.common.planner.order.order import Order
from akrossworker.common.planner.order.order_manager import OrderManager
from akrossworker.common.planner.plan_object import PlanObject
from akrossworker.common.protocol import OrderbookStreamProtocol, PlanItemOrderItem, PlanItemStatus, PriceStreamProtocol
from akross.connection.aio.account_channel import AccountChannel, Account


class SellOrderItem(Order):
    def __init__(
        self,
        plan_object: PlanObject,
        order_item: PlanItemOrderItem,
        channel: AccountChannel,
        account: Account
    ):
        super().__init__(
            plan_object.plan_item.symbolId,
            channel,
            account,
            float(order_item.price),
            float(order_item.weight)
        )
        self.plan_object = plan_object

    def is_triggered(self, price: str) -> bool:
        if self.get_price() <= float(price):
            return True
        return False

    def is_buy(self) -> bool:
        return False

    async def status_callback(self) -> None:
        # send to actionsell to check finish
        pass

    async def trade_qty_callback(self) -> None:
        # record trade qty to plan object
        pass

    async def order_info_callback(self) -> None:
        # record order info to plan object
        pass


class ActionSell(ActionInterface):
    def __init__(
        self,
        plan_object: PlanObject,
        order_manager: OrderManager
    ):
        super().__init__(plan_object, order_manager)
        self.items: List[SellOrderItem] = []
        self.first_sell_triggered = False

    async def init(self):
        for item in self.get_plan().plan_item.sell.items:
            self.items.append(SellOrderItem(
                self.get_symbol_id(),
                self.get_plan(),
                item,
                self.get_order_manager().get_channel(),
                self.get_order_manager().get_account()
            ))

        if len(self.items) == 0:
            await self.get_plan().change_status(PlanItemStatus.Finish)
        else:
            for item in self.items:
                if item.is_waiting():
                    await self.get_order_manager().execute_order(item)

    async def cancel_buy_side(self):
        if not self.first_sell_triggered:
            await self.get_order_manager().cancel_orders(
                self.get_symbol_id(), OrderManager.SideBuy)
            self.first_sell_triggered = False

    async def on_price_stream(self, stream: PriceStreamProtocol) -> None:
        if self.plan_status() == PlanItemStatus.Progress:
            stop_price = float(self.get_plan().plan_item.stopPrice)
            if stream.time_type != TickTimeType.Normal:
                return

            if float(stream.price) <= stop_price:
                await self.get_order_manager().stop_loss(self.get_symbol_id())
                self.get_plan().change_status(PlanItemStatus.Finish)

    async def on_orderbook_stream(self, stream: OrderbookStreamProtocol) -> None:
        pass

    async def update_column(self, columns: List[str], plan_object: PlanObject) -> None:
        pass
