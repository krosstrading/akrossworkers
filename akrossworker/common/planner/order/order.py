import logging

from akross.connection.aio.account_channel import AccountChannel, Account
from akrossworker.common.command import AccountApiCommand
from akrossworker.common.protocol import OrderTradeEvent
from akrossworker.common.args_constants import OrderResultType
from akrossworker.common.util import get_symbol_from_id
from akross.common.enums import Command


LOGGER = logging.getLogger(__name__)


class OrderStatus:
    Registered = 0
    Requested = 1
    Submitted = 2
    CancelRequest = 3
    StopLossCall = 4
    Done = 10

    @staticmethod
    def StatusToStr(value: int):
        if value == OrderStatus.Registered:
            return 'Registered'
        elif value == OrderStatus.Requested:
            return 'Requested'
        elif value == OrderStatus.Submitted:
            return 'Submitted'
        elif value == OrderStatus.CancelRequest:
            return 'CancelRequest'
        elif value == OrderStatus.Done:
            return 'Done'
        return 'Unknown'


class Order:
    def __init__(
        self,
        symbol_id: str,
        channel: AccountChannel,
        account: Account,
        price: float,
        weight: float,
        amount: float = 0
    ):
        self.channel = channel
        self.account = account
        self.symbol_id = symbol_id
        self.price = price
        self.weight = weight
        self.amount = amount

        self.order_qty = 0
        self.order_price = 0
        self.trade_qty = 0
        self.order_id = 0
        self.status = OrderStatus.Registered

    def get_price(self) -> float:
        return self.price

    def get_weight(self) -> float:
        return self.weight

    def get_symbol_id(self) -> str:
        return self.symbol_id

    def get_amount(self) -> float:
        return self.amount

    def get_order_price(self) -> float:
        return self.order_price

    def get_order_qty(self) -> float:
        return self.order_qty

    def is_buy(self) -> bool:
        return True

    def is_waiting(self) -> bool:
        return self.get_status() == OrderStatus.Registered

    def is_triggered(self, price: str) -> bool:
        return False

    def new_matched(self, trade_event: OrderTradeEvent) -> bool:
        if self.status == OrderStatus.Registered:
            if (
                trade_event.symbol_id == self.symbol_id and
                float(trade_event.order_orig_price) == self.order_price and
                float(trade_event.order_orig_qty) == self.order_qty
            ):
                return True
        return False

    def get_trade_qty(self) -> float:
        return self.trade_qty

    async def set_order_info(self, price: float, qty: float):
        self.order_price = price
        self.order_qty = qty
        await self.order_info_callback()

    def set_order_id(self, order_id: int):
        self.order_id = order_id

    async def set_trade_qty(self, qty: float) -> None:
        self.trade_qty = qty
        await self.trade_qty_callback()

    async def status_callback(self) -> None:
        pass

    async def trade_qty_callback(self) -> None:
        pass

    async def order_info_callback(self) -> None:
        pass

    async def change_status(self, new_status):
        if self.status != new_status:
            LOGGER.warning(
                'change status(%s) from: %s, to: %s',
                self.symbol_id,
                OrderStatus.StatusToStr(self.status),
                OrderStatus.StatusToStr(new_status))
            self.status = new_status
            await self.status_callback(self.status)

    def get_status(self):
        return self.status

    async def handle_trade_event(self, event: OrderTradeEvent):
        if event.event_type == OrderResultType.Canceled:
            await self.change_status(OrderStatus.Done)
        elif event.event_type == OrderResultType.Trade:
            if event.event_subtype == OrderResultType.SubTypeFilled:
                self.trade_qty = float(event.trade_cum_qty)
                LOGGER.warning('order fullfilled (%s)', self.symbol_id)
                await self.change_status(OrderStatus.Done)
            elif event.event_subtype == OrderResultType.SubTypePartial:
                await self.set_trade_qty(float(event.trade_cum_qty))
                if self.get_trade_qty() == self.get_order_qty():
                    LOGGER.warning('order fullfilled (%s)', self.symbol_id)
                    await self.change_status(OrderStatus.Done)

    def cancelable(self):
        return (
            self.status == OrderStatus.Submitted and
            self.order_id > 0 and
            self.order_qty > self.trade_qty
        )

    async def cancel_order(self):
        if self.cancelable():
            await self.change_status(OrderStatus.CancelRequest)
            await self.channel.api_call(
                self.account,
                AccountApiCommand.CancelOrder,
                symbol=get_symbol_from_id(self.symbol_id),
                orderId=self.order_id
            )

    async def order(self):
        return False

    async def buy(self):
        if self.status == OrderStatus.Requested:
            if self.order_qty <= 0:
                await self.change_status(OrderStatus.Done)
            else:
                cmd, _ = await self.channel.api_call(
                    self.account,
                    AccountApiCommand.CreateOrder,
                    side='buy',
                    symbol=get_symbol_from_id(self.symbol_id),
                    quantity=self.order_qty,
                    price=self.order_price
                )
                is_ok = cmd == Command.OK

                if not is_ok:
                    await self.change_status(OrderStatus.Done)
                return is_ok
        return False

    async def sell(self):
        if self.status == OrderStatus.Requested:
            if self.order_qty <= 0:
                await self.change_status(OrderStatus.Done)
            else:
                cmd, _ = await self.channel.api_call(
                    self.account,
                    AccountApiCommand.CreateOrder,
                    side='sell',
                    symbol=get_symbol_from_id(self.symbol_id),
                    quantity=self.order_qty,
                    price=self.order_price
                )
                is_ok = cmd == Command.OK

                if not is_ok:
                    await self.change_status(OrderStatus.Done)
                return is_ok
        return False
