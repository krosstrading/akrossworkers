import asyncio
import logging
from typing import Dict, List
from akross.connection.aio.account_channel import AccountChannel, Account
from akrossworker.common.args_constants import OrderResultType

from akrossworker.common.command import AccountApiCommand
from akrossworker.common.krx_orderbook_unit import get_buy_priority_price, get_under_nearest_unit_price
from akrossworker.common.planner.order.order import Order, OrderStatus
from akrossworker.common.planner.order.stop_loss_order import StopLossOrder
from akrossworker.common.protocol import HoldAssetList, OrderTradeEvent, OrderbookStreamProtocol


LOGGER = logging.getLogger(__name__)


class OrderManager:
    SideAll = 0
    SideBuy = 1
    SideSell = 2

    def __init__(
        self,
        market: str,
        base_asset: str
    ):
        self.market = market
        self.base_asset = base_asset
        self.account: Account = None
        self.channel = AccountChannel(market)
        self.balance = 0
        self.orders: Dict[int, Order] = {}
        self.order_by_symbol: Dict[str, List[Order]] = {}
        self.asset_balance: Dict[str, float] = {}
        self.new_orders: List[Order] = []
        self.orderbook_snapshot: Dict[str, OrderbookStreamProtocol] = {}
        self.queue = asyncio.Queue()

    def get_channel(self) -> AccountChannel:
        return self.channel

    def get_account(self) -> Account:
        return self.account

    async def connect(self):
        await self.channel.connect()
        await self.channel.account_discovery()
        await self.channel.wait_for_accounts(self.market)
        self.account = self.channel.get_accounts(self.market)[0]
        await self.channel.subscribe_account(
            self.account, self.on_account_event)
        asyncio.create_task(self.start_loop())

    def get_price_qty(self, order: Order):
        if order.is_buy():
            seed = order.get_amount() * order.get_weight() / 100
            if order.get_price() > 0:
                order_price = get_buy_priority_price(order.get_price())
                qty = seed / order_price
                if order_price * qty < self.balance:
                    qty = 0
            else:  # market buy
                order_price = 0
                estimated_price = self.get_top_orderbook_price(
                    order.get_symbol_id(), True)
                qty = 0
                if estimated_price > 0:
                    qty = seed / estimated_price
                    if estimated_price * qty < self.balance:
                        qty = 0

            return (order_price, int(qty))
        else:
            asset_qty = self.get_asset_balance(order.get_symbol_id())
            sell_price = get_under_nearest_unit_price(order.get_price())
            sell_qty = asset_qty * order.get_weight() / 100
            return (sell_price, int(sell_qty))

    async def start_loop(self):
        while True:
            item = await self.queue.get()

            if isinstance(item, tuple):  # symbol_id
                symbol_id, side = item
                if symbol_id not in self.order_by_symbol:
                    continue

                orders = self.order_by_symbol[symbol_id]
                symbol_orders: List[Order] = []
                for order in orders:
                    if order.cancelable():
                        if side == OrderManager.SideAll:
                            symbol_orders.append(order)
                        elif side == OrderManager.SideBuy:
                            if order.is_buy():
                                symbol_orders.append(order)
                        else:  # SideSell
                            if not order.is_buy():
                                symbol_orders.append(order)

                for order in symbol_orders:
                    await order.cancel_order()
                    while order.get_status() != OrderStatus.Done:
                        await asyncio.sleep(0.1)

            elif isinstance(item, Order):
                price, qty = self.get_price_qty(item)
                if qty > 0:
                    await item.set_order_info(price, qty)
                    if await item.order():
                        while item.get_status() != OrderStatus.Requested:
                            await asyncio.sleep(0.1)
                else:
                    await item.change_status(OrderStatus.Done)

    async def on_account_event(self, cmd, payload) -> None:
        if cmd == AccountApiCommand.OrderEvent:
            event = OrderTradeEvent.ParseNetwork(payload)
            order_id = event.order_id
            symbol_id = event.symbol_id
            if event.event_type == OrderResultType.New:
                found = None
                for new_order in self.new_orders:
                    if new_order.new_matched(event):
                        found = new_order
                        new_order.set_order_id(order_id)
                        await new_order.change_status(OrderStatus.Submitted)
                        self.orders[order_id] = new_order
                        self.order_by_symbol[symbol_id] = new_order
                        break
                if found is not None:
                    self.new_orders.remove(found)
            else:
                if order_id in self.orders:
                    self.orders[order_id].handle_trade_event()

        elif cmd == AccountApiCommand.AssetEvent:
            hold_assets = HoldAssetList.ParseHoldAssetList(payload)
            for asset in hold_assets:
                if asset.asset_name.lower() == self.base_asset.lower():
                    self.balance = float(asset.free)
                else:
                    self.asset_balance[asset.ref_symbol_id] = float(asset.free)

    async def execute_order(
        self,
        order: Order
    ) -> None:
        if order.get_status() == OrderStatus.Registered:
            await order.change_status(OrderStatus.Requested)
            self.new_orders.append(order)
            await self.queue.put(order)

    async def cancel_orders(
        self,
        symbol_id: str,
        side: int = SideAll
    ) -> None:
        if symbol_id in self.order_by_symbol:
            await self.queue.put((symbol_id, side))

    async def stop_loss(self, symbol_id: str) -> None:
        await self.cancel_orders(symbol_id, OrderManager.SideAll)
        order = StopLossOrder(symbol_id, self.channel, self.account)
        await self.queue.put(order)
        self.new_orders.append(order)
        return order

    def get_asset_balance(self, symbol_id: str) -> float:
        if symbol_id in self.asset_balance:
            return self.asset_balance[symbol_id]
        return 0

    def get_top_orderbook_price(self, symbol_id: str, buy_side: bool) -> float:
        if symbol_id in self.orderbook_snapshot:
            orderbook = self.orderbook_snapshot[symbol_id]
            if buy_side:
                if len(orderbook.ask_arr) > 0:
                    return float(orderbook.ask_arr[0][0])
            else:
                if len(orderbook.bid_arr) > 0:
                    return float(orderbook.bid_arr[0][0])
        return 0

    async def on_orderbook_stream(self, msg: OrderbookStreamProtocol):
        self.orderbook_snapshot[msg.symbol] = msg
