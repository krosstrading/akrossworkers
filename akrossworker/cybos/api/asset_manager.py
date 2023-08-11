from typing import Dict, List
import math
import logging

from akross.common import aktime
from akrossworker.common.args_constants import OrderResultType, OrderType

from akrossworker.common.protocol import CybosTradeEvent, HoldAsset, HoldAssetList, OrderTradeEvent, SymbolInfo


LOGGER = logging.getLogger(__name__)
KRW = 'KRW'
IGNORE = ''
MARKET = 'krx.spot'

TAX_RATE = 0.0025
AFTER_TAX_RATE = (1 - TAX_RATE)  # 0.25%


class OrderItem:
    def __init__(
            self,
            symbol_id: str,
            price: int,
            qty: int,
            is_buy: bool,
            trade_cum_qty: int = 0,
            order_id: int = -1
    ):
        self.side = 'BUY' if is_buy else 'SELL'
        self.is_buy = is_buy
        self.symbol_id = symbol_id
        self.code = symbol_id.split('.')[-1]
        self.orig_qty = qty
        self.orig_price = price
        self.orig_order_id = -1
        self.order_id = order_id
        self.order_type = OrderType.OrderMarket if price == 0 else OrderType.OrderLimit
        self.price = 0
        self.qty = 0
        self.trade_cum_qty = trade_cum_qty

    def get_cancel_amount(self):
        if self.is_buy:
            return self.orig_price * (self.get_remained_qty())
        return 0
    
    def is_market_price(self):
        return self.order_type == OrderType.OrderMarket

    def get_remained_qty(self):
        return self.orig_qty - self.trade_cum_qty

    def get_new_report(self) -> OrderTradeEvent:
        return OrderTradeEvent(
            self.symbol_id, self.side, aktime.get_msec(),
            self.order_type, OrderResultType.New, OrderResultType.SubTypeNew,
            self.order_id, str(self.orig_qty), str(self.orig_price),
            '0', '0', '0', '0', None
        )
    
    def get_open_order_report(self) -> OrderTradeEvent:
        # cancel 일 때는 orig_order_id 가 1이 아님
        if self.get_remained_qty() > 0 and self.orig_order_id == -1:
            return OrderTradeEvent(
                self.symbol_id, self.side, aktime.get_msec(),
                self.order_type,
                OrderResultType.New if self.trade_cum_qty == 0 else OrderResultType.Trade,
                OrderResultType.SubTypeNew if self.trade_cum_qty == 0 else OrderResultType.SubTypePartial,
                self.order_id, str(self.orig_qty), str(self.orig_price),
                '0', str(self.trade_cum_qty), '0', '0', None
            )
        return None

    def get_trade_report(self, subtype: str) -> OrderTradeEvent:
        return OrderTradeEvent(
            self.symbol_id, self.side, aktime.get_msec(),
            self.order_type, OrderResultType.Trade, subtype,
            self.order_id, str(self.orig_qty), str(self.orig_price),
            str(self.qty), str(self.trade_cum_qty), str(self.price), '0', None
        )
    
    def get_cancel_report(self) -> OrderTradeEvent:
        # client 에서는 이전 order id 를 가지고 있기 때문에, orig_order_id 전달
        return OrderTradeEvent(
            self.symbol_id, self.side, aktime.get_msec(),
            self.order_type, OrderResultType.Canceled, OrderResultType.Canceled,
            self.orig_order_id, str(self.orig_qty), str(self.orig_price),
            '0', str(self.trade_cum_qty), str(self.price), '0', None
        )

    def prepare_cancel(self, new_order_id):
        self.orig_order_id = self.order_id
        self.order_id = new_order_id

    def is_matched(self, event: CybosTradeEvent):
        # 신규 주문이고, 아직 order id 받지 못한 상태
        # 취소 주문의 경우, order_id 가 이미 부여 받았기 때문에, match 안됨
        if (self.order_id == -1 and
                self.code == event.symbol and
                self.orig_qty == event.qty and
                self.orig_price == event.price and
                self.is_buy == event.is_buy):
            self.order_id = event.order_num
            return True
        return False
    
    def is_order_id_matched(self, event: CybosTradeEvent):
        if self.order_id == event.order_num:
            return True
        return False

    def set_traded(self, event: CybosTradeEvent):
        if self.order_id != -1 and event.order_num == self.order_id:
            self.qty = event.qty
            self.price = event.price
            self.trade_cum_qty += event.qty
            return (OrderResultType.SubTypeFilled if self.trade_cum_qty == self.orig_qty
                    else OrderResultType.SubTypePartial)
        return IGNORE
    
    @classmethod
    def CreateItemFromEvent(cls, event: CybosTradeEvent):
        return OrderItem(
            MARKET + '.' + event.symbol,
            event.price,
            event.qty,
            event.is_buy,
            0, event.order_num
        )


class AssetManager:
    def __init__(
        self,
        free_balance: int,
        open_orders: List[OrderTradeEvent],
        symbol_dict: Dict[str, SymbolInfo],
        callback
    ):
        self.assets: Dict[str, HoldAsset] = {}
        self.open_orders: List[OrderItem] = []
        self.symbol_dict = symbol_dict
        self.callback = callback
        locked_balance = 0
        for open_order in open_orders:
            # 1. 시장가는 open order로 남아있지 않음
            # 2. SELL 의 경우 open order로 남아있는 경우, asset list 에 있기 때문에 add_asset에서 처리
            # 3. BUY의 경우 open order로 남아있는 경우, 이미 매수한 수량은 asset에 있음
            order_qty = int(open_order.order_orig_qty)
            traded_qty = int(open_order.trade_cum_qty)
            price = int(open_order.order_orig_price)

            if order_qty - traded_qty > 0:
                self.open_orders.append(OrderItem(
                    open_order.symbol_id,
                    price,
                    order_qty,
                    open_order.side.lower() == 'buy',
                    traded_qty,
                    open_order.order_id
                ))

            if open_order.side.lower() == 'buy' and order_qty - traded_qty > 0:
                locked_balance += math.floor(price * (order_qty - traded_qty))

        self.assets[KRW] = HoldAsset(
            KRW,
            '원화',
            str(free_balance),
            '0',
            str(locked_balance),
            '0', '0', '0', ''
        )

    def add_intial_asset(self, asset_list: HoldAssetList):
        for asset_name, asset in asset_list.hold_assets.items():
            if asset_name not in self.assets:
                self.assets[asset_name] = asset

    def get_open_orders(self) -> list:
        reports = []
        for order in self.open_orders:
            report = order.get_open_order_report()
            if report is not None:
                reports.append(report.to_network())
        return reports

    def get_hold_assets(self) -> list:
        assets = []
        for asset in self.assets.values():
            assets.append(asset.to_network())
        return assets

    def add_new_order(self, symbol: str, is_buy: bool, price: int, qty: int):
        if is_buy:  # 편의 위해 매수시에는 세금 추가하지 않음
            if price > 0:  # 시장가 매수, test case 필요
                self.lock_balance(price * qty)
        else:
            self.lock_asset(symbol, qty)
        
        self.open_orders.append(OrderItem(
            MARKET + '.' + symbol.lower(),
            price,
            qty,
            is_buy
        ))

    def cancel_order(self, order_id, new_order_id):
        for open_order in self.open_orders:
            if open_order.order_id == order_id:
                open_order.prepare_cancel(new_order_id)
                break

    def handle_new_order_event(self, event: CybosTradeEvent):
        found = False
        for order_item in self.open_orders:
            if order_item.is_matched(event):
                found = True
                self.callback(order_item.get_new_report().to_network())
                break
            elif order_item.is_order_id_matched(event):  # 취소 경우
                found = True
                break
        if not found:
            self.open_orders.append(OrderItem.CreateItemFromEvent(event))

    def handle_trade_event(self, event: CybosTradeEvent):
        done_item = None
        item = None
        report = None
        for order_item in self.open_orders:
            result = order_item.set_traded(event)
            if result != IGNORE:
                report = order_item.get_trade_report(result).to_network()
                item = order_item
                done_item = order_item if result == OrderResultType.SubTypeFilled else None
                break

        if event.is_buy:
            if item is None or (item is not None and item.is_market_price()):
                # item is None: 외부에서 매수
                self.subtract_free_balance(int(event.price) * int(event.qty))
            else:
                self.subtract_lock_balance(int(event.price) * int(event.qty))
            self.add_asset(event.symbol, int(event.price), int(event.qty))
        else:
            self.add_balance(int(event.price) * int(event.qty), False)
            self.subtract_lock_asset(event.symbol, int(event.qty))

        if done_item is not None:
            self.open_orders.remove(done_item)

        if report is not None:
            self.callback(report)

    def handle_cancel_event(self, event: CybosTradeEvent):
        done_item = None
        report = None
        for order_item in self.open_orders:
            if order_item.order_id == event.order_num:
                done_item = order_item
                if order_item.is_buy:
                    # print('add balance', order_item.get_cancel_amount())
                    self.add_balance(order_item.get_cancel_amount(), True)
                else:
                    self.move_lock_asset_to_free(event.symbol, order_item.get_remained_qty())
                report = order_item.get_cancel_report().to_network()
                break
        if done_item is not None:
            self.open_orders.remove(done_item)

        if report is not None:
            self.callback(report)

    def order_event(self, event: CybosTradeEvent):
        if event.status == CybosTradeEvent.Submit:
            self.handle_new_order_event(event)
        elif event.status == CybosTradeEvent.Trade:
            self.handle_trade_event(event)
        elif event.status == CybosTradeEvent.Confirm:  # 취소, 정정에만 사용
            self.handle_cancel_event(event)

    def lock_balance(self, amount: int):
        free = int(self.assets[KRW].free)
        locked = int(self.assets[KRW].locked)
        self.assets[KRW].free = str(free - amount)
        self.assets[KRW].locked = str(locked + amount)

    def subtract_free_balance(self, amount: int):
        free = int(self.assets[KRW].free)
        self.assets[KRW].free = str(free - amount)

    def subtract_lock_balance(self, amount: int):
        # 매수 후 매수 되었을 경우
        locked = int(self.assets[KRW].locked)
        self.assets[KRW].locked = str(locked - amount)

    def add_balance(self, amount: int, is_cancel: bool):
        free = int(self.assets[KRW].free)
        if is_cancel:  # move from locked to free
            locked = int(self.assets[KRW].locked)
            self.assets[KRW].free = str(free + amount)
            self.assets[KRW].locked = str(locked - amount)
        else:  # by sell
            after_tax = math.floor(amount * AFTER_TAX_RATE)
            self.assets[KRW].free = str(free + after_tax)

    def lock_asset(self, symbol: str, qty: int):
        if symbol.lower() in self.assets:
            asset = self.assets[symbol.lower()]
            asset.free = str(int(asset.free) - qty)
            asset.locked = str(int(asset.locked) + qty)

    def add_asset(self, symbol: str, price: int, qty: int):
        symbol = symbol.lower()
        if symbol not in self.assets:
            desc = ''
            if symbol in self.symbol_dict:
                desc = self.symbol_dict[symbol].desc
            self.assets[symbol] = HoldAsset(
                symbol, desc, '0', '0', '0', '0', '0', '0', MARKET + '.' + symbol)
        
        asset = self.assets[symbol]
        total_qty = int(asset.free) + qty
        asset_amount = float(asset.free) * float(asset.buyestimated)
        asset.free = str(total_qty)
        amount = price * qty
        amount_after_tax = amount + (amount * TAX_RATE)  # 실제 가격보다 세금만큼 높게 산 가격
        asset.buyestimated = str(math.ceil((amount_after_tax + asset_amount) / total_qty))

    def subtract_lock_asset(self, symbol: str, qty: int):
        if symbol.lower() in self.assets:
            asset = self.assets[symbol.lower()]
            asset.locked = str(int(asset.locked) - qty)
            if int(asset.locked) == 0 and int(asset.free) == 0:
                del self.assets[symbol.lower()]

    def move_lock_asset_to_free(self, symbol: str, qty: int):
        if symbol.lower() in self.assets:
            asset = self.assets[symbol.lower()]
            asset.locked = str(int(asset.locked) - qty)
            asset.free = str(int(asset.free) + qty)
