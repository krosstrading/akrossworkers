from __future__ import annotations
from typing import Dict, List, Optional, Union
from akross.common import aktime

from akrossworker.common.args_constants import TickTimeType


class PriceCandleProtocol:
    def __init__(self,
                 price_open: float,
                 price_high: float,
                 price_low: float,
                 price_close: float,
                 start_time: int,
                 end_time: int,
                 base_asset_volume: float,
                 quote_asset_volume: float,
                 time_type: str,
                 adjusted: bool):
        self._price_open = price_open
        self._price_high = price_high
        self._price_low = price_low
        self._price_close = price_close
        self._start_time = start_time
        self._end_time = end_time
        self._base_asset_volume = base_asset_volume
        self._quote_asset_volume = quote_asset_volume
        self._time_type = time_type
        self._adjusted = adjusted

    @property
    def price_open(self) -> float:
        return float(self._price_open)

    @price_open.setter
    def price_open(self, price) -> None:
        self._price_open = price

    @property
    def price_high(self) -> float:
        return float(self._price_high)

    @price_high.setter
    def price_high(self, price) -> None:
        self._price_high = price

    @property
    def price_low(self) -> float:
        return float(self._price_low)

    @price_low.setter
    def price_low(self, price) -> None:
        self._price_low = price

    @property
    def price_close(self) -> float:
        return float(self._price_close)

    @price_close.setter
    def price_close(self, price) -> None:
        self._price_close = price

    @property
    def base_asset_volume(self) -> float:
        return float(self._base_asset_volume)

    def add_base_asset_volume(self, vol: Union[float, str]) -> None:
        self._base_asset_volume += float(vol)

    @property
    def quote_asset_volume(self) -> float:
        return float(self._quote_asset_volume)

    def add_quote_asset_volume(self, vol: Union[float, str]) -> None:
        self._quote_asset_volume += float(vol)

    @property
    def time_type(self) -> str:
        return self._time_type

    @property
    def adjusted(self) -> bool:
        return self._adjusted

    @property
    def start_time(self) -> int:
        return self._start_time

    @property
    def end_time(self) -> int:
        return self._end_time

    def merge(self, candle: PriceCandleProtocol) -> PriceCandleProtocol:
        if self.time_type != candle.time_type:
            return None

        return self.CreatePriceCandle(
            self._price_open,
            self._price_high if self._price_high > candle._price_high else candle._price_high,
            self._price_low if self._price_low < candle._price_low else candle._price_low,
            candle._price_close,
            self.start_time,
            candle.end_time,
            self.base_asset_volume + candle.base_asset_volume,
            self.quote_asset_volume + candle.quote_asset_volume,
            self.time_type,
            self.adjusted  # ignore now
        )

    def to_network(self):
        return [
            str(self._price_open),
            str(self._price_high),
            str(self._price_low),
            str(self._price_close),
            self._start_time,
            self._end_time,
            str(self._base_asset_volume),
            str(self._quote_asset_volume),
            self._time_type,
            '1' if self._adjusted else '0']

    def to_database(self):
        return {
            'startTime': self._start_time,
            'endTime': self._end_time,
            'timeType': self._time_type,
            'arr': [
                str(self._price_open),
                str(self._price_high),
                str(self._price_low),
                str(self._price_close),
                str(self._base_asset_volume),
                str(self._quote_asset_volume),
                '1' if self._adjusted else '0'
            ]
        }

    @classmethod
    def CreatePriceCandle(
        cls,
        price_open: Union[str, float],
        price_high: Union[str, float],
        price_low: Union[str, float],
        price_close: Union[str, float],
        start_time: int,
        end_time: int,
        base_asset_volume: Union[str, float],
        quote_asset_volume: Union[str, float],
        time_type: str = TickTimeType.Normal,
        adjusted: bool = False
    ):
        return PriceCandleProtocol(
            float(price_open),
            float(price_high),
            float(price_low),
            float(price_close),
            start_time,
            end_time,
            float(base_asset_volume),
            float(quote_asset_volume),
            time_type,
            adjusted
        )

    @classmethod
    def ParseNetwork(cls, arr: list) -> PriceCandleProtocol:
        if len(arr) == 10:
            return PriceCandleProtocol(
                float(arr[0]),
                float(arr[1]),
                float(arr[2]),
                float(arr[3]),
                int(arr[4]), int(arr[5]),
                float(arr[6]),
                float(arr[7]),
                arr[8],
                arr[9] == '1'
            )
        return None

    @classmethod
    def ParseDatabase(cls, row: dict) -> PriceCandleProtocol:
        return PriceCandleProtocol(
            float(row['arr'][0]),
            float(row['arr'][1]),
            float(row['arr'][2]),
            float(row['arr'][3]),
            int(row['startTime']), int(row['endTime']),
            float(row['arr'][4]),
            float(row['arr'][5]),
            row['timeType'],
            row['arr'][6] == '1'
        )


class SymbolInfo:
    def __init__(
        self,
        market: str,
        symbol: str,
        sectors: List[str],
        status: str,
        desc: str,
        base_asset: str,
        quote_asset: str,
        base_asset_precision: int,  # precision >= 0
        quote_asset_precision: int,  # precision >= 0
        # filter: ['0.01000', '100000.00000', '0.01000'] min, max, tick
        # tick '0' means client will calculate based on
        # current price, market, sector
        price_filter,
        qty_filter,
        order_total_min,  # '10.00000' or '0'
        base_asset_total_qty: str,
        market_cap: str,
        listed: int,
        tz: str,
        version: int
    ):
        """
        all strings are case insensitive, use lower as default and
        only convert it appropriately when send to external server.
        precision will be applied when sending number string to network
        """
        self.symbol = symbol
        self.sectors = sectors
        self.status = status
        self.desc = desc
        self.base_asset = base_asset
        self.quote_asset = quote_asset
        self.base_asset_precision = base_asset_precision
        self.quote_asset_precision = quote_asset_precision
        self.price_filter = price_filter
        self.qty_filter = qty_filter
        self.order_total_min = order_total_min
        self.market_cap = market_cap
        self.base_asset_total_qty = base_asset_total_qty
        self.market = market
        self.listed = listed
        self.tz = tz
        self.version = version

    def to_network(self) -> dict:
        return {
            'market': self.market,
            'symbol': self.symbol,
            'sectors': self.sectors,
            'status': self.status,
            'desc': self.desc,
            'baseAsset': self.base_asset,
            'quoteAsset': self.quote_asset,
            'baseAssetPrecision': self.base_asset_precision,
            'quoteAssetPrecision': self.quote_asset_precision,
            'priceFilter': self.price_filter,
            'qtyFilter': self.qty_filter,
            'orderTotalMin': self.order_total_min,
            'baseAssetTotal': self.base_asset_total_qty,
            'marketCap': self.market_cap,
            'listed': self.listed,
            'tz': self.tz,
            'version': self.version
        }

    @classmethod
    def CreateSymbolInfo(cls, info: dict) -> SymbolInfo:
        return SymbolInfo(
            info['market'],
            info['symbol'],
            info['sectors'],
            info['status'],
            info['desc'],
            info['baseAsset'],
            info['quoteAsset'],
            info['baseAssetPrecision'],
            info['quoteAssetPrecision'],
            info['priceFilter'],
            info['qtyFilter'],
            info['orderTotalMin'],
            info['baseAssetTotal'],
            info['marketCap'],
            info['listed'],
            info['tz'],
            info['version']
        )


class PriceStreamProtocol:
    def __init__(
        self,
        symbol: str,
        event_time: int,
        price: Union[str, float],
        volume: Union[str, float],
        is_sell: bool,  # is sell
        time_type: str = TickTimeType.Normal
    ):
        self.symbol = symbol
        self.price = price
        self.event_time = event_time
        self.volume = volume
        # 1: buy, 0: sell
        self.is_buy = 'b' if not is_sell else 's'
        self.time_type = time_type

    def to_network(self) -> list:
        return [
            self.symbol,
            self.event_time,
            str(self.price),
            str(self.volume),
            self.is_buy,
            self.time_type
        ]

    def to_database(self) -> dict:
        return {
            'time': self.event_time,
            'timeType': self.time_type,
            'price': str(self.price),
            'qty': str(self.volume),
            'position': self.is_buy
        }

    # 동시호가, 장외시간 등 구분이 필요할까?
    @classmethod
    def CreatePriceStream(
        cls,
        symbol,
        event_time,
        price,
        volume,
        is_sell,
        time_type=TickTimeType.Normal
    ) -> PriceStreamProtocol:
        return PriceStreamProtocol(
            symbol,
            event_time,
            price,
            volume,
            is_sell,
            time_type
        )

    @classmethod
    def ParseNetwork(cls, arr: list) -> Optional[PriceStreamProtocol]:
        if len(arr) == 6:
            return PriceStreamProtocol(
                arr[0], arr[1], float(arr[2]), float(arr[3]), arr[4] == 's', arr[5])
        return None

    @classmethod
    def ParseDatabase(cls, symbol: str, db_data: dict) -> Optional[PriceStreamProtocol]:
        return PriceStreamProtocol(
            symbol,
            db_data['time'], float(db_data['price']),
            float(db_data['qty']), db_data['position'] == 's',
            db_data['timeType']
        )


class OrderbookStreamProtocol:
    def __init__(self,
                 bid_all,
                 ask_all,
                 bid_arr,
                 ask_arr,
                 time_type
                 ):
        self.bid_all = bid_all
        self.ask_all = ask_all
        self.bid_arr = bid_arr
        self.ask_arr = ask_arr
        self.event_time = aktime.get_msec()
        self.time_type = time_type
        self.symbol = ''

    #  set_target / get_target are convenient member property to identify target
    #  not for delivery
    def set_target(self, symbol: str):
        self.symbol = symbol

    def to_database(self) -> dict:
        return self.to_network()

    def to_network(self) -> dict:
        return {
            'bidAll': self.bid_all,
            'askAll': self.ask_all,
            'bids': self.bid_arr,
            'asks': self.ask_arr,
            'time': self.event_time,
            'timeType': self.time_type
        }

    @classmethod
    def CreateOrderbookStream(
        cls,
        bid_all,
        ask_all,
        bid_arr,
        ask_arr,
        time_type=TickTimeType.Normal
    ) -> OrderbookStreamProtocol:
        return OrderbookStreamProtocol(
            bid_all,
            ask_all,
            bid_arr,
            ask_arr,
            time_type
        )

    @classmethod
    def ParseNetwork(cls, data: dict) -> Optional[OrderbookStreamProtocol]:
        if 'bidAll' in data:
            orderbook = OrderbookStreamProtocol(
                data['bidAll'], data['askAll'], data['bids'], data['asks'], data['timeType'])
            orderbook.event_time = data['time']
            return orderbook
        return None


class ProgramTradeEvent:
    def __init__(
        self,
        symbol: str,
        event_time: int,
        day_qty: str,
        buy_qty: str,
        sell_qty: str,
        net_qty: str,
        buy_amount: str,  # * 1000
        sell_amount: str,  # * 1000
    ):
        self.symbol = symbol
        self.event_time = event_time
        self.day_qty = day_qty
        self.buy_qty = buy_qty
        self.sell_qty = sell_qty
        self.net_qty = net_qty
        self.buy_amount = buy_amount
        self.sell_amount = sell_amount

    def to_network(self):
        return {
            'symbol': self.symbol,
            'eventTime': self.event_time,
            'dayQty': self.day_qty,
            'buyQty': self.buy_qty,
            'sellQty': self.sell_qty,
            'netQty': self.net_qty,
            'buyAmount': self.buy_amount,
            'sellAmount': self.sell_amount
        }


class BrokerTradeEvent:
    def __init__(self,
                 symbol,
                 event_time,
                 broker,
                 side,
                 quantity,
                 qty_cumulated,
                 foreign_cumulated):
        self.symbol = symbol
        self.event_time = event_time
        self.broker = broker
        self.side = side
        self.quantity = quantity
        self.quantity_cumulated = qty_cumulated
        self.foreign_cumulated = foreign_cumulated

    def to_network(self):
        return {
            'symbol': self.symbol,
            'side': self.side,
            'eventTime': self.event_time,
            'broker': self.broker,
            'qty': self.quantity,
            'qtyCum': self.quantity_cumulated,  # net qty is accurate
            'foreignCum': self.foreign_cumulated
        }


class HoldAsset:
    def __init__(self,
                 asset_name: str,
                 asset_desc: str,
                 afree: str,
                 afreeze: str,
                 alocked: str,
                 awithdrawing: str,
                 evalprice: str,
                 buyestimated: str,
                 ref_symbol_id: str):
        self.asset_name: str = asset_name
        self.asset_desc = asset_desc
        self.free = afree
        self.freeze = afreeze
        self.locked = alocked
        self.withdrawing = awithdrawing
        self.evalprice = evalprice
        self.buyestimated = buyestimated
        self.ref_symbol_id = ref_symbol_id

    def to_network(self):
        return {'asset': self.asset_name,
                'desc': self.asset_desc,
                'free': self.free,
                'freeze': self.freeze,
                'locked': self.locked,
                'evalPrice': self.evalprice,
                'buyEstimated': self.buyestimated,
                'ref': self.ref_symbol_id,
                'currentPrice': '0',
                'profit': '0'}


class HoldAssetList:
    def __init__(self):
        self.hold_assets: Dict[str, HoldAsset] = {}

    def to_array(self):
        result = []
        for asset in self.hold_assets.values():
            result.append(asset.to_network())
        return result

    def __iter__(self):
        return iter(self.hold_assets.values())

    def add_asset(self, hold_asset: HoldAsset):
        self.hold_assets[hold_asset.asset_name] = hold_asset

    def add_hold_asset(self,
                       asset_name: str,
                       asset_desc: str,
                       afree: str,
                       afreeze: str,
                       alocked: str,
                       awithdrawing: str,
                       evalprice: str,
                       buyestimated: str,
                       ref_symbol_id: str):
        self.hold_assets[asset_name] = HoldAsset(
            asset_name,
            asset_desc, afree, afreeze, alocked,
            awithdrawing, evalprice, buyestimated, ref_symbol_id
        )

    @classmethod
    def ParseHoldAssetList(
        cls,
        arr: list
    ) -> HoldAssetList:
        hold_asset_list = HoldAssetList()
        for asset in arr:
            hold_asset_list.add_hold_asset(
                asset['asset'],
                asset['desc'],
                asset['free'],
                asset['freeze'],
                asset['locked'],
                '0',
                asset['evalPrice'],
                asset['buyEstimated'],
                asset['ref'],
            )
        return hold_asset_list


class KrxDailyInvestor:
    def __init__(self,
                 yyyymmdd: str,
                 individual: str,
                 foreigner: str,
                 group_all: str,
                 financial_invest: str,
                 insurance: str,
                 trust: str,
                 bank: str,
                 etc_financial: str,
                 pension: str,
                 etc_corp: str,
                 etc_foreigner: str,
                 pef: str,
                 national_local: str,
                 day_qty: str,
                 close_price: str,
                 change: str):
        self.yyyymmdd = yyyymmdd
        self.individual = individual
        self.foreigner = foreigner
        self.group_all = group_all
        self.financial_invest = financial_invest
        self.insurance = insurance
        self.trust = trust
        self.bank = bank
        self.etc_financial = etc_financial
        self.pension = pension
        self.etc_corp = etc_corp
        self.etc_foreigner = etc_foreigner
        self.pef = pef
        self.national_local = national_local
        self.day_qty = day_qty
        self.close_price = close_price
        self.change = change

    def to_network(self):
        return {
            'yyyymmdd': self.yyyymmdd,
            'individual': self.individual,
            'foreigner': self.foreigner,
            'groupAll': self.group_all,
            'financialInvest': self.financial_invest,
            'insurance': self.insurance,
            'trust': self.trust,
            'bank': self.bank,
            'etcFinancial': self.etc_financial,
            'pension': self.pension,
            'etcCorp': self.etc_corp,
            'etcForeigner': self.etc_foreigner,
            'pef': self.pef,
            'nationalLocal': self.national_local,
            'qty': self.day_qty,
            'closePrice': self.close_price,
            'change': self.change
        }


class KrxDailyProgram:
    def __init__(
        self,
        yyyymmdd: str,
        qty: str,
        sell_qty: str,
        buy_qty: str,
        sell_amount: str,
        buy_amount: str,
        program_qty_ratio: str
    ):
        self.yyyymmdd = yyyymmdd
        self.qty = qty
        self.sell_qty = sell_qty
        self.buy_qty = buy_qty
        self.sell_amount = sell_amount
        self.buy_amount = buy_amount
        self.program_qty_ratio = program_qty_ratio

    def to_network(self):
        return {
            'yyyymmdd': self.yyyymmdd,
            'qty': self.qty,
            'sellQty': self.sell_qty,
            'buyQty': self.buy_qty,
            'sellAmount': self.sell_amount,
            'buyAmount': self.buy_amount,
            'programRatio': self.program_qty_ratio
        }


class KrxDailyCredit:
    def __init__(
        self,
        yyyymmdd: str,
        qty: str,
        new_loan: str,
        payoff: str,
        balance_qty: str,
        amount: str,
        loan_day_ratio: str,
        loan_stock_ratio: str
    ):
        self.yyyymmdd = yyyymmdd
        self.qty = qty
        self.new_loan = new_loan
        self.payoff = payoff
        self.balance_qty = balance_qty
        self.amount = amount
        self.loan_day_ratio = loan_day_ratio
        self.loan_stock_ratio = loan_stock_ratio

    def to_network(self):
        return {
            'yyyymmdd': self.yyyymmdd,
            'qty': self.qty,
            'newLoan': self.new_loan,
            'payoff': self.payoff,
            'balanceQty': self.balance_qty,
            'amount': self.amount,
            'loanDayRatio': self.loan_day_ratio,
            'loanStockRatio': self.loan_stock_ratio
        }


class KrxDailyShortSell:
    def __init__(
        self,
        yyyymmdd: str,
        qty: str,
        short_sell_qty: str,
        loan: str,
        payoff: str,
        loan_total_qty: str,
        loan_total_amount: str
    ):
        self.yyyymmdd = yyyymmdd
        self.qty = qty
        self.short_sell_qty = short_sell_qty
        self.loan = loan
        self.payoff = payoff
        self.loan_total_qty = loan_total_qty
        self.loan_total_amount = loan_total_amount

    def to_network(self):
        return {
            'yyyymmdd': self.yyyymmdd,
            'qty': self.qty,
            'shortSellQty': self.short_sell_qty,
            'loan': self.loan,
            'payoff': self.payoff,
            'loanTotalQty': self.loan_total_qty,
            'loanTotalAmount': self.loan_total_amount
        }


class KrxDailyBroker:
    def __init__(
        self,
        yyyymmdd: str,
        buy_qty: str,
        sell_qty: str
    ):
        self.yyyymmdd = yyyymmdd
        self.buy_qty = buy_qty
        self.sell_qty = sell_qty

    def to_network(self):
        return {
            'yyyymmdd': self.yyyymmdd,
            'buyQty': self.buy_qty,
            'sellQty': self.sell_qty
        }


class OrderTradeEvent:
    def __init__(
        self,
        symbol_id,
        side,
        event_time,
        order_type,  # LIMIT
        event_type,
        event_subtype,
        order_id,
        order_orig_qty,
        order_orig_price,
        trade_qty,
        trade_cum_qty,
        trade_price,
        commission_amount,
        commission_asset
    ):
        self.symbol_id = symbol_id
        self.side = side
        self.event_time = event_time
        self.order_type = order_type
        self.event_type = event_type
        self.event_subtype = event_subtype
        self.order_id = order_id
        self.order_orig_qty = order_orig_qty
        self.order_orig_price = order_orig_price
        self.trade_qty = trade_qty
        self.trade_cum_qty = trade_cum_qty
        self.trade_price = trade_price
        self.commission_amount = commission_amount
        self.commission_asset = commission_asset

    def to_network(self):
        return {
            'symbolId': self.symbol_id,
            'side': self.side,
            'eventTime': self.event_time,
            'orderType': self.order_type,
            'eventType': self.event_type,
            'eventSubtype': self.event_subtype,
            'orderId': self.order_id,
            'orderOrigQty': self.order_orig_qty,
            'orderOrigPrice': self.order_orig_price,
            'tradeQty': self.trade_qty,
            'tradeCumQty': self.trade_cum_qty,
            'tradePrice': self.trade_price,
            'commissionAmount': self.commission_amount,
            'commissionAsset': self.commission_asset
        }

    @classmethod
    def ParseNetwork(cls, data: dict) -> Optional[OrderTradeEvent]:
        event = OrderTradeEvent(
            data['symbolId'],
            data['side'],
            data['eventTime'],
            data['orderType'],
            data['eventType'],
            data['eventSubtype'],
            data['orderId'],
            data['orderOrigQty'],
            data['orderOrigPrice'],
            data['tradeQty'],
            data['tradeCumQty'],
            data['tradePrice'],
            data['commissionAmount'],
            data['commissionAsset']
        )
        return event


class OrderResponse:
    def __init__(self, symbol, side, price, qty, status):
        self.symbol = symbol
        self.side = side
        self.price = price
        self.qty = qty
        self.status = status  # NEW, FILLED ..

    def to_network(self):
        return {
            'symol': self.symbol,
            'side': self.side,
            'price': self.price,
            'qty': self.qty,
            'status': self.status
        }

    @classmethod
    def CreateOrderResponse(
        cls, symbol, side,
        price, qty, status
    ):
        return OrderResponse(symbol, side, price, qty, status)


class CybosTradeEvent:
    Unknown = 0
    Trade = 1
    Confirm = 2
    Denied = 3
    Submit = 4

    def __init__(
        self,
        flag: str,
        symbol: str,
        order_num: int,
        qty: int,
        price: int,
        order_type: str
    ):
        self.status = CybosTradeEvent.Unknown
        if flag == '4':
            self.status = CybosTradeEvent.Submit
        elif flag == '1':
            self.status = CybosTradeEvent.Trade
        elif flag == '2':
            self.status = CybosTradeEvent.Confirm
        elif flag == '3':
            self.status = CybosTradeEvent.Denied
        self.symbol = symbol.lower()
        self.order_num = order_num
        self.qty = qty
        self.price = price
        self.is_buy = True if order_type == '2' else False
