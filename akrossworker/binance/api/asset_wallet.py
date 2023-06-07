from typing import Dict, List

import logging
import numpy

from akrossworker.common.protocol import HoldAsset, HoldAssetList, OrderTradeEvent

LOGGER = logging.getLogger(__name__)


class BinanceAsset:
    """
    withdraw and deposit is not counted in calculation
    only use trade history for buy price estimation,
    commissions are also skipped now
    """
    def __init__(self, asset: HoldAsset):
        self.trade_list = []
        self.asset = asset

    def set_trade_history(self, trade_list):
        self.trade_list = trade_list
        self.calculate_buy_est()

    def get_asset_name(self):
        return self.asset.asset_name.lower()

    def add_trade_event(self, order_event: OrderTradeEvent):
        if float(order_event.trade_price) != 0 and float(order_event.trade_qty) > 0:
            self.trade_list.append({
                'price': order_event.trade_price,
                'qty': order_event.trade_qty,
                'isBuyer': True if order_event.side.lower() == 'buy' else False,
                'time': order_event.event_time,
                # trade history 에서 commission 을 쓰기 때문에 commissionAmount가 아닌 commission
                'commission': order_event.commission_amount,
                'commissionAsset': order_event.commission_asset
            })

    def calculate_buy_est(self):
        # supporting up to 1000 history for fast search
        # binance also supporting 1000 limit on one request(recvWindow is bigger)
        trade_list = self.trade_list[-1000:]
        qty = 0
        calc_trade_list = []
        for trade in reversed(trade_list):
            trade_qty = 0
            if trade['isBuyer']:
                trade_qty += float(trade['qty'])
            else:
                trade_qty -= float(trade['qty'])

            if ('commissionAsset' in trade and 'commission' in trade and
                    trade['commissionAsset'].lower() == self.get_asset_name()):
                """
                qty가 1000 이고 commission 을 같은 암호화폐로 10 을 내었다면,
                commission 을 제외한 990이 지갑으로 들어오기 때문에, - 로 계산
                TODO: 매도인 경우는 qty가 commission 포함여부 확인 필요
                """
                trade_qty -= float(trade['commission'])
            calc_trade_list.insert(0, {'qty': trade_qty, 'price': float(trade['price'])})
            qty += trade_qty
            # LOGGER.debug('find sum(%f) current(%f), time(%s), %s',
            #              float(self.asset.free) + float(self.asset.locked),
            #              qty, akross.msec_to_string(trade['time']), trade)

            # LOGGER.debug(
            #     'compare %f == %f = %s',
            #     float(self.asset.free) + float(self.asset.locked),
            #     qty, qty == float(self.asset.free) + float(self.asset.locked))

            if numpy.isclose(
                qty,
                float(self.asset.free) + float(self.asset.locked),
                rtol=1e-05, atol=1e-08, equal_nan=False
            ).all():
                LOGGER.debug('matched done')
                break

        buy_average = 0
        current_qty = 0
        current_amount = 0

        for transaction in calc_trade_list:
            current_qty += transaction['qty']
            if transaction['qty'] > 0:
                current_amount += transaction['qty'] * transaction['price']
                buy_average = current_amount / current_qty
            else:
                current_amount += buy_average * transaction['qty']
                
        self.asset.buyestimated = str(buy_average)

        LOGGER.info(
            f'{self.asset.asset_name}'
            f' asset estimation(search {len(calc_trade_list)}) '
            f'est:{self.asset.buyestimated} qty:{self.asset.free}')

    def balance_update(self, afree, alocked):
        self.asset.free = afree
        self.asset.locked = alocked
        self.calculate_buy_est()

    def get_asset(self):
        return self.asset


class AssetWallet:
    """
    1. based on asset list, calculate buy estimated(USDT)
    2. when trade event occurs, keep it for balance update
    3. when balance updated, add as new or update and calculate buy estimation
    history format
    [{
        "symbol": "BNBBTC",
        "id": 28457,
        "orderId": 100234,
        "orderListId": -1, //Unless OCO, the value will always be -1
        "price": "4.00000100",
        "qty": "12.00000000",
        "quoteQty": "48.000012",
        "commission": "10.10000000",
        "commissionAsset": "BNB",
        "time": 1499865549590,
        "isBuyer": true,
        "isMaker": false,
        "isBestMatch": true
    }]

    """
    def __init__(self, market):
        self.dollar_backed = ['usdt', 'busd']
        self.assets: Dict[str, BinanceAsset] = {}
        self.market = market
        self.unknown_trade = {}

    def setup(self, hold_asset_list: HoldAssetList):
        for asset in hold_asset_list:
            self.assets[asset.asset_name.lower()] = BinanceAsset(asset)

    def get_assets_array(self):
        result = []
        for asset in self.assets.values():
            result.append(asset.get_asset().to_network())
        return result

    def calculate_asset_list(self) -> List[BinanceAsset]:
        assets = []
        for asset in self.assets.values():
            if not self._is_dollar_backed(asset.get_asset_name()):
                assets.append(asset)
        return assets

    def add_trade_event(self, symbol_name, e):
        # consider if trade event come first and asset is created
        asset_name = self._get_asset_name_from_symbol(symbol_name)
        if len(asset_name) > 0:
            if asset_name in self.assets:
                self.assets[asset_name].add_trade_event(e)
            else:
                if asset_name in self.unknown_trade:
                    self.unknown_trade[asset_name].append(e)
                else:
                    self.unknown_trade[asset_name] = [e]

    def balance_update(self, msg):
        # parse binance msg directly
        assets_updated = msg['B']
        for basset in assets_updated:
            asset_name = basset['a'].lower()
            if asset_name in self.assets:
                if float(basset['f']) == 0 and float(basset['l']) == 0:
                    del self.assets[asset_name]
                else:
                    self.assets[asset_name].balance_update(basset['f'], basset['l'])  # free, locked
            else:
                if float(basset['f']) > 0 or float(basset['l']) > 0:
                    self.assets[asset_name] = BinanceAsset(
                        HoldAsset(
                            basset['a'],
                            basset['a'],
                            basset['f'],
                            '0',
                            basset['l'],
                            '0', '0', '0',
                            ('' if self._is_dollar_backed(basset['a'])
                             else self.market + '.' + basset['a'] + 'USDT')
                        )
                    )
                    if asset_name in self.unknown_trade:
                        for trade_event in self.unknown_trade[asset_name]:
                            self.assets[asset_name].add_trade_event(trade_event)
                        del self.unknown_trade[asset_name]
                        self.assets[asset_name].calculate_buy_est()
                else:
                    LOGGER.warning('unknown and no balance %s', basset)

    def _get_asset_name_from_symbol(self, symbol):
        symbol_name: str = symbol.lower()
        for dollar in self.dollar_backed:
            index = symbol_name.index(dollar)
            if index > 0:
                return symbol_name[:index]
        return ''

    def _is_dollar_backed(self, name: str):
        if name.lower() in self.dollar_backed:
            return True
        return False
