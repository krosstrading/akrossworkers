import logging
from typing import Dict
from PyQt5.QtCore import QCoreApplication

from akross.common import aktime
from akross.connection.pika_qt.account_channel import AccountChannel
from akross.connection.pika_qt.rpc_handler import RpcHandler
from akross.common import util
from akross.common.exception import CommunicationError

from akrossworker.cybos.api import stock_code
from akrossworker.common.args_constants import OrderResultType, TradingStatus
from akrossworker.common.command import AccountApiCommand
from akrossworker.common.protocol import OrderResponse, SymbolInfo
from akrossworker.cybos.api.account import CybosAccount
from akrossworker.cybos.api.asset import CybosAsset
from akrossworker.cybos.api.asset_manager import AssetManager
from akrossworker.cybos.api.open_order import CybosOpenOrder
from akrossworker.cybos.api import balance
from akrossworker.cybos.api.order import CybosOrder
from akrossworker.cybos.api.connection import CybosConnection


LOGGER = logging.getLogger(__name__)


MARKET = 'krx.spot'
CYBOS_BASE_ASSET = 'krw'


class CybosAccountWorker(RpcHandler):
    def __init__(self):
        super().__init__()
        self._account: CybosAccount = None
        self._asset = None
        self._open_order: CybosOpenOrder = None
        self._order: CybosOrder = None
        self._market = MARKET
        self.assetList = self.on_asset_list
        self.createOrder = self.on_create_order
        self.cancelOrder = self.on_cancel_order
        self.openOrder = self.on_open_order
        self.refreshBalance = self.on_refresh_balance
        self.discovery = self.on_discovery
        self._conn: AccountChannel = None
        self._asset_manager: AssetManager = None
        self._symbol_dict: Dict[str, SymbolInfo] = {}

    def _create_symbol_info(self):
        type_name = ['kospi', 'kosdaq', 'etf', 'index']
        call_func = [
            stock_code.get_kospi_company_code_list,
            stock_code.get_kosdaq_company_code_list,
            stock_code.get_index_etf_list,
            stock_code.get_index_list
        ]
        for i, sector in enumerate(type_name):
            code_list = call_func[i]()
            if sector == 'index':
                min_move = '0.01'
                market_caps = {}
                base_asset_precision = 13
            else:
                min_move = '0'
                market_caps = stock_code.get_marketcaps(code_list)
                base_asset_precision = 0
            
            for code in code_list:
                listed = aktime.intdate_to_msec(
                    stock_code.get_stock_listed_date(code), 'KRX')
                symbol_info = SymbolInfo(
                    MARKET, code.lower(), ['stock', sector],
                    (TradingStatus.Trading if not stock_code.is_stopped(code) else TradingStatus.Stop),
                    stock_code.code_to_name(code), code, CYBOS_BASE_ASSET, base_asset_precision, 0,
                    ['0', '0', min_move], ['1', '0', '1'], '1',
                    market_caps[code][0] if code in market_caps else '0',
                    market_caps[code][1] if code in market_caps else '0',
                    listed, "Asia/Seoul", 0
                )
                self._symbol_dict[code.lower()] = symbol_info

    def preload(self):
        self._create_symbol_info()
        self._account = CybosAccount()
        free_balance = balance.get_balance(
            self._account.get_account_number(), self._account.get_account_type())
        
        self._open_order = CybosOpenOrder(self._account.get_account_number(),
                                          self._account.get_account_type(),
                                          self._market)
        open_orders = self._open_order.request()
        self._asset_manager = AssetManager(
            free_balance, open_orders, self._symbol_dict, self.on_report_event)

        self._asset = CybosAsset(self._market,
                                 self._account.get_account_number(),
                                 self._account.get_account_type())
        self._asset_manager.add_intial_asset(self._asset.get_long_list())

        self._order = CybosOrder(self._account.get_account_number(),
                                 self._account.get_account_type(),
                                 self._asset_manager)

        self._conn = AccountChannel(
            MARKET,
            self._account.get_account_number(),
            self._event_callback
        )
        self._conn.connect()
        self._conn.run_bus(self)
        self._order.start_subscribe()

    def _event_callback(self, msg):
        pass  # deprecated

    def on_report_event(self, msg):
        self._conn.send_event(AccountApiCommand.OrderEvent, msg)
        LOGGER.warning('send orderevent: %s', msg)
        assets = self._asset_manager.get_hold_assets()
        self._conn.send_event(AccountApiCommand.AssetEvent, assets)
        LOGGER.warning('send assetevent: %s', assets)
        # client will handle open orders, not sending open order event separately

    def on_asset_list(self, **kwargs):
        LOGGER.warning('')
        return self._asset_manager.get_hold_assets()

    def on_refresh_balance(self, **kwargs):
        free_balance = balance.get_balance(
            self._account.get_account_number(), self._account.get_account_type())
        self._asset_manager.set_balance(free_balance)
        assets = self._asset_manager.get_hold_assets()
        self._conn.send_event(AccountApiCommand.AssetEvent, assets)
        return {}

    def on_create_order(self, **kwargs):
        util.check_required_parameters(
            kwargs,
            'symbol',
            'side',
            'quantity',
            'price'
        )
        symbol = kwargs['symbol'].upper()
        is_buy = kwargs['side'].lower() == 'buy'
        result_code, msg = self._order.order(
            symbol, kwargs['quantity'], kwargs['price'], is_buy)
        if result_code != 0:
            raise CommunicationError(msg)
        return OrderResponse.CreateOrderResponse(
            symbol, kwargs['side'], kwargs['price'], kwargs['quantity'],
            OrderResultType.New
        ).to_network()

    def on_cancel_order(self, **kwargs):
        LOGGER.info('cancel order %s', kwargs)
        assert self._order is not None
        util.check_required_parameters(kwargs, 'symbol', 'orderId')
        result_code, msg = self._order.cancel_order(
            kwargs['orderId'], kwargs['symbol'].upper())
        if result_code != 0:
            raise CommunicationError(msg)
        return msg

    def on_open_order(self, **kwargs):
        LOGGER.warning('')
        return self._asset_manager.get_open_orders()

    def on_discovery(self, **kwargs):
        LOGGER.info('')
        return {
            'market': MARKET,
            'account': self._account.get_account_number(),
            'broker': 'cybos',
            'baseAsset': CYBOS_BASE_ASSET,
        }


def run():
    import signal
    import sys
    LOG_FORMAT = ('%(levelname) -10s %(asctime)s %(name) -30s %(funcName) '
                  '-35s %(lineno) -5d: %(message)s')
    logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)
    app = QCoreApplication([])

    signal.signal(signal.SIGINT, signal.SIG_DFL)
    cybos_conn = CybosConnection()
    if cybos_conn.is_connected():
        worker = CybosAccountWorker()
        worker.preload()
        sys.exit(app.exec_())
    else:
        LOGGER.error('cybos is not connected')


if __name__ == '__main__':
    run()
