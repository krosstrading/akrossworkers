import logging
from PyQt5.QtCore import QCoreApplication, QMutex, QMutexLocker

from akross.connection.pika_qt.account_channel import AccountChannel
from akross.connection.pika_qt.rpc_handler import RpcHandler
from akross.common import util
from akross.common.exception import CommunicationError

from akrossworker.common.args_constants import OrderResultType
from akrossworker.common.command import AccountApiCommand
from akrossworker.common.protocol import OrderResponse
from akrossworker.cybos.api.account import CybosAccount
from akrossworker.cybos.api.asset import CybosAsset
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
        self._mutex = QMutex()
        self.assetList = self.on_asset_list
        self.createOrder = self.on_create_order
        self.cancelOrder = self.on_cancel_order
        self.openOrder = self.on_open_order
        self.discovery = self.on_discovery
        self._conn: AccountChannel = None

    def preload(self):
        self._account = CybosAccount()
        self._asset = CybosAsset(self._market,
                                 self._account.get_account_number(),
                                 self._account.get_account_type())
        self._open_order = CybosOpenOrder(self._account.get_account_number(),
                                          self._account.get_account_type(),
                                          self._market)
        self._order = CybosOrder(self._market,
                                 self._account.get_account_number(),
                                 self._account.get_account_type(),
                                 self.on_order_event)
        open_orders = self._open_order.request()
        for open_order in open_orders:
            self._order.add_open_order(open_order)
        
        self._conn = AccountChannel(
            MARKET,
            self._account.get_account_number(),
            self._event_callback
        )
        self._conn.connect()
        self._conn.run_bus(self)

    def _event_callback(self, msg):
        LOGGER.warning('%s', msg)
        self._order.order_event(msg)

    def on_order_event(self, data):
        LOGGER.warning('enter')
        with QMutexLocker(self._mutex):
            self._conn.send_event(AccountApiCommand.OrderEvent, data)
            self._conn.send_event(AccountApiCommand.AssetEvent, self._get_hold_asset())
        LOGGER.warning('done')

    def _get_hold_asset(self):
        LOGGER.warning('')
        hold_list = self._asset.get_long_list()
        krw = balance.get_balance(self._account.get_account_number(),
                                  self._account.get_account_type())
        hold_list.add_hold_asset(
            'KRW', '원화',
            str(krw), '0', '0', '0', '0', '0', '')
        LOGGER.warning('hold asset %s', hold_list.to_array())
        return hold_list.to_array()

    def on_asset_list(self, **kwargs):
        LOGGER.warning('')
        return self._get_hold_asset()

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
        assert self._order
        return self._order.get_open_orders()

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
