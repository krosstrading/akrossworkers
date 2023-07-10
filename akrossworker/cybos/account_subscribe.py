import logging
from PyQt5.QtCore import QCoreApplication

from akross.connection.pika_qt.account_subscribe_channel import AccountSubscribeChannel
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
from akrossworker.cybos.api import com_obj


LOGGER = logging.getLogger(__name__)


MARKET = 'krx.spot'
CYBOS_BASE_ASSET = 'krw'


class _OrderRealtime:
    def set_params(self, obj, listener):
        self.obj = obj
        self.callback = listener

    def OnReceived(self):
        """
        매수, 매도는 (1)접수 -> (2)체결
        취소, 정정은 (1)접수 -> (2) 확인
        오류시에는 거부
        """
        # string flag '1': 체결, '2': 확인, '3': 거부, '4':접수
        flag = self.obj.GetHeaderValue(14)
        # long order number
        order_num = self.obj.GetHeaderValue(5)
        # long quantity
        quantity = self.obj.GetHeaderValue(3)
        # long price 
        price = self.obj.GetHeaderValue(4)
        # 6번은 원주문
        code = self.obj.GetHeaderValue(9)  # code
        # string '1' 매도, '2' 매수, int return 아님
        order_type = self.obj.GetHeaderValue(12)
        # 체결 기준 잔고 수량, 이전 산거 부터 전체 잔고 수량
        # total_quantity = self.obj.GetHeaderValue(23)    # count of stock left
        result = {
            'flag': flag,
            'code': code.lower(),
            'order_number': order_num,
            'quantity': quantity,
            'price': price,
            'order_type': order_type,
            # 'total_quantity': total_quantity
        }
        LOGGER.info('OnReceived %s', result)
        self.callback(result.copy())


class CybosAccountSubscribe:
    def __init__(self):
        self.realtime_order = com_obj.get_com_obj('DsCbo1.CpConclusion')
        self.handler = com_obj.with_events(self.realtime_order, _OrderRealtime)
        self.handler.set_params(self.realtime_order, self.order_event)
        self._account: CybosAccount = None
        self._conn: AccountSubscribeChannel = None

    def preload(self):
        self._account = CybosAccount()
        self._conn = AccountSubscribeChannel(
            MARKET,
            self._account.get_account_number()
        )
        self._conn.connect()
        self._conn.wait_for_connected()
        
        self.realtime_order.Subscribe()

    def order_event(self, msg):
        self._conn.send_event(msg)


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
        worker = CybosAccountSubscribe()
        worker.preload()
        sys.exit(app.exec_())
    else:
        LOGGER.error('cybos is not connected')


if __name__ == '__main__':
    run()
