# flake8: noqa
from PyQt5.QtCore import QCoreApplication

import logging
from PyQt5 import QtCore
from datetime import datetime

from akross.providers.cybos.cybos_api.subscribe import (
    BrokerTradeSubscribe,
    ProgramSubscribe,
    StockExpectSubscribe,
    StockSubscribe,
    StockExtendedSubscribe,
    OrderbookSubscribe,
    OrderbookExtendedSubscribe
)


LOGGER = logging.getLogger(__name__)


class CybosTest(QtCore.QObject):
    MARKET = 'KRX'

    def __init__(self):
        super().__init__()
        self.subscribe = {}
        self._timer = QtCore.QTimer()
        self._timer.setInterval(3000)
        self._timer.timeout.connect(self.timer_test)
        self._timer.start()

    def timer_test(self):
        print('timer test')

    def add_subscribe(self, symbol):
        self.subscribe[symbol+'.price'] = \
          StockSubscribe(symbol, self.stock_data_arrived)
        # self.subscribe[symbol+'.exprice'] = \
        #   StockExpectSubscribe(symbol, self.stock_expected_arrived)
        # self.subscribe[symbol+'.orderbook'] = \
        #   OrderbookSubscribe(symbol, self.orderbook_data_arrived)
        # self.subscribe[symbol+'.eprice'] = \
        #   StockExtendedSubscribe(symbol, self.stock_extended_arrived)
        # self.subscribe[symbol+'.eorderbook'] = \
        #   OrderbookExtendedSubscribe(symbol, self.orderbook_data_arrived)
        # self.subscribe[symbol+'.program'] = ProgramSubscribe(
        # symbol, self.program_data_arrived)
        # self.subscribe[symbol+'.broker'] = BrokerTradeSubscribe(
            # '*', self.broker_trade_arrived)
        # self.subscribe[symbol+'.program'].start_subscribe()
        # self.subscribe[symbol+'.broker'].start_subscribe()
        self.subscribe[symbol+'.price'].start_subscribe()
        # self.subscribe[symbol+'.eprice'].start_subscribe()
        # self.subscribe[symbol+'.orderbook'].start_subscribe()
        # self.subscribe[symbol+'.exprice'].start_subscribe()

    def stock_expected_arrived(self, symbol, data):
        print('stock expected', symbol, data)

    def stock_data_arrived(self, symbol, data):
        print('stock data', symbol, data)

    def orderbook_data_arrived(self, symbol, data):
        print('orderbook data', symbol, data)

    def stock_extended_arrived(self, symbol, data):
        print('stock extended', symbol, data)

    def broker_trade_arrived(self, symbol, data):
        etime = ('' if 'eventTime' not in data
                 else datetime.fromtimestamp(int(data['eventTime'] / 1000)))
        print('broker', symbol, etime, data)

    def program_data_arrived(self, symbol, data):
        print('program', symbol, data)


def run():
    LOG_FORMAT = ('%(levelname) -10s %(asctime)s %(name) -30s %(funcName) '
                  '-35s %(lineno) -5d: %(message)s')

    logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)

    app = QCoreApplication([])
    import signal
    import sys

    signal.signal(signal.SIGINT, signal.SIG_DFL)
    
    ct = CybosTest()        
    ct.add_subscribe('b943510')

    sys.exit(app.exec_())


if __name__ == '__main__':
    run()
