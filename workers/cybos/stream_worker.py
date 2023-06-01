from datetime import timedelta
import logging
from typing import Dict, List
from PyQt5.QtCore import QCoreApplication, QTimer

from akross.connection.pika_qt.quote_channel import QuoteChannel
from akross.connection.pika_qt.rpc_handler import RpcHandler
from akross.common import util
from akross.common import aktime
from workers.common.command import ApiCommand

from workers.cybos.api import stock_code

from workers.common.args_constants import TickTimeType
from workers.cybos.api.connection import CybosConnection
from workers.cybos.api.subscribe import (
    ProgramSubscribe,
    StockExpectSubscribe,
    StockSubscribe,
    OrderbookSubscribe,
    StockExtendedSubscribe,
    OrderbookExtendedSubscribe
)
from workers.cybos.api.subscribe_base import SubscribeBase


LOGGER = logging.getLogger(__name__)


class CybosStreamWorker(RpcHandler):
    def __init__(self, conn: QuoteChannel):
        super().__init__()
        self._conn = conn
        self._code_dict: Dict[str, bool] = {}
        self._market_time = None
        self._current_market_type = TickTimeType.Normal
        self._exchange_to_objs: Dict[str, List[SubscribeBase]] = {}
        self._market_check = QTimer()
        self._market_check.setInterval(10000)  # 10 seconds
        self._market_check.timeout.connect(self.check_time)
        # APIs
        self.priceStream = self.on_price_stream
        self.orderbookStream = self.on_orderbook_stream

    def preload(self):
        for code in stock_code.get_kospi_company_code_list():
            self._code_dict[code.upper()] = True
        for code in stock_code.get_kosdaq_company_code_list():
            self._code_dict[code.upper()] = True

    def check_time(self):
        if self.get_market_type() != self._current_market_type:
            self._current_market_type = self.get_market_type()
            self.switch_subscribe()

    def on_price_stream(self, **kwargs):
        util.check_required_parameters(kwargs, 'exchange', 'target')
        self._handle_realtime_request(
            kwargs['target'],
            kwargs['exchange'],
            ApiCommand.PriceStream,
            False
        )

    def on_orderbook_stream(self, **kwargs):
        util.check_required_parameters(kwargs, 'exchange', 'target')
        self._handle_realtime_request(
            kwargs['target'],
            kwargs['exchange'],
            ApiCommand.OrderbookStream,
            False
        )

    def _handle_realtime_request(
        self,
        symbol: str,
        exchange_name: str,
        cmd: str,
        is_stop: bool
    ) -> None:
        symbol = symbol.upper()
        LOGGER.info('symbol: %s, exchange_name: %s, cmd: %s, is_stop: %s',
                    symbol, exchange_name, cmd, is_stop)
        if symbol not in self._code_dict:
            # cybos 에서 코드 없다는 팝업 띄워 program block 되는 현상 방지
            LOGGER.error('skip - cannot find symbol in code list %s', symbol)
        elif is_stop:
            pass
        else:
            if exchange_name not in self._exchange_to_objs:
                objs = self.get_subscribe_objects(cmd, exchange_name, symbol)
                if len(objs) > 0:
                    self._exchange_to_objs[exchange_name] = objs
                    for obj in objs:
                        self._conn.add_subscribe_count(1)
                        obj.start_subscribe()
                else:
                    LOGGER.error('cannot find subscribe object')

    def stock_data_arrived(self, _symbol, exchange_name, data):
        if exchange_name in self._exchange_to_objs:
            self._conn.send_realtime(exchange_name, ApiCommand.PriceStream, data)

    def orderbook_data_arrived(self, _symbol, exchange_name, data):
        if exchange_name in self._exchange_to_objs:
            self._conn.send_realtime(exchange_name, ApiCommand.OrderbookStream, data)

    def program_data_arrived(self, _symbol, exchange_name, data):
        if exchange_name in self._exchange_to_objs:
            self._conn.send_realtime(exchange_name, ApiCommand.ProgramStream, data)

    def get_subscribe_objects(
        self,
        cmd: str,
        exchange_name: str,
        symbol: str
    ) -> List[SubscribeBase]:
        market_time = self.get_market_type()
        if cmd == ApiCommand.PriceStream:
            if market_time == TickTimeType.Normal:
                return [
                    StockSubscribe(symbol,
                                   exchange_name,
                                   self.stock_data_arrived),
                    StockExpectSubscribe(symbol,
                                         exchange_name,
                                         self.stock_data_arrived)
                ]
            else:
                return [
                    StockExtendedSubscribe(symbol,
                                           exchange_name,
                                           self.stock_data_arrived)
                ]
        elif cmd == ApiCommand.OrderbookStream:
            if market_time == TickTimeType.Normal:
                return [
                    OrderbookSubscribe(symbol,
                                       exchange_name,
                                       self.orderbook_data_arrived)
                ]
            else:
                return [
                    OrderbookExtendedSubscribe(symbol,
                                               exchange_name,
                                               self.orderbook_data_arrived)
                ]
        elif cmd == ApiCommand.ProgramStream:
            if market_time == TickTimeType.Normal:
                return [
                    ProgramSubscribe(symbol,
                                     exchange_name,
                                     self.program_data_arrived)
                ]
            else:
                return []
        return []

    def get_market_type(self) -> str:
        if self._market_time is None:
            start = aktime.inttime_to_datetime(
                stock_code.get_market_start_time(), 'KRX'
            ) - timedelta(minutes=30)
            end = aktime.inttime_to_datetime(
                stock_code.get_market_end_time(), 'KRX'
            ) + timedelta(minutes=30)
            self._market_time = (start, end)

        # because provider will be rebooted on every day morning
        # does not require strict check
        if aktime.get_datetime_now('KRX') <= self._market_time[1]:
            return TickTimeType.Normal
        return TickTimeType.ExtendedTrading

    def switch_subscribe(self):
        switch_to = self.get_market_type()
        LOGGER.warning('switch subscribe to %d', switch_to)
        for k, v in self._exchange_to_objs.items():
            if len(v) > 0 and v[0].time_type != switch_to:
                for obj in self._exchange_to_objs[k]:
                    obj.stop_subscribe()
                self._exchange_to_objs[k] = self.get_subscribe_objects(
                    v[0].subscribe_type, k, v[0].code)
                for obj in self._exchange_to_objs[k]:
                    obj.start_subscribe()


if __name__ == '__main__':
    import signal
    import sys
    LOG_FORMAT = ('%(levelname) -10s %(asctime)s %(name) -30s %(funcName) '
                  '-35s %(lineno) -5d: %(message)s')
    logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)
    app = QCoreApplication([])

    signal.signal(signal.SIGINT, signal.SIG_DFL)
    conn = CybosConnection()
    if conn.is_connected():
        conn = QuoteChannel('krx.spot')
        conn.set_capacity(380)
        conn.connect()
        worker = CybosStreamWorker(conn)
        worker.preload()
        conn.run_bus(worker)
        sys.exit(app.exec_())
    else:
        LOGGER.error('cybos is not connected')
