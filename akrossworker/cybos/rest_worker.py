from datetime import timedelta
import logging
from PyQt5.QtCore import QCoreApplication

from akross.connection.pika_qt.quote_channel import QuoteChannel
from akross.connection.pika_qt.rpc_handler import RpcHandler
from akross.common import util
from akross.common import aktime
from akrossworker.common.args_constants import (
    TickTimeType,
    TradingStatus
)
from akrossworker.cybos.api import stock_chart, stock_code
from akrossworker.common.protocol import SymbolInfo
from akrossworker.cybos.api.connection import CybosConnection
from akrossworker.cybos.api.daily_credit import get_daily_credit
from akrossworker.cybos.api.daily_investor_group import get_daily_investor_group
from akrossworker.cybos.api.daily_program_trade import get_daily_program_trade
from akrossworker.cybos.api.orderbook import get_orderbook
from akrossworker.cybos.api.orderbook_extended import get_orderbook_extended
from akrossworker.cybos.api.rank_codes import get_rank_codes


LOGGER = logging.getLogger(__name__)


class CybosRestWorker(RpcHandler):
    def __init__(self):
        super().__init__()
        self.candle = self.on_candle
        self.brokerList = self.on_broker_list
        self.dailyInvetorGroup = self.on_daily_investor_group
        self.dailyProgramTrade = self.on_daily_program_trade
        self.dailyCredit = self.on_daily_credit
        self.symbolInfo = self.on_symbol_info
        self._symbols = []
        self._market_time = None
        self.marketEndTime = self.on_market_end_time
        self.marketStartTime = self.on_market_start_time
        self.amountRank = self.on_amount_rank_list

    def preload(self):
        LOGGER.warning('create symbol info')
        self._create_symbol_info()
        LOGGER.warning('create symbol info done')

    def _create_symbol_info(self):
        type_name = ['kospi', 'kosdaq']
        call_func = [
            stock_code.get_kospi_company_code_list,
            stock_code.get_kosdaq_company_code_list
        ]
        result = []
        for i, sector in enumerate(type_name):
            code_list = call_func[i]()
            market_caps = stock_code.get_marketcaps(code_list)
            for code in code_list:
                listed = aktime.intdate_to_msec(
                    stock_code.get_stock_listed_date(code), 'KRX')
                symbol_info = SymbolInfo(
                    'krx.spot',
                    code.lower(),
                    ['stock', sector],
                    (TradingStatus.Trading
                        if not stock_code.is_stopped(code)
                        else TradingStatus.Stop),
                    stock_code.code_to_name(code),
                    code,
                    'krw',
                    0,
                    0,
                    ['0', '0', '0'],
                    ['1', '0', '1'],
                    '1',
                    market_caps[code][0] if code in market_caps else '0',
                    market_caps[code][1] if code in market_caps else '0',
                    listed,
                    "Asia/Seoul",
                    0
                )
                result.append(symbol_info.to_network())
        self._symbols = result

    def on_orderbook(self, **kwargs):
        util.check_required_parameters(kwargs, 'symbol')
        symbol = kwargs['symbol'].upper()
        if self.get_market_type() == TickTimeType.Normal:
            return get_orderbook(symbol)
        return get_orderbook_extended(symbol)

    def on_candle(self, **kwargs):
        util.check_required_parameters(kwargs, 'symbol', 'interval')
        LOGGER.info('%s', kwargs)
        symbol = kwargs['symbol'].upper()
        interval = kwargs['interval']

        try:
            del kwargs['symbol']
            del kwargs['interval']
            res = stock_chart.get_kline(symbol, interval, **kwargs)
        except Exception as e:
            LOGGER.error('cannot get candles %s', str(e))
            return []
        res = res[-700000:]
        protocol_result = []
        for data in res:
            protocol_result.append(data.to_network())
        LOGGER.info('send candle response')
        return protocol_result

    def on_symbol_info(self, **kwargs):
        return self._symbols

    def on_market_end_time(self, **kwargs):
        return int(aktime.inttime_to_datetime(
            stock_code.get_market_end_time(), 'KRX').timestamp() * 1000)

    def on_market_start_time(self, **kwargs):
        return int(aktime.inttime_to_datetime(
            stock_code.get_market_start_time(), 'KRX').timestamp() * 1000)

    def on_amount_rank_list(self, **kwargs):
        return get_rank_codes()

    def on_broker_list(self, **kwargs):
        LOGGER.info('%s', kwargs)
        result = []
        members = stock_code.get_member_list()
        for member in members:
            name = stock_code.get_member_name(member)
            result.append({
                'code': member,
                'name': ''.join(name.split(' ')),
                'foreign': stock_code.is_member_foreign(member)
            })
        return result

    def on_daily_investor_group(self, **kwargs):
        LOGGER.info('%s', kwargs)
        util.check_required_parameters(kwargs, 'symbol')
        return get_daily_investor_group(kwargs['symbol'].upper())

    def on_daily_program_trade(self, **kwargs):
        LOGGER.info('%s', kwargs)
        util.check_required_parameters(kwargs, 'symbol')
        return get_daily_program_trade(kwargs['symbol'].upper())

    def on_daily_credit(self, **kwargs):
        LOGGER.info('%s', kwargs)
        util.check_required_parameters(kwargs, 'symbol')
        return get_daily_credit(kwargs['symbol'].upper())

    def get_market_type(self) -> str:
        if self._market_time is None:
            start = \
                aktime.inttime_to_datetime(
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
        conn.connect()
        worker = CybosRestWorker()
        worker.preload()
        conn.run_bus(worker)
        sys.exit(app.exec_())
    else:
        LOGGER.error('cybos is not connected')
