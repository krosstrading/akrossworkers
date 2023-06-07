import asyncio
import logging
import sys
from typing import Dict
import aio_pika

from akross.common import env
from akross.common import aktime
from akross.connection.aio.quote_channel import QuoteChannel, Market
from akross.common import enums, util
from akrossworkers.common.backtest.backtest_candle import BacktestCandle
from akrossworkers.common.command import ApiCommand
from akrossworkers.common.db import DBEnum, Database
from akrossworkers.common.protocol import (
    SymbolInfo
)
from akross.rpc.base import RpcBase


LOGGER = logging.getLogger(__name__)
MARKET_NAME = 'krx.spot'


class CybosBacktestWorker(RpcBase):
    def __init__(self):
        super().__init__()
        self.candle = self.on_candle
        self.backtestStream = self.on_backtest_stream
        self.createBacktest = self.on_create_backtest
        self.finishBacktest = self.on_finish_backtest
        self.next = self.on_next
        self._worker: Market = None
        self._timeFrame = None
        self._symbols: Dict[str, SymbolInfo] = {}
        self._conn = QuoteChannel(MARKET_NAME)
        self._backtestCandle: Dict[str, BacktestCandle] = {}
        self._exchange: aio_pika.abc.AbstractExchange = None
        self._db = Database()

    async def preload(self):
        await self._conn.connect()
        await self._conn.market_discovery()
        await self._conn.wait_for_market(MARKET_NAME)
        cybos = self._conn.get_markets(MARKET_NAME)
        if cybos is None:
            LOGGER.error('cannot find market')
            sys.exit(1)
        self._worker = cybos[0]
        await self.regist_symbols()

    async def regist_symbols(self) -> None:
        ret, symbols = await self._conn.api_call(
            self._worker, ApiCommand.SymbolInfo, cache=False)
        if not isinstance(symbols, list) or len(symbols) == 0:
            LOGGER.error('no symbols')
            sys.exit(1)

        for symbol in symbols:
            symbol_info = SymbolInfo.CreateSymbolInfo(symbol)
            symbol_name = symbol_info.symbol.lower()
            if symbol_name not in self._symbols:
                self._symbols[symbol_name] = symbol_info

    async def on_backtest_stream(self, **kwargs):
        return {'exchange': self._exchange.name}
    
    async def on_create_backtest(self, **kwargs):
        # remove all datas existing
        util.check_required_parameters(kwargs, 'backtest', 'targets', 'startTime', 'endTime', 'timeFrame')
        await self.on_finish_backtest()
        self._exchange = await self._conn.get_stream_exchange(kwargs['backtest'], auto_delete=False)
        self._timeFrame = {
            'start': kwargs['startTime'],
            'end': kwargs['endTime'],
            'current': kwargs['startTime'],
            'step': aktime.interval_type_to_msec(kwargs['timeFrame'])
        }
        for target in kwargs['targets']:
            target_name = target.lower()
            if target_name in self._symbols:
                self._backtestCandle[target_name] = BacktestCandle(
                    self._db, DBEnum.KRX_QUOTE_DB, self._conn,
                    self._worker, self._exchange, self._symbols[target_name],
                    kwargs['startTime'], kwargs['endTime'], kwargs['timeFrame'])
                await self._backtestCandle[target_name].setup()
        return {}

    async def on_finish_backtest(self, **kwargs):
        self._backtestCandle.clear()
        return {}

    async def on_next(self, **kwargs):
        util.check_required_parameters(kwargs, 'currentTime')

        LOGGER.info('current %d, next %d', self._timeFrame['current'], kwargs['currentTime'])
        if self._timeFrame is not None:
            while (self._timeFrame['current'] <= self._timeFrame['end'] and
                   self._timeFrame['current'] <= kwargs['currentTime']):
                self._timeFrame['current'] += self._timeFrame['step']
                for backtest in self._backtestCandle.values():
                    await backtest.set_time(self._timeFrame['current'])
        LOGGER.info('done')
        return {}

    async def on_candle(self, **kwargs):
        util.check_required_parameters(kwargs, 'symbol', 'interval')
        interval = kwargs['interval']
        symbol = kwargs['symbol'].lower()
        if symbol in self._backtestCandle:
            return await self._backtestCandle[symbol].get_data(interval)
        return []


async def main() -> None:
    LOGGER.warning('run rest provider')
    conn = QuoteChannel('krx.spot', env.get_rmq_url())

    backtester = CybosBacktestWorker()
    await backtester.preload()
    await conn.connect(True)
    await conn.run_with_bus_queue(enums.WorkerType.Backtest, backtester)
    await asyncio.get_running_loop().create_future()


if __name__ == '__main__':
    import logging
    LOG_FORMAT = ('%(levelname) -10s %(asctime)s %(name) -30s %(funcName) '
                  '-35s %(lineno) -5d: %(message)s')
    logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)
    asyncio.run(main())