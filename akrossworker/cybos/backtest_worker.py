import asyncio
import logging
import sys
from typing import Dict
import aio_pika

from akross.common import env
from akross.common import aktime
from akross.connection.aio.quote_channel import QuoteChannel, Market
from akross.common import enums, util
from akrossworker.common.backtest.backtest_candle import BacktestCandle
from akrossworker.common.command import ApiCommand
from akrossworker.common.db import DBEnum, Database
from akrossworker.common.protocol import (
    SymbolInfo
)
from akross.rpc.base import RpcBase


LOGGER = logging.getLogger(__name__)
MARKET_NAME = 'krx.spot'


class StreamStatus:
    NONE = 0
    STOP = 1
    PLAY = 2
    PAUSE = 3

    def __init__(self):
        self.speed = 1
        self.status = StreamStatus.STOP
        self.control = StreamStatus.NONE
        self.is_transition = False

    def can_play(self):
        if self.control == StreamStatus.NONE and self.status == StreamStatus.STOP:
            return True
        return False

    def request(self, status):
        if not self.is_transition:
            self.control = status
            self.is_transition = True
        else:
            LOGGER.warning('request(%d), but stream in transition', status)

    def set_status(self, status, force=False):
        if force or self.is_transition:
            self.is_transition = False
            self.control = StreamStatus.NONE
            self.status = status
        else:
            LOGGER.warning('cannot set status %d', status)


class CybosBacktestWorker(RpcBase):
    def __init__(self):
        super().__init__()
        self.candle = self.on_candle
        self.backtestStream = self.on_backtest_stream
        self.createBacktest = self.on_create_backtest
        self.finishBacktest = self.on_finish_backtest
        self.next = self.on_next
        self.play = self.on_play
        self.setSpeed = self.on_set_speed
        self.pause = self.on_pause
        self.krxRank = self.on_krx_rank_symbols
        self.krxAmountRank = self.on_krx_amount_rank

        self._worker: Market = None
        self._timeFrame = None
        self._stream = StreamStatus()
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
        LOGGER.info('')
        return {'exchange': self._exchange.name}
    
    async def on_create_backtest(self, **kwargs):
        # remove all datas existing
        util.check_required_parameters(
            kwargs, 'backtest', 'targets', 'startTime', 'endTime', 'timeFrame')
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
        if self._stream.status != StreamStatus.STOP:
            self._stream.request(StreamStatus.STOP)
            while not self._stream.can_play():
                await asyncio.sleep(1)
        
        self._backtestCandle.clear()
        return {}

    async def on_play(self, **kwargs):
        if self._stream.status == StreamStatus.PAUSE:
            self._stream.request(StreamStatus.PLAY)
        elif self._stream.status == StreamStatus.STOP:
            self._stream.request(StreamStatus.PLAY)
            asyncio.create_task(self.start_stream())
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

    async def on_krx_amount_rank(self, **kwargs):
        return []

    async def on_krx_rank_symbols(self, **kwargs):
        return []

    async def start_stream(self, **kwargs):
        self._stream.set_status(StreamStatus.PLAY)
        interval = self._timeFrame['step']
        prev_time = aktime.get_msec()
        while self._timeFrame['current'] <= self._timeFrame['end']:
            if self._stream.control == StreamStatus.PAUSE:
                self._stream.set_status(StreamStatus.PAUSE)
            elif self._stream.control == StreamStatus.STOP:
                break
            elif self._stream.control == StreamStatus.PLAY:
                self._stream.set_status(StreamStatus.PLAY)
            
            if self._stream.status == StreamStatus.PAUSE:
                await asyncio.sleep(1)
                continue

            self._timeFrame['current'] += interval
            ticks = 0
            for backtest in self._backtestCandle.values():
                ticks += await backtest.set_time(self._timeFrame['current'])
            if ticks == 0:
                continue  # not waiting for empty period

            time_diff = aktime.get_msec() - prev_time
            stream_per_sec = 1000 / self._stream.speed
            if time_diff < stream_per_sec:
                await asyncio.sleep((stream_per_sec - time_diff) / 1000)
            prev_time = aktime.get_msec()

        self._stream.set_status(StreamStatus.STOP, True)

    async def on_pause(self, **kwargs):
        if self._stream.status == StreamStatus.PLAY:
            self._stream.request(StreamStatus.PAUSE)
        return {}

    async def on_set_speed(self, **kwargs):
        util.check_required_parameters(kwargs, 'speed')
        LOGGER.warning('change speed to %d', kwargs['speed'])
        self._stream.speed = float(kwargs['speed'])
        return {}


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
