import asyncio
import logging
import sys
from typing import Dict, List
import aio_pika

from akross.common import env
from akross.common import aktime
from akross.connection.aio.quote_channel import QuoteChannel, Market
from akross.common import enums, util
from akrossworker.common.backtest.backtest_candle import BacktestCandle
from akrossworker.common.command import ApiCommand
from akrossworker.common.db import DBEnum, Database
from akrossworker.common.protocol import (
    SymbolInfo,
    PriceStreamProtocol,
    OrderbookStreamProtocol
)
from akross.rpc.base import RpcBase

from akrossworker.common.util import datetime_str


LOGGER = logging.getLogger(__name__)
MARKET_NAME = 'krx.spot'


class CybosBacktestWorker(RpcBase):
    def __init__(self):
        super().__init__()
        self.candle = self.on_candle
        self.backtestStream = self.on_backtest_stream
        self.createBacktest = self.on_create_backtest
        self.finishBacktest = self.on_finish_backtest
        self.play = self.on_play
        self.next = self.on_next
        self.speed = self.on_speed
        self.pause = self.on_pause

        self._worker: Market = None
        self._timeFrame = None
        self._is_streaming = False
        self._stream_pause = False
        self._stream_speed = 2.0
        self._stream_stop = False
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
        util.check_required_parameters(kwargs, 'backtest', 'startTime', 'endTime', 'targets')
        await self.on_finish_backtest()
        self._exchange = await self._conn.get_stream_exchange(kwargs['backtest'], auto_delete=False)
        self._timeFrame = {
            'start': kwargs['startTime'],
            'end': kwargs['endTime'],
            'current': kwargs['startTime'],
            'targets': kwargs['targets']
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
        if self._is_streaming:
            self._stream_stop = True
        self._backtestCandle.clear()
        return {}

    async def on_play(self, **kwargs):
        if not self._is_streaming:
            asyncio.create_task(self.start_stream())
        elif self._is_streaming and self._stream_pause:
            self._stream_pause = False
        return {}

    async def on_pause(self, **kwargs):
        if self._is_streaming:
            self._stream_pause = True
        return {}

    async def on_speed(self, **kwargs):
        util.check_required_parameters(kwargs, 'speed')
        self._stream_speed = float(kwargs['speed'])
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

    async def _prefetch_stream(self, targets: List[str], current: int, interval: int):
        stream_data = []
        for symbol in targets:
            prices = await self._db.get_price_stream_data(symbol, current, current + interval)
            for price in prices:
                stream_data.append(PriceStreamProtocol.ParseDatabase(symbol, price))
            orderbooks = await self._db.get_orderbook_stream_data(symbol, current, current + interval)
            for orderbook in orderbooks:
                obs = OrderbookStreamProtocol.ParseNetwork(orderbook)
                obs.set_target(symbol)
                stream_data.append(obs)
        LOGGER.warning('total %d fetched', len(stream_data))
        if len(stream_data) > 0:
            return sorted(stream_data, key=lambda x: x.event_time)
        return []
        
    async def start_stream(self, **kwargs):
        targets = self._timeFrame['targets']
        LOGGER.warning('start stream targets: %s', targets)
        interval = aktime.interval_type_to_msec('m') * 10
        self._is_streaming = True
        stream_data = []
        while not self._stream_stop and self._timeFrame['current'] <= self._timeFrame['end'] + interval:
            prefetch_task = asyncio.create_task(
                self._prefetch_stream(targets, self._timeFrame['current'], interval))
            current = self._timeFrame['current']
            LOGGER.warning('current %s, total tick: %d', datetime_str(current), len(stream_data))

            self._timeFrame['current'] += interval
            if len(stream_data) > 0:
                current_time = aktime.get_msec()
                current_frametime = stream_data[0].event_time
                while len(stream_data) > 0:
                    if self._stream_pause:
                        LOGGER.warning('pause state')
                        await asyncio.sleep(1)
                        continue
                        
                    stream = stream_data.pop(0)
                    frame_timegap = (stream.event_time - current_frametime) / self._stream_speed
                    realtime_gap = aktime.get_msec() - current_time
                    if self._stream_stop:
                        LOGGER.warning('stream stop')
                        break
                    elif frame_timegap > realtime_gap:
                        # LOGGER.warning('sleep %f', (frame_timegap - realtime_gap) / 1000)
                        await asyncio.sleep((frame_timegap - realtime_gap) / 1000)

                    current_frametime = stream.event_time
                    current_time = aktime.get_msec()

                    if isinstance(stream, PriceStreamProtocol):
                        if stream.symbol.lower() in self._backtestCandle:
                            self._backtestCandle[stream.symbol.lower()].add_stream_data(stream)
                        await self._conn.publish_backtest_stream(
                            self._exchange,
                            ApiCommand.PriceStream,
                            stream.symbol,
                            stream.to_network()
                        )
                    elif isinstance(stream, OrderbookStreamProtocol):
                        await self._conn.publish_backtest_stream(
                            self._exchange,
                            ApiCommand.OrderbookStream,
                            stream.symbol,
                            stream.to_network()
                        )
            stream_data = await prefetch_task
            LOGGER.warning('current %s, total tick: %d', datetime_str(current), len(stream_data))
            await asyncio.sleep(0.1)
        self._is_streaming = False
        self._stream_pause = False
        self._stream_stop = False


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