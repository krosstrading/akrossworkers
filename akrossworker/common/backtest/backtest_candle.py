from datetime import datetime
from typing import Dict
import aio_pika
import logging

from akross.connection.aio.quote_channel import QuoteChannel, Market
from akross.common import aktime
from akrossworker.common.backtest.realtime_candle import RealtimeCandle

from akrossworker.common.backtest.time_frame_candle import TimeFrameCandle
from akrossworker.common.backtest.unit_candle import BacktestUnitCandle
from akrossworker.common.command import ApiCommand

from akrossworker.common.db import Database
from akrossworker.common.protocol import (
    PriceStreamProtocol,
    SymbolInfo
)


LOGGER = logging.getLogger(__name__)


class BacktestCandle:
    def __init__(
        self,
        db: Database,
        db_name: str,
        conn: QuoteChannel,
        market: Market,
        exchange: aio_pika.abc.AbstractExchange,
        symbol_info: SymbolInfo,
        start_time: int,
        end_time: int,
        time_frame: str
    ):
        self.db = db
        self.db_name = db_name
        self.conn = conn
        self.market = market
        self.symbol_info = symbol_info
        self.current_time = start_time
        self.candles: Dict[str, BacktestUnitCandle] = {}
        self.time_frame_candle = None
        self.exchange = exchange
        if len(time_frame) > 0:
            if time_frame == 'r':
                self.time_frame_candle = RealtimeCandle(
                    db, db_name, conn, market, symbol_info, start_time)
            else:
                self.time_frame_candle = TimeFrameCandle(
                    db, db_name, conn, market, symbol_info, time_frame, start_time, end_time)

    async def setup(self):
        # read from database
        if self.time_frame_candle is not None:
            await self.time_frame_candle.fetch()

    async def set_time(self, ms: int) -> int:
        if self.time_frame_candle is None:
            pass  # realtime
        else:
            frames = self.time_frame_candle.next(ms)
            LOGGER.info('forward %s, count : %d', self.symbol_info.symbol, len(frames))
            for frame in frames:
                for candle in self.candles.values():
                    await candle.update_candle_data(frame)
                if self.exchange is not None:
                    await self.conn.publish_backtest_stream(
                        self.exchange,
                        ApiCommand.CandleStream,
                        self.symbol_info.symbol,
                        frame.to_network()
                    )
            return len(frames)
        return 0

    def add_stream_data(self, stream: PriceStreamProtocol):
        if self.time_frame_candle is not None:
            self.time_frame_candle.add_stream(stream)
            for candle in self.candles.values():
                candle.update_stream_data(stream)

    def get_time_frame_data(self):
        return self.time_frame_candle.get_raw_candle()

    async def get_data(self, interval: str):
        interval, interval_type = aktime.interval_dissect(interval)
        # print(self.time_frame_candle is not None, self.time_frame_candle.get_interval_type(), interval_type)
        if (self.time_frame_candle is not None and
                self.time_frame_candle.get_interval_type() == interval_type):
            return self.time_frame_candle.get_data(interval)
        elif interval_type not in self.candles:
            # TODO: interval type should be greater than time frame
            self.candles[interval_type] = BacktestUnitCandle(
                self.db, self.db_name, self.conn, self.market, self.symbol_info, interval_type, self.current_time)
            end_time = await self.candles[interval_type].fetch()
            
            if self.time_frame_candle is None:  # realtime
                pass  # push realtime data
            else:
                until_now = self.time_frame_candle.get_data_until_now(end_time)
                for data in until_now:
                    LOGGER.info('update candle data(%s): %s',
                                interval_type,
                                datetime.fromtimestamp(data.end_time / 1000))
                    await self.candles[interval_type].update_candle_data(data)

        return self.candles[interval_type].get_candle(interval)
