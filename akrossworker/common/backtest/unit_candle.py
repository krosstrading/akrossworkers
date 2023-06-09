from datetime import datetime
import logging
from typing import List

from akross.connection.aio.quote_channel import QuoteChannel, Market
from akross.common import aktime
from akrossworker.common.args_constants import (
    CandleLimitDays,
    TickTimeType,
    CandleLimitCount
)
from akrossworker.common import grouping

from akrossworker.common.db import Database
from akrossworker.common.protocol import (
    PriceCandleProtocol,
    PriceStreamProtocol,
    SymbolInfo
)


LOGGER = logging.getLogger(__name__)


class BacktestUnitCandle:
    def __init__(
        self,
        db: Database,
        db_name: str,
        conn: QuoteChannel,
        market: Market,
        symbol_info: SymbolInfo,
        interval_type: str,
        current_time: int
    ):
        self.db = db
        self.conn = conn
        self.market = market
        self.symbol_info = symbol_info
        self.db_name = db_name
        self.interval_type = interval_type
        self.fetch_done = False
        self.data: List[PriceCandleProtocol] = []
        self.current_time = current_time

    def get_start_time(self, ms: int) -> int:
        return aktime.get_start_time(
            ms, self.interval_type, self.symbol_info.tz)

    def get_end_time(self, start_time: int) -> int:
        return aktime.get_end_time(start_time, self.interval_type, self.symbol_info.tz)

    def get_db_start_search(self) -> int:
        return aktime.get_msec_before_day(
            CandleLimitDays.get_limit_days(self.interval_type), self.current_time)

    def get_limit_count(self) -> int:
        return CandleLimitCount.get_limit_count(self.interval_type)

    def get_candle(self, interval: int) -> list:
        return grouping.get_candle(self.data, self.interval_type, interval)

    async def fetch(self) -> int:
        """
        check when extended time and normal time mixed
        """
        col = self.symbol_info.symbol.lower() + '_1' + self.interval_type
        LOGGER.info('%s, current: %s', col, datetime.fromtimestamp(self.current_time / 1000))

        stored = await self.db.get_data(
            self.db_name, col,
            {
                'endTime': {
                    '$lte': self.current_time,
                    '$gte': self.get_db_start_search()
                }
            }
        )

        for data in stored:
            candle = PriceCandleProtocol.ParseDatabase(data)
            self.data.append(candle)

        self.fetch_done = True
        return self.data[-1].end_time if len(self.data) > 0 else 0

    def add_new_candle(self, stream: PriceStreamProtocol, is_same_type=True):
        start_time = self.get_start_time(stream.event_time)
        end_time = self.get_end_time(start_time)
        return PriceCandleProtocol(
            int(stream.price), int(stream.price), int(stream.price), int(stream.price),
            start_time if is_same_type else stream.event_time, end_time,
            int(stream.volume), int(stream.volume) * int(stream.price), stream.time_type, False)

    def add_new_by_candle(self, candle: PriceCandleProtocol, is_same_type=True):
        # candle should be more smaller time unit
        start_time = self.get_start_time(candle.start_time)
        end_time = self.get_end_time(start_time)
        self.data.append(PriceCandleProtocol.CreatePriceCandle(
            candle.price_open, candle.price_high, candle.price_low, candle.price_close,
            start_time if is_same_type else candle.start_time, end_time,
            candle.base_asset_volume, candle.quote_asset_volume,
            candle.time_type
        ))

    def _is_apply_extended(self):
        if self.interval_type == 'm' or self.interval_type == 'h':
            return True
        return False

    async def update_candle_data(self, candle: PriceCandleProtocol):
        if not self.fetch_done:
            return
        elif not self._is_apply_extended() and candle.time_type != TickTimeType.Normal:
            return

        if len(self.data) == 0:
            self.data.append(self.add_new_by_candle(candle))
        else:
            # unit candle 이므로 단위가 걸쳐 있을 수 없음
            last_candle = self.data[-1]
            if candle.start_time > last_candle.end_time:
                self.add_new_by_candle(candle)
            elif last_candle.time_type != candle.time_type:
                self.data.append(self.add_new_by_candle(candle, False))
            elif (last_candle.end_time >= candle.end_time and
                    last_candle.start_time <= candle.start_time):
                #  잘못된 데이터가 들어오는 경우 대비 start_time 도 비교
                last_candle.price_close = candle.price_close
                if candle.price_high > last_candle.price_high:
                    last_candle.price_high = candle.price_high
                if candle.price_low < last_candle.price_low:
                    last_candle.price_low = candle.price_low
                last_candle.add_base_asset_volume(candle.base_asset_volume)
                last_candle.add_quote_asset_volume(candle.quote_asset_volume)

    def update_stream_data(self, stream: PriceStreamProtocol):
        if not self.fetch_done:
            return
        elif not self._is_apply_extended() and stream.time_type != TickTimeType.Normal:
            return

        if len(self.data) == 0:
            self.data.append(self.add_new_candle(stream))
        else:
            last_candle = self.data[-1]
            last_end_time = last_candle.end_time
            last_start_time = last_candle.start_time
            if last_end_time < stream.event_time:
                self.data.append(self.add_new_candle(stream))
            elif last_candle.time_type != stream.time_type:
                self.data.append(self.add_new_candle(stream, False))
            elif last_start_time <= stream.event_time:
                last_candle.price_close = int(stream.price)
                if int(stream.price) > int(last_candle.price_high):
                    last_candle.price_high = int(stream.price)
                if int(stream.price) < int(last_candle.price_low):
                    last_candle.price_low = int(stream.price)
                last_candle.add_base_asset_volume(stream.volume)
                last_candle.add_quote_asset_volume(int(stream.price) * int(stream.volume))

