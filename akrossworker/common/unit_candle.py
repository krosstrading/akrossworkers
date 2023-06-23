import logging
from typing import List
from datetime import datetime

from akross.connection.aio.quote_channel import QuoteChannel, Market
from akross.common import aktime
from akrossworker.common.args_constants import (
    CandleLimitDays,
    TickTimeType,
    CandleLimitCount
)
from akrossworker.common.command import ApiCommand
from akrossworker.common import grouping

from akrossworker.common.db import Database
from akrossworker.common.protocol import (
    PriceCandleProtocol,
    PriceStreamProtocol,
    SymbolInfo
)


LOGGER = logging.getLogger(__name__)


class UnitCandle:
    def __init__(
        self,
        db: Database,
        db_name: str,
        conn: QuoteChannel,
        market: Market,
        symbol_info: SymbolInfo,
        interval_type: str
    ):
        self.db = db
        self.conn = conn
        self.market = market
        self.symbol_info = symbol_info
        self.db_name = db_name
        self.interval_type = interval_type
        self.fetch_done = False
        self.data: List[PriceCandleProtocol] = []
        self.db_last_record = 0

    def get_start_time(self, ms: int) -> int:
        return aktime.get_start_time(
            ms, self.interval_type, self.symbol_info.tz)

    def get_end_time(self, start_time: int) -> int:
        return aktime.get_end_time(start_time, self.interval_type, self.symbol_info.tz)

    def get_db_start_search(self) -> int:
        return aktime.get_msec_before_day(
            CandleLimitDays.get_limit_days(self.interval_type))

    def get_limit_count(self) -> int:
        return CandleLimitCount.get_limit_count(self.interval_type)

    def get_candle(self, interval: int) -> list:
        return grouping.get_candle(self.data, self.interval_type, interval)

    async def fetch(self):
        """
        check when extended time and normal time mixed
        """
        col = self.symbol_info.symbol.lower() + '_1' + self.interval_type
        # LOGGER.info('%s', col)
        now = aktime.get_msec()
        stored = await self.db.get_data(
            self.db_name, col, {'startTime': {'$gte': self.get_db_start_search()}})

        for data in stored:
            candle = PriceCandleProtocol.ParseDatabase(data)
            if candle.end_time < now:
                self.data.append(candle)

        query = {
            'cache': False,
            'symbol': self.symbol_info.symbol,
            'interval': '1' + self.interval_type
        }
        if len(self.data) > 0:
            LOGGER.info('%s db last time %s',
                        col, datetime.fromtimestamp(int(self.data[-1].end_time / 1000)))
            query['startTime'] = self.data[-1].end_time + 1
            query['endTime'] = now
            self.db_last_record = self.data[-1].end_time
        else:
            pass

        ret, resp = await self.conn.api_call(
            self.market, ApiCommand.Candle, **query)

        if resp is not None and isinstance(resp, list):
            LOGGER.info('api query %s, data len(%d)', query, len(resp))
            for data in resp:
                candle = PriceCandleProtocol.ParseNetwork(data)
                if candle.start_time <= self.db_last_record:
                    LOGGER.warning('skip data')
                    continue
                elif candle.end_time < now and await self.db.connected():
                    await self.db.insert_one(self.db_name, col, candle.to_database())
                    self.db_last_record = candle.end_time
                    # LOGGER.info('%s db insert %s',
                    #             col,
                    #             datetime.fromtimestamp(int(candle.end_time / 1000)))
                self.data.append(candle)
        else:
            LOGGER.error('fetch error(%s) interval:%s',
                         self.symbol_info.symbol, self.interval_type)
        self.fetch_done = True

    async def add_new_candle(self, s: PriceStreamProtocol):
        col = self.symbol_info.symbol.lower() + '_1' + self.interval_type
        start_time = self.get_start_time(s.event_time)
        end_time = self.get_end_time(start_time)

        self.data.append(PriceCandleProtocol.CreatePriceCandle(
            s.price, s.price, s.price, s.price,
            start_time, end_time,
            s.volume, float(s.volume) * float(s.price),
            s.time_type
        ))

        if len(self.data) > self.get_limit_count():
            self.data = self.data[-self.get_limit_count():]

        if len(self.data) > 1 and self.data[-2].end_time > self.db_last_record:
            self.db_last_record = self.data[-2].end_time
            await self.db.insert_one(self.db_name, col, self.data[-2].to_database())
            LOGGER.info('%s db new candle insert (candle last:%s), (db last:%s)',
                        col,
                        datetime.fromtimestamp(int(self.data[-2].end_time / 1000)),
                        datetime.fromtimestamp(int(self.db_last_record / 1000)))

    def _is_apply_extended(self):
        if self.interval_type == 'm' or self.interval_type == 'h':
            return True
        return False

    def update_stream_data(self, stream: list):
        if not self.fetch_done:
            return

        s = PriceStreamProtocol.ParseNetwork(stream)
        as_new = False
        if not self._is_apply_extended() and s.time_type != TickTimeType.Normal:
            return

        if len(self.data) > 0:
            last_time_type = self.data[-1].time_type
            if s.time_type != last_time_type:
                as_new = True
        else:
            as_new = True

        if as_new:
            self.add_new_candle(s)
        else:
            last_candle = self.data[-1]
            if last_candle.end_time >= s.event_time:
                last_candle.price_close = s.price
                if float(s.price) > last_candle.price_high:
                    last_candle.price_high = s.price
                if float(s.price) < last_candle.price_low:
                    last_candle.price_low = s.price
                last_candle.add_base_asset_volume(s.volume)
                last_candle.add_quote_asset_volume(float(s.volume) * float(s.price))
            elif s.event_time < last_candle.start_time:
                LOGGER.warning('stream time is past')
            else:
                self.add_new_candle(s)
