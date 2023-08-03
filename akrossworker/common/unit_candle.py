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

    def get_start_time(self, ms: int, last_start: int) -> int:
        start_time = aktime.get_start_time(ms, self.interval_type, self.symbol_info.tz)
        """
        time type 이 09:00:30 초까지 장전 tick으로 나오고,
        이후 09:00:30 초에 동일하게 장중 tick 으로 나올시
        분봉의 경우 start_time 이 09:00 으로 동일하게 나오는 문제(lightweight chart error 발생)
        type change 되는 경우 발생한 시간으로 start time을 변경하고, 
        ms + 1 의 경우 09:00:00 초에 장전과 장중 동시에 발생하는 경우,
        겹치게 되는 문제 해결을 위해 +1 추가
        """
        if last_start > 0 and last_start >= start_time:
            start_time = last_start + 1

        return start_time

    def get_end_time(self, start_time: int) -> int:
        return aktime.get_end_time(start_time, self.interval_type, self.symbol_info.tz)

    def get_db_start_search(self) -> int:
        return aktime.get_msec_before_day(
            CandleLimitDays.get_limit_days(self.interval_type))

    def get_limit_count(self) -> int:
        return CandleLimitCount.get_limit_count(self.interval_type)

    def get_candle(self, interval: int) -> list:
        return grouping.get_candle(self.data, self.interval_type, interval)

    def get_raw_candle(self) -> List[PriceCandleProtocol]:
        return self.data

    async def fetch(self):
        """
        check when extended time and normal time mixed
        """
        col = self.symbol_info.symbol.lower() + '_1' + self.interval_type
        # LOGGER.info('%s', col)
        now = aktime.get_msec()
        krx_start_time = aktime.get_start_time(now, 'd', self.symbol_info.tz) + aktime.interval_type_to_msec('h') * 9
        stored = await self.db.get_data(
            self.db_name, col, {'startTime': {'$gte': self.get_db_start_search()}})

        for data in stored:
            candle = PriceCandleProtocol.ParseDatabase(data)
            if candle.end_time < now:
                self.data.append(candle)

        query = {
            'cache': False,
            'symbol': self.symbol_info.symbol,
            'interval': '1' + self.interval_type,
            'timeout': 180
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
                elif self.interval_type == 'd':
                    """
                    일봉이 실제 장 시작전에 어제 종가로 들어와서, 
                    잘못된 open price로 설정되는 문제 방지
                    stream 통해서 일봉 생성하도록 장 시작전 일봉은 무시
                    """
                    if candle.start_time <= now <= candle.end_time and now < krx_start_time:
                        continue
                self.data.append(candle)
        else:
            LOGGER.error('fetch error(%s) interval:%s',
                         self.symbol_info.symbol, self.interval_type)
        self.fetch_done = True

    async def add_new_candle(self, s: PriceStreamProtocol, last_start: int = 0):
        # col = self.symbol_info.symbol.lower() + '_1' + self.interval_type
        start_time = self.get_start_time(s.event_time, last_start)
        end_time = self.get_end_time(start_time)

        self.data.append(PriceCandleProtocol.CreatePriceCandle(
            s.price, s.price, s.price, s.price,
            start_time, end_time,
            s.volume, float(s.volume) * float(s.price),
            s.time_type
        ))

        if len(self.data) > self.get_limit_count():
            self.data = self.data[-self.get_limit_count():]

    def _is_apply_extended(self):
        if self.interval_type == 'm' or self.interval_type == 'h':
            return True
        return False

    async def update_stream_data(self, stream: list):
        if not self.fetch_done:
            return

        s = PriceStreamProtocol.ParseNetwork(stream)
        if not self._is_apply_extended() and s.time_type != TickTimeType.Normal:
            return
        elif len(self.data) == 0:
            await self.add_new_candle(s)
        else:
            last_candle = self.data[-1]
            if s.event_time > last_candle.end_time:
                await self.add_new_candle(s)
            elif s.event_time >= last_candle.start_time and s.event_time <= last_candle.end_time:
                if s.time_type != last_candle.time_type:
                    await self.add_new_candle(s, last_candle.start_time)
                else:
                    last_candle.price_close = s.price
                    if float(s.price) > last_candle.price_high:
                        last_candle.price_high = s.price
                    if float(s.price) < last_candle.price_low:
                        last_candle.price_low = s.price
                    last_candle.add_base_asset_volume(s.volume)
                    last_candle.add_quote_asset_volume(float(s.volume) * float(s.price))
            else:
                pass  # stream time is past
