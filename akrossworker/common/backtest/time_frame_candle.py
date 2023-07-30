import logging
from typing import List

from akross.connection.aio.quote_channel import QuoteChannel, Market
from akross.common import aktime
from akrossworker.common.args_constants import (
    CandleLimitDays
)

from akrossworker.common import grouping

from akrossworker.common.db import Database
from akrossworker.common.protocol import (
    PriceCandleProtocol,
    SymbolInfo
)

LOGGER = logging.getLogger(__name__)


class TimeFrameCandle:
    def __init__(
        self,
        db: Database,
        db_name: str,
        conn: QuoteChannel,
        market: Market,
        symbol_info: SymbolInfo,
        interval_type: str,
        start_time: int,
        end_time: int
    ):
        self.db = db
        self.conn = conn
        self.market = market
        self.symbol_info = symbol_info
        self.db_name = db_name
        self.interval_type = interval_type
        self.fetch_done = False
        self.data: List[PriceCandleProtocol] = []
        self.start_time = start_time
        self.end_time = end_time
        self.next_index = 0

    def get_db_start_search(self) -> int:
        return aktime.get_msec_before_day(
            CandleLimitDays.get_limit_days(self.interval_type), self.start_time)

    def get_interval_type(self) -> str:
        return self.interval_type

    def get_data(self, interval: int) -> list:
        if self.next_index == 0:
            return []
        return grouping.get_candle(
            self.data[:self.next_index], self.interval_type, interval)

    def get_raw_candle(self) -> List[PriceCandleProtocol]:
        return self.data

    def get_data_until_now(self, start_time: int) -> List[PriceCandleProtocol]:
        result = []
        for data in self.data[:self.next_index]:
            if data.start_time > start_time:
                result.append(data)
        return result

    def _set_current_time(self, ms: int) -> None:
        for i, candle in enumerate(self.data):
            if candle.start_time >= ms:
                self.next_index = i
                break

    def next(self, ms: int) -> List[PriceCandleProtocol]:
        result = []
        for candle in self.data[self.next_index:]:
            if candle.start_time <= ms:
                result.append(candle)
                self.next_index += 1
            else:
                break
        return result

    async def fetch(self):
        """
        check when extended time and normal time mixed
        """
        col = self.symbol_info.symbol.lower() + '_1' + self.interval_type
        LOGGER.info('%s', col)
        stored = await self.db.get_data(
            self.db_name, col,
            {
                'endTime': {'$gte': self.get_db_start_search()},
                'startTime': {'$lte': self.end_time}
            }
        )

        for data in stored:
            candle = PriceCandleProtocol.ParseDatabase(data)
            self.data.append(candle)

        self._set_current_time(self.start_time)
        # only use database data for a speed