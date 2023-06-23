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
    PriceStreamProtocol,
    SymbolInfo
)

LOGGER = logging.getLogger(__name__)


class RealtimeCandle:
    def __init__(
        self,
        db: Database,
        db_name: str,
        conn: QuoteChannel,
        market: Market,
        symbol_info: SymbolInfo,
        start_time: int
    ):
        self.db = db
        self.conn = conn
        self.market = market
        self.symbol_info = symbol_info
        self.db_name = db_name
        self.interval_type = 'm'
        self.fetch_done = False
        self.data: List[PriceCandleProtocol] = []
        self.start_time = start_time

    def get_db_start_search(self) -> int:
        return aktime.get_msec_before_day(
            CandleLimitDays.get_limit_days(self.interval_type), self.start_time)

    def get_interval_type(self) -> str:
        return self.interval_type

    def get_data(self, interval: int) -> list:
        return grouping.get_candle(
            self.data, self.interval_type, interval)

    def get_data_until_now(self, _: int) -> List[PriceCandleProtocol]:
        return self.data

    def _create_new_candle(self, stream: PriceStreamProtocol, is_same_type=True) -> PriceCandleProtocol:
        if is_same_type:
            start_time = stream.event_time
        else:
            start_time = aktime.get_start_time(stream.event_time, 'm', 'KRX')
        end_time = aktime.get_start_time(stream.event_time, 'm', 'KRX') + aktime.interval_type_to_msec('m') - 1
        return PriceCandleProtocol(
            int(stream.price), int(stream.price), int(stream.price), int(stream.price),
            start_time, end_time,
            int(stream.volume), int(stream.volume) * int(stream.price), stream.time_type, False)

    def add_stream(self, stream: PriceStreamProtocol):
        if len(self.data) == 0:
            self.data.append(self._create_new_candle(stream))
        else:
            last_candle = self.data[-1]
            last_end_time = last_candle.end_time
            last_start_time = last_candle.start_time
            if last_end_time < stream.event_time:
                self.data.append(self._create_new_candle(stream))
            elif last_candle.time_type != stream.time_type:
                self.data.append(self._create_new_candle(stream, False))
            elif last_start_time <= stream.event_time:
                last_candle.price_close = int(stream.price)
                if int(stream.price) > int(last_candle.price_high):
                    last_candle.price_high = int(stream.price)
                if int(stream.price) < int(last_candle.price_low):
                    last_candle.price_low = int(stream.price)
                last_candle.add_base_asset_volume(stream.volume)
                last_candle.add_quote_asset_volume(int(stream.price) * int(stream.volume))

    async def fetch(self):
        """
        check when extended time and normal time mixed
        """
        col = self.symbol_info.symbol.lower() + '_1' + self.interval_type
        LOGGER.info('%s', col)
        stored = await self.db.get_data(
            self.db_name, col,
            {'endTime': {'$gte': self.get_db_start_search(), '$lt': self.start_time}}
        )

        for data in stored:
            candle = PriceCandleProtocol.ParseDatabase(data)
            self.data.append(candle)
