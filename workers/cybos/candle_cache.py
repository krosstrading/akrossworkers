from akross.connection.aio.quote_channel import QuoteChannel, Market

from workers.common.db import Database
from akross.common import aktime
from workers.common.protocol import SymbolInfo
from workers.common.candle_cache import CandleCache
from workers.common.unit_candle import UnitCandle


class CybosCandleCache(CandleCache):
    def __init__(
        self,
        db: Database,
        db_name: str,
        conn: QuoteChannel,
        market: Market,
        symbol_info: SymbolInfo
    ):
        super().__init__(db, db_name, conn, market, symbol_info)

    def create_candles(self, db, db_name):
        cybos_intervals = ['m', 'd', 'w', 'M']
        for interval_type in cybos_intervals:
            self.candles[interval_type] = UnitCandle(
                db, db_name, self.conn, self.market,
                self.symbol_info, interval_type
            )

    def get_data(self, interval: str):
        interval, interval_type = aktime.interval_dissect(interval)
        if interval_type == 'h':
            interval_type = 'm'
            interval = interval * 60

        if interval_type in self.candles:
            return self.candles[interval_type].get_candle(interval)
        return []
