from typing import Dict, List, Union

from akross.connection.aio.quote_channel import QuoteChannel, Market
from akross.common import aktime
from akrossworker.common.unit_candle import UnitCandle
from akrossworker.common.db import Database
from akrossworker.common.protocol import SymbolInfo
from akrossworker.common.command import ApiCommand
from akrossworker.common.args_constants import ApiArgKey as Args


class CandleCache:
    def __init__(
        self,
        db: Database,
        db_name: str,
        conn: QuoteChannel,
        market: Market,
        symbol_info: SymbolInfo
    ):
        self.conn = conn
        self.market = market
        self.symbol_info = symbol_info
        self.default_intervals = ['m', 'h', 'd', 'w', 'M']
        self.candles: Dict[str, UnitCandle] = {}
        self.create_candles(db, db_name)

    def create_candles(self, db, db_name):
        for interval_type in self.default_intervals:
            self.candles[interval_type] = UnitCandle(
                db, db_name, self.conn, self.market,
                self.symbol_info, interval_type
            )

    def get_data(self, interval: str):
        interval, interval_type = aktime.interval_dissect(interval)

        if interval_type in self.candles:
            return self.candles[interval_type].get_candle(interval)
        return []

    def get_symbol_info(self):
        return self.symbol_info

    def symbol_matched(self, **kwargs):
        if Args.KEYWORD in kwargs and Args.SECTORS in kwargs:
            return (self._is_sector_matched(kwargs[Args.SECTORS]) and
                    self._name_matched(kwargs[Args.KEYWORD]))
        elif Args.KEYWORD in kwargs:
            return self._name_matched(kwargs[Args.KEYWORD])
        elif Args.SECTORS in kwargs:
            return self._is_sector_matched(kwargs[Args.SECTORS])
        return False

    def _is_sector_matched(self, sectors: Union[List[str], str]):
        sectors = sectors if isinstance(sectors, list) else [sectors]
        if set(sectors).issubset(self.symbol_info.sectors):
            return True
        return False

    def _name_matched(self, keyword: str):
        keyword = keyword.lower()
        if keyword in self.symbol_info.symbol.lower():
            return True
        if keyword in self.symbol_info.desc.lower():
            return True
        return False

    async def on_price_stream(self, msg):
        for candle in self.candles.values():
            candle.update_stream_data(msg)

    async def run(self):
        # read from database
        for candle in self.candles.values():
            await candle.fetch()

        await self.conn.subscribe_stream(
            self.market,
            ApiCommand.PriceStream,
            self.on_price_stream,
            target=self.symbol_info.symbol
        )
