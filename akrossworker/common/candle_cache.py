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

    def get_coefficient_variation(self) -> float:
        min_unit_candle = self.candles['m']
        price = min_unit_candle.get_last_price()
        volatility_calc = min_unit_candle.get_volatility_calc()
        if price > 0 and volatility_calc is not None:
            if volatility_calc.is_under_mean(price):
                return volatility_calc.get_coefficient_variation()
        return 0
    
    def get_drop_ratio(self) -> float:
        day_candles = self.candles['d'].get_raw_candle()
        if len(day_candles) < 2:
            return 0
        above_candle_count = 0
        current_low = int(day_candles[-1].price_low)
        current_price = int(day_candles[-1].price_close)
        high = int(day_candles[-1].price_high)
        for candle in reversed(day_candles[:-1]):
            if int(candle.price_low) < current_low:
                break

            if int(candle.price_high) > high:
                high = int(candle.price_high)
            above_candle_count += 1
        if above_candle_count == 0:
            return 0
        return (high / current_price - 1) * 100 / above_candle_count

    def get_data(self, interval: str):
        interval, interval_type = aktime.interval_dissect(interval)

        if interval_type in self.candles:
            return self.candles[interval_type].get_candle(interval)
        return []

    def get_interval_type_data(self, interval_type: str):
        if interval_type in self.candles:
            return self.candles[interval_type].get_raw_candle()
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
            await candle.update_stream_data(msg)

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
