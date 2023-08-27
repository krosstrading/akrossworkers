from akrossworker.common.args_constants import TickTimeType
from akrossworker.common.protocol import (
    PriceCandleProtocol
)
from akross.common import aktime


class VolatilityCalculator:
    def __init__(self, ms: int):
        self.today_start = aktime.get_start_time(ms, 'd', 'KRX')
        self.count = 0
        self.high = 0
        self.high_index = 0
        self.low = 0
        self.low_index = 0
        self.start_price = 0

    def add_complete_candle(self, candle: PriceCandleProtocol):
        if (candle.time_type != TickTimeType.Normal or candle.start_time < self.today_start):
            return
        low = float(candle.price_low)
        high = float(candle.price_high)
        if self.start_price == 0:
            self.start_price = float(candle.price_open)
        if self.high == 0 or high > self.high:
            self.high = high
            self.high_index = self.count
        if self.low == 0 or low < self.low:
            self.low = low
            self.low_index = self.count
        self.count += 1
    
    def get_triangle_score(
        self,
        yesterday_close: int,
        current: int
    ) -> float:
        if self.count == 0 or self.high == 0 or self.low == 0 or self.start_price == 0:
            return 0
        elif current < yesterday_close:
            return 0
        elif self.high - self.low == 0:
            return 0

        low_to_high = (self.high / self.low - 1) * 100
        if low_to_high < 3:
            return 0
        current_pos = (current - self.low) / (self.high - self.low)

        if self.high_index >= self.low_index:
            if current_pos > 0.8:
                return 0
            return (low_to_high * (self.count - self.high_index)) / 2
        else:  # yesterday close check will guard to catch contiuous plunge
            if current_pos > 0.2:
                return 0
        return (low_to_high * self.low_index) / 2
