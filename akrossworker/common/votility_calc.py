from typing import List
import numpy as np

from akrossworker.common.args_constants import TickTimeType
from akrossworker.common.protocol import (
    PriceCandleProtocol
)
from akross.common import aktime


class VolatilityCalculator:
    def __init__(self, ms: int):
        self.low_highs: List[float] = []
        self.today_start = aktime.get_start_time(ms, 'd', 'KRX')

    def add_complete_candle(self, candle: PriceCandleProtocol):
        if (candle.time_type != TickTimeType.Normal or candle.start_time < self.today_start):
            return
        
        self.low_highs.append(float(candle.price_high))
        self.low_highs.append(float(candle.price_low))
    
    def is_under_mean(self, current_price: int):
        if len(self.low_highs) == 0:
            return False
        high = max(self.low_highs)
        low = min(self.low_highs)
        if high - low > 0:
            return (current_price - low) / (high - low) < 0.5
        return False
    
    def get_coefficient_variation(self) -> float:
        if len(self.low_highs) == 0:
            return 0
        return np.std(self.low_highs) / np.mean(self.low_highs)