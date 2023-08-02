from typing import List

from akross.common import aktime
from akrossworker.common.protocol import PriceCandleProtocol


def _can_grouping(
    interval: int,
    interval_type: str,
    first: PriceCandleProtocol,
    candle: PriceCandleProtocol
):
    if first.time_type != candle.time_type:
        return False

    # 일봉보다 작은 단위는 시간단위 연결 확인
    if interval_type == 'm' or interval_type == 'h':
        end_time = (first.start_time +
                    (aktime.interval_type_to_msec(interval_type) * interval) - 1)
        return candle.end_time <= end_time
    return True


def get_candle(
    candles: List[PriceCandleProtocol],
    interval_type: str,
    interval: int
) -> list:
    if len(candles) == 0 or interval < 1:
        return []

    result = []
    grouped: List[PriceCandleProtocol] = []
    for data in candles:
        if len(grouped) == 0 or _can_grouping(interval, interval_type, grouped[0], data):
            grouped.append(data)
            if len(grouped) % interval == 0:
                result.append(grouped.copy())
                grouped.clear()
        else:
            result.append(grouped.copy())
            grouped.clear()
            grouped.append(data)

    if len(grouped) > 0:
        result.append(grouped.copy())

    arr = []
    for candles in result:
        current = None
        for candle in candles:
            if current is None:
                current = candle
            else:
                current = current.merge(candle)
        if current is not None:
            arr.append(current.to_network())
    return arr
