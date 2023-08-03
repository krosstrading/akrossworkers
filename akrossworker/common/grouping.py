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
    interval: int,
    as_network: bool = True
) -> list:
    if len(candles) == 0 or interval < 1:
        return []

    result = []
    interval_len = aktime.interval_type_to_msec(interval_type) * interval
    grouped: List[PriceCandleProtocol] = []
    for data in candles:
        if len(grouped) == 0 or _can_grouping(interval, interval_type, grouped[0], data):
            if (interval_type == 'm' or interval_type == 'h') and len(grouped) == 0:
                if data.start_time % interval_len != 0:
                    data.adjust_start_time(data.start_time - (data.start_time % interval_len))

            grouped.append(data)
            if len(grouped) % interval == 0:
                result.append(grouped.copy())
                grouped.clear()
        else:
            result.append(grouped.copy())
            grouped.clear()
            if (interval_type == 'm' or interval_type == 'h') and len(grouped) == 0:
                if data.start_time % interval_len != 0:
                    data.adjust_start_time(data.start_time - (data.start_time % interval_len))
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
            if as_network:
                arr.append(current.to_network())
            else:
                arr.append(current)
    return arr
