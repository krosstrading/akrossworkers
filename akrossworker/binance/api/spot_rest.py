import asyncio
import logging
from binance.spot import Spot as SpotClient
from akrossworker.common.args_constants import ApiArgKey as argkey
from akross.common import aktime


LOGGER = logging.getLogger(__name__)


class BinanceSpotRest:
    def __init__(self):
        self.spot = SpotClient()
    
    def depth(self, symbol):
        return self.spot.depth(symbol)

    def exchange_info(self):
        return self.spot.exchange_info()

    def _get_klines_by_count(self, symbol, interval, count):
        response = []
        if count == 0:
            return response
        data = self.spot.klines(symbol, interval, limit=1000)
        if len(data) < 1000 or len(data) > count:
            response.extend(data)
            response = response[-count:]
        else:
            expected_range = data[-1][0] - data[0][6]
            end_time = data[0][0] - 1
            while True:
                data = self.spot.klines(
                    symbol, interval,
                    startTime=end_time - expected_range,
                    endTime=end_time, limit=1000)
                response[:0] = data
                if len(data) == 0 or len(response) > count:
                    break
                end_time = data[0][0] - 1
            response = response[-count:]
        return response

    async def klines(self, symbol, interval, **kwargs):
        # return recent data when either startTime or endTime is not set
        start_time = None if argkey.START_TIME not in kwargs else kwargs[argkey.START_TIME]
        end_time = None if argkey.END_TIME not in kwargs else kwargs[argkey.END_TIME]

        if end_time is not None and end_time > aktime.get_msec():
            end_time = aktime.get_msec()  # cannot over future time

        response = []
        # old data comes first

        if argkey.CANDLE_COUNT in kwargs:
            return self._get_klines_by_count(symbol, interval, kwargs[argkey.CANDLE_COUNT])
        elif start_time is None or end_time is None:
            return self.spot.klines(symbol, interval, limit=1000)
        else:  # both are set
            start_dt = aktime.msec_to_datetime(start_time, 'UTC')
            end_dt = aktime.msec_to_datetime(end_time, 'UTC')
            LOGGER.info('loop candles start(%d, %s), %s to %s',
                        start_time, symbol, start_dt, end_dt)
            while True:
                data = self.spot.klines(
                    symbol, interval, limit=1000, startTime=start_time, endTime=end_time)
                data_end_time = aktime.get_msec() if len(data) == 0 else data[-1][6]

                response.extend(data)
                if end_time > data_end_time:
                    start_time = data_end_time + 1
                    await asyncio.sleep(0.01)
                else:
                    break
            
            LOGGER.info('loop candles done(%s), data count: %d', symbol, len(response))
        return response
