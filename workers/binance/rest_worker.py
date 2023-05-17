import asyncio
import logging

from akross.connection.aio.quote_channel import QuoteChannel
from akross.common import enums, util
from akross.rpc.base import RpcBase

from workers.common.db import Database
from workers.common.protocol import (
    OrderbookStreamProtocol,
    PriceCandleProtocol
)
from workers.binance.api.spot_rest import BinanceSpotRest
from workers.binance.api.utils import create_symbol_info
from workers.common.args_constants import ApiArgKey as apikey


LOGGER = logging.getLogger(__name__)


class BinanceRestWorker(RpcBase):
    def __init__(self):
        super().__init__()
        self.candle = self.on_candle
        self.symbolInfo = self.on_symbol_info
        self.orderbook = self.on_orderbook
        self._supported_interval = [
            '1s', '1m', '3m', '5m', '15m', '30m',
            '1h', '2h', '4h', '6h', '8h', '12h',
            '1d', '3d', '1w', '1M'
        ]
        self._db = Database()
        self._spot = BinanceSpotRest()

    async def on_orderbook(self, **kwargs):
        util.check_required_parameters(kwargs, 'symbol')
        # limit to 20 / 20 for orderbook
        LOGGER.warning('on_orderbook %s', kwargs)

        symbol = kwargs['symbol'].upper()
        res = self._spot.depth(symbol)
        bids = res['bids'][:20]
        asks = res['asks'][:20]
        return OrderbookStreamProtocol.CreateOrderbookStream(
            0, 0, bids, asks
        ).to_network()

    async def on_candle(self, **kwargs):
        util.check_required_parameters(kwargs, 'symbol', 'interval')

        LOGGER.warning('on_history %s', kwargs)
        symbol = kwargs['symbol'].upper()
        interval = kwargs['interval']

        if len(interval) < 2 or interval not in self._supported_interval:
            return []  # raise error

        time_args = {}
        if apikey.START_TIME in kwargs and kwargs[apikey.START_TIME] != 0:
            time_args[apikey.START_TIME] = kwargs[apikey.START_TIME]

        if apikey.END_TIME in kwargs and kwargs[apikey.END_TIME] != 0:
            time_args[apikey.END_TIME] = kwargs[apikey.END_TIME]

        # default is 500, max: 1000
        try:
            res = await self._spot.klines(symbol, interval, limit=1000, **time_args)
        except Exception as e:
            LOGGER.error(f'binance service raise error {str(e)}')
            # raise akross.ResponseError(f'binance service raise error {str(e)}')
        protocol_result = []
        # rabbitmq msg cannot exceed 128MB
        res = res[-700000:]

        for data in res:
            adqp = PriceCandleProtocol.CreatePriceCandle(
                data[1], data[2], data[3], data[4],
                data[0], data[6], data[5], data[7]
            )
            protocol_result.append(adqp.to_network())
        LOGGER.warning('on_history done %s', kwargs)

        return protocol_result

    async def on_symbol_info(self, **kwargs):
        result = []
        exchange_info = self._spot.exchange_info()
        if 'symbols' in exchange_info:
            for d in exchange_info['symbols']:
                symbol_info = await create_symbol_info('binance.spot', self._db, d)
                result.append(symbol_info.to_network())

        return result


async def main() -> None:
    LOGGER.warning('run rest worker')
    conn = QuoteChannel('binance.spot')

    rest_provider = BinanceRestWorker()
    await conn.connect()
    await conn.run_with_bus_queue(enums.WorkerType.Online, rest_provider)
    await asyncio.get_running_loop().create_future()


if __name__ == '__main__':
    import logging
    LOG_FORMAT = ('%(levelname) -10s %(asctime)s %(name) -30s %(funcName) '
                  '-35s %(lineno) -5d: %(message)s')
    logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)
    asyncio.run(main())
