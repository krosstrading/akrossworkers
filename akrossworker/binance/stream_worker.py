import asyncio
import logging
from binance_aio import BinanceWsAsync

from akross.connection.aio.quote_channel import QuoteChannel
from akross.common import enums, util
from akrossworker.common.args_constants import TickTimeType
from akrossworker.common.command import ApiCommand
from akrossworker.common.protocol import (
    PriceStreamProtocol
)
from akross.rpc.base import RpcBase
from akross.common import env


LOGGER = logging.getLogger(__name__)


class BinanceStreamWorker(RpcBase):
    def __init__(self):
        super().__init__()
        self._conn = QuoteChannel('binance.spot', env.get_rmq_url())
        self._conn.set_capacity(200)
        self._ws = BinanceWsAsync()
        self.priceStream = self.on_price_stream
        self._streams = {}

    async def run(self):
        await self._conn.connect()
        await self._conn.run_with_bus_queue(enums.WorkerType.Online, self)
        await self._ws.run()

    async def on_price_stream(self, **kwargs):
        util.check_required_parameters(kwargs, 'exchange', 'target')
        LOGGER.info('%s', kwargs)
        exchange_name = kwargs['exchange']
        if exchange_name in self._streams:
            return

        exchange = await self._conn.get_stream_exchange(exchange_name)

        async def msg_callback(msg):
            if exchange_name in self._streams:
                tsp = PriceStreamProtocol.CreatePriceStream(
                    msg['s'], msg['T'], msg['p'], msg['q'], msg['m'], TickTimeType.Normal)
                await self._conn.publish_stream(
                    exchange, ApiCommand.PriceStream, tsp.to_network())

        await self._ws.subscribe_trade(kwargs['target'], msg_callback)
        self._streams[exchange_name] = 1
        self._conn.add_subscribe_count(1)


async def main() -> None:
    LOGGER.warning('run rest provider')
    stream_provider = BinanceStreamWorker()
    await stream_provider.run()
    await asyncio.get_running_loop().create_future()


if __name__ == '__main__':
    import logging
    LOG_FORMAT = ('%(levelname) -10s %(asctime)s %(name) -30s %(funcName) '
                  '-35s %(lineno) -5d: %(message)s')
    logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)
    asyncio.run(main())
