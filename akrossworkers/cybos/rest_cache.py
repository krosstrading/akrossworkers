import asyncio
import logging
import sys
from typing import Dict

from akross.connection.aio.quote_channel import QuoteChannel, Market
from akross.common import enums, util
from akrossworkers.common.candle_cache import CandleCache
from akrossworkers.common.command import ApiCommand
from akrossworkers.common.db import DBEnum, Database
from akrossworkers.common.protocol import (
    SymbolInfo
)
from akross.rpc.base import RpcBase
from akrossworkers.common.args_constants import ApiArgKey as Args
from akrossworkers.cybos.candle_cache import CybosCandleCache
from akross.common import env


LOGGER = logging.getLogger(__name__)
MARKET_NAME = 'krx.spot'


class CybosRestCache(RpcBase):
    def __init__(self):
        super().__init__()
        self.candle = self.on_candle
        self.symbolInfo = self.on_symbol_info
        self.orderbook = self.on_orderbook
        self.search = self.on_search

        self._worker: Market = None
        self._symbols: Dict[str, CandleCache] = {}
        self._conn = QuoteChannel(MARKET_NAME)
        self._db = Database()

    async def preload(self):
        await self._conn.connect()
        await self._conn.market_discovery()
        await self._conn.wait_for_market(MARKET_NAME)
        krx = self._conn.get_markets(MARKET_NAME)
        if krx is None:
            LOGGER.error('cannot find market')
            sys.exit(1)
        self._worker = krx[0]
        await self.regist_symbols()

    async def regist_symbols(self) -> None:
        ret, symbols = await self._conn.api_call(
            self._worker, ApiCommand.SymbolInfo, cache=False)
        if not isinstance(symbols, list) or len(symbols) == 0:
            LOGGER.error('no symbols')
            sys.exit(1)
        
        for i, symbol in enumerate(symbols):
            symbol_info = SymbolInfo.CreateSymbolInfo(symbol)
            LOGGER.info('progress(%s) %d/%d', symbol_info.symbol, i+1, len(symbols))
            if symbol_info.symbol not in self._symbols:
                symbol_name = symbol_info.symbol.lower()
                if symbol_name == 'a247540' or symbol_name == 'a086520':
                    self._symbols[symbol_name] = \
                        CybosCandleCache(
                            self._db, DBEnum.KRX_QUOTE_DB,
                            self._conn, self._worker, symbol_info)
                    await self._symbols[symbol_name].run()

    async def on_orderbook(self, **kwargs):
        util.check_required_parameters(kwargs, 'symbol')
        # limit to 20 / 20 for orderbook
        LOGGER.warning('on_orderbook %s', kwargs)
        return []

    async def on_candle(self, **kwargs):
        util.check_required_parameters(kwargs, 'symbol', 'interval')
        
        interval = kwargs['interval']
        symbol = kwargs['symbol'].lower()
        if symbol in self._symbols:
            return self._symbols[symbol].get_data(interval)
        return []

    async def on_symbol_info(self, **kwargs):
        result = []
        for symbol in self._symbols.values():
            result.append(symbol.get_symbol_info().to_network())
        return result

    async def on_search(self, **kwargs):
        LOGGER.info(
            'keyword(%s), sectors(%s)',
            kwargs[Args.KEYWORD] if Args.KEYWORD in kwargs else 'no keyword',
            kwargs[Args.SECTORS] if Args.SECTORS in kwargs else 'no sectors'
        )
        result = []
        for symbol in self._symbols.values():
            if symbol.symbol_matched(**kwargs):
                result.append(symbol.get_symbol_info().to_network())
        return result


async def main() -> None:
    LOGGER.warning('run rest provider')
    conn = QuoteChannel(MARKET_NAME, env.get_rmq_url())

    rest_provider = CybosRestCache()
    await rest_provider.preload()
    await conn.connect()
    await conn.run_with_bus_queue(enums.WorkerType.Cache, rest_provider)
    await asyncio.get_running_loop().create_future()


if __name__ == '__main__':
    import logging
    LOG_FORMAT = ('%(levelname) -10s %(asctime)s %(name) -30s %(funcName) '
                  '-35s %(lineno) -5d: %(message)s')
    logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)
    asyncio.run(main())
