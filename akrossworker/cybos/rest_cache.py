import asyncio
import logging
import sys
from typing import Dict, List

from akross.connection.aio.quote_channel import QuoteChannel, Market
from akross.common import enums, util
from akrossworker.common.candle_cache import CandleCache
from akrossworker.common.command import ApiCommand
from akrossworker.common.db import DBEnum, Database
from akrossworker.common.protocol import SymbolInfo
from akross.common import aktime
from akross.rpc.base import RpcBase
from akrossworker.common.args_constants import ApiArgKey as Args
from akrossworker.common.args_constants import TickTimeType
from akrossworker.cybos.candle_cache import CybosCandleCache
from akross.common import env
from datetime import datetime


LOGGER = logging.getLogger(__name__)
MARKET_NAME = 'krx.spot'
FAVORITE_COLLECTION = 'favorite'
GROUP_COLLECTION = 'group'


class CybosRestCache(RpcBase):
    def __init__(self):
        super().__init__()
        self.candle = self.on_candle
        self.symbolInfo = self.on_symbol_info
        self.orderbook = self.on_orderbook
        self.search = self.on_search
        self.krxMomentRank = self.on_krx_moment_rank
        self.krxPick = self.on_krx_pick
        self.favorite = self.on_favorite
        self.setFavorite = self.on_set_favorite
        self.group = self.on_group
        self.set_group = self.on_set_group

        self._worker: Market = None
        self._symbols: Dict[str, CandleCache] = {}
        self._symbol_info_cache: Dict[str, SymbolInfo] = {}
        self._conn = QuoteChannel(MARKET_NAME)
        self._db = Database()

    async def preload(self):
        asyncio.create_task(self._check_exit())
        await self._conn.connect()
        await self._conn.market_discovery()
        await self._conn.wait_for_market(MARKET_NAME)
        krx = self._conn.get_markets(MARKET_NAME)
        if krx is None:
            LOGGER.error('cannot find market')
            sys.exit(1)
        self._worker = krx[0]
        await self.regist_symbols()

    async def _check_exit(self):
        prev = datetime.now()
        while True:
            now = datetime.now()
            if prev.hour == 4 and now.hour == 5:
                LOGGER.error('turn off cache')
                sys.exit(0)
            prev = datetime.now()
            await asyncio.sleep(60)

    async def regist_symbols(self) -> None:
        _, symbols = await self._conn.api_call(
            self._worker, ApiCommand.SymbolInfo, cache=False)
        if not isinstance(symbols, list) or len(symbols) == 0:
            LOGGER.error('no symbols')
            sys.exit(1)
        
        for i, symbol in enumerate(symbols):
            symbol_info = SymbolInfo.CreateSymbolInfo(symbol)
            LOGGER.info('progress(%s) %d/%d', symbol_info.symbol, i+1, len(symbols))
            if symbol_info.symbol not in self._symbols:
                symbol_name = symbol_info.symbol.lower()
                self._symbol_info_cache[symbol_name] = symbol_info
                self._symbols[symbol_name] = \
                    CybosCandleCache(
                        self._db, DBEnum.KRX_QUOTE_DB,
                        self._conn, self._worker, symbol_info)
                await self._symbols[symbol_info.symbol.lower()].run()

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

    def get_market_type(self) -> str:
        now = aktime.get_msec()
        hmsec = aktime.interval_type_to_msec('h')
        mmsec = aktime.interval_type_to_msec('m')
        day_start = aktime.get_start_time(now, 'd', 'KRX')
        if now < day_start + aktime.interval_type_to_msec('h') * 9:
            return TickTimeType.PreBid
        elif now >= day_start + hmsec * 15 + mmsec * 20:
            return TickTimeType.MarketCloseBid
        return TickTimeType.Normal

    async def on_krx_moment_rank(self, **kwargs):
        candidates = []
        for symbol, cache in self._symbols.items():
            candles = cache.get_interval_type_data('m')
            market_type = self.get_market_type()
            if (len(candles) > 0 and symbol in self._symbol_info_cache):
                try:
                    amount = cache.get_amount(market_type)
                    market_cap = int(self._symbol_info_cache[symbol].market_cap)
                except Exception:
                    continue

                if market_cap <= 0:
                    continue

                ratio = amount / market_cap
                candidates.append({'symbol_info': self._symbol_info_cache[symbol],
                                   'ratio': ratio})
        candidates.sort(key=lambda candidate: candidate['ratio'], reverse=True)
        candidates = candidates[:15]
        return [candidate['symbol_info'].to_network() for candidate in candidates]

    async def on_krx_pick(self, **kwargs):
        candidates = []
        for symbol, cache in self._symbols.items():
            if symbol in self._symbol_info_cache:
                score = cache.get_triangle_score()
                if score > 0:
                    candidates.append({
                        'symbol_info': self._symbol_info_cache[symbol],
                        'score': score
                    })
        candidates.sort(key=lambda candidate: candidate['score'], reverse=True)
        candidates = candidates[:15]
        return [candidate['symbol_info'].to_network() for candidate in candidates]

    async def on_symbol_info(self, **kwargs):
        result = []
        for symbol in self._symbols.values():
            result.append(symbol.get_symbol_info().to_network())
        return result

    async def on_favorite(self, **kwargs):
        util.check_required_parameters(kwargs, 'user')
        data = await self._db.find_one(
            DBEnum.FAVORITE_DB,
            FAVORITE_COLLECTION,
            {'user': kwargs['user']})
        symbol_infos: List[SymbolInfo] = []
        if data is not None and 'symbols' in data:
            symbol_infos = data['symbols']
        return symbol_infos

    async def on_group(self, **kwargs):
        return []

    async def on_set_group(self, **kwargs):
        return {}

    async def on_set_favorite(self, **kwargs):
        util.check_required_parameters(kwargs, 'symbols', 'user')
        await self._db.upsert_data(
            DBEnum.FAVORITE_DB,
            FAVORITE_COLLECTION,
            {'user': kwargs['user']},
            {'symbols': kwargs['symbols']}
        )
        return {}

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
