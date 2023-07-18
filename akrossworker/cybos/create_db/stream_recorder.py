import asyncio
import logging
from datetime import datetime
from typing import Dict, List
from akross.connection.aio.quote_channel import QuoteChannel, Market
from akrossworker.common.command import ApiCommand
import sys

from common.db_quote_query import DBQuoteQuery
from akrossworker.common.db import Database
from common.util import get_symbol_id
from akross.common import aktime
from akrossworker.common.protocol import (
    PriceStreamProtocol,
    SymbolInfo
)


LOGGER = logging.getLogger(__name__)
DB_NAME = 'throwback'


class SymbolSubscriber:
    def __init__(
        self,
        quote: QuoteChannel,
        market: Market,
        db: DBQuoteQuery,
        symbol: str
    ):
        self.db = db
        self.quote = quote
        self.market = market
        self.symbol = symbol.lower()

    async def start_subscribe(self):
        await self.quote.subscribe_stream(
            self.market,
            ApiCommand.PriceStream,
            self.price_stream_arrived,
            target=self.symbol
        )
        await self.quote.subscribe_stream(
            self.market,
            ApiCommand.OrderbookStream,
            self.orderbook_stream_arrived,
            target=self.symbol
        )

    async def orderbook_stream_arrived(self, msg):
        await self.db.insert_one(
            DB_NAME,
            'o_' + self.symbol,
            msg
        )

    async def price_stream_arrived(self, msg):
        await self.db.insert_one(
            DB_NAME,
            'p_' + self.symbol,
            PriceStreamProtocol.ParseNetwork(msg).to_database()
        )


class Collector:
    def __init__(
        self,
        quote: QuoteChannel,
        market: Market,
        db: DBQuoteQuery,
        candidates: List[SymbolInfo]
    ):
        self.quote = quote
        self.market = market
        self.db = db
        self.symbols: Dict[str, SymbolSubscriber] = {}
        for candidate in candidates:
            self.symbols[get_symbol_id(candidate)] = SymbolSubscriber(
                self.quote,
                self.market,
                self.db,
                candidate.symbol
            )

    async def start(self):
        for symbol in self.symbols.values():
            await symbol.start_subscribe()


async def check_exit():
    prev = datetime.now()
    while True:
        now = datetime.now()
        if prev.hour == 4 and now.hour == 5:
            LOGGER.error('turn off cache')
            sys.exit(0)
        prev = datetime.now()
        await asyncio.sleep(60)


async def main():
    today_start = aktime.get_start_time(aktime.get_msec(), 'd', 'KRX')
    if datetime.fromtimestamp(today_start / 1000).weekday() > 4:
        LOGGER.warning('SKIP, today is holiday')
        return
    
    db = Database()
    quote = QuoteChannel('krx.spot')
    await quote.connect()
    await quote.market_discovery()
    await quote.wait_for_market('krx.spot')
    krx_spot = quote.get_markets('krx.spot')[0]
    krx_symbols: List[SymbolInfo] = []
    _, resp = await quote.api_call(krx_spot, ApiCommand.SymbolInfo, cache=False)

    for symbol_info_raw in resp:
        krx_symbols.append(SymbolInfo.CreateSymbolInfo(symbol_info_raw))

    collector = Collector(
        quote,
        krx_spot,
        db,
        krx_symbols
    )
    LOGGER.warning('start stream recorder')
    asyncio.create_task(check_exit())

    await collector.start()
    await asyncio.get_running_loop().create_future()


if __name__ == '__main__':
    import logging
    LOG_FORMAT = ('%(levelname) -10s %(asctime)s %(name) -30s %(funcName) '
                  '-35s %(lineno) -5d: %(message)s')
    logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)
    asyncio.run(main())
