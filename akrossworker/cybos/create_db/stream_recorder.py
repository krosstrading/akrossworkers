import asyncio
import logging
from datetime import datetime
from typing import Dict, List
from akross.connection.aio.quote_channel import QuoteChannel, Market
from akrossworker.common.command import ApiCommand
import sys

from akrossworker.common.db import Database
from akrossworker.common.db_quote_query import DBQuoteQuery
from akross.common import aktime
from akrossworker.common.protocol import (
    PriceStreamProtocol,
    SymbolInfo
)
from akrossworker.common.util import get_symbol_id


LOGGER = logging.getLogger(__name__)
DB_NAME = 'throwback'


class StreamWriter:
    Price = 0
    Orderbook = 1

    def __init__(self, db: DBQuoteQuery):
        self.db = db
        self.queue = asyncio.Queue()
        
        asyncio.create_task(self.start_loop())

    async def add_price_stream(self, symbol: str, data):
        self.queue.put_nowait((StreamWriter.Price, symbol.lower(), data))

    async def add_orderbook_stream(self, symbol: str, data):
        self.queue.put_nowait((StreamWriter.Orderbook, symbol.lower(), data))

    async def write_price_streams(self, symbol: str, data):
        LOGGER.warning('write price streams %s(len:%d), queue left: %d',
                       symbol, len(data), self.queue.qsize)
        try:
            await self.db.insert_many(DB_NAME, 'p_' + symbol, data)
        except Exception as e:
            LOGGER.warning('write price error %s, %s', symbol, str(e))
        LOGGER.warning('write price done')

    async def write_orderbook_streams(self, symbol: str, data):
        LOGGER.warning('write orderbook streams %s(len:%d) queue left: %d',
                       symbol, len(data), self.queue.qsize)
        try:
            await self.db.insert_many(DB_NAME, 'o_' + symbol, data)
        except Exception as e:
            LOGGER.warning('write orderbook error %s, %s', symbol, str(e))
        LOGGER.warning('write orderbook done')

    async def start_loop(self):
        price_streams: Dict[str, List[int, List]] = {}   # key: symbol, value: (count, streams[])
        orderbook_streams: Dict[str, List[int, List]] = {}
        last_process_time = aktime.get_msec()
        while True:
            try:
                item = self.queue.get_nowait()
            except asyncio.QueueEmpty:
                item = None

            if item is not None:
                last_process_time = aktime.get_msec()
                data_type, symbol, data = item
                if data_type == StreamWriter.Price:
                    if symbol not in price_streams:
                        price_streams[symbol] = [1, [data]]
                    else:
                        price_streams[symbol][0] += 1
                        price_streams[symbol][1].append(data)
                        if price_streams[symbol][0] >= 1000:
                            await self.write_price_streams(symbol, price_streams[symbol][1])
                            del price_streams[symbol]
                elif data_type == StreamWriter.Orderbook:
                    if symbol not in orderbook_streams:
                        orderbook_streams[symbol] = [1, [data]]
                    else:
                        orderbook_streams[symbol][0] += 1
                        orderbook_streams[symbol][1].append(data)
                        if orderbook_streams[symbol][0] >= 1000:
                            await self.write_orderbook_streams(symbol, orderbook_streams[symbol][1])
                            del orderbook_streams[symbol]
            else:
                if aktime.get_msec() - last_process_time > 1000 * 60 * 10:
                    last_process_time = aktime.get_msec()
                    for symbol, value in price_streams.items():
                        await self.write_price_streams(symbol, value[1])
                        del price_streams[symbol]

                    for symbol, value in orderbook_streams.items():
                        await self.write_orderbook_streams(symbol, value[1])
                        del orderbook_streams[symbol]
                else:
                    await asyncio.sleep(1)


class SymbolSubscriber:
    def __init__(
        self,
        quote: QuoteChannel,
        market: Market,
        writer: StreamWriter,
        symbol: str
    ):
        self.writer = writer
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
        await self.writer.add_orderbook_stream(self.symbol, msg)

    async def price_stream_arrived(self, msg):
        await self.writer.add_price_stream(self.symbol,
                                           PriceStreamProtocol.ParseNetwork(msg).to_database())


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
        self.stream_writer = StreamWriter(db)
        self.db = db
        self.symbols: Dict[str, SymbolSubscriber] = {}
        for candidate in candidates:
            if candidate.symbol.lower() == 'a005930':
                self.symbols[get_symbol_id(candidate)] = SymbolSubscriber(
                    self.quote,
                    self.market,
                    self.stream_writer,
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
            LOGGER.error('turn off stream recorder')
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
