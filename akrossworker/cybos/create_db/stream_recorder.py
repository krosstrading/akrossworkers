import asyncio
import logging
from datetime import datetime
from typing import Dict, List
import sys
from pymongo import MongoClient
from urllib.parse import quote_plus

from akross.connection.aio.quote_channel import QuoteChannel, Market
from akross.common import env
from akrossworker.common.command import ApiCommand
from akross.common import aktime
from akrossworker.common.protocol import (
    PriceStreamProtocol,
    SymbolInfo
)
from akrossworker.common.util import get_symbol_id


LOGGER = logging.getLogger(__name__)
DB_NAME = 'throwback'
MONGO_URI = f"mongodb://{quote_plus(env.get_mongo_user())}:{quote_plus(env.get_mongo_password())}" \
            "@" + env.get_mongo_stream_url()


class StreamWriter:
    Price = 0
    Orderbook = 1
    Program = 2

    def __init__(self, db_collection):
        self.db = db_collection
        self.queue = asyncio.Queue()
        
        asyncio.create_task(self.start_loop())

    async def add_price_stream(self, symbol: str, data):
        self.queue.put_nowait((StreamWriter.Price, symbol.lower(), data))

    async def add_orderbook_stream(self, symbol: str, data):
        self.queue.put_nowait((StreamWriter.Orderbook, symbol.lower(), data))

    async def add_program_stream(self, symbol: str, data):
        self.queue.put_nowait((StreamWriter.Program, symbol.lower(), data))

    async def write_price_streams(self, symbol: str, data):
        LOGGER.debug('write price streams %s(len:%d), queue left: %d',
                     symbol, len(data), self.queue.qsize())
        try:
            self.db['p_' + symbol].insert_many(data)
        except Exception as e:
            LOGGER.warning('write price error %s, %s', symbol, str(e))
            sys.exit(0)
        LOGGER.debug('write price done')

    async def write_orderbook_streams(self, symbol: str, data):
        LOGGER.debug('write orderbook streams %s(len:%d) queue left: %d',
                     symbol, len(data), self.queue.qsize())
        try:
            self.db['o_' + symbol].insert_many(data)
        except Exception as e:
            LOGGER.warning('write orderbook error %s, %s', symbol, str(e))
            sys.exit(0)
        LOGGER.debug('write orderbook done')

    async def write_program_streams(self, symbol: str, data):
        LOGGER.debug('write program streams %s(len:%d) queue left: %d',
                     symbol, len(data), self.queue.qsize())
        try:
            self.db['r_' + symbol].insert_many(data)
        except Exception as e:
            LOGGER.warning('write program error %s, %s', symbol, str(e))
            sys.exit(0)
        LOGGER.debug('write program done')

    async def start_loop(self):
        price_streams: Dict[str, List[int, List]] = {}   # key: symbol, value: (count, streams[])
        orderbook_streams: Dict[str, List[int, List]] = {}
        program_streams: Dict[str, List[int, List]] = {}
        last_process_time = aktime.get_msec()
        while True:
            try:
                await asyncio.sleep(0)  # to prevent occupy process
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
                            price_streams[symbol][1].clear()
                            del price_streams[symbol]
                elif data_type == StreamWriter.Orderbook:
                    if symbol not in orderbook_streams:
                        orderbook_streams[symbol] = [1, [data]]
                    else:
                        orderbook_streams[symbol][0] += 1
                        orderbook_streams[symbol][1].append(data)
                        if orderbook_streams[symbol][0] >= 1000:
                            await self.write_orderbook_streams(symbol, orderbook_streams[symbol][1])
                            orderbook_streams[symbol][1].clear()
                            del orderbook_streams[symbol]
                elif data_type == StreamWriter.Program:
                    if symbol not in program_streams:
                        program_streams[symbol] = [1, [data]]
                    else:
                        program_streams[symbol][0] += 1
                        program_streams[symbol][1].append(data)
                        if program_streams[symbol][0] >= 1000:
                            await self.write_program_streams(symbol, program_streams[symbol][1])
                            program_streams[symbol][1].clear()
                            del program_streams[symbol]
            else:
                if aktime.get_msec() - last_process_time > 1000 * 60 * 10:
                    last_process_time = aktime.get_msec()
                    for symbol, value in price_streams.items():
                        if len(value[1]) > 0:
                            await self.write_price_streams(symbol, value[1])
                            value[1].clear()
                    price_streams.clear()

                    for symbol, value in orderbook_streams.items():
                        if len(value[1]) > 0:
                            await self.write_orderbook_streams(symbol, value[1])
                            value[1].clear()
                    orderbook_streams.clear()

                    for symbol, value in program_streams.items():
                        if len(value[1]) > 0:
                            await self.write_program_streams(symbol, value[1])
                            value[1].clear()
                    program_streams.clear()
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
        if self.symbol[0] == 'a':
            await self.quote.subscribe_stream(
                self.market,
                ApiCommand.OrderbookStream,
                self.orderbook_stream_arrived,
                target=self.symbol
            )
        if self.symbol == 'a086520':
            await self.quote.subscribe_stream(
                self.market,
                ApiCommand.ProgramStream,
                self.program_stream_arrived,
                target=self.symbol
            )

    async def orderbook_stream_arrived(self, msg):
        await self.writer.add_orderbook_stream(self.symbol, msg)

    async def price_stream_arrived(self, msg):
        await self.writer.add_price_stream(self.symbol,
                                           PriceStreamProtocol.ParseNetwork(msg).to_database())

    async def program_stream_arrived(self, msg):
        await self.writer.add_program_stream(self.symbol, msg)


class Collector:
    def __init__(
        self,
        quote: QuoteChannel,
        market: Market,
        candidates: List[SymbolInfo]
    ):
        self.db = MongoClient(MONGO_URI)
        self.quote = quote
        self.market = market
        self.stream_writer = StreamWriter(self.db[DB_NAME])
        self.symbols: Dict[str, SymbolSubscriber] = {}
        for candidate in candidates:
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
