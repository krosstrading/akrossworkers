from typing import Dict, List
import asyncio
import logging
from datetime import datetime

from akross.connection.aio.quote_channel import QuoteChannel, Market
from akrossworker.common.command import ApiCommand
from akrossworker.common.protocol import SymbolInfo
from akrossworker.common import args_constants as args
from akrossworker.common.util import get_symbol_id

from akrossworker.cybos.krxinfo.common import (
    KrxTaskEnum,
    get_daily_intdate,
    is_daily_check_time
)
from akrossworker.cybos.krxinfo.stat.krxstat import KrxInfo, KrxStat
from akrossworker.cybos.krxinfo.stat.statfactory import KrxStatFactory
from akrossworker.cybos.krxinfo.task_database import DailyStatDatabase, TaskDatabase


LOGGER = logging.getLogger(__name__)


class KrxSymbol:
    TASKS = [KrxTaskEnum.INVESTOR_STAT,
             KrxTaskEnum.SHORT_SELL,
             KrxTaskEnum.CREDIT,
             KrxTaskEnum.PROGRAM_TRADE]

    def __init__(self, info: KrxInfo):
        self.info = info
        self.tasks: List[KrxStat] = []
        for task_name in KrxSymbol.TASKS:
            self.tasks.append(
                KrxStatFactory.create_instance(task_name, self.info))

    def get_symbol_name(self) -> str:
        return self.info.symbol_info.symbol.lower()

    async def preload(self) -> None:
        LOGGER.info('%s', self.get_symbol_name())
        for task in self.tasks:
            await task.preload()

    async def do_daily_task(self, intdate: str, force, **kwargs):
        LOGGER.info('%s, %s', intdate, self.get_symbol_name())
        for task in self.tasks:
            await task.start_daily_task(intdate, force, **kwargs)

    async def do_market_time_task(self):
        for task in self.tasks:
            await task.do_market_time_task()

    def get_daily_report(self):
        table = {}

        def upsert_row(row):
            row_id = row['yyyymmdd']
            if row_id not in table:
                table[row_id] = row
            else:
                del row['yyyymmdd']
                for k, v in row.items():
                    if k in table[row_id]:
                        LOGGER.warning('column %s is already in table', k)
                    else:
                        table[row_id][k] = v

        for task in self.tasks:
            report = task.report()
            for row in report:
                upsert_row(row)

        rows = list(table.values())
        rows.sort(key=lambda x: int(x['yyyymmdd']))
        return rows


class KrxService:
    CHECK_SYMBOL_INTERVAL = 600  # sec

    def __init__(self):
        self.symbols: Dict[str, KrxSymbol] = {}
        self.initial_done = False
        self.market: Market = None
        self.quote: QuoteChannel = None
        self.db = TaskDatabase()
        self.stat_db = DailyStatDatabase()

    async def connect(self):
        self.quote = QuoteChannel('krx.spot')
        await self.quote.connect()
        await self.quote.market_discovery()
        await self.quote.wait_for_market('krx.spot')
        self.market = self.quote.get_markets('krx.spot')[0]
        asyncio.create_task(self.check_symbols())
        asyncio.create_task(self.do_daily_task())

    async def do_daily_task(self):
        while True:
            # get daily task list from database
            if not self.initial_done:
                await asyncio.sleep(1)
                continue

            target_intdate = get_daily_intdate()
            if is_daily_check_time():
                now = datetime.now()
                LOGGER.info('triggered %s', target_intdate)
                for symbol in self.symbols.values():
                    await symbol.do_daily_task(
                        target_intdate, False)
                    await asyncio.sleep(0.2)
                LOGGER.info('done took(%f)',
                            (datetime.now() - now).total_seconds())
                await self.db.delete_out_date(target_intdate)
            await asyncio.sleep(60 * 10)

    async def check_symbols(self):
        while True:
            _, resp = await self.quote.api_call(
                self.market,
                ApiCommand.SymbolInfo, cache=False)
            krx_symbols: List[SymbolInfo] = []
            for symbol_info_raw in resp:
                krx_symbols.append(SymbolInfo.CreateSymbolInfo(symbol_info_raw))
            
            for symbol_info in krx_symbols:
                if symbol_info.status.lower() != args.TradingStatus.Trading:
                    continue

                symbol_id = get_symbol_id(symbol_info)
                if symbol_id not in self.symbols:
                    info = KrxInfo(symbol_info,
                                   self.quote,
                                   self.market,
                                   self.db,
                                   self.stat_db)
                    self.symbols[symbol_id] = KrxSymbol(info)
                    await self.symbols[symbol_id].preload()

            if not self.initial_done and len(self.symbols) > 0:
                self.initial_done = True
            await asyncio.sleep(KrxService.CHECK_SYMBOL_INTERVAL)


async def main():
    krx_service = KrxService()
    await krx_service.connect()
    await asyncio.get_running_loop().create_future()


if __name__ == '__main__':
    import sys
    LOG_FORMAT = ('%(levelname) -10s %(asctime)s %(name) -30s %(funcName) '
                  '-35s %(lineno) -5d: %(message)s')
    logging.basicConfig(
        level=logging.INFO,
        format=LOG_FORMAT,
        filename='krxinfo.log'
    )
    ch = logging.StreamHandler(sys.stdout)
    ch.setLevel(logging.WARNING)
    ch.setFormatter(logging.Formatter(LOG_FORMAT))
    logging.getLogger().addHandler(ch)
    asyncio.run(main())
