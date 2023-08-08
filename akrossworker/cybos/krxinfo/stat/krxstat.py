import asyncio
import logging
from akross.connection.aio.quote_channel import QuoteChannel, Market
from akrossworker.common.protocol import SymbolInfo
from akrossworker.cybos.krxinfo.common import KrxTaskEnum, stat_to_command

from akrossworker.cybos.krxinfo.task_database import (
    DailyStatDatabase,
    TaskDatabase
)


LOGGER = logging.getLogger(__name__)


class KrxInfo:
    def __init__(
        self,
        symbol_info: SymbolInfo,
        quote: QuoteChannel,
        market: Market,
        db: TaskDatabase,
        stat_db: DailyStatDatabase,
    ):
        self.symbol_info = symbol_info
        self.quote = quote
        self.market = market
        self.db = db
        self.stat_db = stat_db


class KrxStat:
    def __init__(self, stat_name: str, info: KrxInfo):
        self.info = info
        self.stat_name = stat_name
        self.data = {}

    async def preload(self):
        data = await self.info.stat_db.get_data(self.stat_name,
                                                self.get_symbol_name())
        if data and len(data) > 0:
            for row in data:
                self.data[row['yyyymmdd']] = row

    async def _upsert(self, server_row):
        date = server_row['yyyymmdd']

        if date in self.data:
            if server_row != self.data[date]:
                LOGGER.debug('%s exist in data but diff, server :%s, db: %s',
                             self.get_symbol_name(),
                             server_row, self.data[date])
                self.data[date] = server_row
                copy_data = server_row.copy()
                copy_data['symbol'] = self.get_symbol_name()
                await self.info.stat_db.upsert_data(self.stat_name, copy_data)
        else:
            self.data[date] = server_row
            copy_data = server_row.copy()
            copy_data['symbol'] = self.get_symbol_name()
            await self.info.stat_db.insert_data(self.stat_name, copy_data)

    def report(self):
        data = list(self.data.values())
        formatted = []
        for row in data:
            d = {}
            for k, v in row.items():
                key_name = k if k == 'yyyymmdd' else self.stat_name + '_' + k
                d[key_name] = v
            formatted.append(d)
        return formatted

    def clear_market_data(self):
        pass

    def get_symbol_name(self) -> str:
        return self.info.symbol_info.symbol.lower()

    async def start_daily_task(self, intdate: str, force, **kwargs):
        done_task = await self.info.db.has_task_log(self.stat_name, intdate,
                                                    self.get_symbol_name())
        if force or not done_task:
            await self.do_daily_task(**kwargs)
            await self.info.db.update_task_log(self.stat_name,
                                               intdate,
                                               self.get_symbol_name())

    async def do_market_time_task(self):
        pass

    async def do_daily_task(self, **kwargs):
        self.clear_market_data()
        cmd = stat_to_command(self.stat_name)
        _, payload = await self.info.quote.api_call(
            self.info.market, cmd, symbol=self.get_symbol_name(), cache=False)

        if isinstance(payload, list):
            for row in payload:
                if len(row['yyyymmdd']) != 8:
                    continue  # time stat
                await self._upsert(row)


async def test_run():
    import logging
    LOG_FORMAT = ('%(levelname) -10s %(asctime)s %(name) -30s %(funcName) '
                  '-35s %(lineno) -5d: %(message)s')
    logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)
    logging.getLogger('akross').setLevel(logging.DEBUG)
    quote = QuoteChannel('krx.spot')
    await quote.connect()
    await quote.market_discovery()
    await quote.wait_for_market('krx.spot')
    market = quote.get_markets('krx.spot')[0]
    
    krx_info = KrxInfo({'symbol': 'A005930'},
                       quote,
                       market,
                       TaskDatabase(),
                       DailyStatDatabase())
    # krx_stat = KrxStat(KrxTaskEnum.INVESTOR_STAT, krx_info)
    # await krx_stat.preload()
    # await krx_stat.start_daily_task('20230212', True)
    # print(krx_stat.report()[-1])
    # krx_stat = KrxStat(KrxTaskEnum.PROGRAM_TRADE, krx_info)
    # await krx_stat.preload()
    # await krx_stat.start_daily_task('20230212', True)
    # print(krx_stat.report()[-1])
    # krx_stat = KrxStat(KrxTaskEnum.CREDIT, krx_info)
    # await krx_stat.preload()
    # await krx_stat.start_daily_task('20230212', True)
    # print(krx_stat.report()[-1])
    krx_stat = KrxStat(KrxTaskEnum.SHORT_SELL, krx_info)
    await krx_stat.preload()
    await krx_stat.start_daily_task('20230212', True)
    print(krx_stat.report()[-1])


if __name__ == '__main__':
    asyncio.run(test_run())
