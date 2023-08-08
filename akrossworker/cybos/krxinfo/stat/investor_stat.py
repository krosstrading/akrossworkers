# from akrossworker.cybos.krxinfo.common import (
#     get_today_intdate,
#     stat_to_command
# )
from akrossworker.cybos.krxinfo.stat.krxstat import KrxInfo, KrxStat


class InvestorStat(KrxStat):
    def __init__(self, stat_name: str, info: KrxInfo):
        super().__init__(stat_name, info)
        self.today_data = None

    def report(self):
        table = super().report()
        if self.today_data is not None:
            d = {}
            for k, v in self.today_data.items():
                key_name = k if k == 'yyyymmdd' else self.stat_name + '_' + k
                d[key_name] = v
            table.append(d)
        return table

    def clear_market_data(self):
        self.today_data = None

    async def do_market_time_task(self):
        pass
        # today = get_today_intdate()

        # cmd = stat_to_command(self.stat_name)
        # command, payload = await self.info.quote.api_call(
        #     self.info.market, cmd, symbol=self.get_symbol_name(), cache=False)

        # if command.is_valid() and isinstance(payload, list):
        #     for row in payload:
        #         date = row['yyyymmdd']
        #         if len(date) == 3 or len(date) == 4:
        #             row['yyyymmdd'] = today
        #             row['time'] = date
        #             self.today_data = row
