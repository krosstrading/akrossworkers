from akrossworker.cybos.krxinfo.common import KrxTaskEnum
from akrossworker.cybos.krxinfo.stat.investor_stat import InvestorStat
from akrossworker.cybos.krxinfo.stat.krxstat import KrxInfo, KrxStat
from akrossworker.cybos.krxinfo.stat.program_stat import ProgramStat


class KrxStatFactory:
    @classmethod
    def create_instance(cls, stat_name: str, info: KrxInfo):
        if stat_name == KrxTaskEnum.INVESTOR_STAT:
            return InvestorStat(stat_name, info)
        elif stat_name == KrxTaskEnum.PROGRAM_TRADE:
            return ProgramStat(stat_name, info)
        return KrxStat(stat_name, info)
