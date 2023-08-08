from datetime import timedelta
from enum import Enum

from akross.common import aktime
from akrossworker.common.command import ApiCommand


class KrxTaskEnum(str, Enum):
    TASK_LOG = 'Log'
    PERFORMANCE = 'Performance'
    CREDIT = 'Credit'
    SHORT_SELL = 'ShortSell'
    INVESTOR_STAT = 'InvestorStat'
    PROGRAM_TRADE = 'ProgramTrade',
    BROKER = 'Broker'


def stat_to_command(stat_name):
    if stat_name == KrxTaskEnum.INVESTOR_STAT:
        return ApiCommand.DailyInvestorGroup
    elif stat_name == KrxTaskEnum.CREDIT:
        return ApiCommand.DailyCredit
    elif stat_name == KrxTaskEnum.SHORT_SELL:
        return ApiCommand.DailyShortSell
    elif stat_name == KrxTaskEnum.PROGRAM_TRADE:
        return ApiCommand.DailyProgramTrade
    elif stat_name == KrxTaskEnum.BROKER:
        return ApiCommand.DailyBroker
    return ''


def is_daily_check_time():
    dt = aktime.get_datetime_now('KRX')
    if dt.hour >= 21 or dt.hour <= 4:
        return True
    return False


def get_daily_intdate():
    dt = aktime.get_datetime_now('KRX')
    if dt.hour <= 4:
        # 8시 이전은 전일 기준으로 판단
        dt = dt - timedelta(days=1)
    return str(dt.year * 10000 + dt.month * 100 + dt.day)


def get_today_intdate():
    dt = aktime.get_datetime_now('KRX')
    return str(dt.year * 10000 + dt.month * 100 + dt.day)


def is_market_time():
    dt = aktime.get_datetime_now('KRX')
    if dt.weekday() <= 4 and 8 <= dt.hour <= 17:
        return True
    return False
