

class ApiArgKey:
    CANDLE_COUNT = 'count'
    START_TIME = 'startTime'
    END_TIME = 'endTime'

    SECTORS = 'sectors'
    KEYWORD = 'keyword'


class TradingStatus:
    Trading = 'trading'
    Stop = 'stop'


class TickTimeType:
    """
    단순 장중, 장외로 할 때는 Normal, ExtendedTrading 사용
    """
    Normal = 'n'
    PreCloseBid = 'pcb'         # 장전 전일 종가 동시호가
    PreClose = 'pc'             # 장전 전일 종가 체결
    PreBid = 'pb'               # 장전 동시호가
    TradingBid = 'tb'           # 장중 동시호가
    MarketCloseBid = 'mcb'      # 장 마감 전 동시호가
    ExtendedCloseBid = 'ecb'    # 시간외 종가 동시호가
    ExtendedClose = 'ec'        # 시간외 종가 체결
    ExtendedTradingBid = 'etb'  # 시간외 단일가 동시호가
    ExtendedTrading = 'et'      # 시간외 단일가 체결


class CandleLimitCount:
    MaxCandle = 43200
    Minute = 43200  # 60 * 24 * 30
    Hour = 365 * 24
    Day = 3650
    Week = 520  # 10 years
    Month = 240  # 20 years

    @classmethod
    def get_limit_count(cls, interval_type):
        if interval_type == 'h':
            return CandleLimitCount.Hour
        elif interval_type == 'd':
            return CandleLimitCount.Day
        elif interval_type == 'w':
            return CandleLimitCount.Week
        elif interval_type == 'M':
            return CandleLimitCount.Month
        return CandleLimitCount.Minute


class CandleLimitDays:
    """
    Use this limit when set query as startTime
    """
    Minute = 7  # 1 week
    Hour = 365  # 1 year
    Day = 3650  # 10 years
    Week = 3650  # 10 years
    Month = 7300  # 20 years

    @classmethod
    def get_limit_days(cls, interval_type):
        if interval_type == 'h':
            return CandleLimitDays.Hour
        elif interval_type == 'd':
            return CandleLimitDays.Day
        elif interval_type == 'w':
            return CandleLimitDays.Week
        elif interval_type == 'M':
            return CandleLimitDays.Month
        return CandleLimitDays.Minute


class OrderType:
    OrderLimit = 'LIMIT'


class OrderResultType:
    New = 'NEW'
    Trade = 'TRADE'
    Canceled = 'CANCELED'
    SubTypeNew = 'NEW'
    SubTypePartial = 'PARTIALLY_FILLED'
    SubTypeFilled = 'FILLED'

