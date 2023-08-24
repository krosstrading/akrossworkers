import logging
from datetime import datetime, timedelta

from akross.common import aktime
from akrossworker.common.command import ApiCommand
from akrossworker.common.protocol import (
    BrokerTradeEvent,
    OrderbookStreamProtocol,
    ProgramTradeEvent,
    PriceStreamProtocol
)
from akrossworker.common.args_constants import TickTimeType
from akrossworker.cybos.api.subscribe_base import SubscribeBase


LOGGER = logging.getLogger(__name__)


class StockSubscribe(SubscribeBase):
    """
    장전 단일가, 종가 단일가에서는 
    type == '2', flag == '4' 는 들어옴 2023/02/07, '4'는 장후시간외
    장전시간외 flag '3' 은 StockOutCur, flag '4'는 들어오는 것으로 이해
    현재는 '2'가 아니면 TIME_BID_IN_MARKET 으로 처리(VI와 동일)
    """
    def __init__(self, code: str, exchange_name: str, callback):
        super().__init__(TickTimeType.Normal,
                         ApiCommand.PriceStream,
                         code,
                         exchange_name,
                         callback,
                         'DsCbo1.StockCur',
                         [code])

    def eventToData(self, obj):
        # header 14, 19 return as int type
        time_type = obj.GetHeaderValue(19)
        time_flag = obj.GetHeaderValue(20)
        # print('time type', time_type, 'time_flag', time_flag)
        if time_type != ord('2'):
            print('unknown time', time_type, time_flag)

        stream = PriceStreamProtocol.CreatePriceStream(
            self.code,
            aktime.get_msec(),
            obj.GetHeaderValue(13),
            obj.GetHeaderValue(17),
            # return 49, 50(int), '1': buy, '2': sell
            obj.GetHeaderValue(26) == ord('2'),
            (
                TickTimeType.Normal if time_flag == ord('2')
                else TickTimeType.ExtendedCloseBid
            )
        )  # return 49, 50 (int)
        return stream.to_network()


class StockExpectSubscribe(SubscribeBase):
    def __init__(self, code, exchange_name, callback):
        self.prev = (datetime.now(), 0)
        super().__init__(TickTimeType.Normal,
                         ApiCommand.PriceStream,
                         code,
                         exchange_name,
                         callback,
                         'DsCbo1.StockExpectCur',
                         [code])

    def eventToData(self, obj):
        """
        ** 거래량은 예상체결 수량(total)
        8  - (char) 세션 구분코드
        '1': 시가단일가, '2': 장중단일가, '3':종가단일가

        *** int 로 들어옴, not string
        """
        if datetime.now() - self.prev[0] > timedelta(minutes=30):
            # 장전 동시호가 -> 장 중 -> 장후 동시호가 진입시 이전 데이터 reset
            self.prev = (datetime.now(), 0)

        cum_qty = obj.GetHeaderValue(4)
        time_type = obj.GetHeaderValue(8)
        
        if time_type == ord('1'):
            time_type = TickTimeType.PreBid
        elif time_type == ord('2'):
            time_type = TickTimeType.TradingBid
        elif time_type == ord('3'):
            time_type = TickTimeType.MarketCloseBid

        stream = PriceStreamProtocol.CreatePriceStream(
            self.code,
            aktime.get_msec(),
            str(obj.GetHeaderValue(2)),
            str(cum_qty - self.prev[1]),  # 마이너스 qty 상관없음
            True,  # no fields for buy / sell
            time_type)
        self.prev = self.prev = (datetime.now(), cum_qty)
        return stream.to_network()


class StockExtendedSubscribe(SubscribeBase):
    def __init__(self, code, exchange_name, callback):
        super().__init__(TickTimeType.ExtendedTrading,
                         ApiCommand.PriceStream,
                         code,
                         exchange_name,
                         callback,
                         'CpSysDib.StockUniCur',
                         [code])

    def eventToData(self, obj):
        """
         obj.GetHeaderValue(13), obj.GetHeaderValue(19) - int 로 들어옴
        """
        stream = PriceStreamProtocol.CreatePriceStream(
            self.code,
            aktime.get_msec(),
            str(obj.GetHeaderValue(5)),
            # str(cum_sum - self.prev_qty),
            str(obj.GetHeaderValue(18)),
            False if obj.GetHeaderValue(13) == ord('1') else True,
            (
                TickTimeType.ExtendedTrading if obj.GetHeaderValue(19) == ord('2')
                else TickTimeType.ExtendedTradingBid
            )
        )
        return stream.to_network()


class OrderbookSubscribe(SubscribeBase):
    def __init__(self, code, exchange_name, callback):
        super().__init__(TickTimeType.Normal,
                         ApiCommand.OrderbookStream,
                         code,
                         exchange_name,
                         callback,
                         "DsCbo1.StockJpBid",
                         [code])

    def eventToData(self, obj):
        d = {}
        d['total_ask_remain'] = str(obj.GetHeaderValue(23))
        d['total_bid_remain'] = str(obj.GetHeaderValue(24))
        d['bids'] = []
        d['asks'] = []

        for i in range(3, 19+1, 4):
            ask = obj.GetHeaderValue(i)
            bid = obj.GetHeaderValue(i+1)
            if bid > 0:
                d['bids'].append([str(bid), str(obj.GetHeaderValue(i+3))])

            if ask > 0:
                d['asks'].append([str(ask), str(obj.GetHeaderValue(i+2))])

        for i in range(27, 43+1, 4):
            ask = obj.GetHeaderValue(i)
            bid = obj.GetHeaderValue(i+1)
            if bid > 0:
                d['bids'].append([str(bid), str(obj.GetHeaderValue(i+3))])

            if ask > 0:
                d['asks'].append([str(ask), str(obj.GetHeaderValue(i+2))])

        orderbook_stream = OrderbookStreamProtocol.CreateOrderbookStream(
            d['total_bid_remain'],
            d['total_ask_remain'],
            d['bids'],
            d['asks']
        )
        return orderbook_stream.to_network()


class OrderbookExtendedSubscribe(SubscribeBase):
    def __init__(self, code, exchange_name, callback):
        super().__init__(TickTimeType.ExtendedTrading,
                         ApiCommand.OrderbookStream,
                         code,
                         exchange_name,
                         callback,
                         "CpSysDib.StockUniJpBid",
                         [code])

    def eventToData(self, obj):
        d = {}
        d['total_ask_remain'] = str(obj.GetHeaderValue(23))
        d['total_bid_remain'] = str(obj.GetHeaderValue(24))
        d['bids'] = []
        d['asks'] = []
        for i in range(3, 19+1, 4):
            ask = obj.GetHeaderValue(i)
            bid = obj.GetHeaderValue(i+1)
            if bid > 0:
                d['bids'].append([str(bid), str(obj.GetHeaderValue(i+3))])

            if ask > 0:
                d['asks'].append([str(ask), str(obj.GetHeaderValue(i+2))])

        orderbook_stream = OrderbookStreamProtocol.CreateOrderbookStream(
            d['total_bid_remain'],
            d['total_ask_remain'],
            d['bids'],
            d['asks'],
            TickTimeType.ExtendedTrading
        )
        return orderbook_stream.to_network()


class BrokerTradeSubscribe(SubscribeBase):
    """
        [주의] 100주 이상 변화시에만 데이터가 수신됩니다.
        코드에 * 를 넣는 경우 전체 코드
        {'0': 1406,
        '1': '이베스트',
        '2': 'A005930',
        '3': '삼성전자',
        '4': 50,
        '5': 9857,
        '6': -555896,
        '7': 45,
        '8': 3061741}

        0 - (short) 수신 시각
        1 - (string) 회원사명
        2 - (string)  종목 코드
        3 - (string) 종목명
        4 - (char) 매도/매수 구분 '1' 매도, '2' 매수
        5 - (long) 매수/매도량
        6 - (long) 순매수
        7 - (char) 순매수부호('+','-')
        8 - (long) 외국계순매매
    """
    def __init__(self, code, exchange_name, callback):
        super().__init__(
            TickTimeType.Normal,
            ApiCommand.BrokerStream,
            code,
            exchange_name,
            callback,
            "DsCbo1.CpSvr8091S",
            ['*', code]
        )
        self._prev_data = None

    def eventToData(self, obj):
        d = {}
        for i in range(9):
            d[str(i)] = obj.GetHeaderValue(i)

        if self._prev_data is not None:
            # prevent sending duplicated data
            if (self._prev_data['2'] == d['2'] and
                self._prev_data['1'] == d['1'] and
                    self._prev_data['6'] == d['6']):
                return None

        self._prev_data = d
        return BrokerTradeEvent(d['2'],
                                aktime.get_msec(),
                                d['1'],
                                'buy' if d['4'] == ord('2') else 'sell',
                                str(d['5']),
                                str(d['6']),
                                str(d['8'])).to_network()


class ProgramSubscribe(SubscribeBase):
    """
    0 - (string) 종목코드
    1 - (ulong) 시간
    2 - (ulong) 현재가
    3 - (char) 대비부호 ('1' 상한, '2' 상승, '3' 보합, '4' 하한, '5' 하락)
    4 - (long) 전일대비
    5 - (float) 현재가 등락률
    6 - (ulong) 거래량(금일 거래량)
    7 - (ulong) 프로그램 매수 수량
    8 - (ulong) 프로그램 매도 수량
    9 - (long)  프로그램 순매수 수량
    10 - (ulong) 프로그램 매수 금액(단위:천원)
    11 - (ulong) 프로그램 매도 금액(단위:천원)
    12 - (long) 프로그램 순매수 금액(단위:천원)

    # noqa: E501
    {'0': 'A005930', '1': 121417, '2': 63600, '3': 50, '4': 700, '5': 1.1100000143051147, '6': 4691746, '7': 1422637, '8': 1106590, '9': 316047, '10': 90556588, '11': 70416664, '12': 20139924}
    {'0': 'A005930', '1': 121419, '2': 63500, '3': 50, '4': 600, '5': 0.949999988079071, '6': 4691750, '7': 1422637, '8': 1106594, '9': 316043, '10': 90556588, '11': 70416918, '12': 20139670}
    {'0': 'A005930', '1': 121422, '2': 63600, '3': 50, '4': 700, '5': 1.1100000143051147, '6': 4691815, '7': 1422652, '8': 1106609, '9': 316043, '10': 90557542, '11': 70417872, '12': 20139670}
    {'0': 'A005930', '1': 121423, '2': 63600, '3': 50, '4': 700, '5': 1.1100000143051147, '6': 4691915, '7': 1422652, '8': 1106707, '9': 315945, '10': 90557542, '11': 70424104, '12': 20133438}
    {'0': 'A005930', '1': 121425, '2': 63600, '3': 50, '4': 700, '5': 1.1100000143051147, '6': 4691969, '7': 1422652, '8': 1106722, '9': 315930, '10': 90557542, '11': 70425058, '12': 20132484}
    {'0': 'A005930', '1': 121426, '2': 63600, '3': 50, '4': 700, '5': 1.1100000143051147, '6': 4691973, '7': 1422652, '8': 1106726, '9': 315926, '10': 90557542, '11': 70425313, '12': 20132229}
    {'0': 'A005930', '1': 121427, '2': 63500, '3': 50, '4': 600, '5': 0.949999988079071, '6': 4692954, '7': 1422667, '8': 1107707, '9': 314960, '10': 90558496, '11': 70487608, '12': 20070888}
    {'0': 'A005930', '1': 121430, '2': 63600, '3': 50, '4': 700, '5': 1.1100000143051147, '6': 4692956, '7': 1422667, '8': 1107708, '9': 314959, '10': 90558496, '11': 70487671, '12': 20070825}
    """
    def __init__(self, code, exchange_name, callback):
        super().__init__(
            TickTimeType.Normal,
            ApiCommand.ProgramStream,
            code,
            exchange_name,
            callback,
            "CpSysDib.CpSvr8119S",
            [code]
        )

    def eventToData(self, obj):
        d = {}
        for i in range(13):
            d[str(i)] = obj.GetHeaderValue(i)
        return ProgramTradeEvent(
            d['0'],
            aktime.get_msec(),
            str(d['6']),
            str(d['7']),
            str(d['8']),
            str(d['9']),
            str(d['11']),
            str(d['12'])
        ).to_network()


class IndexSubscribe(SubscribeBase):
    def __init__(self, code, exchange_name, callback):
        super().__init__(TickTimeType.Normal,
                         ApiCommand.PriceStream,
                         code,
                         exchange_name,
                         callback,
                         "DsCbo1.StockIndexIS",
                         [code])

    def eventToData(self, obj):
        stream = PriceStreamProtocol.CreatePriceStream(
            self.code,
            aktime.get_msec(),
            str(obj.GetHeaderValue(2)),
            # str(cum_sum - self.prev_qty),
            str(obj.GetHeaderValue(5) * 1000000),  # 백만단위, Index는 Volume 에 거래대금
            True,
            TickTimeType.Normal
        )
        return stream.to_network()
