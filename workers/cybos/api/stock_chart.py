from datetime import datetime, timedelta
import logging
from typing import List

from akross.common import aktime

from workers.common.args_constants import ApiArgKey as apikey
from workers.cybos.api import com_obj
from workers.cybos.api.connection import CybosConnection
from workers.common.protocol import PriceCandleProtocol


LOGGER = logging.getLogger(__name__)
MAX_DATA_COUNT = 2499


def add_month(intdate: int):
    """
    intdate: example) 20220100
    """
    result = intdate + 100
    year = int(result / 10000)

    if result % 10000 > 1201:
        result = (year + 1) * 10000 + 101
    else:
        result += 1
    return result


def decrease_minute(inttime):
    # for 1000 -> 0959: Cybos 는 0901부터 시작하기 때문에 분을 -1 해야됨
    hour = int(inttime / 100)
    min = inttime % 100

    if min == 0:
        return (hour - 1) * 100 + 59
    return hour * 100 + (min - 1)


def yyyymmdd(dt):
    return dt.year * 10000 + dt.month * 100 + dt.day


def cybos_week_interval(intweek: int):
    """
    사이보스에서는 10월 첫번째 주를 20221010 으로 표현(마지막 수는 항상 0)
    첫번째 주 기준은 10월의 첫번째 일요일 기준으로 월요일부터 1주차
    ex) 10월 1주는 10월 6일이 첫번째 일요일인 경우, 10월 7일부터가 1주차
    해당월의 첫번째 월요일 찾고, 해당일에서 +7을 하는 형태
    """
    start_month = int(intweek / 100) * 100 + 1

    week = int((intweek % 100) / 10)
    dt = aktime.intdate_to_datetime(start_month, 'KRX')
    while dt.weekday() != 6:
        dt += timedelta(days=1)
    dt += timedelta(days=1)  # first monday
    dt += timedelta(days=(week - 1) * 7)
    return int(dt.timestamp() * 1000), int((dt + timedelta(days=7)).timestamp() * 1000) - 1


def get_interval(start_date, start_time, period_type):
    if period_type == 'm':
        msec = aktime.intdatetime_to_msec(
            start_date * 1000000 + decrease_minute(start_time) * 100,
            'KRX'
        )
        return msec, msec + aktime.interval_type_to_msec('m') - 1
    elif period_type == 'w':
        return cybos_week_interval(start_date)
    elif period_type == 'd':
        msec = aktime.intdate_to_msec(start_date, 'KRX')
        return msec, msec + aktime.interval_type_to_msec('d') - 1
    elif period_type == 'M':
        msec = aktime.intdate_to_msec(start_date + 1, 'KRX')
        return msec, aktime.intdate_to_msec(add_month(start_date), 'KRX') - 1
        # 6: D - day, W - week, M - month, m - minute, T - tick
    return 0, 0


def convert_period(period_type):
    if period_type == 'd':
        return 'D'
    elif period_type == 'w':
        return 'W'
    return period_type


def get_period_data_raw(
        code,
        period_type,
        startdate,
        enddate
) -> List[PriceCandleProtocol]:
    """
    2499 is count of max result for one query
    """
    data: List[PriceCandleProtocol] = []
    try:
        # LOGGER.info('get_period_data_raw %s(%s) %d %d',
        #             code, period_type, startdate, enddate)
        conn = CybosConnection()
        conn.wait_until_available()

        chart_obj = com_obj.get_com_obj("CpSysDib.StockChart")
        chart_obj.SetInputValue(0, code)

        # 기간으로 요청 '1' 기간, '2' 개수
        # if by_count > 0:
        #     chart_obj.SetInputValue(1, ord('2'))
        # else:
        chart_obj.SetInputValue(1, ord('1'))  # 기간
        chart_obj.SetInputValue(2, enddate)  # 요청종료일
        chart_obj.SetInputValue(3, startdate)  # 요청시작일
        # MAX는 2499개 출력
        chart_obj.SetInputValue(4, 2499)

        data_list = [2, 3, 4, 5, 0, 1, 8, 9]
        # GetDataValue index 가 필드값이 아니라 요청한 column index를 따르므로, order 별도로 추가

        """
        0: 날짜, 1: 시간, 2: 시가, 3: 고가, 4: 저가, 5: 종가, 6: 전일대비, 8: 거래량
        9: 거래대금, 10: 누적체결매도수량(호가비교방식), 11: 누적체결매수수량(호가비교방식)
        12: 상장주식수, 13: 시가총액, 14: 외국인주문한도수량, 15: 외국인주문가능수량
        16: 외국인현보유수량, 17: 외국인현보유비율, 18: 수정주가일자, 19: 수정주가비율(float)
        20: 기관순매수, 21: 기관누적매수, 22: 등락주선, 23: 등락비율, 24:예탁금,
        25:주식회전율, 26: 거래성립율, 37: 대비부호
        """
        chart_obj.SetInputValue(5, data_list)

        # 6: D - day, W - week, M - month, m - minute, T - tick
        chart_obj.SetInputValue(6, ord(convert_period(period_type)))

        # '0': 무수정 주가, '1': 수정 주가
        chart_obj.SetInputValue(9, ord('1'))

        # '1' 시간외거래량 모두 포함, '2' 장종료시간외거래량 만 포함
        # '3' 시간외거래량 모두 제외, '4' 장전시간외거래량만 표시
        chart_obj.SetInputValue(10, ord('1'))

        chart_obj.BlockRequest()

        data_len = chart_obj.GetHeaderValue(3)
        # 분봉의 경우 시간 변경 필요, 1530 제외하고 -1
        # 1시간 연장되는 날은 어떻게 처리?
        # print('count', data_len)

        for i in range(data_len):
            startTime, endTime = get_interval(
                chart_obj.GetDataValue(0, i),
                chart_obj.GetDataValue(1, i),
                period_type
            )
            data.insert(0, PriceCandleProtocol.CreatePriceCandle(
                chart_obj.GetDataValue(2, i),
                chart_obj.GetDataValue(3, i),
                chart_obj.GetDataValue(4, i),
                chart_obj.GetDataValue(5, i),
                startTime,
                endTime,
                chart_obj.GetDataValue(6, i),
                chart_obj.GetDataValue(7, i),
            ))

        if period_type == 'd':
            filtered = []
            date_dict = {}
            for d in data:
                if d.start_time not in date_dict:
                    filtered.append(d)
                    date_dict[d.start_time] = 1
            data = filtered
    except Exception as e:
        import traceback
        print(traceback.format_exc())
        LOGGER.error('stock chart error %s', str(e))

    return data


def get_expected_range(interval_type):
    if interval_type == 'd':
        return timedelta(days=2499)
    elif interval_type == 'w':
        return timedelta(days=3650)  # 10 years
    elif interval_type == 'M':
        return timedelta(days=3650 * 2)  # 20 years

    return timedelta(days=8)


def get_kline_by_count(symbol, interval_type, count):
    today = aktime.msec_to_datetime(aktime.get_msec(), 'KRX')
    expected_range = get_expected_range(interval_type)
    data = get_period_data_raw(symbol,
                               interval_type,
                               yyyymmdd(today - expected_range),
                               yyyymmdd(today))
    response: List[PriceCandleProtocol] = data
    if len(response) == 0:
        return []
    if len(response) > count:
        response = response[-count:]
    else:
        while True:
            if interval_type == 'm':
                end_time = aktime.msec_to_datetime(
                    response[0].start_time - aktime.interval_type_to_msec('d'), 'KRX')
            else:
                end_time = aktime.msec_to_datetime(response[0].start_time - 1, 'KRX')
            data = get_period_data_raw(symbol,
                                       interval_type,
                                       yyyymmdd(end_time - expected_range),
                                       yyyymmdd(end_time))
            response[:0] = data
            if len(data) == 0 or len(response) > count:
                break
        response = response[-count:]
    return response


def get_kline_by_period(symbol, interval_type, start_time, end_time):
    end_dt = aktime.msec_to_datetime(end_time, 'KRX')
    expected_range = get_expected_range(interval_type)
    data = get_period_data_raw(symbol,
                               interval_type,
                               yyyymmdd(end_dt - expected_range),
                               yyyymmdd(end_dt))
    # print('request', yyyymmdd(end_dt - expected_range), yyyymmdd(end_dt))
    empty_count = 0
    response: List[PriceCandleProtocol] = data
    if len(data) == 0:
        return []
    else:
        fetched_start_time = response[0].start_time if len(response) > 0 else 0
        while True:
            if len(response) > 0 and fetched_start_time < start_time:
                break

            if interval_type == 'm':
                end_time = aktime.msec_to_datetime(
                    fetched_start_time - aktime.interval_type_to_msec('d'), 'KRX')
            else:
                end_time = aktime.msec_to_datetime(fetched_start_time - 1, 'KRX')
            data = get_period_data_raw(symbol,
                                       interval_type,
                                       yyyymmdd(end_time - expected_range),
                                       yyyymmdd(end_time))
            # print('request cont',
            #       yyyymmdd(end_time - expected_range),
            #       yyyymmdd(end_time),
            #       'data len', len(data))

            if len(data) == 0:
                if empty_count >= 5:
                    """
                    데이터가 있더라도 cybos 에서 제공하는 기간 넘기는 경우 데이터 없음
                    """
                    break
                else:
                    empty_count += 1
                    fetched_start_time = (end_time - expected_range).timestamp() * 1000
                    continue
            response[:0] = data
            fetched_start_time = response[0].start_time

    filtered = []
    for data in response:
        if data.end_time >= start_time:
            filtered.append(data)
    return filtered


def get_kline(symbol: str, interval_type: str, **kwargs):
    if len(interval_type) > 1:
        interval_type = interval_type[-1]

    start_time = None if apikey.START_TIME not in kwargs else kwargs[apikey.START_TIME]
    end_time = None if apikey.END_TIME not in kwargs else kwargs[apikey.END_TIME]

    if end_time is not None and end_time > aktime.get_msec():
        end_time = aktime.get_msec()  # cannot over future time

    if interval_type == 'h':  # cybos does not support 'h'
        return []  # support 'h' in cache, not here

    if apikey.CANDLE_COUNT in kwargs:
        return get_kline_by_count(symbol, interval_type, kwargs[apikey.CANDLE_COUNT])
    elif start_time is None or end_time is None:
        return get_kline_by_count(symbol, interval_type, MAX_DATA_COUNT)
    return get_kline_by_period(symbol, interval_type, start_time, end_time)


def test_data_order():
    data = get_period_data_raw('A005930', 'm', 20130404, 20230402)
    print(datetime.fromtimestamp(int(data[0].start_time / 1000)),
          datetime.fromtimestamp(int(data[-1].end_time / 1000)))
    # 20230402 기준으로 뒤에서부터 전달


def test_week():
    data = get_period_data_raw('A005930', 'w', 20130404, 20230402)
    print(len(data))
    print(data[-1])
    print(datetime.fromtimestamp(int(data[-1].start_time / 1000)),
          datetime.fromtimestamp(int(data[-1].end_time / 1000)))


def test_count():
    # result = get_kline('A005930', 'm', count=1000)
    # print('1 minutes(1000)', len(result))
    # for data in result:
    #     print(datetime.fromtimestamp(int(data[4]/1000)),
    #           datetime.fromtimestamp(int(data[5]/1000)))
    # result = get_kline('A005930', 'd', count=3000)
    # print('1 day(3000)', len(result))
    result = get_kline('A005930', 'W', count=3000)
    print('1 week(3000)', len(result))
    # 1 month 519
    # 1 minutes 2499
    # 1 day 2499
    # 1 week 2250


def test_period():
    print('test period')
    # result = get_kline('A005930', 'm', startTime=aktime.get_msec(), endTime=aktime.get_msec())
    # print('result', len(result))
    # result = get_kline('A005930', 'd', startTime=aktime.get_msec_before_day(1000),
    #                    endTime=aktime.get_msec())
    
    support_week = get_period_data_raw('A005930', 'w', 20130404, 20230402)
    print(len(support_week))
    # for data in result:
    #     print(datetime.fromtimestamp(int(data[4]/1000)),
    #           datetime.fromtimestamp(int(data[5]/1000)))
    # print('result', len(result))


def test():
    print('run test')
    # 월봉: 시작 일자, 종료 상관없이 1980년부터 전체 출력
    # 월봉 date는 20220100
    # result = get_period_data_raw('A005930', 'M', 0, 20200101)
    # print(len(result))

    # result = get_period_data_raw('A005930', 'M', 0, 20200101, 20230101)

    # count 로는 10개만 출력 OK

    # 분봉은 2499개가 MAX
    # result = get_period_data_raw('A005930', 'm', 10000)
    # print(len(result))

    # 분봉 2000개 정상 출력
    # result = get_period_data_raw('A005930', 'm', 2000)
    # print(len(result))

    # 분봉 기간 정상 출력, Cybos 는 09:00 분봉이 0901 로 표시, 1520 -> 1530 으로 종료
    # result = get_period_data_raw('A005930', 'm', 0, 20220103, 20220110)
    # for res in result:
    #     print(res)
    # print(result[0][4], result[0][5], result[-1][4], result[-1][5])

    # 기간이 긴 경우 최신 데이터부터 2499개까지 출력 20221229
    # result = get_period_data_raw('A005930', 'm', 0, 20221201, 20221231)
    # print(len(result))
    # for res in result:
    #     print(res)

    # 1990년 1월 전체 데이터 출력 없음, 2000년 1월도 없음 ~ 2020년 없고, 2021년 있음
    # result = get_period_data_raw('A005930', 'm', 0, 1990101, 19900131)
    # print(len(result))
    # for res in result:
    #     print(res)
    # result = get_period_data_raw('A005930', 'm', 0, 20210101, 20210131)
    # print(len(result))
    # for res in result:
    #     print(res)

    # 일봉도 2499개가 MAX
    # result = get_period_data_raw('A005930', 'D', 10000)
    # print(len(result))

    # 일봉, 일요일인데 20230113 데이터 출력
    # result = get_period_data_raw('A005930', 'D', 0, 20230114, 20230114)
    # for res in result:
    #     print(res)

    # 일봉 Period 의 경우 
    # 정상 출력 19900101 ~ 19910101
    # result = get_period_data_raw('A005930', 'D', 0, 19900101, 19910101)
    # for res in result:
    #     print(res)

    # 아래는 1991/05/04 ~ 1999/12/28
    # result = get_period_data_raw('A005930', 'D', 0, 19900101, 20000101)
    # print(len(result))

    # 아래는 2012/11/05 ~ 2022/12/29
    # result = get_period_data_raw('A005930', 'D', 0, 19900101, 20230101)
    # for res in result:
    #     print(res)

    # 아래는 데이터 0개, holiday
    # result = get_period_data_raw('A005930', 'D', 0, 19900102, 19900102)
    # for res in result:
    #     print(res)


if __name__ == '__main__':
    import logging
    LOG_FORMAT = ('%(levelname) -10s %(asctime)s %(name) -30s %(funcName) '
                  '-35s %(lineno) -5d: %(message)s')
    logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)
    # test()
    test_week()
    # res = get_kline('A005930', '1d', count=100)
    # print('fetched all', len(res))
    # for candle in res:
    #     print(candle)