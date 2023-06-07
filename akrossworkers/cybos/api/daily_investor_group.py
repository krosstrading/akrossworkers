import logging
from datetime import datetime

from akrossworkers.cybos.api import com_obj
from akrossworkers.cybos.api.connection import CybosConnection
from akrossworkers.common.protocol import KrxDailyInvestor


LOGGER = logging.getLogger(__name__)


def convert_to_yyyymmdd(dt: datetime):
    return dt.year * 10000 + dt.month * 100 + dt.day


# Caution: Only for 1 year search is available when set date manually
# 당일 조회 가능
def get_daily_investor_group(code):
    datas = []
    try:
        conn = CybosConnection()
        obj = com_obj.get_com_obj('CpSysDib.CpSvr7254')
        # today = akross.msec_to_datetime(akross.get_msec(), 'KRX')
        # from_date = today - timedelta(days=30)
        obj.SetInputValue(0, code)
        obj.SetInputValue(1, 6)  # 6번 일때만 누적이 아닌, 일별 데이터 조회 가능, 대신 날짜 기간 입력 안됨
        # obj.SetInputValue(2, convert_to_yyyymmdd(from_date))
        # obj.SetInputValue(3, convert_to_yyyymmdd(today))
        obj.SetInputValue(4, ord('0'))  # '0': 순매수, '1': 매매비중
        obj.SetInputValue(5, 0)  # 투자자, 0: 전체, 1~13: 주체별
        obj.SetInputValue(6, ord('2'))  # '1': 순매수량, '2': 추정금액
        # now = datetime.now()
        continue_request = True

        prev = {}
        # Block Request 당 17 개 data 전달
        while continue_request:
            if len(datas) >= 50:
                # request limit 로 50개로 제한
                break

            conn.wait_until_available()
            obj.BlockRequest()
            count = obj.GetHeaderValue(1)
            
            if count == 0:
                break
            for i in range(count):
                d = {}
                for j in range(19):
                    d[str(j)] = obj.GetDataValue(j, i)

                if '0' in prev and prev['0'] <= d['0']:
                    continue_request = False
                    break
                # if now - time_converter.intdate_to_datetime(d['0'])> 
                #       timedelta(days=365*5):
                #     continue_request = False
                prev = d
                datas.insert(0, KrxDailyInvestor(
                    str(d['0']),
                    str(d['1']),
                    str(d['2']),
                    str(d['3']),
                    str(d['4']),
                    str(d['5']),
                    str(d['6']),
                    str(d['7']),
                    str(d['8']),
                    str(d['9']),
                    str(d['10']),
                    str(d['11']),
                    str(d['12']),
                    str(d['13']),
                    str(d['17']),
                    str(d['14']),
                    str(d['16'])
                ).to_network())
    except Exception as e:
        LOGGER.error('fetch data failed: %s, %s', code, str(e))

    """
    금액 단위: 100만원
    {'0': 20230203, '1': -42203308, '2': 45309619, '3': -2912018,
     '4': -972617, '5': 379446, '6': 598732, '7': -9621, '8': -46498,
     '9': -2082425, '10': -58057, '11': -136236, '12': -779035, '13': 0,
     '14': 63800, '15': 300, '16': 0.47, '17': 14816489, '18': 49}
    0: 일자, 1: 개인, 2: 외국인, 3: 기관계, 4: 금융투자, 5:보험, 6:투신, 7:은행, 8:기타금융
    9: 연기금, 10: 기타법인, 11:기타외인, 12:사모펀드, 13:국가지자체,
    14: 종가, 15: 대비, 16: 대비율, 17: 거래량, 18 확정치('0': 잠정치, '1': 확정치)
    """
    return datas


if __name__ == '__main__':
    result = get_daily_investor_group('A005930')
    for row in result:
        print(row)
