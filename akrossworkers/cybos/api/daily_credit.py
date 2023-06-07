import logging

from akrossworkers.cybos.api import com_obj
from akrossworkers.cybos.api.connection import CybosConnection
from akrossworkers.common.protocol import KrxDailyCredit


LOGGER = logging.getLogger(__name__)


def get_daily_credit(code):
    datas = []
    try:
        conn = CybosConnection()
        obj = com_obj.get_com_obj('CpSysDib.CpSvr7151')
        obj.SetInputValue(0, code)  # '0': 최근 5일, '1': 한달, '2': 3개월, '3': 6개월
        obj.SetInputValue(1, ord('y'))  # 'y': 융자, 'd': 대주
        obj.SetInputValue(2, ord('1'))  # '1': 결제일, '2': 매매일
        conn.wait_until_available()
        obj.BlockRequest()
        count = obj.GetHeaderValue(0)
        # print('count', count)
        """
        0 - (long) 일자
        1 - (double) 종가
        2 - (double) 대비
        3 - (double) 등락율
        4 - (long) 거래량
        5 - (long) 신규
        6 - (long) 상환
        7 - (long) 잔고
        8 - (long) 금액 100만원
        9 - (long) 잔고대비
        10 - (long) 공여율 - 전체 거래에서 신용거래 비중
        11 - (double) 잔고율 - 상장주식수에서 신용거래 통해 매수한 주식 비율
        """
        for i in range(count):
            d = {}
            
            for j in range(12):
                d[str(j)] = obj.GetDataValue(j, i)
            
            datas.insert(0, KrxDailyCredit(
                str(d['0']),
                str(d['4']),
                str(d['5']),
                str(d['6']),
                str(d['7']),
                str(d['8']),
                str(d['10']),
                str(d['11'])
            ).to_network())
    except Exception as e:
        LOGGER.error('fetch data failed %s, %s', code, str(e))

    return datas


if __name__ == '__main__':
    result = get_daily_credit('A343510')
    print('code A343510')
    for row in result:
        print(row)