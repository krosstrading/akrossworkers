from workers.cybos.api import com_obj
from workers.cybos.api.connection import CybosConnection
import logging

from workers.cybos.api.stock_code import is_company_stock


LOGGER = logging.getLogger(__name__)


def get_rank_codes():
    datas = []
    try:
        conn = CybosConnection()
        obj = com_obj.get_com_obj('CpSysDib.CpSvr7049')
        # "1": 거래소, "2": 코스닥, "4" 전체
        obj.SetInputValue(0, '4')
        #  "V":거래량 상위, "A":거래대금 상위, "U":상승률 상위, "D":하락률 상위
        obj.SetInputValue(1, 'A')
        obj.SetInputValue(2, 'Y')  # 관리 구분 "Y" or "N"
        obj.SetInputValue(3, 'Y')  # 우선주 구분 "Y" or "N"
        conn.wait_until_available()
        obj.BlockRequest()
        count = obj.GetHeaderValue(0)
        # print('count', count)
        """
        0 - (ulong) 일자
        1 - (long) 종가
        2 - (double) 전일대비
        3 - (long) 대비율
        4 - (long) 거래량
        5 - (long) 공매도량
        6 - (long) 대차
        7 - (long) 상환
        8 - (long) 대차잔고증감
        9 - (long) 대차잔고주수
        10 - (long) 대차잔고금액 (백만원 단위)
        """
        for i in range(count):
            d = {}

            for j in range(8):
                d[str(j)] = obj.GetDataValue(j, i)

            if is_company_stock(d['1']):
                datas.append(d['1'])
    except Exception as e:
        LOGGER.error('fetch data failed %s', str(e))

    return datas


if __name__ == '__main__':
    result = get_rank_codes()
    for row in result:
        print(row)
