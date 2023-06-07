from akrossworker.cybos.api import com_obj
from akrossworker.cybos.api.connection import CybosConnection
import logging

from akrossworker.cybos.api.stock_code import is_company_stock


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
        0 - (short) 순위
        1 - (string) 종목코드
        2 - (string) 종목명
        3 - (long) 현재가
        4 - (long) 전일대비
        5 - (float) 전일대비율
        6 - (long) 거래량
        7 - (long) 거래대금 (만원, 코스닥으로 선택시에는 천원)
        """
        for i in range(count):
            d = {}

            for j in range(8):
                d[str(j)] = obj.GetDataValue(j, i)

            if is_company_stock(d['1']):
                datas.append([d['1'], d['7'] * 10000])
    except Exception as e:
        LOGGER.error('fetch data failed %s', str(e))

    return datas


if __name__ == '__main__':
    result = get_rank_codes()
    for row in result:
        print(row)
