from workers.cybos.api import com_obj
from workers.cybos.api.connection import CybosConnection
from workers.common.protocol import KrxDailyShortSell
import logging


LOGGER = logging.getLogger(__name__)


def get_daily_short_sell(code):
    datas = []
    try:
        conn = CybosConnection()
        obj = com_obj.get_com_obj('CpSysDib.CpSvr7240')
        obj.SetInputValue(0, code)
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
            
            for j in range(11):
                d[str(j)] = obj.GetDataValue(j, i)
            
            datas.insert(0, KrxDailyShortSell(
                str(d['0']),
                str(d['4']),
                str(d['5']),
                str(d['6']),
                str(d['7']),
                str(d['9']),
                str(d['10'])).to_network())
    except Exception as e:
        LOGGER.error('fetch data failed %s, %s', code, str(e))

    return datas


if __name__ == '__main__':
    result = get_daily_short_sell('A005930')
    for row in result:
        print(row)
