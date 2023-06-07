from akrossworkers.cybos.api import com_obj
from akrossworkers.cybos.api.connection import CybosConnection
import logging


LOGGER = logging.getLogger(__name__)


def get_broker_major_trade(code: str):
    """
    주식종목에대해기관들의매도상위 5, 매수상위 5와거래원의수량
    """
    datas = []
    conn = CybosConnection()
    obj = com_obj.get_com_obj('Dscbo1.StockMember1')
    obj.SetInputValue(0, code)

    try:
        conn.wait_until_available()
        obj.BlockRequest()
        count = obj.GetHeaderValue(1)
        print('time', obj.GetHeaderValue(2), 'price', obj.GetHeaderValue(3))
        """
            GetHeaderValue
            0 - (string) 종목코드
            1 - (short) count
            2 - (long) 시각
            3 - (long) 액면가

            GetDataValue
            0 - (string) 매도거래원
            1 - (string) 매수거래원
            2 - (long) 총매도수량
            3 - (long) 총매수수량
        """
        for i in range(count):
            d = {}
            for j in range(4):
                d[str(j)] = obj.GetDataValue(j, i)

            datas.insert(0, d)

    except Exception as e:
        LOGGER.error('fetch data failed %s, %s', code, str(e))

    return datas


if __name__ == '__main__':
    from akross.providers.cybos.cybos_api import stock_code

    result = get_broker_major_trade('A005930')
    for row in result:
        print(row['0'], stock_code.get_member_name(row['0']), row['2'],
              row['1'], stock_code.get_member_name(row['1']), row['3'])
