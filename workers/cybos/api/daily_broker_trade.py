import logging

from workers.cybos.api import com_obj
from workers.cybos.api.connection import CybosConnection
from workers.common.protocol import KrxDailyBroker


LOGGER = logging.getLogger(__name__)


def get_daily_broker_trade(code: str, broker_code: str):
    """
    해당 code, broker_code 일자별 종목 매매동향
    당일 데이터 출력 안됨(전날까지)
    """
    datas = []
    try:
        conn = CybosConnection()
        obj = com_obj.get_com_obj('Dscbo1.CpSvr8412')
        obj.SetInputValue(0, code)
        obj.SetInputValue(1, broker_code)
        
        conn.wait_until_available()
        obj.BlockRequest()
        count = obj.GetHeaderValue(0)
        """
        0 - (ulong) 일자
        1 - (ulong) 매수수량
        2 - (ulong) 매도수량
        3 - (long) 순매수
        4 - (ulong) 종가
        5 - (long) 전일대비
        6 - (ulong) 거래량
        """
        for i in range(count):
            datas.insert(
                0, KrxDailyBroker(
                    obj.GetDataValue(0, i),
                    obj.GetDataValue(1, i),
                    obj.GetDataValue(2, i)
                ).to_network()
            )
    except Exception as e:
        LOGGER.error('fetch data failed %s, %s', code, str(e))

    return datas


if __name__ == '__main__':
    result = get_daily_broker_trade('A005930', '037')
    for row in result:
        print(row)