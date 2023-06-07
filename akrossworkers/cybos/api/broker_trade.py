import logging

from akrossworkers.cybos.api import com_obj
from akrossworkers.cybos.api.connection import CybosConnection


LOGGER = logging.getLogger(__name__)


def get_broker_trade(code: str, broker_code: str):
    datas = []
    
    prev = {}
    conn = CybosConnection()
    obj = com_obj.get_com_obj('Dscbo1.CpSvr8091')
    obj.SetInputValue(0, ord('4'))
    obj.SetInputValue(1, broker_code)
    obj.SetInputValue(2, code)

    continue_request = True
    while continue_request and len(datas) < 500:
        try:
            conn.wait_until_available()
            obj.BlockRequest()
            count = obj.GetHeaderValue(0)
            """
            0 - (short) 수신 시각
            1 - (string) 회원사명
            2 - (string)  종목 코드
            3 - (string) 종목명
            4 - (char) 매도/매수 구분 '1' 매도, '2' 매수
            5 - (long) 매수/매도량
            6 - (long) 순매수
            7 - (char) 순매수부호('+','-')
            8 - 상태구분(상한, 상승, 보합...)
            9 - (long) 현재가등락율
            10 - (long) 외국계전체누적순매수 
            """
            if count == 0:
                break

            for i in range(count):
                d = {}
                for j in range(11):
                    d[str(j)] = obj.GetDataValue(j, i)

                if '0' in prev and prev['0'] <= d['0']:
                    continue_request = False
                    break
                prev = d
                datas.insert(0, d)

        except Exception as e:
            LOGGER.error('fetch data failed %s, %s', code, str(e))
            break

    return datas


if __name__ == '__main__':
    result = get_broker_trade('A005930', '063')
    buy_total = 0
    sell_total = 0
    for row in result:
        buy_qty = 0 if row['4'] == ord('1') else row['5']
        sell_qty = row['5'] if row['4'] == ord('1') else 0
        print({'time': row['0'],
               'buyQty': buy_qty,
               'sellQty': sell_qty,
               'netQty': row['6']})
        buy_total += buy_qty
        sell_total += sell_qty
    print('total buyQty', buy_total, 'sellQty', sell_total)