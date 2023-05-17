import logging

from workers.cybos.api import com_obj
from workers.cybos.api.connection import CybosConnection
from workers.common.protocol import KrxDailyProgram


LOGGER = logging.getLogger(__name__)


def get_daily_program_trade(code):
    datas = []
    try:
        conn = CybosConnection()
        obj = com_obj.get_com_obj('DsCbo1.CpSvrNew8119Day')
        # '0': 최근 5일, '1': 한달, '2': 3개월, '3': 6개월
        obj.SetInputValue(0, ord('1'))
        obj.SetInputValue(1, code)
        conn.wait_until_available()
        obj.BlockRequest()
        count = obj.GetHeaderValue(0)
        # print('count', count)
        """
        0 - (long) 일자
        1 - (long)  현재가
        2 - (long) 전일대비
        3 - (double) 대비율
        4 - (long) 거래량
        5 - (long)  매도량
        6 - (long) 매수량
        7 - (long)  순매수 증감 수량 (6-5)
        8 - (long)  순매수 누적수량   
        9- (long) 매도 금액(단위:만원)
        10 - (long) 매수 금액(단위:만원)
        11 - (long)  순매수 증감 금액(단위:만원)
        12 - (long) 순매수 누적 금액(단위:만원)
        {'0': 20230203,
         '1': 63800,
         '2': 300,
         '3': 0.47,
         '4': 15194598,
         '5': 2736730,
         '6': 6119160,
         '7': 3382430,
         '8': 23154304,
         '9': 17431317,
         '10': 38970623,
         '11': 21539306,
         '12': 144274454}
        """
        prev = None
        for i in range(count):
            d = {}
            
            for j in range(13):
                d[str(j)] = obj.GetDataValue(j, i)
            ratio = (d['5'] + d['6']) / d['4'] * 100 if d['4'] != 0 else 0
            # 2023/02/12: 10일 데이터 중복확인(2개) 처리
            # pylint: disable=unsubscriptable-object
            if prev is not None and prev['yyyymmdd'] == str(d['0']):
                datas.pop(0)

            program_format = KrxDailyProgram(
                    str(d['0']),
                    str(d['4']),
                    str(d['5']),
                    str(d['6']),
                    str(int(d['9'] / 100)),
                    str(int(d['10'] / 100)),
                    str(f'{ratio:.1f}')
            ).to_network()
            datas.insert(0, program_format)
            prev = program_format
    except Exception as e:
        LOGGER.error('fetch data failed %s, %s', code, str(e))

    return datas


if __name__ == '__main__':
    result = get_daily_program_trade('A005930')
    for row in result:
        print(row)