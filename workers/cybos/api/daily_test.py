import time
from datetime import datetime
from akross.providers.cybos.cybos_api.daily_credit import (
    get_daily_credit
)
from akross.providers.cybos.cybos_api.daily_investor_group import (
    get_daily_investor_group
)
from akross.providers.cybos.cybos_api.daily_program_trade import (
    get_daily_program_trade
)
from akross.providers.cybos.cybos_api.daily_short_sell import (
    get_daily_short_sell
)


def print_last(title, result):
    if len(result) > 0:
        if 'yyyymmdd' in result[0]:
            print(title, result[0]['yyyymmdd'], result[-1]['yyyymmdd'])
    else:
        print(title, "no data")


if __name__ == '__main__':
    # 테스트 결과
    # investor의 경우 장중(9~15:30)에는 시간별 데이터 920, 1020과 같이, 장 후에는 일별
    # program trade의 경우는 당일 데이터까지 들어옴
    # credit과 short_sell의 경우 장중에는 당일 데이터 안 들어옴
    while True:
        time.sleep(30)
        print(datetime.now())
        result = get_daily_investor_group('A005930')
        print_last('get_daily_investor_group', result)
        time.sleep(1)
        result = get_daily_credit('A005930')
        print_last('get_daily_credit', result)
        time.sleep(1)
        result = get_daily_program_trade('A005930')
        print_last('get_daily_program_trade', result)
        time.sleep(1)
        result = get_daily_short_sell('A005930')
        print_last('get_daily_short_sell', result)
