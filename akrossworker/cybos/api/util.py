from datetime import datetime, date, timedelta
import pandas as pd
import pkg_resources

_holidays = []


def read_holiday_excel():
    global _holidays
    files = pkg_resources.resource_listdir('akrossworker', 'data')
    
    for fname in files:
        stream = pkg_resources.resource_stream('akrossworker', 'data/' + fname)
        df = pd.read_excel(stream)
        for d in df.iloc[:, 0:1].iloc[:, 0]:
            date_str = d.split(' ')[0].split('-')
            if len(date_str) > 0:
                _holidays.append(date(int(date_str[0]), int(date_str[1]), int(date_str[2])))


read_holiday_excel()


def is_holiday(d: date):
    if d.weekday() > 4:
        return True
    else:
        if d in _holidays:
            return True
    return False


def _get_yesterday(today):
    today = today if today.__class__.__name__ == 'date' else today.date()

    yesterday = today - timedelta(days=1)
    while is_holiday(yesterday):
        yesterday -= timedelta(days=1)

    return yesterday


def yyyymmdd(dt: datetime):
    return dt.year * 10000 + dt.month * 100 + dt.day


def yyyymmdd_except_holiday(dt: datetime):
    if is_holiday(dt.date()):
        yday = _get_yesterday(dt)
        return yday.year * 10000 + yday.month * 100 + yday.day
    return dt.year * 10000 + dt.month * 100 + dt.day


if __name__ == '__main__':
    print(yyyymmdd_except_holiday(datetime(2023, 6, 4)))