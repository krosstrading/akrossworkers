from datetime import datetime
import logging
import sys
from typing import List
from PyQt5.QtCore import QCoreApplication
from workers.cybos.api import stock_chart, stock_code
from urllib.parse import quote_plus
from pymongo import MongoClient
from akross.common import aktime
from workers.common.args_constants import TradingStatus
from workers.common.protocol import SymbolInfo
from akross.common import env
from workers.common.db import DBEnum
from workers.cybos.api.connection import CybosConnection


MONGO_URI = f"mongodb://{quote_plus('akross')}:{quote_plus('Akross@q')}" \
            "@" + env.get_rmq_url()
LOGGER = logging.getLogger(__name__)
MARKET_NAME = 'krx.spot'


def create_symbol_info() -> List[SymbolInfo]:
    type_name = ['kospi', 'kosdaq']
    call_func = [
        stock_code.get_kospi_company_code_list,
        stock_code.get_kosdaq_company_code_list
    ]
    result = []
    for i, sector in enumerate(type_name):
        code_list = call_func[i]()
        market_caps = stock_code.get_marketcaps(code_list)
        for code in code_list:
            listed = aktime.intdate_to_msec(
                stock_code.get_stock_listed_date(code), 'KRX')
            symbol_info = SymbolInfo(
                'krx.spot',
                code.lower(),
                ['stock', sector],
                (TradingStatus.Trading
                    if not stock_code.is_stopped(code)
                    else TradingStatus.Stop),
                stock_code.code_to_name(code),
                code,
                'krw',
                0,
                0,
                ['0', '0', '0'],
                ['1', '0', '1'],
                '1',
                market_caps[code][0] if code in market_caps else '0',
                market_caps[code][1] if code in market_caps else '0',
                listed,
                "Asia/Seoul",
                0
            )
            result.append(symbol_info)
    return result


def main() -> None:
    intervals = [
        '1m', '1d', '1w', '1M'
    ]
    symbol_infos = create_symbol_info()
    client = MongoClient(MONGO_URI)
    now = aktime.get_msec()
    for progress, symbol_info in enumerate(symbol_infos):
        if len(sys.argv) > 1 and sys.argv[1].lower() != symbol_info.symbol.lower():
            continue
        elif symbol_info.status != TradingStatus.Trading:
            LOGGER.warning('not trading status %s', symbol_info.symbol.upper())
            continue
        
        start_time = symbol_info.listed
        LOGGER.warning('start %s(%d/%d) listed: %s',
                       symbol_info.symbol.upper(),
                       progress+1,
                       len(symbol_infos),
                       datetime.fromtimestamp(start_time / 1000))
        for interval in intervals:
            interval_start_time = start_time
            col = symbol_info.symbol.lower() + '_' + interval
            db = client[DBEnum.KRX_QUOTE_DB]
            db_col = db[col]
            for latest_data in db_col.find().limit(1).sort([('$natural', -1)]):
                interval_start_time = latest_data['endTime'] + 1

            query = {
                'startTime': interval_start_time,
                'endTime': now,
            }

            candles = stock_chart.get_kline(symbol_info.symbol.upper(), interval, **query)
            record_data = []
            for data in candles:
                record_data.append(data.to_database())

            if len(record_data) > 0:
                LOGGER.warning('write to db(%s): len %d, from: %s, until: %s',
                               interval, len(record_data),
                               datetime.fromtimestamp(record_data[0]['startTime'] / 1000),
                               datetime.fromtimestamp(record_data[-1]['endTime'] / 1000))
                db_col.insert_many(record_data)
                LOGGER.warning('write to db done')
        LOGGER.warning('done %s', symbol_info.symbol.upper())
    app.quit()


if __name__ == '__main__':
    import signal
    LOG_FORMAT = ('%(levelname) -10s %(asctime)s %(name) -30s %(funcName) '
                  '-35s %(lineno) -5d: %(message)s')
    logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)
    app = QCoreApplication([])
    signal.signal(signal.SIGINT, signal.SIG_DFL)

    conn = CybosConnection()
    if conn.is_connected():
        main()
        sys.exit(app.exec_())
    else:
        LOGGER.error('cybos is not connected')
