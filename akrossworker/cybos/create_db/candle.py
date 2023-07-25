from datetime import datetime
import logging
import sys
from typing import List
from PyQt5.QtCore import QCoreApplication
from akrossworker.cybos.api import stock_chart, stock_code
from urllib.parse import quote_plus
from pymongo import MongoClient
from akross.common import aktime
from akrossworker.common.args_constants import TradingStatus
from akrossworker.common.protocol import SymbolInfo
from akross.common import env
from akrossworker.common.db import DBEnum
from akrossworker.cybos.api.connection import CybosConnection


MONGO_URI = f"mongodb://{quote_plus('akross')}:{quote_plus('Akross@q')}" \
            "@" + env.get_rmq_url()
LOGGER = logging.getLogger(__name__)
MARKET_NAME = 'krx.spot'


def create_symbol_info() -> List[SymbolInfo]:
    type_name = ['kospi', 'kosdaq', 'etf', 'index']
    call_func = [
        stock_code.get_kospi_company_code_list,
        stock_code.get_kosdaq_company_code_list,
        stock_code.get_index_etf_list,
        stock_code.get_index_list
    ]
    result = []
    for i, sector in enumerate(type_name):
        code_list = call_func[i]()
        if sector == 'index':
            min_move = '0.01'
            market_caps = {}
            base_asset_precision = 13
        else:
            min_move = '0'
            market_caps = stock_code.get_marketcaps(code_list)
            base_asset_precision = 0
        
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
                base_asset_precision,
                0,
                ['0', '0', min_move],
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
        if len(sys.argv) > 1:
            if sys.argv[1] == 'force':
                pass
            elif sys.argv[1].lower() != symbol_info.symbol.lower():
                continue

        if symbol_info.status != TradingStatus.Trading:
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
                LOGGER.warning('last data for[%s](%s): %s',
                               interval,
                               symbol_info.symbol,
                               datetime.fromtimestamp(latest_data['endTime'] / 1000))

            query = {
                'startTime': interval_start_time,
                'endTime': now,
            }

            if query['startTime'] >= query['endTime']:
                LOGGER.warning('startTime is later than endTime')
                continue
            
            candles = stock_chart.get_kline(symbol_info.symbol.upper(), interval, **query)
            record_data = []
            for data in candles:
                if data.end_time > now:
                    if 'force' in sys.argv and interval == '1d':
                        pass
                    else:
                        continue
                record_data.append(data.to_database())

            if len(record_data) > 0:
                LOGGER.warning('write to db(%s): len %d, from: %s, until: %s',
                               interval, len(record_data),
                               datetime.fromtimestamp(record_data[0]['startTime'] / 1000),
                               datetime.fromtimestamp(record_data[-1]['endTime'] / 1000))
                db_col.insert_many(record_data)
                LOGGER.warning('write to db done')
        LOGGER.warning('done %s', symbol_info.symbol.upper())


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
        # sys.exit(app.exec_())
    else:
        LOGGER.error('cybos is not connected')
