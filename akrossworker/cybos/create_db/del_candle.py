import logging
import sys
from typing import List
from PyQt5.QtCore import QCoreApplication
from akrossworker.cybos.api import stock_code
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
        LOGGER.warning('start %s(%d/%d)',
                       symbol_info.symbol.upper(),
                       progress+1,
                       len(symbol_infos))
        for interval in intervals:
            col = symbol_info.symbol.lower() + '_' + interval
            db = client[DBEnum.KRX_QUOTE_DB]
            db_col = db[col]
            query = {
                'endTime': {'$gte': now}
            }
            result = db_col.delete_many(query)
            # query2 = {
            #     'startTime': {'$gte': datetime(2023, 5, 22).timestamp() * 1000}
            # }
            # result = db_col.delete_many(query2)

            LOGGER.warning('deleted(%s): %d', interval, result.deleted_count)
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
