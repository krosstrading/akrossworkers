import logging
from typing import List
from urllib.parse import quote_plus
from PyQt5.QtCore import QCoreApplication
from pymongo import MongoClient
from datetime import datetime

from akross.common import aktime
from akross.common import env

from akrossworker.common.db import DBEnum
from akrossworker.common.protocol import PriceCandleProtocol, SymbolInfo
from akrossworker.cybos.create_db.candle import create_symbol_info
from akrossworker.cybos.api.connection import CybosConnection


LOGGER = logging.getLogger(__name__)
MONGO_URI = f"mongodb://{quote_plus('akross')}:{quote_plus('Akross@q')}" \
            "@" + env.get_rmq_url()


class Ranking:
    def __init__(self, symbol_info: SymbolInfo, candle: PriceCandleProtocol):
        self.symbol = symbol_info.symbol.lower() if symbol_info is not None else ''
        self.desc = symbol_info.desc if symbol_info is not None else ''
        self.amount = int(candle.quote_asset_volume) if candle is not None else 0
        self.ranking = 0

    def set_ranking(self, rank: int):
        self.ranking = rank

    def __str__(self):
        return str(self.to_database())

    def to_database(self):
        return {'symbol': self.symbol,
                'desc': self.desc,
                'amount': self.amount,
                'ranking': self.ranking}
    
    @classmethod
    def ParseDatabase(cls, arr: list) -> list:
        result = []

        for data in arr:
            ranking = Ranking(None, None)
            ranking.symbol = data['symbol']
            ranking.desc = data['desc']
            ranking.amount = data['amount']
            ranking.ranking = data['ranking']
            result.append(ranking)
        return result


def main():
    krx_symbols = create_symbol_info(False)
    client = MongoClient(MONGO_URI)
    rank_db = client[DBEnum.KRX_AMOUNT_RANKING_DB]
    quote_db = client[DBEnum.KRX_QUOTE_DB]
    rank_db_col = rank_db['rank']

    today = aktime.get_start_time(aktime.get_msec(), 'd', 'KRX')
    record_start = today - aktime.interval_type_to_msec('d') * 365

    for latest_data in rank_db_col.find().limit(1).sort([('$natural', -1)]):
        record_start = latest_data['time'] + 1
        LOGGER.warning('last data time: %s',
                       datetime.fromtimestamp(latest_data['time'] / 1000))

    while record_start < today:
        if datetime.fromtimestamp(record_start / 1000).weekday() >= 5:
            LOGGER.warning('skip holiday %s', datetime.fromtimestamp(record_start / 1000))
            record_start += aktime.interval_type_to_msec('d')
            continue

        LOGGER.warning('handle %s', datetime.fromtimestamp(record_start / 1000))
        rankings: List[Ranking] = []
        for symbol_info in krx_symbols:
            symbol = symbol_info.symbol.lower()
            day_candle = list(quote_db[symbol + '_1d'].find({
                'startTime': {'$gte': record_start},
                'endTime': {'$lte': record_start + aktime.interval_type_to_msec('d') - 1}
            },  projection={'_id': False}))

            if len(day_candle) == 1:
                rankings.append(Ranking(symbol_info, PriceCandleProtocol.ParseDatabase(day_candle[0])))
            elif len(day_candle) > 1:
                LOGGER.warning('candle over count of 1(%s)', symbol_info.symbol)

        if len(rankings) > 0:
            rankings = sorted(rankings, key=lambda rank: rank.amount, reverse=True)
            for i, rank_data in enumerate(rankings):
                rank_data.set_ranking(i + 1)
            LOGGER.warning('total to be written %d', len(rankings))
            db_data = {
                'time': record_start + aktime.interval_type_to_msec('d') - 1,
                'rank': [rank.to_database() for rank in rankings]
            }
            rank_db_col.insert_one(db_data)
        record_start += aktime.interval_type_to_msec('d')


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
