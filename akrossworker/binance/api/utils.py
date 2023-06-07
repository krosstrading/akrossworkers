from typing import List
from urllib.parse import urlencode
import aiohttp
import logging
from akrossworker.common import args_constants as args
from akrossworker.common.db import DBEnum, Database
from akrossworker.common.protocol import SymbolInfo


LOGGER = logging.getLogger(__name__)


LISTED_COLLECTION_NAME = 'listed'


def _get_filter(exchange,
                filter_type: str,
                keys: List[str],
                default_value: str = '0'):
    result = []
    if 'filters' in exchange:
        filters = exchange['filters']
        for _filter in filters:
            if ('filterType' in _filter and
                    _filter['filterType'] == filter_type):
                for key in keys:
                    result.append(
                        _filter[key] if key in _filter else default_value
                    )
                return result if len(result) > 1 else result[0]

    return [default_value] * len(keys) if len(keys) > 0 else default_value


async def _fetch_from_binance(symbol):
    url = 'https://api.binance.com/api/v1/klines?'
    params = {
        'symbol': symbol.upper(),
        'interval': '1m',
        'startTime': 0,
        'limit': 1
    }
    LOGGER.info('%s', urlencode(params))
    async with aiohttp.ClientSession() as session:
        async with session.get(url + urlencode(params)) as response:
            if response.status == 200:
                payload = await response.json()
                if len(payload) == 1 and isinstance(payload[0], list):
                    return payload[0][0]
                else:
                    LOGGER.error('unknown response type %s', payload)
            else:
                LOGGER.error('failed to get response')

    return 0


async def _get_listed_time(symbol: str, db: Database):
    data = await db.find_one(DBEnum.BINANCE_DB,
                             LISTED_COLLECTION_NAME,
                             {'symbol': symbol.lower()})
    if data is None:
        ms = await _fetch_from_binance(symbol)
        if ms != 0:
            await db.insert_one(
                DBEnum.BINANCE_DB,
                LISTED_COLLECTION_NAME,
                {
                    'symbol': symbol.lower(),
                    'listed': ms
                }
            )
        return ms
    return 0


async def create_symbol_info(market: str, db: Database, d) -> SymbolInfo:
    # convert binance exchange symbol information to SymbolInfo
    listed_time = await _get_listed_time(d['symbol'], db)
    return SymbolInfo(
        market,
        d['symbol'].lower(),
        ['crypto'],
        (args.TradingStatus.Trading if d['status'] == 'TRADING'
            else args.TradingStatus.Stop),
        d['symbol'].upper(),  # DESC
        d['baseAsset'],
        d['quoteAsset'],
        d['baseAssetPrecision'],
        d['quoteAssetPrecision'],
        _get_filter(d, 'PRICE_FILTER', ['minPrice', 'maxPrice', 'tickSize']),
        _get_filter(d, 'LOT_SIZE', ['minQty', 'maxQty', 'stepSize']),
        _get_filter(d, 'MIN_NOTIONAL', ['minNotional']),
        # https://nomics.com/docs/#operation/getCurrenciesTicker
        '0',  # count of base asset issued
        '0',  # market cap
        listed_time,
        'UTC',
        0,  # version
    )