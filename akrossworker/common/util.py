from datetime import datetime
from akrossworker.common.protocol import SymbolInfo


def get_symbol_id(symbol_info: SymbolInfo):
    return symbol_info.market.lower() + '.' + symbol_info.symbol.lower()


def datetime_str(ms: int) -> str:
    return datetime.fromtimestamp(ms / 1000).strftime("%Y/%m/%d %H:%M:%S")


def date_str(ms: int) -> str:
    return datetime.fromtimestamp(ms / 1000).strftime("%Y/%m/%d")


def file_date_str(ms: int) -> str:
    return datetime.fromtimestamp(ms / 1000).strftime("%Y%m%d")
