from datetime import datetime
from akrossworker.common.protocol import SymbolInfo


def get_symbol_id(symbol_info: SymbolInfo) -> str:
    return symbol_info.market.lower() + '.' + symbol_info.symbol.lower()


def get_symbol_from_id(symbol_id: str) -> str:
    if '.' in symbol_id:
        return symbol_id.split('.')[-1].lower()
    return symbol_id


def datetime_str(ms: int) -> str:
    return datetime.fromtimestamp(ms / 1000).strftime("%Y/%m/%d %H:%M:%S")


def date_str(ms: int) -> str:
    return datetime.fromtimestamp(ms / 1000).strftime("%Y/%m/%d")


def file_date_str(ms: int) -> str:
    return datetime.fromtimestamp(ms / 1000).strftime("%Y%m%d")
