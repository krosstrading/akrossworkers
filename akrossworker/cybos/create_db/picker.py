from typing import Dict, List, Tuple

from akross.common import aktime
from akrossworker.common.db import DBEnum, Database
from akrossworker.common.db_quote_query import DBQuoteQuery
from akrossworker.common.protocol import PriceCandleProtocol, SymbolInfo
from akrossworker.cybos.create_db.candidate import Candidate, Ranking


class AmountRank:
    def __init__(self):
        self.db = Database()
        self.data: Dict[int, List[Ranking]] = {}

    async def load(self):
        data = await self.db.get_data(DBEnum.KRX_AMOUNT_RANKING_DB, 'rank')
        for row in data:
            if 'time' not in row or 'rank' not in row:
                print('no time or rank column')
                continue
            self.data[row['time']] = Ranking.ParseDatabase(row['rank'])

    async def find_symbol(self, symbol: str) -> List[Tuple[int, int, int]]:
        records: List[tuple[int, int, int]] = []  # (ms, rank, amount)
        for key, value in self.data.items():
            for data in value:
                if data.symbol.lower() == symbol.lower():
                    records.append((key, data.ranking, data.amount))
        return records
    
    async def find_symbol_with_period(
        self,
        symbol: str,
        start_time: int,
        end_time: int
    ) -> List[Tuple[int, int, int]]:
        records: List[Tuple[int, int, int]] = []  # (ms, rank, amount)
        for key, value in self.data.items():
            if start_time <= key <= end_time:
                for data in value:
                    if data.symbol.lower() == symbol.lower():
                        records.append((key, data.ranking, data.amount))
        return records

    async def find_symbol_with_ymd(self, symbol: str, year: int, month: int, day: int) -> Ranking:
        ms = aktime.intdate_to_msec(year * 10000 + month * 100 + day, 'KRX')
        ms += aktime.interval_type_to_msec('d') - 1
        # DB 기록은 해당일 + 1 day - 1 ms 이므로
        if ms in self.data:
            for rank_data in self.data[ms]:
                if rank_data.symbol.lower() == symbol.lower():
                    return rank_data
        return None


class CandleWithRank:
    def __init__(self, symbol_info: SymbolInfo, candle: PriceCandleProtocol, rank: int):
        self.symbol_info = symbol_info
        self.candle = candle
        self.rank = rank


def is_matched(
    rank_candles: List[CandleWithRank],
    profit_percent: int,
    amount_multiplier: int,
    rank_limit: int,
    distance_limit: int
):
    for i in range(distance_limit - 1, len(rank_candles) - 1):
        tcandle = rank_candles[i].candle
        ycandle = rank_candles[i+1].candle
        yclose = int(ycandle.price_close)
        tclose = int(tcandle.price_close)
        yquote = int(ycandle.quote_asset_volume)
        tquote = int(tcandle.quote_asset_volume)
        profit = (tclose / yclose - 1) * 100
        if (
            profit > profit_percent and
            yquote * amount_multiplier < tquote and
            rank_candles[i].rank <= rank_limit
        ):
            return True
    return False


async def past_ranked_list(
    db: DBQuoteQuery,
    symbol_infos: List[SymbolInfo],
    yesterday_ms: int
) -> List[Candidate]:
    """
    어제부터 120 일간, 특정 거래대금 순위, 거래량 증가, 가격 상승 조건으로 리스트 생성
    """
    amount_rank = AmountRank()
    await amount_rank.load()

    candle_ranks: Dict[SymbolInfo, List[CandleWithRank]] = {}
    for symbol_info in symbol_infos:
        if int(symbol_info.market_cap) >= 700000000000:
            continue

        candle_rank: List[CandleWithRank] = []
        arr = await db.get_data(
            symbol_info.symbol.lower(),
            'd',
            yesterday_ms - aktime.interval_type_to_msec('d') * 120,
            yesterday_ms + aktime.interval_type_to_msec('d') - 1,
        )
        for data in reversed(arr):
            candle = PriceCandleProtocol.ParseDatabase(data)

            ranks = await amount_rank.find_symbol_with_period(
                symbol_info.symbol, candle.start_time, candle.end_time)
            if len(ranks) == 1:
                candle_rank.append(CandleWithRank(symbol_info, candle, ranks[0][1]))
        candle_ranks[symbol_info] = candle_rank

    candidates: List[Candidate] = []
    for symbol_info, candlek in candle_ranks.items():
        is_ok = is_matched(candlek, 25, 5, 200, 1)
        if is_ok:
            candidates.append(Candidate(symbol_info, 0, 0))

    return candidates
