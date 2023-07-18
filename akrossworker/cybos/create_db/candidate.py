from akrossworker.common.protocol import PriceCandleProtocol, SymbolInfo


class Candidate:
    def __init__(
        self,
        symbol_info: SymbolInfo,
        time: int,
        amount: int
    ):
        self.symbol_info = symbol_info
        self.time = time
        self.amount = amount

    @property
    def desc(self):
        return self.symbol_info.desc

    @property
    def symbol(self):
        return self.symbol_info.symbol
    

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