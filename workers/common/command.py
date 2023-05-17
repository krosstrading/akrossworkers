

class ApiCommand:
    Candle = 'candle'
    SymbolInfo = 'symbolInfo'
    PriceStream = 'priceStream'
    BacktestStream = 'backtestStream'
    CandleStream = 'candleStream'
    OrderbookStream = 'orderbookStream'
    ProgramStream = 'programStream'
    BrokerStream = 'brokerStream'
    Next = 'next'


class AccountApiCommand:
    AssetList = 'assetList'
    CreateOrder = 'createOrder'
    CancelOrder = 'cancelOrder'
    OpenOrder = 'openOrder'
    AssetEvent = 'assetEvent'
    OrderEvent = 'orderEvent'
