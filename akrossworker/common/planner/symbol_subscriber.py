from typing import List
from akross.connection.aio.quote_channel import QuoteChannel, Market

from akrossworker.common.command import ApiCommand
from akrossworker.common.planner.order.order_manager import OrderManager
from akrossworker.common.planner.plan_runner import PlanRunner
from akrossworker.common.protocol import OrderbookStreamProtocol, PriceStreamProtocol
from akrossworker.common.util import get_symbol_from_id


class SymbolSubscriber:
    def __init__(
        self,
        quote: QuoteChannel,
        market: Market,
        symbol_id: str,
        order_manager: OrderManager
    ):
        self.quote = quote
        self.market = market
        self.symbol_id = symbol_id
        self.symbol = get_symbol_from_id(self.symbol_id)
        self.order_manager = order_manager
        self.runners: List[PlanRunner] = []
        
    async def start_subscribe(self):
        await self.quote.subscribe_stream(
            self.market,
            ApiCommand.PriceStream,
            self.price_stream_arrived,
            target=self.symbol
        )
        if self.symbol[0] == 'a':
            await self.quote.subscribe_stream(
                self.market,
                ApiCommand.OrderbookStream,
                self.orderbook_stream_arrived,
                target=self.symbol
            )
    
    def add_runner(self, plan_runner: PlanRunner):
        self.runners.append(plan_runner)

    def remove_runner(self, plan_runner: PlanRunner):
        self.runners.remove(plan_runner)

    async def price_stream_arrived(self, msg):
        stream = PriceStreamProtocol.ParseNetwork(msg)
        for runner in self.runners:
            await runner.on_price_stream(stream)

    async def orderbook_stream_arrived(self, msg):
        stream = OrderbookStreamProtocol.ParseNetwork(msg)
        stream.set_target(self.symbol_id)
        self.order_manager.on_orderbook_stream(stream)
        for runner in self.runners:
            await runner.on_orderbook_stream(stream)
