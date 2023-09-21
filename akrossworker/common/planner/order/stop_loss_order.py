from akrossworker.common.planner.order.order import Order
from akross.connection.aio.account_channel import AccountChannel, Account


class StopLossOrder(Order):
    def __init__(
        self,
        symbol_id: str,
        channel: AccountChannel,
        account: Account,
    ):
        super().__init__(
            symbol_id,
            channel,
            account,
            0, 100)

    async def order(self) -> bool:
        return await self.sell()

    def is_buy(self) -> bool:
        return False
