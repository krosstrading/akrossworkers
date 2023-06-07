import asyncio
import logging
import os

from akross.connection.aio.account_channel import AccountChannel
from binance_aio import BinanceWsUserdata
from akross.rpc.base import RpcBase
from akrossworkers.binance.api.asset_wallet import AssetWallet
from akrossworkers.common.command import AccountApiCommand
from akross.common.exception import CommunicationError

from akrossworkers.common.protocol import HoldAssetList, OrderResponse, OrderTradeEvent
from akross.common import util


LOGGER = logging.getLogger(__name__)
MARKET = 'binance.spot'
EXECUTION_REPORT = 'executionReport'
ASSET_OUTBOUND = 'outboundAccountPosition'


class BinanceAccount(RpcBase):
    def __init__(self):
        super().__init__()
        api_key = os.getenv('BINANCE_API_KEY')
        api_password = os.getenv('BINANCE_API_PASSWORD')
        self._market = MARKET
        self._conn = AccountChannel(self._market)
        self._ws = BinanceWsUserdata(api_key, api_password)
        self._ws.subscribe(self.on_user_message, self)
        self._asset_wallet = AssetWallet(self._market)

    async def run(self):
        await self._conn.connect()
        await self._conn.run(self)
        self._asset_wallet.setup(await self._get_asset_list())
        for asset in self._asset_wallet.calculate_asset_list():
            if (asset.get_asset_name().lower() + 'usdt'
                    in list(self._symbol_info.keys())):
                asset.set_trade_history(
                    await self._get_mytrade(
                        asset.get_asset_name().upper() + 'USDT'
                    )
                )
        await self._ws.run()

    async def on_user_message(self, msg):
        if 'e' in msg:
            event = msg['e']
            if event == EXECUTION_REPORT:
                trade_event = OrderTradeEvent(
                    self._market + '.' + msg['s'],  # symbol
                    msg['S'],  # side (BUY or SELL)
                    msg['E'],  # event time
                    msg['o'],  # order type (LIMIT,..)
                    msg['x'],  # type (NEW, TRADE)
                    msg['X'],  # (NEW, PARTIALLY_FILLED, FILLED)
                    msg['i'],  # order id
                    msg['q'],  # order qty
                    msg['p'],  # order price
                    msg['l'],  # executed qty
                    msg['z'],  # cumulated qty executed
                    msg['L'],  # executed price
                    msg['n'],  # commission amount
                    msg['N']   # commission asset
                )
                self._asset_wallet.add_trade_event(msg['s'], trade_event)
                await self._conn.send_event(AccountApiCommand.OrderEvent, trade_event.to_network())
            elif event == ASSET_OUTBOUND:
                if 'B' in msg:
                    self._asset_wallet.balance_update(msg)
                    await self.send_event(
                        AccountApiCommand.AssetEvent,
                        self._asset_wallet.get_assets_array()
                    )

        LOGGER.info('on_user_message %s', msg)

    async def _get_mytrade(self, symbol):
        return await self._ws.get_trade_list(symbol)

    async def _get_asset_list(self):
        response = await self._ws.get_asset_list()
        hold_list = HoldAssetList()
        LOGGER.info('asset list %s', response)

        for res in response:
            ref_id = self._market + '.' + res['asset'] + 'USDT'
            if self._asset_wallet._is_dollar_backed(res['asset']):
                ref_id = ''
            hold_list.add_hold_asset(res['asset'],
                                     res['asset'],
                                     res['free'],
                                     res['freeze'],
                                     res['locked'], 
                                     res['withdrawing'],
                                     '0',
                                     '0',
                                     ref_id)
        return hold_list

    async def on_asset_list(self, **kwargs):
        return self._asset_wallet.get_assets_array()

    async def on_create_order(self, **kwargs):
        LOGGER.info('create order %s', kwargs)
        util.check_required_parameters(kwargs, 'symbol', 'side', 'quantity', 'price')
        msg = 'cannot make a order'
        try:
            res = await self._ws.new_order(
                kwargs['symbol'].upper(),
                kwargs['side'], 'LIMIT',
                quantity=float(kwargs['quantity']),
                price=float(kwargs['price']),
                timeInForce='GTC'
            )
            order = OrderResponse.CreateOrderResponse(
                res['symbol'],
                res['side'],
                res['price'],
                res['origQty'],
                res['status']
            )
            return order.to_network()
        except Exception as e:
            LOGGER.error('cannot make a order %s', str(e))
            msg = str(e)
        raise CommunicationError(msg)

    async def on_cancel_order(self, **kwargs):
        LOGGER.info('cancel order %s', kwargs)
        util.check_required_parameters(kwargs, 'symbol', 'orderId')
        msg = 'cannot cancel an order'
        try:
            response = await self._ws.cancel_order(
                kwargs['symbol'].upper(), orderId=kwargs['orderId'])
            return response
        except Exception as e:
            LOGGER.error('cannot cancel a order %s', str(e))
            msg = str(e)
        raise CommunicationError(msg)

    async def on_open_order(self, **kwargs):
        msg = 'cannot get open orders'
        try:
            res = await self._ws.get_open_orders()
            arr = []
            for order in res:
                arr.append(OrderTradeEvent(
                    self._market + '.' + order['symbol'],
                    order['side'],
                    order['time'],
                    order['type'],  # ORDER_LIMIT
                    order['status'],
                    order['status'],
                    order['orderId'],
                    order['origQty'],
                    order['price'],
                    '0',
                    order['executedQty'],
                    '0',
                    '0',
                    None
                ).to_network())
            return arr
        except Exception as e:
            LOGGER.error('cannot get open orders %s', str(e))
            msg = str(e)
        raise CommunicationError(msg)


async def main() -> None:
    LOGGER.warning('run account')
    binance_account = BinanceAccount()
    await binance_account.run()
    
    await asyncio.get_running_loop().create_future()


if __name__ == '__main__':
    import logging
    LOG_FORMAT = ('%(levelname) -10s %(asctime)s %(name) -30s %(funcName) '
                  '-35s %(lineno) -5d: %(message)s')
    logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)
    asyncio.run(main())
