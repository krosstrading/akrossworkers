import time
import logging
from akross.common import aktime
from akross.common.exception import CommunicationError
from akrossworker.common.args_constants import OrderResultType, OrderType

from akrossworker.cybos.api import com_obj
from akrossworker.cybos.api.connection import CybosConnection
from akrossworker.common.protocol import OrderTradeEvent


LOGGER = logging.getLogger(__name__)


PASS = 0
PARTIALLY_FILLED = 1
FILLED = 2


class _OrderRealtime:
    def set_params(self, obj, listener):
        self.obj = obj
        self.callback = listener

    def OnReceived(self):
        """
        매수, 매도는 (1)접수 -> (2)체결
        취소, 정정은 (1)접수 -> (2) 확인
        오류시에는 거부
        """
        # string flag '1': 체결, '2': 확인, '3': 거부, '4':접수
        flag = self.obj.GetHeaderValue(14)
        # long order number
        order_num = self.obj.GetHeaderValue(5)
        # long quantity
        quantity = self.obj.GetHeaderValue(3)
        # long price 
        price = self.obj.GetHeaderValue(4)
        # 6번은 원주문
        code = self.obj.GetHeaderValue(9)  # code
        # string '1' 매도, '2' 매수, int return 아님
        order_type = self.obj.GetHeaderValue(12)
        # 체결 기준 잔고 수량, 이전 산거 부터 전체 잔고 수량
        # total_quantity = self.obj.GetHeaderValue(23)    # count of stock left
        result = {
            'flag': flag,
            'code': code.lower(),
            'order_number': order_num,
            'quantity': quantity,
            'price': price,
            'order_type': order_type,
            # 'total_quantity': total_quantity
        }
        LOGGER.info('OnReceived %s', result)
        self.callback(result.copy())


class OrderItem:
    def __init__(
            self,
            code: str,
            qty: int,
            price: int,
            trade_cum_qty: int,
            is_buy: bool
    ):
        self.side = 'buy' if is_buy else 'sell'
        self.code = code.lower()
        self.orig_qty = qty
        self.orig_price = price
        self.orig_order_id = -1
        self.order_id = -1
        self.price = 0
        self.qty = 0
        self.trade_cum_qty = trade_cum_qty
    
    def prepare_cancel(self, new_order_id):
        self.orig_order_id = self.order_id
        self.order_id = new_order_id

    def is_matched(self, trade_msg):
        side = 'buy' if trade_msg['order_type'] == '2' else 'sell'
        if (self.order_id == -1 and
                self.code == trade_msg['code'].lower() and
                self.orig_qty == trade_msg['quantity'] and
                self.orig_price == trade_msg['price'] and
                self.side == side):
            self.order_id = trade_msg['order_number']
            return True
        return False

    def set_traded(self, trade_msg):
        if trade_msg['order_number'] == self.order_id:
            self.qty = trade_msg['quantity']
            self.price = trade_msg['price']
            self.trade_cum_qty += self.qty
            return (FILLED if self.trade_cum_qty == self.orig_qty
                    else PARTIALLY_FILLED)
        return PASS


class CybosOrder:
    def __init__(self, market, account_num, account_type, callback):
        self.started = False
        self.account_num = account_num
        self.account_type = account_type
        self.market = market
        self.callback = callback
        self.conn = CybosConnection()
        # self.realtime_order = com_obj.get_com_obj('DsCbo1.CpConclusion')
        self.cancel_obj = com_obj.get_com_obj('CpTrade.CpTd0314')
        # self.handler = com_obj.with_events(self.realtime_order, _OrderRealtime)
        # self.handler.set_params(self.realtime_order, self.order_event)
        self._order_container = []

    def add_open_order(self, open_order):
        # careful open_order number is string
        order_item = OrderItem(
            open_order.symbol_id.split('.')[-1].lower(),
            int(open_order.order_orig_qty),
            int(open_order.order_orig_price),
            int(open_order.trade_cum_qty),
            open_order.side.lower() == 'buy'
        )
        order_item.order_id = open_order.order_id
        self._order_container.append(order_item)

    def get_open_orders(self):
        orders = []
        for order in self._order_container:
            orders.append(OrderTradeEvent(
                self.market + '.' + order.code,
                order.side,
                # ignore currently and set current time
                aktime.get_msec(),
                # TODO: change according to 21 flag
                OrderType.OrderLimit,
                (OrderResultType.New if order.trade_cum_qty == 0
                 else OrderResultType.Trade),
                (OrderResultType.SubTypeNew if order.trade_cum_qty == 0
                 else OrderResultType.SubTypePartial),
                order.order_id,
                str(order.orig_qty),
                str(order.orig_price),
                '0',
                str(order.trade_cum_qty),
                '0',
                '0',
                None
            ).to_network())
        return orders

    def order_event(self, msg):
        if msg['flag'] == '4':
            # confirm order msg
            found = False
            for order_item in self._order_container:
                if order_item.is_matched(msg):
                    found = True
                    self.callback(OrderTradeEvent(
                        self.market + '.' + order_item.code,
                        order_item.side,
                        aktime.get_msec(),
                        OrderType.OrderLimit,
                        OrderResultType.New,
                        OrderResultType.SubTypeNew,
                        order_item.order_id,
                        str(order_item.orig_qty),
                        str(order_item.orig_price),
                        str(order_item.qty),
                        str(order_item.trade_cum_qty),
                        str(order_item.price),
                        '0', None).to_network()
                    )
                    break
            if not found:
                LOGGER.error('not found matched order')
        elif msg['flag'] == '1':  # trade msg
            done_item = None
            for order_item in self._order_container:
                result = order_item.set_traded(msg)
                if result != PASS:
                    self.callback(OrderTradeEvent(
                        self.market + '.' + order_item.code,
                        order_item.side,
                        aktime.get_msec(),
                        OrderType.OrderLimit,
                        OrderResultType.Trade,
                        (OrderResultType.SubTypeFilled if result == FILLED
                         else OrderResultType.SubTypePartial),
                        order_item.order_id,
                        str(order_item.orig_qty),
                        str(order_item.orig_price),
                        str(order_item.qty),
                        str(order_item.trade_cum_qty),
                        str(order_item.price),
                        '0', None).to_network()
                    )
                    done_item = order_item if result == FILLED else None
                    break
            if done_item is not None:
                self._order_container.remove(done_item)
        elif msg['flag'] == '2':
            # if we do not use modify order then only cancel will set flag '2'
            done_item = None
            for order_item in self._order_container:
                if order_item.order_id == msg['order_number']:
                    done_item = order_item
                    # self._order_container.remove(order_item)
                    self.callback(OrderTradeEvent(
                        self.market + '.' + order_item.code,
                        order_item.side,
                        aktime.get_msec(),
                        OrderType.OrderLimit,
                        OrderResultType.Canceled,
                        OrderResultType.Canceled,
                        # use orig_order_id because client remove orig order id
                        order_item.orig_order_id,
                        str(order_item.orig_qty),
                        str(order_item.orig_price),
                        str(order_item.qty),
                        str(order_item.trade_cum_qty),
                        str(order_item.price),
                        '0', None).to_network()
                    )                    
                    break
            if done_item is not None:
                self._order_container.remove(done_item)

    def cancel_order(self, order_number, code):
        LOGGER.info('Cancel Order(%s) %s', code, str(order_number))
        order_item = None
        for item in self._order_container:
            if item.order_id == order_number:
                order_item = item
                break
        if order_item is None:
            raise CommunicationError('Cannot find order number %d',
                                     order_number)

        try:
            self.cancel_obj.SetInputValue(1, order_number)
            self.cancel_obj.SetInputValue(2, self.account_num)
            self.cancel_obj.SetInputValue(3, self.account_type)
            self.cancel_obj.SetInputValue(4, code)
            self.cancel_obj.SetInputValue(5, 0)  # 0이면 전량 취소 0, 일단 0으로 고정
            while True:
                ret = self.cancel_obj.BlockRequest()
                if ret == 0:
                    new_order_num = self.cancel_obj.GetHeaderValue(6)
                    order_item.prepare_cancel(new_order_num)
                    break
                elif ret == 4:
                    if self.conn.request_left_count() <= 0:
                        time.sleep(self.conn.get_remain_time() / 1000)
                    continue
                else:
                    LOGGER.error('TD0314 Cancel Order Failed')
                    return -1, f'cancel order failed ret:{ret}'
        except Exception as e:
            LOGGER.error('Order Failed %s', str(e))
            return -1, str(e)
        return (self.cancel_obj.GetDibStatus(),
                str(self.cancel_obj.GetDibMsg1()))

    def order(self, code: str, quantity: int, price: int, is_buy: bool):
        self.conn.wait_until_order_available()
        msg = f'ORDER Failed({code}) qty({quantity}) price({price}) ' \
              f'side({"buy" if is_buy else "sell"})'

        if quantity == 0:
            pass
        else:
            try:
                self.obj = com_obj.get_com_obj('CpTrade.CpTd0311')
                order_type = '2' if is_buy else '1'
                self.obj.SetInputValue(0, order_type)
                self.obj.SetInputValue(1, self.account_num)
                self.obj.SetInputValue(2, self.account_type)
                self.obj.SetInputValue(3, code.upper())
                self.obj.SetInputValue(4, quantity)
                if price == 0:  # 시장가
                    self.obj.SetInputValue(8, '03')
                else:
                    self.obj.SetInputValue(5, price)
                self.obj.BlockRequest()

                status, msg = self.obj.GetDibStatus(), self.obj.GetDibMsg1()
                if status == 0:
                    self._order_container.append(
                        OrderItem(code.lower(), quantity, price, 0, is_buy))

                LOGGER.info("process order %s, %s", str(status), msg)
                return status, msg
            except Exception as e:
                msg = msg + ' reason: ' + str(e)
                LOGGER.error('order process failed %s', str(e))
        return -1, msg

    # def start_subscribe(self):
    #     if not self.started:
    #         self.started = True
    #         self.realtime_order.Subscribe()
    #         LOGGER.warning('START ORDER SUBSCRIBE')

    # def stop_subscribe(self):
    #     if self.started:
    #         self.started = False
    #         self.realtime_order.Unsubscribe()
    #         LOGGER.warning('STOP ORDER SUBSCRIBE')
