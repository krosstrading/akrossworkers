import time
import logging

from akross.common import aktime
from akrossworker.common.args_constants import OrderResultType, OrderType

from akrossworker.cybos.api import com_obj
from akrossworker.cybos.api.connection import CybosConnection
from akrossworker.common.protocol import OrderTradeEvent


LOGGER = logging.getLogger(__name__)


class CybosOpenOrder:
    """
    refer:https://cybosplus.github.io/cptrade_new_rtf_1_/cptd5339_.htm
    """
    def __init__(self, account_num, account_type, market):
        self.market = market
        self.account_num = account_num
        self.account_type = account_type
        self.conn = CybosConnection()

    def _td5339(self):
        obj = com_obj.get_com_obj('CpTrade.CpTd5339')
        obj.SetInputValue(0, self.account_num)
        obj.SetInputValue(1, self.account_type)
        obj.SetInputValue(4, '0')
        obj.SetInputValue(6, '0')
        obj.SetInputValue(7, 20)

        orders = []
        LOGGER.warning('START REQUEST TD5339 ORDER IN QUEUE')
        while True:
            try:
                LOGGER.warning('TD5339 START')
                self.conn.wait_until_order_available()
                LOGGER.warning('TD5339 block after')
                ret = obj.BlockRequest()
                LOGGER.warning('TD5339 block after ret:%d', ret)
            except Exception:
                LOGGER.error('TD5339 BlockRequest COM Error')
                return []

            if obj.GetDibStatus() != 0:
                LOGGER.error('TD5339 failed')
                return []
            elif ret == 2 or ret == 3:
                LOGGER.error('TD5339 communication failed')
                return []

            while ret == 4:
                self.conn.wait_until_order_available()
                try:
                    ret = obj.BlockRequest()
                except Exception:
                    LOGGER.error('TD5339 failed(ret=4)')
                    return []

            count = obj.GetHeaderValue(5)

            LOGGER.warning('ORDER IN QUEUE COUNT: %d', count)
            for i in range(count):
                order = dict()
                order['number'] = obj.GetDataValue(1, i)
                # 원주문 번호?
                order['prev'] = obj.GetDataValue(2, i) 
                order['code'] = obj.GetDataValue(3, i)
                order['name'] = obj.GetDataValue(4, i)
                order['desc'] = obj.GetDataValue(5, i)
                order['quantity'] = obj.GetDataValue(6, i)
                order['price'] = obj.GetDataValue(7, i)
                # 체결수량
                order['traded_quantity'] = obj.GetDataValue(8, i) 
                # 신용구분
                order['credit_type'] = obj.GetDataValue(9, i) 
                # 정정취소 가능수량
                order['edit_available_quantity'] = obj.GetDataValue(11, i)  
                # 매매구분코드, '1' == 매도, '2' == 매수, int return 아님
                order['is_buy'] = obj.GetDataValue(13, i)
                # 대출일
                order['credit_date'] = obj.GetDataValue(17, i)
                # 주문호가구분코드 내용
                order['flag_describe'] = obj.GetDataValue(19, i)
                # 주문호가구분코드
                order['flag'] = obj.GetDataValue(21, i)    
                # print('ORDER IN QUEUE ', type(order['is_buy']), order)

                orders.append(OrderTradeEvent(
                    self.market + '.' + order['code'],
                    'BUY' if order['is_buy'] == '2' else 'SELL',
                    aktime.get_msec(),
                    OrderType.OrderLimit,  # TODO: change according to 21 flag
                    (OrderResultType.New if order['traded_quantity'] == 0
                     else OrderResultType.Trade),
                    (OrderResultType.SubTypeNew if order['traded_quantity'] == 0
                     else OrderResultType.SubTypePartial),
                    order['number'],
                    str(order['quantity']),
                    str(order['price']),
                    '0',
                    str(order['traded_quantity']),
                    '0',
                    '0',
                    None
                ))
            if not obj.Continue:
                break
            
        return orders

    def request(self):
        return self._td5339()


if __name__ == '__main__':
    from akrossworker.cybos.api.account import CybosAccount

    account = CybosAccount()
    open_order = CybosOpenOrder(
        account.get_account_number(), account.get_account_type(), 'KRX')
    while True:
        print(open_order.request())
        time.sleep(0.5)

