import time
import logging

from akrossworker.cybos.api import com_obj
from akrossworker.cybos.api.asset_manager import AssetManager
from akrossworker.cybos.api.connection import CybosConnection
from akrossworker.common.protocol import CybosTradeEvent


LOGGER = logging.getLogger(__name__)


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
        self.callback(CybosTradeEvent(flag, code, order_num, quantity, price, order_type))


class CybosOrder:
    def __init__(
        self,
        account_num,
        account_type,
        asset_manager: AssetManager
    ):
        self.started = False
        self.account_num = account_num
        self.account_type = account_type
        self.asset_manager = asset_manager
        self.conn = CybosConnection()
        self.realtime_order = com_obj.get_com_obj('DsCbo1.CpConclusion')
        self.cancel_obj = com_obj.get_com_obj('CpTrade.CpTd0314')
        self.handler = com_obj.with_events(self.realtime_order, _OrderRealtime)
        self.handler.set_params(self.realtime_order, self.order_event)

    def order_event(self, event: CybosTradeEvent):
        self.asset_manager.order_event(event)

    def cancel_order(self, order_number, code):
        LOGGER.info('Cancel Order(%s) %s', code, str(order_number))
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
                    self.asset_manager.cancel_order(order_number, new_order_num)
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
                    self.asset_manager.add_new_order(code.lower(), is_buy, price, quantity)
                LOGGER.info("process order %s, %s", str(status), msg)
                return status, msg
            except Exception as e:
                msg = msg + ' reason: ' + str(e)
                LOGGER.error('order process failed %s', str(e))
        return -1, msg

    def start_subscribe(self):
        if not self.started:
            self.started = True
            self.realtime_order.Subscribe()
            LOGGER.warning('START ORDER SUBSCRIBE')

    def stop_subscribe(self):
        if self.started:
            self.started = False
            self.realtime_order.Unsubscribe()
            LOGGER.warning('STOP ORDER SUBSCRIBE')
