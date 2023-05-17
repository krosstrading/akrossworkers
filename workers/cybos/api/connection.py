from workers.cybos.api import com_obj
from PyQt5 import QtCore


# get_remain_time return as milliseconds

class CybosConnection(QtCore.QObject):
    request_available = QtCore.pyqtSignal()
    order_available = QtCore.pyqtSignal()

    def __init__(self):
        super().__init__()
        self.obj = com_obj.get_com_obj("CpUtil.CpCybos")
        self._available_check_timer = QtCore.QTimer()
        self._available_check_timer.setInterval(100)
        self._available_check_timer.timeout.connect(self.check_available)
        self._order_check_timer = QtCore.QTimer()
        self._order_check_timer.setInterval(100)
        self._order_check_timer.timeout.connect(self.check_order_available)

    # return example: 14906
    def get_remain_time(self):
        return self.obj.LimitRequestRemainTime

    def realtime_left_count(self):
        return self.obj.GetLimitRemainCount(2)

    def request_left_count(self):
        return self.obj.GetLimitRemainCount(1)

    def order_left_count(self):
        return self.obj.GetLimitRemainCount(0)

    def is_connected(self):
        return self.obj.IsConnect

    def check_available(self):
        if self.request_left_count() > 0:
            self.request_available.emit()
            self._available_check_timer.stop()

    def check_order_available(self):
        if self.order_left_count() > 0:
            self.order_available.emit()
            self._order_check_timer.stop()

    def wait_until_available(self):
        if self.request_left_count() <= 0:
            print('*' * 10, 'Request Limit', '*' * 10)
            event_loop = QtCore.QEventLoop()
            self.request_available.connect(event_loop.quit)
            self._available_check_timer.start()
            event_loop.exec()
            print('*' * 10, 'quit Limit Block', '*' * 10)

    def wait_until_order_available(self):
        if self.order_left_count() <= 0:
            print('*' * 10, 'Request Order Limit', '*' * 10)
            event_loop = QtCore.QEventLoop()
            self.order_available.connect(event_loop.quit)
            self._order_check_timer.start()
            event_loop.exec()
            print('*' * 10, 'quit Order Limit Block', '*' * 10)


if __name__ == '__main__':
    conn = CybosConnection()
    print(conn.is_connected())

    if conn.is_connected():
        print("Remain Time", conn.get_remain_time())
        print("Realtime", conn.realtime_left_count())
        print("Request", conn.request_left_count())
        print("Order", conn.order_left_count())
