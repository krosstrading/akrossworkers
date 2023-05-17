from workers.cybos.api import com_obj
import sys


class CybosAccount:
    def __init__(self):
        self.trade_obj = com_obj.get_com_obj('CpTrade.CpTdUtil')
        ret = self.trade_obj.TradeInit(0)
        if ret != 0:
            print('Trade Init Failed', ret)
            sys.exit(1)

    def get_account_number(self):
        return self.trade_obj.AccountNumber[0]

    def get_account_type(self):
        return self.trade_obj.GoodsList(self.get_account_number(), 1)[0]

