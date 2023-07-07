import logging

from akrossworker.common.protocol import HoldAssetList
from akrossworker.cybos.api import com_obj
from akrossworker.cybos.api.connection import CybosConnection


LOGGER = logging.getLogger(__name__)


class CybosAsset:
    def __init__(self, market, account_num, account_type):
        self.market = market
        self.account_num = account_num
        self.account_type = account_type

    def get_count(self):
        conn = CybosConnection()
        conn.wait_until_available()

        self.stock_obj = com_obj.get_com_obj('CpTrade.CpTd6033')
        self.stock_obj.SetInputValue(0, self.account_num)
        self.stock_obj.SetInputValue(1, self.account_type)
        self.stock_obj.SetInputValue(2, 50)
        self.stock_obj.BlockRequest()
        return self.stock_obj.GetHeaderValue(7)

    def get_long_list(self):
        conn = CybosConnection()
        conn.wait_until_available()

        self.stock_obj = com_obj.get_com_obj('CpTrade.CpTd6033')
        self.stock_obj.SetInputValue(0, self.account_num)
        self.stock_obj.SetInputValue(1, self.account_type)
        self.stock_obj.SetInputValue(2, 50)
        self.stock_obj.BlockRequest()
        hold_list = HoldAssetList()
        for i in range(self.stock_obj.GetHeaderValue(7)):
            code = self.stock_obj.GetDataValue(12, i)
            name = self.stock_obj.GetDataValue(0, i)
            quantity = self.stock_obj.GetDataValue(7, i)
            sell_available = self.stock_obj.GetDataValue(15, i)
            price = self.stock_obj.GetDataValue(17, i)
            LOGGER.warning(
                'code: %s, name: %s, quantity: %d, '
                'sell_available: %d, price: %d',
                code, name, quantity, sell_available, price)
            hold_list.add_hold_asset(code,
                                     name,
                                     str(sell_available),
                                     str(0),
                                     str(quantity - sell_available),
                                     str(0),
                                     str(0),
                                     str(price),
                                     self.market + '.' + code.lower())
            
        return hold_list

    def get_long_codes(self):
        conn = CybosConnection()
        conn.wait_until_available()

        self.stock_obj = com_obj.get_com_obj('CpTrade.CpTd6033')
        self.stock_obj.SetInputValue(0, self.account_num)
        self.stock_obj.SetInputValue(1, self.account_type)
        self.stock_obj.SetInputValue(2, 50)
        self.stock_obj.BlockRequest()

        long_codes = []
        for i in range(self.stock_obj.GetHeaderValue(7)):
            code = self.stock_obj.GetDataValue(12, i)
            long_codes.append(code.lower())

        return long_codes


if __name__ == '__main__':
    from akrossworker.cybos.api.account import CybosAccount

    account = CybosAccount()
    asset = CybosAsset('KRX', account.get_account_number(),
                       account.get_account_type())
    print('COUNT:', asset.get_count())
    print('CODES:', asset.get_long_codes())
    print('LIST:', asset.get_long_list().to_array())
