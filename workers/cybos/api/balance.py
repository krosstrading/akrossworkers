from workers.cybos.api import com_obj
import logging


LOGGER = logging.getLogger(__name__)


def get_balance_old(account_num, account_type):
    acc_obj = com_obj.get_com_obj('CpTrade.CpTdNew5331A')
    acc_obj.SetInputValue(0, account_num)
    acc_obj.SetInputValue(1, account_type)
    acc_obj.BlockRequest()
    return acc_obj.GetHeaderValue(47)


def get_balance(account_num, account_type):
    acc_obj = com_obj.get_com_obj('CpTrade.CpTdNew5331A')
    acc_obj.SetInputValue(0, account_num)
    acc_obj.SetInputValue(1, account_type)
    acc_obj.BlockRequest()
    LOGGER.warning('balance:%d', int(acc_obj.GetHeaderValue(9)))
    return acc_obj.GetHeaderValue(9)  # 증거금 100%

