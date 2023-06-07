import logging

from akrossworkers.cybos.api import com_obj
from akrossworkers.cybos.api.connection import CybosConnection
from akrossworkers.common.protocol import OrderbookStreamProtocol
from akrossworkers.common.args_constants import TickTimeType


LOGGER = logging.getLogger(__name__)


def get_orderbook_extended(code):
    try:
        conn = CybosConnection()
        conn.wait_until_available()

        obj = com_obj.get_com_obj("CpSysDib.StockUniMst")
        obj.SetInputValue(0, code)
        obj.BlockRequest()

        d = {}
        d['total_ask_remain'] = str(obj.GetHeaderValue(88))
        d['total_bid_remain'] = str(obj.GetHeaderValue(90))
        d['bids'] = []
        d['asks'] = []
        for i in range(58, 87+1, 6):
            ask = obj.GetHeaderValue(i)
            bid = obj.GetHeaderValue(i+3)
            if bid != 0:
                d['bids'].append([str(bid), str(obj.GetHeaderValue(i+4))])

            if ask != 0:
                d['asks'].append([str(ask), str(obj.GetHeaderValue(i+1))])

        orderbook_stream = OrderbookStreamProtocol.CreateOrderbookStream(
            d['total_bid_remain'],
            d['total_ask_remain'],
            d['bids'],
            d['asks'],
            TickTimeType.ExtendedTrading
        )
        return orderbook_stream.to_network()
    except Exception as e:
        LOGGER.error('orderbook error %s', str(e))

    return OrderbookStreamProtocol.CreateOrderbookStream(0, 0, [], []).to_network()


if __name__ == '__main__':
    print(get_orderbook_extended('A005930'))
