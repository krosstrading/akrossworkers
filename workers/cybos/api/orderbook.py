import logging

from workers.cybos.api import com_obj
from workers.cybos.api.connection import CybosConnection
from workers.common.protocol import OrderbookStreamProtocol


LOGGER = logging.getLogger(__name__)


def get_orderbook(code):
    try:
        conn = CybosConnection()
        conn.wait_until_available()

        obj = com_obj.get_com_obj("dscbo1.StockJpBid2")
        obj.SetInputValue(0, code)
        obj.BlockRequest()

        bids = []
        asks = []
        for i in range(10):
            ask_price = obj.GetDataValue(0, i)
            bid_price = obj.GetDataValue(1, i)

            if bid_price != 0:
                bids.append([str(bid_price), str(obj.GetDataValue(3, i))])

            if ask_price != 0:
                asks.append([str(ask_price), str(obj.GetDataValue(2, i))])

        orderbook_stream = OrderbookStreamProtocol.CreateOrderbookStream(
            str(obj.GetHeaderValue(6)),
            str(obj.GetHeaderValue(4)),
            bids,
            asks
        )
        return orderbook_stream.to_network()
    except Exception as e:
        LOGGER.error('orderbook error %s', str(e))

    return OrderbookStreamProtocol.CreateOrderbookStream(
        0, 0, [], []).to_network()


if __name__ == '__main__':
    print(get_orderbook('A005930'))

