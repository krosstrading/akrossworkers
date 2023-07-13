from akrossworker.common.protocol import CybosTradeEvent
from akrossworker.cybos.api.asset_manager import AssetManager
import math


def callback(msg):
    print('callback msg', msg)


#  TODO: asset 테스트
#  매수 후 cancel 시 balance

def buy_and_cancel():
    asset_manager = AssetManager(10000000, [], {}, callback)
    asset_manager.add_new_order('a005930', True, 50000, 100)
    assert asset_manager.assets['KRW'].locked == '5000000'
    assert asset_manager.assets['KRW'].free == '5000000'
    event1 = CybosTradeEvent('4', 'a005930', 123456, 100, 50000, '2')
    asset_manager.order_event(event1)
    open_orders = asset_manager.get_open_orders()
    assert len(asset_manager.get_hold_assets()) == 1  # KRW
    assert len(open_orders) == 1
    print('send cancel')
    asset_manager.cancel_order(123456, 234567)

    cancel_confirm = CybosTradeEvent('2', 'a005930', 234567, 0, 0, '2')
    asset_manager.order_event(cancel_confirm)
    
    assert asset_manager.assets['KRW'].locked == '0'
    assert asset_manager.assets['KRW'].free == '10000000'
    assert len(asset_manager.get_hold_assets()) == 1


def buy_market():
    asset_manager = AssetManager(10000000, [], {}, callback)
    asset_manager.add_new_order('a005930', True, 0, 100)
    assert asset_manager.assets['KRW'].free == '10000000'
    event1 = CybosTradeEvent('4', 'a005930', 123456, 100, 0, '2')
    asset_manager.order_event(event1)
    
    assert len(asset_manager.get_hold_assets()) == 1  # KRW
    assert len(asset_manager.get_open_orders()) == 1
    event1 = CybosTradeEvent('1', 'a005930', 123456, 100, 50000, '2')
    asset_manager.order_event(event1)
    assert len(asset_manager.get_open_orders()) == 0
    assert len(asset_manager.get_hold_assets()) == 2
    print(asset_manager.assets['KRW'].to_network())
    assert asset_manager.assets['KRW'].free == '5000000'


def asset_remove_test():
    asset_manager = AssetManager(10000000, [], {}, callback)
    asset_manager.add_new_order('a005930', True, 50000, 100)
    assert asset_manager.assets['KRW'].locked == '5000000'
    assert asset_manager.assets['KRW'].free == '5000000'

    event1 = CybosTradeEvent('4', 'a005930', 123456, 100, 50000, '2')
    asset_manager.order_event(event1)

    open_orders = asset_manager.get_open_orders()
    assert len(asset_manager.get_hold_assets()) == 1  # KRW
    assert len(open_orders) == 1
    event1 = CybosTradeEvent('1', 'a005930', 123456, 100, 50000, '2')
    asset_manager.order_event(event1)
    assert len(asset_manager.get_hold_assets()) == 2

    asset_manager.add_new_order('a005930', False, 60000, 100)
    event1 = CybosTradeEvent('4', 'a005930', 345678, 100, 60000, '1')
    asset_manager.order_event(event1)
    event1 = CybosTradeEvent('1', 'a005930', 345678, 100, 60000, '1')
    asset_manager.order_event(event1)    
    assert len(asset_manager.get_open_orders()) == 0
    assert len(asset_manager.get_hold_assets()) == 1  # KRW


def test_case1():
    asset_manager = AssetManager(10000000, [], {}, callback)
    asset_manager.add_new_order('a005930', True, 50000, 100)
    assert asset_manager.assets['KRW'].locked == '5000000'
    assert asset_manager.assets['KRW'].free == '5000000'

    event1 = CybosTradeEvent('4', 'a005930', 123456, 100, 50000, '2')
    asset_manager.order_event(event1)

    open_orders = asset_manager.get_open_orders()
    assert len(asset_manager.get_hold_assets()) == 1  # KRW
    assert len(open_orders) == 1

    assert open_orders[0]['symbolId'] == 'krx.spot.a005930'
    assert open_orders[0]['tradeQty'] == '0'
    assert open_orders[0]['tradeCumQty'] == '0'
    assert open_orders[0]['orderOrigQty'] == '100'
    assert open_orders[0]['orderOrigPrice'] == '50000'
    print('send submit')

    # 50 주 체결
    event1 = CybosTradeEvent('1', 'a005930', 123456, 50, 50000, '2')
    asset_manager.order_event(event1)
    assert len(asset_manager.get_hold_assets()) == 2
    assert asset_manager.assets['a005930'].free == '50'
    assert asset_manager.assets['KRW'].locked == '2500000'
    assert asset_manager.assets['KRW'].free == '5000000'
    assert asset_manager.assets['a005930'].buyestimated == str(math.ceil(50000 * 1.0025))

    # 현재는 살 때는 세금 계산 하지 않고, 매도시에만 세금 계산
    # 나머지 잔량 취소
    asset_manager.cancel_order(123456, 234567)
    open_orders = asset_manager.get_open_orders()
    assert len(open_orders) == 0

    cancel_confirm = CybosTradeEvent('2', 'a005930', 234567, 0, 0, '2')
    # 취소 confirm
    asset_manager.order_event(cancel_confirm)
    assert asset_manager.assets['KRW'].locked == '0'
    assert asset_manager.assets['KRW'].free == '7500000'
    assert asset_manager.assets['a005930'].free == '50'
    assert asset_manager.assets['a005930'].locked == '0'
    assert len(asset_manager.get_open_orders()) == 0

    # 60000 원에 25주 매도
    asset_manager.add_new_order('a005930', False, 60000, 50)
    assert asset_manager.assets['a005930'].free == '0'
    assert asset_manager.assets['a005930'].locked == '50'
    assert len(asset_manager.get_open_orders()) == 1

    # submit
    event1 = CybosTradeEvent('4', 'a005930', 345678, 50, 60000, '1')
    asset_manager.order_event(event1)
    assert asset_manager.assets['a005930'].free == '0'
    assert asset_manager.assets['a005930'].locked == '50'
    assert len(asset_manager.get_open_orders()) == 1

    # 25주 매도 trade
    event1 = CybosTradeEvent('1', 'a005930', 345678, 25, 60000, '1')
    asset_manager.order_event(event1)
    assert asset_manager.assets['a005930'].free == '0'
    assert asset_manager.assets['a005930'].locked == '25'

    assert asset_manager.assets['KRW'].locked == '0'
    calc = math.floor(60000 * 25 * 0.9975)
    assert asset_manager.assets['KRW'].free == str(7500000 + calc)
    assert len(asset_manager.get_open_orders()) == 1

    # 25주 매도 trade
    event1 = CybosTradeEvent('1', 'a005930', 345678, 25, 60000, '1')
    asset_manager.order_event(event1)
    assert len(asset_manager.get_open_orders()) == 0
    calc = math.floor(60000 * 50 * 0.9975)
    assert asset_manager.assets['KRW'].free == str(7500000 + calc)


if __name__ == '__main__':
    buy_and_cancel()
    asset_remove_test()
    buy_market()