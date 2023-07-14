from akrossworker.common.protocol import CybosTradeEvent
from akrossworker.cybos.api.asset_manager import AssetManager
import math


my_asset_manager: AssetManager = None


def callback(msg):
    print('callback msg', msg)
    if my_asset_manager is not None:
        print('asset event', my_asset_manager.get_hold_assets())


#  TODO: asset 테스트
#  매수 후 cancel 시 balance

def remain_asset_bug():
    # 시장가로 산 뒤 3270으로 매도하였으나 asset 이벤트에 그대로 남이 있음
    # createOrder({'side': 'buy', 'symbol': 'a010600', 'quantity': 303, 'price': 0})
    # {'flag': '4', 'code': 'a010600', 'order_number': 23987, 'quantity': 303, 'price': 0, 'order_type': '2'}
    # {'flag': '1', 'code': 'a010600', 'order_number': 23987, 'quantity': 303, 'price': 3315, 'order_type': '2'}
    # createOrder({'side': 'buy', 'symbol': 'a010600', 'quantity': 937, 'price': 3200})
    # {'flag': '4', 'code': 'a010600', 'order_number': 31848, 'quantity': 937, 'price': 3200, 'order_type': '2'}
    # {'flag': '1', 'code': 'a010600', 'order_number': 31848, 'quantity': 937, 'price': 3200, 'order_type': '2'}
    # createOrder({'side': 'sell', 'symbol': 'a010600', 'quantity': 1240, 'price': 3270})
    # {'flag': '4', 'code': 'a010600', 'order_number': 37751, 'quantity': 1240, 'price': 3270, 'order_type': '1'}
    # {'flag': '1', 'code': 'a010600', 'order_number': 37751, 'quantity': 1240, 'price': 3270, 'order_type': '1'}

    asset_manager = AssetManager(10000000, [], {}, callback)
    asset_manager.add_new_order('a168360', True, 0, 54)
    event1 = CybosTradeEvent('4', 'a168360', 23667, 54, 0, '2')
    event2 = CybosTradeEvent('1', 'a168360', 23667, 54, 18450, '2')
    # {'flag': '4', 'code': 'a168360', 'order_number': 23667, 'quantity': 54, 'price': 0, 'order_type': '2'}
    # {'flag': '1', 'code': 'a168360', 'order_number': 23667, 'quantity': 54, 'price': 18450, 'order_type': '2'}
    asset_manager.order_event(event1)
    asset_manager.order_event(event2)

    asset_manager.add_new_order('a254490', True, 0, 33)
    event1 = CybosTradeEvent('4', 'a254490', 23697, 33, 0, '2')
    event2 = CybosTradeEvent('1', 'a254490', 23697, 33, 29950, '2')
    asset_manager.order_event(event1)
    asset_manager.order_event(event2)

    asset_manager.add_new_order('a010600', True, 0, 303)
    # {'flag': '4', 'code': 'a010600', 'order_number': 31848, 'quantity': 937, 'price': 3200, 'order_type': '2'}
    # {'flag': '1', 'code': 'a010600', 'order_number': 31848, 'quantity': 937, 'price': 3200, 'order_type': '2'}
    event1 = CybosTradeEvent('4', 'a010600', 23987, 303, 0, '2')
    event2 = CybosTradeEvent('1', 'a010600', 23987, 303, 3315, '2')
    asset_manager.order_event(event1)
    asset_manager.order_event(event2)

    asset_manager.add_new_order('a053610', True, 0, 15)
    event1 = CybosTradeEvent('4', 'a053610', 25757, 15, 0, '2')
    event2 = CybosTradeEvent('1', 'a053610', 25757, 15, 64800, '2')
    asset_manager.order_event(event1)
    asset_manager.order_event(event2)

    asset_manager.add_new_order('a010600', True, 3200, 937)
    event1 = CybosTradeEvent('4', 'a010600', 31848, 937, 3200, '2')
    event2 = CybosTradeEvent('1', 'a010600', 31848, 937, 3200, '2')
    asset_manager.order_event(event1)
    asset_manager.order_event(event2)
    assert asset_manager.assets['a010600'].free == '1240'

    asset_manager.add_new_order('a168360', True, 17880, 167)
    event1 = CybosTradeEvent('4', 'a168360', 35392, 167, 17880, '2')
    event2 = CybosTradeEvent('1', 'a168360', 35392, 113, 17880, '2')
    event3 = CybosTradeEvent('1', 'a168360', 35392, 54, 17880, '2')
    # {'flag': '4', 'code': 'a168360', 'order_number': 23667, 'quantity': 54, 'price': 0, 'order_type': '2'}
    # {'flag': '1', 'code': 'a168360', 'order_number': 23667, 'quantity': 54, 'price': 18450, 'order_type': '2'}
    asset_manager.order_event(event1)
    asset_manager.order_event(event2)
    asset_manager.order_event(event3)

    asset_manager.add_new_order('a010600', False, 3270, 1240)
    event1 = CybosTradeEvent('4', 'a010600', 37751, 1240, 3270, '1')
    event2 = CybosTradeEvent('1', 'a010600', 37751, 1240, 3270, '1')
    asset_manager.order_event(event1)
    asset_manager.order_event(event2)
    print('hold assets', asset_manager.get_hold_assets())
    assert 'a010600' not in asset_manager.assets  # 아직 남아있는 버그


def balance_test():
    global my_asset_manager
    asset_manager = AssetManager(10000000, [], {}, callback)
    my_asset_manager = asset_manager
    asset_manager.add_new_order('a033170', True, 1725, 57)
    event1 = CybosTradeEvent('4', 'a033170', 144044, 57, 1725, '2')
    event2 = CybosTradeEvent('1', 'a033170', 144044, 57, 1725, '2')
    asset_manager.order_event(event1)
    asset_manager.order_event(event2)


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
    balance_test()
    remain_asset_bug()
    buy_and_cancel()
    asset_remove_test()
    buy_market()