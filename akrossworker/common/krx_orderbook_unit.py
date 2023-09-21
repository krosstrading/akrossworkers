import math


def get_krx_unit(price: int) -> int:
    if price < 2000:
        return 1
    elif 2000 <= price < 5000:
        return 5
    elif 5000 <= price < 20000:
        return 10
    elif 20000 <= price < 50000:
        return 50
    elif 50000 <= price < 200000:
        return 100
    elif 200000 <= price < 500000:
        return 500
    return 1000


def get_vi_price(price_open: int, is_under: bool = False) -> int:
    if not is_under:
        next_price = price_open * 1.1
    else:
        next_price = price_open * 0.9
    unit_price = get_krx_unit(next_price)
    if next_price % unit_price == 0:
        return int(next_price)
    int_price = math.floor(next_price)
    int_price -= int_price % unit_price
    return int_price + get_krx_unit(int_price)


def left_percent_to_vi(price_open: int, current_price: int) -> float:
    return (get_vi_price(price_open) / current_price - 1) * 100


def get_highest(p: float) -> int:
    high = int(p * 1.3)
    unit = get_krx_unit(high)
    return high - high % unit


def get_lowest(p: float) -> int:
    low = int(p * 0.7)
    unit = get_krx_unit(low)
    return low - low % unit


def get_nearest_unit_price(p: float) -> int:
    """
    호가에 맞아 떨어지면 그대로 return,
    호가에 안 맞으면 한 호가 위로 return
    """
    price = int(p)
    unit_price = get_krx_unit(price)
    if price % unit_price == 0:
        return price

    return price - price % unit_price + unit_price


def get_buy_priority_price(p: float) -> int:
    """
    한 호가 위가 0.2% 넘지 않는 선에서 매수 가격 결정
    """
    price = int(p)
    unit_price = get_krx_unit(price)
    price = price - price % unit_price
    original_price = price

    while True:
        unit_price = get_krx_unit(price)
        if original_price * 1.002 < price + unit_price:
            break
        price += unit_price

    return price


def get_under_nearest_unit_price(p: float) -> int:
    """
    호가에 맞아 떨어지면 그대로 return,
    안 맞으면 호가에 맞춰 아래로 return
    """
    price = int(p)
    unit_price = get_krx_unit(price)
    if price % unit_price == 0:
        return price
    return price - price % unit_price


def get_higher_price(p: int) -> int:
    return get_krx_unit(p) + p


if __name__ == '__main__':
    print(get_nearest_unit_price(200050))
    print(get_nearest_unit_price(0))
