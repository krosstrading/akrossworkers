from workers.cybos.api import com_obj

"""
typedef enum {
[helpstring("구분없음")] CPC_MARKET_NULL  = 0,
[helpstring("거래소")]    CPC_MARKET_KOSPI  = 1,
[helpstring("코스닥")]    CPC_MARKET_KOSDAQ = 2,
[helpstring("프리보드")] CPC_MARKET_FREEBOARD = 3,
[helpstring("KRX")]    CPC_MARKET_KRX  = 4,
}CPE_MARKET_KIND;
"""


def get_market_start_time():
    obj = com_obj.get_com_obj("CpUtil.CpCodeMgr")
    return obj.GetMarketStartTime()


def get_market_end_time():
    obj = com_obj.get_com_obj("CpUtil.CpCodeMgr")
    return obj.GetMarketEndTime()


def get_code_list():
    obj = com_obj.get_com_obj("CpUtil.CpCodeMgr")
    return obj.GetStockListByMarket(0)


def get_kospi_code_list():
    obj = com_obj.get_com_obj("CpUtil.CpCodeMgr")
    return obj.GetStockListByMarket(1)


def get_kosdaq_code_list():
    obj = com_obj.get_com_obj("CpUtil.CpCodeMgr")
    return obj.GetStockListByMarket(2)


def is_kospi_200(code):
    obj = com_obj.get_com_obj("CpUtil.CpCodeMgr")
    return obj.GetStockKospi200Kind(code)


def get_kospi_company_code_list():
    obj = com_obj.get_com_obj("CpUtil.CpCodeMgr")
    codes = obj.GetStockListByMarket(1)
    company_codes = []
    for code in codes:
        if is_company_stock(code):
            company_codes.append(code)
    return company_codes


def get_kosdaq_company_code_list():
    obj = com_obj.get_com_obj("CpUtil.CpCodeMgr")
    codes = obj.GetStockListByMarket(2)
    company_codes = []
    for code in codes:
        if is_company_stock(code):
            company_codes.append(code)
    return company_codes


def is_company_stock(code):
    return get_stock_section(code) == 1


def get_stock_section(code):
    obj = com_obj.get_com_obj("CpUtil.CpCodeMgr")
    return obj.GetStockSectionKind(code)


def get_country_code():
    obj = com_obj.get_com_obj('CpUtil.CpUsCode')
    return obj.GetUsCodeList(2)


def get_us_name(code):
    obj = com_obj.get_com_obj('CpUtil.CpUsCode')
    return obj.GetNameByUsCode(code)


def get_stock_listed_date(code):
    try:
        obj = com_obj.get_com_obj('CpUtil.CpCodeMgr')
    except Exception:
        return 0
    return obj.GetStockListedDate(code)


def get_kospi200_list():
    kospi_200 = []
    code_list = get_kospi_code_list()
    for code in code_list:
        if is_kospi_200(code) > 0 and not is_there_warning(code):
            kospi_200.append(code)
    return kospi_200


def code_to_name(code):
    obj = com_obj.get_com_obj("CpUtil.CpCodeMgr")
    return obj.CodeToName(code)


def get_marketcaps(code_arr):
    index = 0
    data = {}
    try:
        obj = com_obj.get_com_obj("CpSysDib.MarketEye")
        code_obj = com_obj.get_com_obj("CpUtil.CpCodeMgr")

        while True:
            obj.SetInputValue(0, [0, 4, 20])
            obj.SetInputValue(1, code_arr[index:index+200])
            obj.BlockRequest()
            count = obj.GetHeaderValue(2)
            for i in range(count):
                multiplier = 1000
                if code_obj.IsBigListingStock(obj.GetDataValue(0, i)) == 0:
                    multiplier = 1
                data[obj.GetDataValue(0, i)] = (
                    str(obj.GetDataValue(2, i) * multiplier),
                    str(
                        obj.GetDataValue(1, i) * obj.GetDataValue(2, i) *
                        multiplier
                    )
                )
            index += 200
            if len(code_arr) <= index:
                break
    except Exception:
        print('get_marketcaps error occurred')

    return data

# GetStockControlKind
#   from 0: 정상, 주의, 경고, 위험예고, 위험
# GetStockStatusKind
#   from 0: 정상, 거래정지, 거래중단
# GetStockSupervisionKind
#   from 0: 일반종목, 관리


def is_there_warning(code):
    obj = com_obj.get_com_obj("CpUtil.CpCodeMgr")
    return (
        obj.GetStockControlKind(code) > 0 or
        obj.GetStockStatusKind(code) > 0 or
        obj.GetStockSupervisionKind(code) > 0
    )


def is_stopped(code):
    obj = com_obj.get_com_obj("CpUtil.CpCodeMgr")
    return obj.GetStockStatusKind(code)


def get_industry_name(code):
    obj = com_obj.get_com_obj("CpUtil.CpCodeMgr")
    return obj.GetIndustryName(obj.GetStockIndustryCode(code))


def get_us_code(ustype):
    obj = com_obj.get_com_obj("CpUtil.CpUsCode")
    result = obj.GetUsCodeList(ustype)
    return list(result)


def get_member_list():
    obj = com_obj.get_com_obj("CpUtil.CpCodeMgr")
    return obj.GetMemberList()


def get_member_name(broker_code: str):
    obj = com_obj.get_com_obj("CpUtil.CpCodeMgr")
    return obj.GetMemberName(broker_code)


def is_member_foreign(broker_code: str):
    obj = com_obj.get_com_obj("CpUtil.CpCodeMgr")
    return obj.IsFrnMember(broker_code) == 1


if __name__ == '__main__':
    # members = get_member_list()
    
    # for member in members:
    #     print(member, get_member_name(member), is_member_foreign(member))

    # print(get_stock_listed_date('A005930'))
    
    kosdaq = get_kosdaq_company_code_list()
    kospi = get_kospi_company_code_list()
    print(kospi[0])
    if 'A343510' in kospi:
        print('KOSPI')
    elif 'A343510' in kosdaq:
        print('KOSDAQ')
    # print(get_market_start_time(), get_market_end_time())
    # kosdaq_marketcaps = get_marketcaps(kosdaq)
    # kospi_marketcaps = get_marketcaps(kospi)
    # print(kospi_marketcaps['A005930'])
    
    # print('KOSPI', len(get_kospi_code_list()))
    # print('KOSDAQ', len(get_kosdaq_code_list()))
    # print(get_stock_section('A272560'))

    # print('KOSPI', len(get_kospi_company_code_list()))
    # print('KOSDAQ', len(get_kosdaq_company_code_list()))

    # if 'A272560' in get_kospi_code_list():
    #     print('YES')
    # else:
    #     print('NO')
    # print(is_company_stock('A005935'))
    # conn = connection.Connection()
    # print("Connected", conn.is_connected())
    # print(get_us_name('A005930'))
    # print(code_to_name('A005930'))
    # print(is_there_warning('A227950'))
    # get_stock_section('A079980')
    # codes = get_kosdaq_code_list()
    # for code in codes:
    #     print(code, get_industry_name(code), code_to_name(code),
    # is_company_stock(code))
    # print("US CODE ALL", get_us_code(1))
    # print('TYPE', type(get_us_code(1)))
    # print("Left", conn.request_left_count())
    # print("KOSPI ", get_kospi_code_list())
    # print("Warning", is_there_warning('A134780'))
    # print("Left", conn.request_left_count())
    # print("GroupName", get_industry_name('A032640'))

    # country_code = get_country_code()
    # for code in country_code:
    #     print(code, get_us_name(code))
