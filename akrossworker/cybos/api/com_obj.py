import os
import re
import sys
import shutil
import win32com.client


def get_com_obj(name):
    com_obj = None
    try:
        com_obj = win32com.client.gencache.EnsureDispatch(name)
    except Exception as e:
        print('get_com_obj', name, 'get exception', e)
        MODULE_LIST = [m.__name__ for m in sys.modules.values()]
        for module in MODULE_LIST:
            if re.match(r'win32com\.gen_py\..+', module):
                print('delete module', sys.modules[module])
                del sys.modules[module]
        print('remove directory',
              os.path.abspath(os.path.join(win32com.__gen_path__, '..')))
        shutil.rmtree(
            os.path.abspath(os.path.join(win32com.__gen_path__, '..'))
        )
        com_obj = win32com.client.gencache.EnsureDispatch(name)
        
    return com_obj


def with_events(obj, cls):
    return win32com.client.WithEvents(obj, cls)


if __name__ == '__main__':
    obj = get_com_obj("CpUtil.CpCodeMgr")
    print(obj.GetStockListByMarket(1))
    print('OK exit')