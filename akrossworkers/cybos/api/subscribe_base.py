from abc import abstractmethod
from akrossworkers.cybos.api import com_obj

import logging


LOGGER = logging.getLogger(__name__)


class _CpEvent:
    def set_params(self, callback):
        self.callback = callback

    def OnReceived(self):
        self.callback()


class SubscribeBase:
    def __init__(
        self,
        time_type: str,
        subscribe_type: str,
        code: str,
        exchange_name: str,
        callback,
        objname,
        inputs: list
    ):
        self.time_type = time_type
        self.subscribe_type = subscribe_type
        self.code = code
        self.exchange_name = exchange_name
        self.callback = callback
        self.started = False
        self.obj = com_obj.get_com_obj(objname)
        self.handler = com_obj.with_events(self.obj, _CpEvent)
        for i, value in enumerate(inputs):
            self.obj.SetInputValue(i, value)
        self.handler.set_params(self.OnReceived)

    def start_subscribe(self):
        if not self.started:
            self.started = True
            self.obj.Subscribe()
            LOGGER.info('start subscribe(%s, %s) %s',
                        self.subscribe_type, self.time_type, self.code)

    def stop_subscribe(self):
        if self.started:
            self.started = False
            self.obj.Unsubscribe()
            LOGGER.info('stop subscribe %s', self.code)

    @abstractmethod
    def eventToData(self, obj):
        return None

    def OnReceived(self):
        result = self.eventToData(self.obj)
        if result:
            self.callback(self.code, self.exchange_name, result)
