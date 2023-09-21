from akrossworker.common.planner.actions.action_interface import ActionInterface
from akrossworker.common.planner.plan_object import PlanObject
from akrossworker.common.protocol import (
    ActivationType,
    PlanItemStatus,
    PriceStreamProtocol
)
from akrossworker.common.args_constants import TickTimeType


class ActionActivation(ActionInterface):
    def __init__(self, plan_object: PlanObject):
        super().__init__(plan_object)
        self.current_time_type = TickTimeType.Unknown
        self.activated = False

    async def init(self):
        self.get_plan().change_status(PlanItemStatus.Watch)

    async def on_price_stream(self, stream: PriceStreamProtocol) -> None:
        if self.activated:
            super().on_price_stream(stream)
        else:
            if self.get_activation().when == ActivationType.OpenPrice:
                if self.current_time_type == TickTimeType.PreBid and stream.time_type == TickTimeType.Normal:
                    ranges = [float(self.get_activation().condition_first),
                              float(self.get_activation().condition_second)]
                    ranges.sort()
                    if ranges[0] <= float(stream.price) <= ranges[1]:
                        self.activated = True
                    else:
                        await self.get_plan().change_status(PlanItemStatus.Cancel)
            elif self.get_activation().when == ActivationType.MarketPrice:
                if stream.time_type == TickTimeType.Normal:
                    if self.is_in_time():
                        trigger = float(self.get_activation().condition_first)
                        upper_bound = float(stream.price) * 1.001
                        lower_bound = float(stream.price) * 0.999
                        if lower_bound <= trigger <= upper_bound:
                            self.activated = True
        self.current_time_type = stream.time_type
