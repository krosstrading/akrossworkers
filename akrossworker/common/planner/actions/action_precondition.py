from typing import List
from akrossworker.common.planner.actions.action_interface import ActionInterface
from akrossworker.common.planner.plan_object import PlanObject
from akrossworker.common.protocol import PlanItemStatus, PlanMutableColumn, PriceStreamProtocol


class ActionPrecondition(ActionInterface):
    """
    precondition is in wait status
    check following items
    1. startTime, endTime
    2. precondition
    """
    def __init__(self, plan_object: PlanObject):
        super().__init__(plan_object)
        self.pre_passed = False
        self.time_passed = False
        self.link_id = ''
        self.when = ''

        if plan_object.plan_item.precondition is not None:
            self.link_id = plan_object.plan_item.precondition.link_id
            self.when = plan_object.plan_item.precondition.when

        if len(self.link_id) == 0:
            self.pre_passed = True

    def check_in_time(self):
        if self.is_in_time():
            self.time_passed = True

    def met_condition(self) -> bool:
        if self.plan_status() != PlanItemStatus.Wait:
            return True
        elif not self.time_passed:
            self.check_in_time()
        return self.pre_passed and self.time_passed

    async def on_price_stream(self, stream: PriceStreamProtocol) -> None:
        if self.met_condition():
            await super().on_price_stream(stream)

    async def on_other_action_event(self, plan_id: str, status: str) -> None:
        if not self.pre_passed:
            if self.link_id == plan_id and self.when == status:
                self.pre_passed = True

    async def update_column(self, columns: List[str], plan_object: PlanObject) -> None:
        if self.plan_status() == PlanItemStatus.Wait:
            if PlanMutableColumn.Precondition in columns:
                if plan_object.plan_item.precondition is not None:
                    self.link_id = plan_object.plan_item.precondition.link_id
                    self.when = plan_object.plan_item.precondition.when
