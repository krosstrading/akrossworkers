from akrossworker.common.protocol import PlanItem, PlanItemLog, PlanItemStatus, PlanMutableColumn
from typing import Callable, Awaitable, List
from akross.common import aktime
from typing_extensions import Self


class PlanObject:
    Update = 'update'
    Add = 'add'

    def __init__(
        self,
        plan_item: PlanItem,
        cb: Callable[[str, List[str], Self], Awaitable[None]]
    ):
        self.plan_item = plan_item
        self.change_callback = cb

    async def notify_event(self, event_type: str, columns: List[str]):
        if self.change_callback:
            await self.change_callback(event_type, columns, self)

    async def add_log(self, log: PlanItemLog):
        self.plan_item.logs.append(log)
        await self.notify_event(PlanObject.Update, [PlanMutableColumn.Logs])

    async def change_status(self, new_status: str) -> None:
        if self.plan_item.status != new_status:
            self.plan_item.status = new_status
            self.notify_event(PlanObject.Update, [PlanMutableColumn.Status])

    def is_alive(self):
        if (
            self.plan_item.status == PlanItemStatus.Wait or
            self.plan_item.status == PlanItemStatus.Watch or
            self.plan_item.status == PlanItemStatus.Progress
        ):
            if self.plan_item.endTime > aktime.get_msec():
                return True
        return False

    async def cancel_plan(self) -> bool:
        if self.is_alive():
            await self.change_status(PlanItemStatus.Cancel)

    async def update_plan(self, plan_item: PlanItem):
        change_columns = []
        status_int = PlanItemStatus.ToInteger(self.plan_item.status)

        if status_int <= PlanItemStatus.IntWait:
            if plan_item.precondition != self.plan_item.precondition:
                self.plan_item.precondition = plan_item.precondition
                change_columns.append(PlanMutableColumn.Precondition)

            if plan_item.startTime != self.plan_item.startTime:
                self.plan_item.startTime = plan_item.startTime
                change_columns.append(PlanMutableColumn.StartTime)

            if plan_item.endTime != self.plan_item.endTime:
                self.plan_item.endTime = plan_item.endTime
                change_columns.append(PlanMutableColumn.EndTime)

            if plan_item.activation != self.plan_item.activation:
                self.plan_item.activation = plan_item.activation
                change_columns.append(PlanMutableColumn.Activation)

        elif status_int <= PlanItemStatus.IntWatch:
            if plan_item.amount != self.plan_item.amount:
                self.plan_item.amount = plan_item.amount
                change_columns.append(PlanMutableColumn.Amount)

            if plan_item.stopPrice != self.plan_item.stopPrice:
                self.plan_item.stopPrice = plan_item.stopPrice
                change_columns.append(PlanMutableColumn.StopPrice)

            if plan_item.buy != self.plan_item.buy:
                self.plan_item.buy = plan_item.buy
                change_columns.append(PlanMutableColumn.Buy)

            if plan_item.sell != self.plan_item.sell:
                self.plan_item.sell = plan_item.sell
                change_columns.append(PlanMutableColumn.Sell)

            # status only can be updated internally, not by external
        if len(change_columns) > 0:
            await self.notify_event(PlanObject.Update, change_columns)
