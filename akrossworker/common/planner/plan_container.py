import logging
from typing import Dict, List
from akrossworker.common.command import PlannerApiCommand
from akrossworker.common.db import DBEnum, Database
from akrossworker.common.planner.plan_object import PlanObject
from akrossworker.common.protocol import PlanItem
from akross.connection.aio.planner_channel import PlannerChannel


LOGGER = logging.getLogger(__name__)


class PlanItemListener:
    async def on_item_changed(self, event_type: str, columns: List[str], plan_object: PlanObject):
        pass


class PlanContainer:
    def __init__(self, channel: PlannerChannel):
        self.db = Database()
        self.channel = channel
        self.plans: Dict[str, PlanObject] = {}
        self.listener: PlanItemListener = None

    async def load(self):
        plans = await self.db.get_data(DBEnum.PLANNER_DB, 'plans', {})
        for plan in plans:
            await self.add_plan(PlanItem.ParseNetwork(plan))

    async def on_plan_changed(self, event_type: str, columns: List[str], plan_object: PlanObject) -> None:
        if self.listener is not None:
            LOGGER.warning('plan changed %s, %s', event_type, columns)
            await self.listener.on_item_changed(event_type, columns, plan_object)
            await self.channel.send_event(
                PlannerApiCommand.PlanUpdated,
                {
                    'eventType': event_type,
                    'columns': columns,
                    'plan': plan_object.plan_item.to_network()
                }
            )

    async def add_plan(self, plan_item: PlanItem) -> None:
        obj = PlanObject(plan_item, self.on_plan_changed)
        self.plans[plan_item.planId] = obj
        await self.on_plan_changed(PlanObject.Add, [], obj)

    def set_listener(self, listener: PlanItemListener):
        self.listener = listener

    def get_all(self):
        result = []
        for plan in self.plans.values():
            result.append(plan.plan_item.to_network())
        return result

    async def update_plan(self, plan_id: str, plan_item: PlanItem) -> None:
        if plan_id in self.plans:
            await self.plans[plan_id].update_plan(plan_item)

    async def cancel_plan(self, plan_id: str) -> None:
        if plan_id in self.plans:
            await self.plans[plan_id].cancel_plan()
