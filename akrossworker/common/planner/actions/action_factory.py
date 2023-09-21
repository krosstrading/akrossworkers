from akrossworker.common.planner.actions.action_activation import ActionActivation
from akrossworker.common.planner.actions.action_buy import ActionBuy
from akrossworker.common.planner.actions.action_interface import ActionInterface
from akrossworker.common.planner.actions.action_precondition import ActionPrecondition
from akrossworker.common.planner.actions.action_sell import ActionSell
from akrossworker.common.planner.order.order_manager import OrderManager
from akrossworker.common.planner.plan_object import PlanObject


class ActionFactory:
    @classmethod
    def CreateActions(
        cls,
        plan_object: PlanObject,
        order_manager: OrderManager
    ) -> ActionInterface:
        head = ActionPrecondition(plan_object)
        activation = ActionActivation(plan_object)
        buy = ActionBuy(plan_object, order_manager)
        tail = ActionSell(plan_object, order_manager)

        head.set_next(activation)
        activation.set_next(buy)
        buy.set_next(tail)

        return head
