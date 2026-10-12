from collections import defaultdict
from dataclasses import dataclass
from operator import attrgetter
from typing import Any, Dict, List, Optional

from devtools_mcp.goap.action import Action
from devtools_mcp.goap.algo import AStarAlgorithm
from devtools_mcp.goap.planner.plan import PlanStep

# Regressive algo faster
# http://alumni.media.mit.edu/~jorkin/GOAP_draft_AIWisdom2_2003.pdf


State = Dict[str, Any]


class _NotDefined:
    def __repr__(self):
        return "NOT_DEFINED"


NOT_DEFINED = _NotDefined()


@dataclass(frozen=True)
class RegressiveGOAPAStarNode:
    current_state: State
    goal_state: State
    action: Optional[Action] = None

    def __hash__(self):
        return id(self)

    @property
    def services(self):
        return {k: self.current_state[k] for k in self.action.service_names}

    def compute_cost(self, other: "RegressiveGOAPAStarNode") -> float:
        # The action determines the cost, and is stored on the "destination" node
        # i.e. x, action --> y is encoded as (x, None), (y, action)
        return other.action.get_cost(other.services)

    def apply_action(self, world_state: State, action: Action) -> "RegressiveGOAPAStarNode":
        new_current_state = world_state.copy()

        for effect_key, effect_value in action.effects.items():
            if effect_value is Ellipsis:
                new_current_state[effect_key] = self.goal_state.get(effect_key, NOT_DEFINED)
            else:
                new_current_state[effect_key] = effect_value

        # Carry forward parent goals this action does not itself satisfy, then add
        # the action's own preconditions. Upstream reset the goal to just the
        # preconditions, which silently dropped conjunctive goals whose keys are
        # produced by separate actions (e.g. d needs b_done AND c_done).
        new_goal_state = {k: v for k, v in self.goal_state.items() if k not in action.effects}
        new_goal_state.update(action.preconditions)

        for prec_key in new_goal_state:
            if prec_key not in new_current_state:
                new_current_state[prec_key] = NOT_DEFINED

        return self.__class__(new_current_state, new_goal_state, action)

    @property
    def is_satisfied(self):
        return not self.unsatisfied_keys

    @property
    def unsatisfied_keys(self) -> List[str]:
        return [
            k
            for k, v in self.goal_state.items()
            # Allow keys not to be defined (e.g. for internal fields)
            if self.current_state[k] != v
        ]


_key_action_precedence = attrgetter("action.precedence")


class RegressiveGOAPAStarSearch(AStarAlgorithm):
    def __init__(self, world_state: State, actions: List[Action]):
        self._actions = actions
        self._effect_to_action = self._create_effect_table(actions)
        self._world_state = world_state

    def _create_effect_table(self, actions: List[Action]):
        effect_to_actions = defaultdict(list)

        for action in actions:
            for effect in action.effects:
                effect_to_actions[effect].append(action)

        effect_to_actions.default_factory = None
        return effect_to_actions

    def reconstruct_path(self, node, goal, parents):
        path = super().reconstruct_path(node, goal, parents)
        return reversed(path)

    def is_finished(self, node: "RegressiveGOAPAStarNode", goal, parents):
        return node.is_satisfied

    def get_neighbours(self, node: "RegressiveGOAPAStarNode"):
        neighbours = []

        for key in node.unsatisfied_keys:
            goal_value = node.goal_state[key]

            for action in self._effect_to_action[key]:
                effect_value = action.effects[key]
                # If this effect does not satisfy the goal
                satisfied = False
                if effect_value is Ellipsis:  # Ellipsis always satisfies
                    satisfied = True
                elif hasattr(goal_value, "goap_effect_reference"):  # goal_value is an EffectReference
                    # If goal is a reference, does this action's effect match the referenced name?
                    if key == goal_value.goap_effect_reference and effect_value:
                        satisfied = True
                elif effect_value == goal_value:  # Direct value match
                    satisfied = True

                if not satisfied:
                    continue

                # Compute neighbour after following action edge.
                neighbour = node.apply_action(self._world_state, action)

                # Ensure we can visit this node
                if not action.check_procedural_precondition(neighbour.services, is_planning=True):
                    continue

                neighbours.append(neighbour)
        neighbours.sort(key=_key_action_precedence, reverse=True)
        return neighbours

    def get_h_score(self, node: "RegressiveGOAPAStarNode"):
        return len(node.unsatisfied_keys)

    def get_g_score(self, node: "RegressiveGOAPAStarNode", neighbour: "RegressiveGOAPAStarNode"):
        return node.compute_cost(neighbour)


class RegressivePlanner:
    def __init__(self, world_state, actions):
        self._actions = actions
        self._world_state = world_state
        self._search = RegressiveGOAPAStarSearch(world_state, actions)

    @property
    def actions(self):
        return self._actions

    @property
    def world_state(self):
        return self._world_state

    def _create_plan_steps(self, path: List[RegressiveGOAPAStarNode]) -> List[PlanStep]:
        plan = []
        for node in path:
            if node.action is None:
                break
            plan.append(PlanStep(node.action, node.services))
        return plan

    def find_plan(self, goal_state: State) -> List[PlanStep]:
        # The "current state" of the start node in a regressive search represents
        # the world just before any actions run; the goal is the user's goal_state.
        start_node_current_state = self._world_state.copy()

        start = RegressiveGOAPAStarNode(
            current_state=start_node_current_state,
            goal_state=goal_state.copy(),
            action=None,
        )

        path = self._search.find_path(start, goal_state)
        return self._create_plan_steps(path)
