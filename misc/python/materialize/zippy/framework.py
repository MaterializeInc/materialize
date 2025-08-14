# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

import random
from collections.abc import Sequence
from datetime import datetime, timedelta
from typing import TYPE_CHECKING, TypeVar, Union

from materialize.mzcompose import get_default_system_parameters
from materialize.mzcompose.composition import Composition

if TYPE_CHECKING:
    from materialize.zippy.scenarios import Scenario


class State:
    mz_service: str
    deploy_generation: int
    system_parameter_defaults: dict[str, str]

    def __init__(self):
        self.mz_service = "materialized"
        self.deploy_generation = 0
        self.system_parameter_defaults = get_default_system_parameters()


class Capability:
    """Base class for a Zippy capability.

    A capability represents a condition that is true about a Zippy test context,
    like "a table with name 'foo' exists".
    """

    name: str

    @classmethod
    def format_str(cls) -> str:
        raise NotImplementedError()


T = TypeVar("T", bound=Capability)
ActionOrFactory = Union[type["Action"], "ActionFactory"]


class Capabilities:
    """A set of `Capability`s."""

    _capabilities: Sequence[Capability]

    def __init__(self) -> None:
        self._capabilities = []

    def _extend(self, capabilities: Sequence[Capability]) -> None:
        """Add new capabilities."""
        new_capabilities = list(capabilities)
        self._capabilities = list(self._capabilities) + new_capabilities

    def remove_capability_instance(self, capability: Capability) -> None:
        """Remove a specific capability."""

        self._capabilities = [
            cap for cap in self._capabilities if not cap == capability
        ]

    def _remove(self, capabilities: set[type[T]]) -> None:
        """Remove all existing capabilities of the specified types."""

        self._capabilities = [
            cap for cap in self._capabilities if type(cap) not in capabilities
        ]

    def provides(self, capability: type[T]) -> bool:
        """Report whether any capability of the specified type exists."""
        return len(self.get(capability)) > 0

    def get(self, capability: type[T]) -> list[T]:
        """Get all capabilities of the specified type."""
        matches: list[T] = [
            # NOTE: unfortunately pyright can't handle this
            cap
            for cap in self._capabilities
            if type(cap) == capability  # type: ignore
        ]
        return matches

    def get_capability_names(self, capability: type[T]) -> list[str]:
        return [t.name for t in self.get(capability)]

    def get_free_capability_name(
        self, capability: type[T], max_objects: int
    ) -> str | None:
        all_object_names = [
            capability.format_str().format(i) for i in range(0, max_objects)
        ]
        existing_object_names = self.get_capability_names(capability)
        remaining_object_names = set(all_object_names) - set(existing_object_names)
        return (
            random.choice(list(remaining_object_names))
            if len(remaining_object_names) > 0
            else None
        )


class Action:
    """Base class for an action that a Zippy test can take."""

    current_seqno: int = 0

    def __init__(self, capabilities: Capabilities) -> None:
        """Construct a new action, possibly conditioning on the available
        capabilities."""
        Action.current_seqno = Action.current_seqno + 1
        self.seqno = Action.current_seqno
        pass

    @classmethod
    def requires(cls) -> set[type[Capability]] | list[set[type[Capability]]]:
        """Compute the capability classes that this action requires."""
        return set()

    @classmethod
    def incompatible_with(cls) -> set[type[Capability]]:
        """The capability classes that this action is not compatible with."""
        return set()

    def withholds(self) -> set[type[Capability]]:
        """Compute the capability classes that this action will make unavailable."""
        return set()

    def provides(self) -> list[Capability]:
        """Compute the capabilities that this action will make available."""
        return []

    def run(self, c: Composition, state: State) -> None:
        """Run this action on the provided composition."""
        raise NotImplementedError

    @classmethod
    def require_explicit_mention(cls) -> bool:
        """Only use if explicitly mentioned by name in a Scenario."""
        return False

    def __str__(self) -> str:
        return f"--- #{self.seqno}: {self.__class__.__name__}"


class Mz0dtDeployBaseAction(Action):
    pass


class ActionFactory:
    """Base class for Action Factories that return parameterized Actions to execute."""

    def new(self, capabilities: Capabilities) -> list[Action]:
        raise NotImplementedError

    @classmethod
    def requires(cls) -> set[type[Capability]] | list[set[type[Capability]]]:
        """Compute the capability classes that this Action Factory requires."""
        return set()

    @classmethod
    def incompatible_with(cls) -> set[type[Capability]]:
        """The capability classes that this action is not compatible with."""
        return set()


class Test:
    """A Zippy test, consisting of a sequence of actions."""

    def __init__(
        self, scenario: "Scenario", actions: int, max_execution_time: timedelta
    ) -> None:
        """Generate a new Zippy test.

        Args:
            scenario: The Scenario to pick actions from.
            actions: The number of actions to generate.
        """
        self._scenario = scenario
        self._actions: list[Action] = []
        self._final_actions: list[Action] = []
        self._capabilities = Capabilities()
        self._actions_with_weight: dict[ActionOrFactory, float] = (
            self._scenario.actions_with_weight()
        )
        self._state = State()
        self._max_execution_time: timedelta = max_execution_time

        for action_or_factory in self._scenario.bootstrap():
            self._actions.extend(self.generate_actions(action_or_factory))

        while len(self._actions) < actions:
            action_or_factory = self._pick_action_or_factory()
            self._actions.extend(self.generate_actions(action_or_factory))

        for action_or_factory in self._scenario.finalization():
            self._final_actions.extend(self.generate_actions(action_or_factory))

    def generate_actions(self, action_def: ActionOrFactory) -> list[Action]:
        if isinstance(action_def, ActionFactory):
            actions = action_def.new(capabilities=self._capabilities)
        elif issubclass(action_def, Action):
            actions = [action_def(capabilities=self._capabilities)]
        else:
            raise RuntimeError(
                f"{type(action_def)} is not a subclass of {ActionFactory} or {Action}"
            )

        for action in actions:
            print("test:", action)
            self._capabilities._extend(action.provides())
            print(" - ", self._capabilities, action.provides())
            self._capabilities._remove(action.withholds())
            print(" - ", self._capabilities, action.withholds())

        return actions

    def run(self, c: Composition) -> None:
        """Run the Zippy test."""
        max_time = datetime.now() + self._max_execution_time
        for action in self._actions:
            print(action)
            action.run(c, self._state)
            if datetime.now() > max_time:
                print(
                    f"--- Desired execution time of {self._max_execution_time} has been reached."
                )
                break

        for action in self._final_actions:
            print(action)
            action.run(c, self._state)

    def _pick_action_or_factory(self) -> ActionOrFactory:
        """Select the next Action to run in the Test"""
        actions_or_factories: list[ActionOrFactory] = []
        class_weights = []

        for action_or_factory in self._actions_with_weight.keys():
            alternatives = []

            # We do not drill down if it is an ActionFactory
            # If it is an Action, drill down for any children
            subclasses: list[ActionOrFactory] = (
                [action_or_factory]
                if isinstance(action_or_factory, ActionFactory)
                else self._all_subclasses(action_or_factory)
            )

            for subclass in subclasses:
                # Do not pick an Action whose requirements can not be satisfied
                if self._can_run(subclass):
                    alternatives.append(subclass)

            for alternative in alternatives:
                actions_or_factories.append(alternative)
                weight = self._actions_with_weight[action_or_factory]
                class_weights.append(weight / len(alternatives))

        assert (
            len(actions_or_factories) > 0
        ), "No actions available to take. You may be stopping or deleting items without starting them again."

        return random.choices(actions_or_factories, weights=class_weights, k=1)[0]

    def _can_run(self, action: ActionOrFactory) -> bool:
        if any(
            self._capabilities.provides(dislike)
            for dislike in action.incompatible_with()
        ):
            return False

        requires = action.requires()
        if isinstance(requires, set):
            return all(self._capabilities.provides(req) for req in requires)
        else:
            for one_alternative in requires:
                if all(self._capabilities.provides(req) for req in one_alternative):
                    return True
            return False

    def _all_subclasses(self, cls: type[Action]) -> list[ActionOrFactory]:
        """Return all Actions that are a subclass of the given cls."""
        children = [c for c in cls.__subclasses__() if not c.require_explicit_mention()]
        if len(children) == 0:
            return [cls]
        else:
            subclasses = []
            for c in children:
                subclasses.extend(self._all_subclasses(c))
            return subclasses
