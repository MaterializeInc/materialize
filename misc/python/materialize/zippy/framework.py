# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

import random
from typing import Dict, List, Optional, Set, Type, TypeVar, Union

from materialize.mzcompose import Composition


class Capability:
    """Base class for a Zippy capability.

    A capability represents a condition that is true about a Zippy test context,
    like "a table with name 'foo' exists".
    """

    name: str

    @classmethod
    def format_str(cls) -> str:
        assert False


T = TypeVar("T", bound="Capability")
ActionOrFactory = Union[Type["Action"], "ActionFactory"]


class Capabilities:
    """A set of `Capability`s."""

    def __init__(self) -> None:
        self._capabilities: List[Capability] = []

    def _extend(self, capabilities: List[Capability]) -> None:
        """Add new capabilities."""
        self._capabilities.extend(capabilities)

    def remove_capability_instance(self, capability: T) -> None:
        """Remove a specific capability."""

        self._capabilities = [
            cap for cap in self._capabilities if not cap == capability
        ]

    def _remove(self, capabilities: Set[Type[T]]) -> None:
        """Remove all existing capabilities of the specified types."""

        self._capabilities = [
            cap for cap in self._capabilities if type(cap) not in capabilities
        ]

    def provides(self, capability: Type[T]) -> bool:
        """Report whether any capability of the specified type exists."""
        return len(self.get(capability)) > 0

    def get(self, capability: Type[T]) -> List[T]:
        """Get all capabilities of the specified type."""
        return [cap for cap in self._capabilities if type(cap) == capability]

    def get_free_capability_name(
        self, capability: Type[T], max_objects: int
    ) -> Optional[str]:
        all_object_names = [
            capability.format_str().format(i) for i in range(0, max_objects)
        ]
        existing_object_names = [t.name for t in self.get(capability)]
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
    def requires(self) -> Union[Set[Type[Capability]], List[Set[Type[Capability]]]]:
        """Compute the capability classes that this action requires."""
        return set()

    def withholds(self) -> Set[Type[Capability]]:
        """Compute the capability classes that this action will make unavailable."""
        return set()

    def provides(self) -> List[Capability]:
        """Compute the capabilities that this action will make available."""
        return []

    def run(self, c: Composition) -> None:
        """Run this action on the provided copmosition."""
        assert False

    @classmethod
    def require_explicit_mention(self) -> bool:
        """Only use if explicitly mentioned by name in a Scenario."""
        return False

    def __str__(self) -> str:
        return f"--- #{self.seqno}: {self.__class__.__name__}"


class ActionFactory:
    """Base class for Action Factories that return parameterized Actions to execute."""

    def new(self, capabilities: Capabilities) -> List[Action]:
        assert False

    @classmethod
    def requires(self) -> Union[Set[Type[Capability]], List[Set[Type[Capability]]]]:
        """Compute the capability classes that this Action Factory requires."""
        return set()


class Scenario:
    def bootstrap(self) -> List[ActionOrFactory]:
        return []

    def config(self) -> Dict[ActionOrFactory, float]:
        assert False


class Test:
    """A Zippy test, consisting of a sequence of actions."""

    def __init__(self, scenario: Scenario, actions: int) -> None:
        """Generate a new Zippy test.

        Args:
            scenario: The Scenario to pick actions from.
            actions: The number of actions to generate.
        """
        self._scenario = scenario
        self._actions: List[Action] = []
        self._capabilities = Capabilities()
        self._config = self._scenario.config()

        for action_or_factory in self._scenario.bootstrap():
            self.append_actions(action_or_factory)

        while len(self._actions) < actions:
            action_or_factory = self._pick_action_or_factory()
            self.append_actions(action_or_factory)

    def append_actions(self, action_def: ActionOrFactory) -> None:
        if isinstance(action_def, ActionFactory):
            actions = action_def.new(capabilities=self._capabilities)
        elif issubclass(action_def, Action):
            actions = [action_def(capabilities=self._capabilities)]
        else:
            assert False

        for action in actions:
            self._actions.append(action)
            self._capabilities._extend(action.provides())
            self._capabilities._remove(action.withholds())

    def run(self, c: Composition) -> None:
        """Run the Zippy test."""
        for action in self._actions:
            print(action)
            action.run(c)

    def _pick_action_or_factory(self) -> ActionOrFactory:
        """Select the next Action to run in the Test"""
        actions_or_factories: List[ActionOrFactory] = []
        class_weights = []

        for action_or_factory in self._config.keys():
            alternatives = []

            # We do not drill down if it is an ActionFactory
            # If it is an Action, drill down for any children
            subclasses: List[ActionOrFactory] = (
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
                class_weights.append(
                    self._config[action_or_factory] / len(alternatives)
                )

        assert (
            len(actions_or_factories) > 0
        ), "No actions available to take. You may be stopping or deleting items without starting them again."

        return random.choices(actions_or_factories, weights=class_weights, k=1)[0]

    def _can_run(self, action: ActionOrFactory) -> bool:
        requires = action.requires()

        if isinstance(requires, Set):
            return all(self._capabilities.provides(req) for req in requires)
        else:
            for one_alternative in requires:
                if all(self._capabilities.provides(req) for req in one_alternative):
                    return True
            return False

    def _all_subclasses(self, cls: Type[Action]) -> List[ActionOrFactory]:
        """Return all Actions that are a subclass of the given cls."""
        children = [c for c in cls.__subclasses__() if not c.require_explicit_mention()]
        if len(children) == 0:
            return [cls]
        else:
            subclasses = []
            for c in children:
                subclasses.extend(self._all_subclasses(c))
            return subclasses
