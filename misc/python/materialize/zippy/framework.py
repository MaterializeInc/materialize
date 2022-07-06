# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

import random
from typing import Dict, List, Set, Type, TypeVar, Union

from materialize.mzcompose import Composition


class Capability:
    """Base class for a Zippy capability.

    A capability represents a condition that is true about a Zippy test context,
    like "a table with name 'foo' exists".
    """

    pass


T = TypeVar("T", bound="Capability")


class Capabilities:
    """A set of `Capability`s."""

    def __init__(self) -> None:
        self._capabilities: List[Capability] = []

    def _extend(self, capabilities: List[Capability]) -> None:
        """Add new capabilities."""
        self._capabilities.extend(capabilities)

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


class Action:
    """Base class for an action that a Zippy test can take."""

    def __init__(self, capabilities: Capabilities) -> None:
        """Construct a new action, possibly conditioning on the available
        capabilities."""
        pass

    @classmethod
    def requires(self) -> Union[Set[Type[Capability]], List[Set[Type[Capability]]]]:
        """Compute the capability classes that this action requires."""
        return set()

    def removes(self) -> Set[Type[Capability]]:
        """Compute the capability classes that this action will make unavailable."""
        return set()

    def provides(self) -> List[Capability]:
        """Compute the capabilities that this action will make available."""
        return []

    def run(self, c: Composition) -> None:
        """Run this action on the provided copmosition."""
        assert False


class Scenario:
    def config(self) -> Dict[Type[Action], float]:
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

        for i in range(0, actions):
            action_class = self._pick_action_class()
            action = action_class(capabilities=self._capabilities)
            self._actions.append(action)
            self._capabilities._extend(action.provides())
            self._capabilities._remove(action.removes())

    def run(self, c: Composition) -> None:
        """Run the Zippy test."""
        for action in self._actions:
            print(action)
            action.run(c)

    def _pick_action_class(self) -> Type[Action]:
        """Select the next Action to run in the Test"""
        action_classes = []
        class_weights = []

        for action_class in self._scenario.config().keys():
            for leaf in self._leaf_subclasses(action_class):
                # Do not pick an Action whose requirements can not be satisfied
                if self._can_run(leaf):
                    action_classes.append(leaf)
                    class_weights.append(self._scenario.config()[action_class])

        return random.choices(action_classes, weights=class_weights, k=1)[0]

    def _can_run(self, action: Type[Action]) -> bool:
        requires = action.requires()

        if isinstance(requires, Set):
            return all(self._capabilities.provides(req) for req in requires)
        else:
            for one_alternative in requires:
                if all(self._capabilities.provides(req) for req in one_alternative):
                    return True
            return False

    def _leaf_subclasses(self, cls: Type[Action]) -> List[Type[Action]]:
        """Return all Actions that are a subclass of the given cls."""
        leafs = []
        if len(cls.__subclasses__()) == 0:
            return [cls]

        class_list = [cls]
        while class_list:
            parent = class_list.pop()
            for child in parent.__subclasses__():
                if child not in leafs:
                    if len(child.__subclasses__()) == 0:
                        leafs.append(child)
                    else:
                        class_list.append(child)
        return leafs
