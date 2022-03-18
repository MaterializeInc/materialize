# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

import random
from typing import List, Set, Type, TypeVar

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
    def requires(self) -> Set[Type[Capability]]:
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


class Test:
    """A Zippy test, consisting of a sequence of actions."""

    def __init__(
        self, action_classes: List[Type[Action]], max_actions: int = 20
    ) -> None:
        """Generate a new Zippy test.

        Args:
            action_classes: The set of actions to choose from.
            max_actions: The number of actions to generate.
        """
        self._actions: List[Action] = []
        capabilities = Capabilities()
        for i in range(0, max_actions):
            action_class = random.choice(
                [
                    action
                    for action in action_classes
                    if all(capabilities.provides(req) for req in action.requires())
                ]
            )
            action = action_class(capabilities)
            self._actions.append(action)
            capabilities._extend(action.provides())
            capabilities._remove(action.removes())

    def run(self, c: Composition) -> None:
        """Run the Zippy test."""
        for action in self._actions:
            action.run(c)
