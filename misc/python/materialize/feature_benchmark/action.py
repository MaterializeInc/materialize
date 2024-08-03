# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

import textwrap
from collections.abc import Callable, Iterator

from materialize.feature_benchmark.executor import Executor


class Action:
    def __init__(self) -> None:
        self._executor: Executor | None = None

    def __iter__(self) -> Iterator[None]:
        return self

    def __next__(self) -> None:
        return self.run()

    def __call__(self, executor: Executor) -> "Action":
        self._executor = executor
        return self

    def run(
        self,
        executor: Executor | None = None,
    ) -> None:
        raise NotImplementedError


class LambdaAction(Action):
    def __init__(self, _lambda: Callable) -> None:
        self._lambda = _lambda

    def run(
        self,
        executor: Executor | None = None,
    ) -> None:
        e = executor or self._executor
        assert e is not None
        e.Lambda(self._lambda)
        return None


class Kgen(Action):
    def __init__(self, topic: str, args: list[str]) -> None:
        self._topic: str = topic
        self._args: list[str] = args
        self._executor: Executor | None = None

    def run(
        self,
        executor: Executor | None = None,
    ) -> None:
        executor = executor or self._executor
        assert executor
        executor.Kgen(topic=self._topic, args=self._args)


class TdAction(Action):
    """Use testdrive to run some queries without measuring"""

    def __init__(self, td_str: str, dedent: bool = True) -> None:
        self._td_str = textwrap.dedent(td_str) if dedent else td_str
        self._executor: Executor | None = None

    def run(
        self,
        executor: Executor | None = None,
    ) -> None:
        executor = executor or self._executor
        assert executor
        # Print each query once so that it is easier to reproduce regressions
        # based on just the logs from CI
        if executor.add_known_fragment(self._td_str):
            print(self._td_str)

        executor.Td(self._td_str)


class DummyAction(Action):
    def run(
        self,
        executor: Executor | None = None,
    ) -> None:
        return None
