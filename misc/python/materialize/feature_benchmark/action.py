# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

from typing import Callable, Iterator, List, Optional

from materialize.feature_benchmark.executor import Executor


class Action:
    def __init__(self) -> None:
        self._executor: Optional[Executor] = None

    def __iter__(self) -> Iterator[None]:
        return self

    def __next__(self) -> None:
        return self.run()

    def __call__(self, executor: Executor) -> "Action":
        self._executor = executor
        return self

    def run(
        self,
        executor: Optional[Executor] = None,
    ) -> None:
        assert False


class LambdaAction(Action):
    def __init__(self, _lambda: Callable) -> None:
        self._lambda = _lambda

    def run(
        self,
        executor: Optional[Executor] = None,
    ) -> None:
        e = executor if executor else self._executor
        assert e is not None
        e.Lambda(self._lambda)
        return None


class Kgen(Action):
    def __init__(self, topic: str, args: List[str]) -> None:
        self._topic: str = topic
        self._args: List[str] = args
        self._executor: Optional[Executor] = None

    def run(
        self,
        executor: Optional[Executor] = None,
    ) -> None:
        getattr((executor if executor else self._executor), "Kgen")(
            topic=self._topic, args=self._args
        )


class TdAction(Action):
    """Use testdrive to run some queries without measuring"""

    def __init__(self, td_str: str) -> None:
        self._td_str = td_str
        self._executor: Optional[Executor] = None

    def run(
        self,
        executor: Optional[Executor] = None,
    ) -> None:
        getattr((executor if executor else self._executor), "Td")(self._td_str)


class DummyAction(Action):
    def run(
        self,
        executor: Optional[Executor] = None,
    ) -> None:
        return None
