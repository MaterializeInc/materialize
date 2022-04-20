# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

from typing import Any, Callable, List

from materialize.mzcompose import Composition


class Executor:
    def Lambda(self, _lambda: Callable[["Executor"], float]) -> float:
        return _lambda(self)


class Docker(Executor):
    def __init__(
        self,
        composition: Composition,
        seed: int,
    ) -> None:
        self._composition = composition
        self._seed = seed

    def RestartMz(self) -> None:
        self._composition.kill("materialized")
        self._composition.up("materialized")
        return None

    def Td(self, input: str) -> Any:
        return self._composition.exec(
            "testdrive",
            "--no-reset",
            f"--seed={self._seed}",
            "--initial-backoff=10ms",
            "--backoff-factor=0",
            stdin=input,
            capture=True,
        ).stdout

    def Kgen(self, topic: str, args: List[str]) -> Any:
        return self._composition.run(
            "kgen", f"--topic=testdrive-{topic}-{self._seed}", *args
        )
