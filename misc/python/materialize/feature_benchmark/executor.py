# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

import os
import subprocess
from tempfile import NamedTemporaryFile
from typing import Any, Callable, List

from materialize.mzcompose import Composition


class Executor:
    def Lambda(self, _lambda: Callable[["Executor"], float]) -> float:
        return _lambda(self)


class Local(Executor):
    def __init__(self, seed: int = 1) -> None:
        self._seed = seed

    def Td(self, input: str) -> Any:
        with NamedTemporaryFile(
            mode="w", prefix="feature-benchmark-", suffix=".td"
        ) as td_file:
            td_file.write(input)
            td_file.flush()
            dirname, basename = os.path.split(td_file.name)
            return subprocess.check_output(
                " ".join(
                    [
                        "target/release/testdrive",
                        "--no-reset",
                        "--initial-backoff=0ms",
                        "--backoff-factor=0",
                        "--default-timeout 30s",
                        f"tmp/{basename}",
                    ]
                ),
                shell=True,
                universal_newlines=True,
            )


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
        with NamedTemporaryFile(
            mode="w",
            dir=self._composition.path / "tmp",
            prefix="tmp-",
            suffix=".td",
        ) as td_file:
            td_file.write(input)
            td_file.flush()
            dirname, basename = os.path.split(td_file.name)
            return self._composition.run(
                "testdrive-svc",
                "--no-reset",
                f"--seed={self._seed}",
                "--initial-backoff=10ms",
                "--backoff-factor=0",
                f"tmp/{basename}",
                capture=True,
            ).stdout

    def Kgen(self, topic: str, args: List[str]) -> Any:
        return self._composition.run(
            "kgen", f"--topic=testdrive-{topic}-{self._seed}", *args
        )
