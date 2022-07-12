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


class MzCloud(Executor):
    def __init__(
        self,
        composition: Composition,
        seed: int,
        mzcloud_url: str,
        external_addr: str,
    ) -> None:
        self._composition = composition
        self._seed = seed
        self._mzcloud_url = mzcloud_url
        self._external_addr = external_addr
        self._testdrive_args = [
            f"--materialize-url={self._mzcloud_url}",
            f"--kafka-addr={self._external_addr}:9092",
            f"--schema-registry-url=http://{self._external_addr}:8081",
            f"--seed={self._seed}",
        ]

    def RestartMz(self) -> None:
        # We can't restart the cloud, so complain.
        assert False

    def Reset(self) -> None:
        print("resetting")
        self._composition.exec(
            "testdrive",
            *self._testdrive_args,
            # Use a lower timeout so we complain if the mzcloud_url was wrong or inaccessible.
            "--default-timeout=10s",
            stdin="",
        )
        print("reset done")

    def Td(self, input: str) -> Any:
        return self._composition.exec(
            "testdrive",
            "--no-reset",
            *self._testdrive_args,
            "--initial-backoff=10ms",
            "--backoff-factor=0",
            stdin=input,
            capture=True,
        ).stdout

    def Kgen(self, topic: str, args: List[str]) -> Any:
        # TODO: Implement
        assert False
