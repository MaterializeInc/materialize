# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

from typing import Any, Callable, List, Set

from materialize.mzcompose import Composition
from materialize.mzcompose.services import Materialized


class Executor:
    _known_fragments: Set[str] = set()

    def Lambda(self, _lambda: Callable[["Executor"], float]) -> float:
        return _lambda(self)

    def Td(self, input: str) -> Any:
        raise NotImplementedError

    def Kgen(self, topic: str, args: List[str]) -> Any:
        raise NotImplementedError

    def add_known_fragment(self, fragment: str) -> bool:
        """
        Record whether a TD fragment has been printed already. Returns true
        if it wasn't added before.
        """
        result = fragment not in self._known_fragments
        self._known_fragments.add(fragment)
        return result

    def DockerMem(self) -> int:
        raise NotImplementedError


class Docker(Executor):
    def __init__(
        self, composition: Composition, seed: int, materialized: Materialized
    ) -> None:
        self._composition = composition
        self._seed = seed
        self._materialized = materialized

    def RestartMz(self) -> None:
        self._composition.kill("materialized")
        # Make sure we are restarting Materialized() with the
        # same parameters (docker tag, SIZE) it was initially started with
        with self._composition.override(self._materialized):
            self._composition.up("materialized")
        return None

    def Td(self, input: str) -> Any:
        return self._composition.exec(
            "testdrive",
            "--no-reset",
            f"--seed={self._seed}",
            "--initial-backoff=10ms",  # Retry every 10ms until success
            "--backoff-factor=0",
            stdin=input,
            capture=True,
        ).stdout

    def Kgen(self, topic: str, args: List[str]) -> Any:
        return self._composition.run(
            "kgen", f"--topic=testdrive-{topic}-{self._seed}", *args
        )

    def DockerMem(self) -> int:
        return self._composition.mem("materialized")


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
