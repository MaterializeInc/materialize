# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

from collections.abc import Callable
from typing import Any

from materialize.mzcompose.composition import Composition
from materialize.mzcompose.services.clusterd import Clusterd
from materialize.mzcompose.services.materialized import Materialized
from materialize.mzcompose.services.mysql import MySql
from materialize.mzcompose.services.sql_server import SqlServer


class Executor:
    _known_fragments: set[str] = set()

    def Lambda(self, _lambda: Callable[["Executor"], float]) -> float:
        return _lambda(self)

    def Td(self, input: str) -> Any:
        raise NotImplementedError

    def Kgen(self, topic: str, args: list[str]) -> Any:
        raise NotImplementedError

    def add_known_fragment(self, fragment: str) -> bool:
        """
        Record whether a TD fragment has been printed already. Returns true
        if it wasn't added before.
        """
        result = fragment not in self._known_fragments
        self._known_fragments.add(fragment)
        return result

    def DockerMemMz(self) -> int:
        raise NotImplementedError

    def DockerMemClusterd(self) -> int:
        raise NotImplementedError

    def RestartClusterdJoin(self) -> None:
        raise NotImplementedError


class Docker(Executor):
    def __init__(
        self,
        composition: Composition,
        seed: int,
        materialized: Materialized,
        clusterd: Clusterd,
        clusterd_join: Clusterd | None = None,
    ) -> None:
        self._composition = composition
        self._seed = seed
        self._materialized = materialized
        self._clusterd = clusterd
        self._clusterd_join = clusterd_join

    def RestartMzClusterd(self) -> None:
        self._composition.kill("materialized")
        self._composition.kill("clusterd")
        if self._clusterd_join is not None:
            self._composition.kill("clusterd_join")
        # Make sure we are restarting Materialized() with the
        # same parameters (docker tag, SIZE) it was initially started with
        overrides = [self._materialized, self._clusterd]
        if self._clusterd_join is not None:
            overrides.append(self._clusterd_join)
        with self._composition.override(*overrides):
            self._composition.up("materialized")
            self._composition.up("clusterd")
            if self._clusterd_join is not None:
                self._composition.up("clusterd_join")
        return None

    def RestartClusterdJoin(self) -> None:
        """Recreate the `clusterd_join` container so the next unmanaged
        replica created on it gets a fresh process: no allocator reuse,
        leftover swap, or stale scratch-directory spill files from the
        previous iteration. `rm` (not just `kill` + `up`) because a killed
        container keeps its filesystem, including the scratch directory."""
        assert self._clusterd_join is not None
        self._composition.kill("clusterd_join")
        self._composition.rm("clusterd_join")
        with self._composition.override(self._clusterd_join):
            self._composition.up("clusterd_join")
        return None

    def Td(self, input: str) -> Any:
        return self._composition.exec(
            "testdrive",
            "--no-reset",
            f"--seed={self._seed}",
            "--initial-backoff=10ms",  # Retry every 10ms until success
            "--backoff-factor=0",
            "--consistency-checks=disable",
            f"--var=mysql-root-password={MySql.DEFAULT_ROOT_PASSWORD}",
            f"--var=sql-server-sa-password={SqlServer.DEFAULT_SA_PASSWORD}",
            stdin=input,
            capture=True,
        ).stdout

    def Kgen(self, topic: str, args: list[str]) -> Any:
        return self._composition.run(
            "kgen", f"--topic=testdrive-{topic}-{self._seed}", *args
        )

    def DockerMemMz(self) -> int:
        return self._composition.mem("materialized")

    def DockerMemClusterd(self) -> int:
        mem = self._composition.mem("clusterd")
        if self._clusterd_join is not None:
            mem += self._composition.mem("clusterd_join")
        return mem


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
            f"--var=mysql-root-password={MySql.DEFAULT_ROOT_PASSWORD}",
        ]

    def RestartMzClusterd(self) -> None:
        assert False, "We can't restart the cloud"

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
            "--consistency-checks=disable",
            stdin=input,
            capture=True,
        ).stdout

    def Kgen(self, topic: str, args: list[str]) -> Any:
        # TODO: Implement
        raise NotImplementedError
