# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

import time
from collections.abc import Callable
from textwrap import dedent
from typing import Any

from materialize.mzcompose.composition import Composition
from materialize.mzcompose.services.materialized import Materialized
from materialize.mzcompose.services.mysql import MySql


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

    def DockerMem(self) -> int:
        raise NotImplementedError

    def Messages(self) -> int | None:
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
            "--consistency-checks=disable",
            f"--var=mysql-root-password={MySql.DEFAULT_ROOT_PASSWORD}",
            stdin=input,
            capture=True,
        ).stdout

    def Kgen(self, topic: str, args: list[str]) -> Any:
        return self._composition.run(
            "kgen", f"--topic=testdrive-{topic}-{self._seed}", *args
        )

    def DockerMem(self) -> int:
        return self._composition.mem("materialized")

    def Messages(self) -> int | None:
        """Return the sum of all messages in the system from mz_internal.mz_message_counts_per_worker"""

        def one_count(e: Docker) -> int | None:
            result = e._composition.sql_query(
                dedent(
                    """
                    SELECT SUM(sent) as cnt
                    FROM
                        mz_internal.mz_message_counts_per_worker mc,
                        mz_internal.mz_dataflow_channel_operators_per_worker c
                    WHERE
                        c.id = mc.channel_id AND
                        c.worker_id = mc.from_worker_id AND
                        from_operator_id IN (
                            SELECT dod.id
                            FROM mz_internal.mz_dataflow_operator_dataflows dod
                            WHERE dod.dataflow_name NOT LIKE '%oneshot-select%'
                            AND dod.dataflow_name NOT LIKE '%subscribe%'
                        )
                    """
                )
            )
            if len(result) == 0:
                return None
            elif result[0][0] is None:
                return None
            else:
                return int(result[0][0])

        # Loop until the message count converges
        prev_count: int | None = None
        for i in range(50):
            new_count = one_count(self)
            if new_count is not None and prev_count is not None:
                pct = (max(prev_count, new_count) / min(prev_count, new_count)) - 1
                # It has converged
                if pct < 0.05 and i > 2:
                    return new_count

            # No message count data available
            if new_count is None and i > 2:
                return new_count

            prev_count = new_count
            time.sleep(0.1)

        return None


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
            "--consistency-checks=disable",
            stdin=input,
            capture=True,
        ).stdout

    def Kgen(self, topic: str, args: list[str]) -> Any:
        # TODO: Implement
        assert False
