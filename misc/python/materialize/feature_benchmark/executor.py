# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

import subprocess
from typing import Any, Callable, List, Optional

from materialize.mzcompose import Composition
from materialize.ui import UIError


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
        kafka_addr: str,
        schema_registry_url: str,
    ) -> None:
        self._composition = composition
        self._seed = seed
        self._mzcloud_url = mzcloud_url
        self._kafka_addr = kafka_addr
        self._schema_registry_url = schema_registry_url

    def RestartMz(self) -> None:
        # We can't restart the cloud, so complain.
        assert False

    def Reset(self) -> None:
        print("resetting")
        self.exec(
            *[
                "target/debug/testdrive",
                f"--materialize-url={self._mzcloud_url}",
                f"--seed={self._seed}",
            ],
            stdin="",
        )

    def Td(self, input: str) -> Any:
        return self.exec(
            *[
                "target/debug/testdrive",
                f"--materialize-url={self._mzcloud_url}",
                f"--kafka-addr={self._kafka_addr}",
                f"--schema-registry-url={self._schema_registry_url}",
                "--no-reset",
                f"--seed={self._seed}",
                "--initial-backoff=10ms",
                "--backoff-factor=0",
                "--default-timeout=100s",
            ],
            stdin=input,
            capture=True,
        ).stdout

    def Kgen(self, topic: str, args: List[str]) -> Any:
        # TODO: Implement
        assert False

    def exec(
        self, *args: str, capture: bool = False, stdin: Optional[str] = None
    ) -> subprocess.CompletedProcess:
        """Invoke the command described by `args` and wait for it to complete.

        Args:
            capture: Whether to capture the child's stdout stream.
            input: A string to provide as stdin for the command.
        """

        stdout = None
        if capture:
            stdout = subprocess.PIPE

        try:
            return subprocess.run(
                [*args],
                close_fds=False,
                check=True,
                stdout=stdout,
                input=stdin,
                text=True,
            )
        except subprocess.CalledProcessError as e:
            print("args:", args)
            if e.stdout:
                print("stdout:", e.stdout)
            raise UIError(f"running exec failed (exit status {e.returncode})")
