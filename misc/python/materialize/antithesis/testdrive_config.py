# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""Shared `testdrive` invocation config for Antithesis workload helpers.

Both `materialize.antithesis.upsert_sources` (the upsert-source prototype) and
`materialize.antithesis.td_runner` (the generic .td file runner) construct the
same set of `testdrive` arguments from the environment. This module owns that
construction so the two paths can't drift.

The corresponding env vars are populated by the test-driver service block in
`antithesis/configs/<scenario>/mzcompose.py`.
"""

from __future__ import annotations

import os
import shlex
from dataclasses import dataclass


@dataclass(frozen=True)
class TestdriveConfig:
    """Connection details and default flags for invoking `testdrive`.

    Constructed via `TestdriveConfig.from_env()` so the test-driver container
    can override defaults through its environment block without touching code.
    """

    testdrive_bin: str
    materialize_url: str
    materialize_internal_url: str
    kafka_addr: str
    schema_registry_url: str
    default_timeout: str
    seed: str | None
    extra_args: tuple[str, ...]

    @classmethod
    def from_env(cls) -> TestdriveConfig:
        return cls(
            testdrive_bin=os.environ.get("MZ_TESTDRIVE", "testdrive"),
            materialize_url=os.environ.get(
                "MZ_MATERIALIZE_URL",
                "postgres://materialize@materialized:6875",
            ),
            materialize_internal_url=os.environ.get(
                "MZ_MATERIALIZE_INTERNAL_URL",
                "postgres://mz_system@materialized:6877",
            ),
            kafka_addr=os.environ.get("MZ_KAFKA_ADDR", "kafka:9092"),
            schema_registry_url=os.environ.get(
                "MZ_SCHEMA_REGISTRY_URL", "http://schema-registry:8081"
            ),
            default_timeout=os.environ.get("MZ_TESTDRIVE_DEFAULT_TIMEOUT", "60s"),
            seed=os.environ.get("MZ_ANTITHESIS_SEED"),
            extra_args=tuple(
                shlex.split(os.environ.get("MZ_TESTDRIVE_EXTRA_ARGS", ""))
            ),
        )

    def base_command(self, *, no_reset: bool, source: str) -> list[str]:
        """Build the testdrive argv prefix shared by all callers.

        Caller is expected to append per-call args (e.g. --var=k=v) and the
        .td path or --no-script-input usage as appropriate.

        `--backoff-factor=1` keeps testdrive's retry budget short; the
        reasoning is that under active Antithesis fault injection we want
        testdrive to surface failures quickly instead of masking them with
        long internal retries. The Antithesis-side wrapper decides whether
        and how to retry.
        """
        cmd = [
            self.testdrive_bin,
            f"--kafka-addr={self.kafka_addr}",
            f"--schema-registry-url={self.schema_registry_url}",
            f"--materialize-url={self.materialize_url}",
            f"--materialize-internal-url={self.materialize_internal_url}",
            f"--default-timeout={self.default_timeout}",
            "--backoff-factor=1",
            f"--source={source}",
            *self.extra_args,
        ]
        if self.seed is not None:
            cmd.append(f"--seed={self.seed}")
        if no_reset:
            cmd.append("--no-reset")
        return cmd
