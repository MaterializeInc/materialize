# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""Stub of `materialize.buildkite` for the Antithesis workload image.

`materialize.checks.checks` imports `buildkite.get_var` +
`BuildkiteEnvVar` at module load time so its `is_running_as_cloudtest`
helper can branch on the running step.  Antithesis isn't Buildkite; the
real module pulls in `requests`, `cargo`, `git`, `rustc_flags`, `xcompile`
and others that we don't ship into the workload image.

This stub exposes just the two names the Check base class touches at
load time, both as no-ops — `is_running_as_cloudtest()` is never True
under Antithesis, which is exactly what `get_var` returning the default
ensures.
"""

from __future__ import annotations

from enum import Enum
from typing import Any


class BuildkiteEnvVar(Enum):
    """Stub matching the relevant subset of the real `BuildkiteEnvVar`.

    Only the values `materialize.checks.checks.is_running_as_cloudtest`
    references at module load are listed.  Anything else surfaced via
    `BuildkiteEnvVar.<name>` would be an unresolved-name error rather
    than a silent miss, which is what we want.
    """

    BUILDKITE_STEP_KEY = "BUILDKITE_STEP_KEY"


def get_var(name: Any, default: str = "") -> str:
    return default


def shard_list(items: list[Any], _key: Any | None = None) -> list[Any]:
    """No-op shard helper used by the upstream platform-checks runner.

    The Antithesis driver doesn't shard — each timeline picks one Check
    at random — but keeping the symbol available means future code that
    imports it (e.g. a future `materialize.checks.scenarios` user) won't
    break on transitive imports.
    """
    return items


def get_parallelism_index() -> int:
    return 0


def get_parallelism_count() -> int:
    return 1
