# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""Stub of `materialize.mzcompose.composition`. See package __init__.py."""

from __future__ import annotations

from typing import Any


class Composition:
    """Placeholder type so that `Composition | None` annotations resolve.

    Every code path in `parallel_workload` that calls methods on a Composition
    is gated on `Scenario.{Kill,BackupRestore,ZeroDowntimeDeploy}` — none of
    which the Antithesis driver selects. Instantiating one in this
    environment is a programming error.
    """

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        raise RuntimeError(
            "materialize.mzcompose.composition.Composition is stubbed in the "
            "Antithesis workload image; Antithesis injects faults at the "
            "container layer, so docker-compose orchestration is unavailable."
        )
