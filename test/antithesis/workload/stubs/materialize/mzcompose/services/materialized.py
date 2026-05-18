# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""Stub of `materialize.mzcompose.services.materialized`. See package
__init__.py for context."""

from __future__ import annotations

from enum import Enum
from typing import Any

LEADER_STATUS_HEALTHCHECK: list[str] = []


class DeploymentStatus(Enum):
    READY_TO_PROMOTE = "ready_to_promote"
    IS_LEADER = "is_leader"


class Materialized:
    """Placeholder; only instantiated by `ZeroDowntimeDeployAction`."""

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        raise RuntimeError(
            "Materialized service stub: zero-downtime-deploy is not "
            "supported inside the Antithesis workload container."
        )
