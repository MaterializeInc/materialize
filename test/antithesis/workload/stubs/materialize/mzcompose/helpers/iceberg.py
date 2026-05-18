# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""Stub of `materialize.mzcompose.helpers.iceberg`. See package __init__.py."""

from __future__ import annotations

from typing import Any


def setup_polaris_for_iceberg(c: Any, *args: Any, **kwargs: Any) -> tuple[str, str]:
    # `Database.create` calls this unconditionally. The Antithesis topology
    # does not run Polaris; the driver overrides `Database.create` to skip the
    # iceberg connection setup, so this function should never be reached.
    raise RuntimeError(
        "setup_polaris_for_iceberg() stub: iceberg sinks are not supported "
        "inside the Antithesis workload container."
    )
