# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""Stub of `materialize.cloudtest.app.materialize_application`.

`materialize.checks.executors.CloudtestExecutor` keeps a
`MaterializeApplication` reference; the Antithesis workload doesn't drive
the cloudtest path, so the symbol just needs to exist as an opaque type.
"""

from __future__ import annotations

from typing import Any


class MaterializeApplication:
    """Opaque placeholder. Instantiating it under Antithesis is a programming
    error — the cloudtest harness isn't available in this image."""

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        raise RuntimeError(
            "MaterializeApplication is stubbed in the Antithesis workload image; "
            "the cloudtest path is not available here."
        )
