# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""Stub of `materialize.mzbuild` for the Antithesis workload image.

`materialize.checks.actions` imports `Profile` at module load time so
`UseOptimizedProfile` (an Action subclass) can hold a Profile reference.
That Action is never executed under Antithesis — the workload image
doesn't even ship the mzbuild toolchain — but the module-level import
still has to resolve.

The real `materialize.mzbuild` pulls in `requests`, `cargo`, `git`,
`rustc_flags`, `xcompile` and a few thousand lines of build orchestration.
This stub exposes only the `Profile` enum.
"""

from __future__ import annotations

from enum import Enum


class Profile(Enum):
    DEV = "dev"
    OPTIMIZED = "optimized"
    RELEASE = "release"
