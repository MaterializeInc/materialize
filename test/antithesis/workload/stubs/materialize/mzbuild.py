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

from enum import Enum, auto


class Profile(Enum):
    # Mirror the value-shape (`auto()` → int) of the real
    # `materialize.mzbuild.Profile` so any caller that compared
    # `Profile.X.value == Y` keeps working against the stub.  Order
    # also mirrors upstream — irrelevant for current callers (which
    # only reference the enum as a type marker via
    # `UseOptimizedProfile`'s class attribute), but matters if a
    # future caller does enum-int comparison.
    RELEASE = auto()
    OPTIMIZED = auto()
    DEV = auto()
