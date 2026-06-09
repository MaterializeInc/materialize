# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""Stub of `materialize.checks.executors` for the Antithesis workload image.

The real module imports `MaterializeApplication`, `Composition`, and
`SqlServer` at module top, then defines three Executor subclasses
(`MzcomposeExecutor`, `MzcomposeExecutorParallel`, `CloudtestExecutor`)
whose constructors expect those types.  The Antithesis platform-checks
driver constructs Check instances directly to read their testdrive
fragments — it never invokes the Executor protocol — so we only need
the `Executor` base type to satisfy `materialize.checks.checks`'
`_can_run(self, e: Executor)` signature.

Shipping a tiny stub here (rather than copying the real module) keeps
the workload image free of the cloudtest + mzcompose imports.
"""

from __future__ import annotations

from typing import Any


class Executor:
    """Sentinel base type matching the real `materialize.checks.executors.Executor`.

    Check subclasses' `_can_run(self, e: Executor)` callers in the
    upstream platform-checks runner pass a concrete Executor instance;
    the Antithesis driver passes `Executor()` so the type-check passes.
    No methods are called on the instance — every Check that overrides
    `_can_run` consults only `e.current_mz_version`, which the driver
    sets as a class attribute below.
    """

    current_mz_version: Any = None
    system_settings: set[str] = set()
