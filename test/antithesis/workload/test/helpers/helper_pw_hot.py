# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""Shared constants for the hot-objects parallel-workload driver.

`parallel_driver_pw_hot_objects` and `first_pw_hot_objects_setup` both
reference a small fixed set of objects in `materialize.public` that
every invocation of the hot driver writes to and reads from. Centralised
here so the setup and the driver stay in lockstep — renaming, for
example, the MV is a one-line change rather than two-file editing.

Why hot objects exist: see `test/antithesis/scratchbook/property-catalog.md`
entries `peek-no-since-violation` and `prepared-execute-no-since-violation`.
Short version: the explicit-txn driver and the `parallel-workload` driver
both fragment workload state per-invocation, which dilutes per-object
contention to roughly one quarter of CI nightly's level. Bugs like
database-issues#11200 need continuous frontier advance on a single MV
across many concurrent peek paths to surface; the hot objects exist to
concentrate that pressure.
"""

from __future__ import annotations

# All hot objects live in the default database+schema so every driver
# invocation finds them at the same fully-qualified name. They are
# **never dropped** by any driver — they belong to the timeline, not
# any single invocation.
CLUSTER = "antithesis_cluster"
TABLE_NAME = "pw_hot_table"
MV_COUNT_NAME = "pw_hot_mv_count"
MV_SUM_NAME = "pw_hot_mv_sum"
INDEX_NAME = "pw_hot_idx_count"

# Time-based pruning window. The hot driver issues
#   DELETE FROM pw_hot_table WHERE ts < now() - INTERVAL '5 minutes'
# at a low rate so the table doesn't grow unboundedly across long
# Antithesis runs. The retention window also doubles as a "how stale
# can a stored read hold get" knob — larger window = longer-lived rows
# = bigger SinceViolation window if the bug fires.
RETENTION = "INTERVAL '5 minutes'"

# Error-message patterns that classify a peek failure as
# "the read-hold-was-downgraded-under-us bug" vs anything else. Lifted
# out of the driver so all consumers can stay in sync if Materialize
# changes its error text.
#
# Pulled from the bug surface:
#   * src/compute-client/src/controller.rs:788, 892 — SinceViolation(...)
#   * src/storage-types/src/read_holds.rs:143    — ReadHoldDowngradeError::SinceViolation
#   * src/persist-client/src/read.rs:680        — "since of our read handle is merely"
#                                                  (database-issues#9510)
#   * adapter error path                         — "insufficient read holds",
#                                                  "as_of not beyond since",
#                                                  "peek timestamp is not beyond the since"
SINCE_VIOLATION_PATTERNS = (
    "sinceviolation",
    "as_of not beyond",
    "as_of of",
    "since of our read handle is merely",
    "insufficient read holds",
    "dataflow has an as_of not beyond",
    "peek timestamp is not beyond the since",
)

# Fault-injection-shape matching lives in `helper_fault_tolerance` —
# the canonical list shared across every Antithesis driver. We expose
# `is_transient` here as a thin alias so callers in this module don't
# need to import from two helpers.
from helper_fault_tolerance import looks_like_fault as is_transient


def matches_any(msg: str, patterns: tuple[str, ...]) -> bool:
    """Case-insensitive substring search across a tuple of patterns."""
    lo = msg.lower()
    return any(p in lo for p in patterns)


def is_since_violation(msg: str) -> bool:
    return matches_any(msg, SINCE_VIOLATION_PATTERNS)
