# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""Error-message substrings produced when Materialize detects a negative
accumulation (or similar "non-positive multiplicity" condition).

Parallel Workload intentionally generates queries (via `repeat_row`) that can
trigger these errors; this module is the single place to update when the
messages change. Source of truth: database-issues#9308 (section "For reference,
here is a list of places in the code / error msgs where we detect negative
accumulations or similar issues").
"""

# Keep this list in sync with database-issues#9308. Each entry is a substring
# that may appear in an error surfaced to the client. When the underlying
# error messages are reworded, update this list in a single commit so the
# rest of Parallel Workload picks up the new strings automatically.
NEGATIVE_ACCUMULATION_ERRORS: list[str] = [
    # Many places
    "Non-monotonic input",
    # TopK
    "Negative multiplicities in TopK",
    # Reduce
    "Net-zero records with non-zero accumulation in ReduceAccumulable",
    "Non-positive multiplicity in DistinctBy",
    "Non-positive accumulation",
    "Invalid negative unsigned aggregation in ReduceAccumulable",
    "saw negative accumulation",
    # Peek handling
    "Invalid data in source, saw retractions",
    "index peek encountered negative multiplicities in ok trace",
    "index peek encountered negative multiplicities in error trace",
    # S3 oneshot sink
    "S3 oneshot sink encountered negative multiplicities",
    # Constant folding
    "Negative multiplicity in constant result",
    # Scalar subquery guard
    "negative number of rows produced in subquery",
]
