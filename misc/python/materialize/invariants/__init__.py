# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""Invariant-checking workload framework.

Runs fine-grained scenarios whose actions preserve a checkable invariant
regardless of whether each individual action succeeds, fails, or has an
unknown outcome (e.g. a bank transfer that debits one account and credits
another keeps the total constant either way). Invariants are verified
continuously by checker threads while toxiproxy disruptions are injected on
the envd<->clusterd, envd<->metadata-store, source, and sink connections,
plus once more strictly after healing.

Unlike parallel-workload (which hunts panics and unexpected errors) this
framework verifies result *correctness*. Unlike zippy it is multi-threaded
and checks invariants during disruptions, not only after quiescing.

Entry point: test/invariants/mzcompose.py.
"""
