# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.


from __future__ import annotations

from materialize.mz_version import MzVersion

"""
Git revisions that are based on commits listed as keys require at least the version specified in the value.
Note that specified versions do not necessarily need to be already published.
Commits must be ordered descending by their date.
"""
_MIN_ANCESTOR_MZ_VERSION_PER_COMMIT_TO_ACCOUNT_FOR_PERFORMANCE_REGRESSIONS: dict[
    str, MzVersion
] = {
    # insert newer commits at the top
    # PR#25502 (JoinFusion across MFPs) increased number of messages
    "67dbc308e53a90df7a9ed520db54468ed98541ee": MzVersion.parse_mz("v0.91.0"),
    # PR#24906 (Compute operator hydration status logging) increased number of messages against v0.88.1
    "067ae870eef724f7eb5851b5745b9ff52b881481": MzVersion.parse_mz("v0.89.0"),
    # PR#24918 (persist-txn: switch to a new operator protocol for lazy) increased number of messages against v0.86.1 (but got reverted in 0.87.1)
    "b648576b52b8ba9bb3a4732f7022ab5c06ebed32": MzVersion.parse_mz("v0.87.0"),
    # PR#23659 (persist-txn: enable in CI with "eager uppers") introduces regressions against v0.79.0
    "c4f520a57a3046e5074939d2ea345d1c72be7079": MzVersion.parse_mz("v0.80.0"),
    # PR#23421 (coord: smorgasbord of improvements for the crdb-backed timestamp oracle) introduces regressions against 0.78.13
    "5179ebd39aea4867622357a832aaddcde951b411": MzVersion.parse_mz("v0.79.0"),
    # insert newer commits at the top
}

"""
See: #_MIN_ANCESTOR_MZ_VERSION_PER_COMMIT_TO_ACCOUNT_FOR_PERFORMANCE_REGRESSIONS
"""
_MIN_ANCESTOR_MZ_VERSION_PER_COMMIT_TO_ACCOUNT_FOR_SCALABILITY_REGRESSIONS: dict[
    str, MzVersion
] = {
    # insert newer commits at the top
    # PR#23659 (persist-txn: enable in CI with "eager uppers") introduces regressions against v0.79.0
    "c4f520a57a3046e5074939d2ea345d1c72be7079": MzVersion.parse_mz("v0.80.0"),
    # PR#23421 (coord: smorgasbord of improvements for the crdb-backed timestamp oracle) introduces regressions against 0.78.13
    "5179ebd39aea4867622357a832aaddcde951b411": MzVersion.parse_mz("v0.79.0"),
    # insert newer commits at the top
}

"""
See: #_MIN_ANCESTOR_MZ_VERSION_PER_COMMIT_TO_ACCOUNT_FOR_PERFORMANCE_REGRESSIONS
"""
_MIN_ANCESTOR_MZ_VERSION_PER_COMMIT_TO_ACCOUNT_FOR_CORRECTNESS_REGRESSIONS: dict[
    str, MzVersion
] = {
    # insert newer commits at the top
    # PR#24497 (Make sure variance never returns a negative number) changes DFR or CTF handling compared to v0.84.0
    "82a5130a8466525c5b3bdb3eff845c7c34585774": MzVersion.parse_mz("v0.85.0"),
}

ANCESTOR_OVERRIDES_FOR_PERFORMANCE_REGRESSIONS = (
    _MIN_ANCESTOR_MZ_VERSION_PER_COMMIT_TO_ACCOUNT_FOR_PERFORMANCE_REGRESSIONS
)
ANCESTOR_OVERRIDES_FOR_SCALABILITY_REGRESSIONS = (
    _MIN_ANCESTOR_MZ_VERSION_PER_COMMIT_TO_ACCOUNT_FOR_SCALABILITY_REGRESSIONS
)
ANCESTOR_OVERRIDES_FOR_CORRECTNESS_REGRESSIONS = (
    _MIN_ANCESTOR_MZ_VERSION_PER_COMMIT_TO_ACCOUNT_FOR_CORRECTNESS_REGRESSIONS
)
