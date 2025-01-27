# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.


from __future__ import annotations

from typing import Any

from materialize.mz_version import MzVersion


def get_ancestor_overrides_for_performance_regressions(
    scenario_class: type[Any], scale: str | None
) -> dict[str, MzVersion]:
    """
    Git revisions that are based on commits listed as keys require at least the version specified in the value.
    Note that specified versions do not necessarily need to be already published.
    Commits must be ordered descending by their date.
    """

    scenario_class_name = scenario_class.__name__

    min_ancestor_mz_version_per_commit = dict()

    if scenario_class_name == "SwapSchema":
        # PR#30883 (Columnar in logging dataflows) increases Mz memory usage
        min_ancestor_mz_version_per_commit[
            "a077232ffcb76ef7498da7637fbc9e80aa88765c"
        ] = MzVersion.parse_mz("v0.131.0")

    if scenario_class_name == "FastPathOrderByLimit":
        # PR#30872 (rust: Upgrade to 1.83.0) increases wallclock
        min_ancestor_mz_version_per_commit[
            "74ebdd68dd2e9ec860837d52866ab9db61a0a49e"
        ] = MzVersion.parse_mz("v0.129.0")

    if scenario_class_name == "OptbenchTPCHQ01":
        # PR#30806 ([optimizer] report per-transform metrics) increases wallclock
        min_ancestor_mz_version_per_commit[
            "a5355b2e89fedef9f7a04a96b737f7434a8e3f62"
        ] = MzVersion.parse_mz("v0.128.0")

    if scenario_class_name in ("KafkaUpsert", "KafkaUpsertUnique", "ParallelIngestion"):
        # PR#30617 (storage/kafka: use separate consumer for metadata probing)
        # adds 1s of delay to Kafka source startup
        min_ancestor_mz_version_per_commit[
            "9f7b634e6824f73d0effcdfa86c2b8b1642a4784"
        ] = MzVersion.parse_mz("v0.127.0")
    if scenario_class_name == "InsertMultiRow":
        # PR#30622 (Refactor how we run FoldConstants) increases wallclock
        min_ancestor_mz_version_per_commit[
            "a558d6bdc4b29abf79457eaba52914a0d6c805b7"
        ] = MzVersion.parse_mz("v0.127.0")
    if "OptbenchTPCH" in scenario_class_name:
        # PR#30602 (Replace ColumnKnowledge with EquivalencePropagation) increases wallclock
        min_ancestor_mz_version_per_commit[
            "1bd45336f8335b3487153beb7ce57f6391a7cf9c"
        ] = MzVersion.parse_mz("v0.126.0")

    if "OptbenchTPCH" in scenario_class_name:
        # PR#30506 (Remove NonNullable transform) increases wallclock
        min_ancestor_mz_version_per_commit[
            "6981cb35f6a64748293867beb67e74b804f9e723"
        ] = MzVersion.parse_mz("v0.126.0")

    if scenario_class_name == "KafkaUpsertUnique":
        # PR#29718 (storage: continual feedback upsert operator) increases CPU and memory
        min_ancestor_mz_version_per_commit[
            "b16b6a2c71f6e52adcbe37988cb262c15074a63f"
        ] = MzVersion.parse_mz("v0.125.0")

    if scenario_class_name in (
        "SmallClusters",
        "AccumulateReductions",
        "CreateIndex",
        "ManySmallUpdates",
        "FastPathOrderByLimit",
        "FastPathFilterIndex",
        "ParallelIngestion",
        "SubscribeParallelTableWithIndex",
        "DeltaJoinMaintained",
        "Update",
        "Retraction",
    ):
        # PR#28307 (Render regions for object build and let bindings) increases messages
        min_ancestor_mz_version_per_commit[
            "ffcafa5b5c3e83845a868cf6103048c045b4f155"
        ] = MzVersion.parse_mz("v0.113.0")

    if "OptbenchTPCH" in scenario_class_name:
        # PR#28664 (Introduce MirScalarExpr::reduce_safely) increases wallclock
        min_ancestor_mz_version_per_commit[
            "0a570022e1b78a205d5d9d3ebcb640b714e738c2"
        ] = MzVersion.parse_mz("v0.111.0")

    if scenario_class_name in {"OptbenchTPCHQ02", "OptbenchTPCHQ18", "OptbenchTPCHQ21"}:
        # PR#28566 (Incorporate non-null information, and prevent its deletion) increased wallclock
        min_ancestor_mz_version_per_commit[
            "45d78090f8fea353dbdff9f1b2de463d475fabc3"
        ] = MzVersion.parse_mz("v0.111.0")

    if scenario_class_name == "ManyKafkaSourcesOnSameCluster":
        # PR#28359 (Reapply "storage: wire up new reclock implementation") increased wallclock
        min_ancestor_mz_version_per_commit[
            "1937ca8b444a919e3077843980c97d61fc072252"
        ] = MzVersion.parse_mz("v0.110.0")

    if scenario_class_name == "ManyKafkaSourcesOnSameCluster":
        # PR#28228 (storage/kafka: round-robin partition/worker assignment) increased wallclock
        min_ancestor_mz_version_per_commit[
            "256e1f839ba5243293e738bcd78d0f36c1be8f3e"
        ] = MzVersion.parse_mz("v0.109.0")

    if scenario_class_name == "MinMax":
        # PR#27988 (adapter: always declare MV imports non-monotonic) increased wallclock and memory
        min_ancestor_mz_version_per_commit[
            "c18aa43828a7d2e9527151a0251c1f75a06d1469"
        ] = MzVersion.parse_mz("v0.108.0")

    if scenario_class_name == "AccumulateReductions":
        # PR#26807 (compute: hydration status based on output frontiers) increased messages
        min_ancestor_mz_version_per_commit[
            "be0e50041169a5cac80c033b083c920b067d049f"
        ] = MzVersion.parse_mz("v0.106.0")

    if scenario_class_name == "SwapSchema":
        # PR#27607 (catalog: Listen for updates in transactions) increased wallclock
        min_ancestor_mz_version_per_commit[
            "eef900de75d25fe854524dff9feeed8057e4bf79"
        ] = MzVersion.parse_mz("v0.105.0")

    if scenario_class_name == "MySqlInitialLoad":
        # PR#27058 (storage: wire up new reclock implementation) increased memory usage
        min_ancestor_mz_version_per_commit[
            "10abb1cca257ffc3d605c99ed961e037bbf3fa51"
        ] = MzVersion.parse_mz("v0.103.0")

    if "OptbenchTPCH" in scenario_class_name:
        # PR#26652 (explain: fix tracing fast path regression) significantly increased wallclock for OptbenchTPCH
        min_ancestor_mz_version_per_commit[
            "96c22562745f59010860bd825de5b4007a172c70"
        ] = MzVersion.parse_mz("v0.97.0")
        # PR#24155 (equivalence propagation) significantly increased wallclock for OptbenchTPCH
        min_ancestor_mz_version_per_commit[
            "3cfaa8207faa7df087942cd44311a3e7b4534c25"
        ] = MzVersion.parse_mz("v0.92.0")

    if scenario_class_name == "FastPathFilterNoIndex":
        # PR#26084 (Optimize OffsetList) increased wallclock
        min_ancestor_mz_version_per_commit[
            "2abcd90ac3201b0235ea41c5db81bdd931a0fda0"
        ] = MzVersion.parse_mz("v0.96.0")

    if scenario_class_name == "ParallelDataflows":
        # PR#26020 (Stage flatmap execution to consolidate as it goes) significantly increased wallclock
        min_ancestor_mz_version_per_commit[
            "da35946d636607a11fa27d5a8ea6e9939bf9525e"
        ] = MzVersion.parse_mz("v0.93.0")

    # add legacy entries
    min_ancestor_mz_version_per_commit.update(
        {
            # insert newer commits at the top
            # PR#25502 (JoinFusion across MFPs) increased number of messages
            "62ea182963be5b956e13115b8ad39f7835fc4351": MzVersion.parse_mz("v0.91.0"),
            # PR#24906 (Compute operator hydration status logging) increased number of messages against v0.88.1
            "067ae870eef724f7eb5851b5745b9ff52b881481": MzVersion.parse_mz("v0.89.0"),
            # PR#24918 (txn-wal: switch to a new operator protocol for lazy) increased number of messages against v0.86.1 (but got reverted in 0.87.1)
            "b648576b52b8ba9bb3a4732f7022ab5c06ebed32": MzVersion.parse_mz("v0.87.0"),
            # PR#23659 (txn-wal: enable in CI with "eager uppers") introduces regressions against v0.79.0
            "c4f520a57a3046e5074939d2ea345d1c72be7079": MzVersion.parse_mz("v0.80.0"),
            # PR#23421 (coord: smorgasbord of improvements for the crdb-backed timestamp oracle) introduces regressions against 0.78.13
            "5179ebd39aea4867622357a832aaddcde951b411": MzVersion.parse_mz("v0.79.0"),
            # insert newer commits at the top
        }
    )

    return min_ancestor_mz_version_per_commit


_MIN_ANCESTOR_MZ_VERSION_PER_COMMIT_TO_ACCOUNT_FOR_SCALABILITY_REGRESSIONS: dict[
    str, MzVersion
] = {
    # insert newer commits at the top
    # PR#30238 (adapter: Remove the global write lock) introduces regressions against v0.123.0
    "98678454a334a470ceea46b126586c7e60a0d8a5": MzVersion.parse_mz("v0.124.0"),
    # PR#28307 (Render regions for object build and let bindings) introduces regressions against v0.112.0
    "ffcafa5b5c3e83845a868cf6103048c045b4f155": MzVersion.parse_mz("v0.113.0"),
    # PR#23659 (txn-wal: enable in CI with "eager uppers") introduces regressions against v0.79.0
    "c4f520a57a3046e5074939d2ea345d1c72be7079": MzVersion.parse_mz("v0.80.0"),
    # PR#23421 (coord: smorgasbord of improvements for the crdb-backed timestamp oracle) introduces regressions against 0.78.13
    "5179ebd39aea4867622357a832aaddcde951b411": MzVersion.parse_mz("v0.79.0"),
    # insert newer commits at the top
}
"""
Git revisions that are based on commits listed as keys require at least the version specified in the value.
Note that specified versions do not necessarily need to be already published.
Commits must be ordered descending by their date.
"""

_MIN_ANCESTOR_MZ_VERSION_PER_COMMIT_TO_ACCOUNT_FOR_CORRECTNESS_REGRESSIONS: dict[
    str, MzVersion
] = {
    # insert newer commits at the top
    # PR#29179: Add client_address to session
    "deb8beb77ddb69895aad899cf2eab90a0a78585d": MzVersion.parse_mz("v0.118.0"),
    # PR#24497 (Make sure variance never returns a negative number) changes DFR or CTF handling compared to v0.84.0
    "82a5130a8466525c5b3bdb3eff845c7c34585774": MzVersion.parse_mz("v0.85.0"),
}
"""
See: #_MIN_ANCESTOR_MZ_VERSION_PER_COMMIT_TO_ACCOUNT_FOR_PERFORMANCE_REGRESSIONS
"""

ANCESTOR_OVERRIDES_FOR_SCALABILITY_REGRESSIONS = (
    _MIN_ANCESTOR_MZ_VERSION_PER_COMMIT_TO_ACCOUNT_FOR_SCALABILITY_REGRESSIONS
)
ANCESTOR_OVERRIDES_FOR_CORRECTNESS_REGRESSIONS = (
    _MIN_ANCESTOR_MZ_VERSION_PER_COMMIT_TO_ACCOUNT_FOR_CORRECTNESS_REGRESSIONS
)
