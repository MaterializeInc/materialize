# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

from dataclasses import replace

import pytest

pytest.importorskip("antithesis")

from materialize.antithesis.drivers import recovery

C = recovery._HydrationContributor
PARENT = C("source", "ingestion", False, None, None, False)
TERMINAL = C("bad", "terminated_export", True, "bad", "r1", False)
HEALTHY = C("good", "healthy_export", True, "good", "r1", True)
EXEMPT = {"terminated_export"}


def explained(contributors):
    return recovery._terminal_children_explain_hydration(
        "source", "r1", contributors, EXEMPT
    )


def test_terminal_child_explains_parent_only_after_healthy_export_proof():
    assert explained([PARENT, TERMINAL, HEALTHY])
    assert explained(
        [
            replace(
                PARENT, statistic_id="source", statistic_replica="r1", hydrated=True
            ),
            TERMINAL,
            HEALTHY,
        ]
    )


@pytest.mark.parametrize(
    "contributor",
    [
        replace(HEALTHY, statistic_id=None, statistic_replica=None, hydrated=False),
        replace(HEALTHY, hydrated=False),
        replace(HEALTHY, statistic_replica="r2"),
        replace(HEALTHY, statistic_id="unknown"),
        replace(HEALTHY, name=None),
        replace(HEALTHY, object_id=None),
        replace(HEALTHY, is_export=False),
    ],
)
def test_unknown_missing_or_unhydrated_healthy_contributor_stays_pending(contributor):
    assert not explained([PARENT, TERMINAL, contributor])


def test_parent_self_null_cannot_be_explained_by_terminal_child():
    parent = replace(PARENT, statistic_id="source", statistic_replica="r1")
    assert not explained([parent, TERMINAL, HEALTHY])


def test_pending_parent_needs_an_actual_null_from_a_modeled_terminal_child():
    assert not explained([PARENT, HEALTHY])
    assert not explained([PARENT, replace(TERMINAL, hydrated=True), HEALTHY])
    assert not explained(
        [PARENT, replace(TERMINAL, statistic_id=None, statistic_replica=None), HEALTHY]
    )
    assert not explained([PARENT, replace(TERMINAL, name="unknown_error"), HEALTHY])


def test_duplicate_contributors_do_not_supply_a_complete_export_proof():
    assert not explained([PARENT, TERMINAL, HEALTHY, HEALTHY])
    assert not explained([TERMINAL, HEALTHY])


def sample(contributor, *, replica="r1", name="ingestion", typ="source"):
    return (
        "source",
        name,
        replica,
        replica,
        typ,
        contributor.object_id,
        contributor.name,
        contributor.is_export,
        contributor.statistic_id,
        contributor.statistic_replica,
        contributor.hydrated,
    )


def run_check(monkeypatch, samples):
    calls = []

    def read(host, query):
        calls.append(query)
        if query == recovery._HYDRATION_SAMPLE_SQL:
            return samples
        return [("source", "ingestion", "r1")]

    monkeypatch.setattr(recovery, "rows", read)
    check = recovery.Recovery.__new__(recovery.Recovery)
    check.host = "host"
    check.exempt = set(EXEMPT)
    check.blocked_ids = lambda: set()
    result = check.hydrated()
    return result, calls, check


def test_hydration_reads_pending_mapping_and_raw_statistics_in_one_snapshot(
    monkeypatch,
):
    result, calls, check = run_check(
        monkeypatch, [sample(PARENT), sample(TERMINAL), sample(HEALTHY)]
    )
    assert result == (True, {"not_hydrated": []})
    assert calls == [recovery._HYDRATION_SAMPLE_SQL]
    assert check.exempt == EXEMPT


def test_bad_other_replica_remains_pending_after_one_replica_is_explained(monkeypatch):
    second = [
        sample(PARENT, replica="r2"),
        sample(replace(TERMINAL, statistic_replica="r2"), replica="r2"),
        sample(replace(HEALTHY, statistic_replica="r2", hydrated=False), replica="r2"),
    ]
    result, _, _ = run_check(
        monkeypatch, [sample(PARENT), sample(TERMINAL), sample(HEALTHY), *second]
    )
    assert result == (False, {"not_hydrated": [["source", "ingestion", "r2"]]})


def test_non_source_hydration_failure_is_never_explained_by_export_names(monkeypatch):
    result, _, _ = run_check(
        monkeypatch,
        [
            sample(PARENT, typ="materialized-view"),
            sample(TERMINAL, typ="materialized-view"),
            sample(HEALTHY, typ="materialized-view"),
        ],
    )
    assert result == (False, {"not_hydrated": [["source", "ingestion", "r1"]]})


def test_legacy_subsource_contributors_use_the_same_complete_proof(monkeypatch):
    subsource = C("legacy", "healthy_subsource", True, "legacy", "r1", True)
    result, _, _ = run_check(
        monkeypatch, [sample(PARENT), sample(TERMINAL), sample(subsource)]
    )
    assert result == (True, {"not_hydrated": []})
