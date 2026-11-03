# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

import pytest

from materialize.mzbisect.catalog import ObjectInfo
from materialize.mzbisect.probes import ProbeResult, Status
from materialize.mzbisect.run import Node, _classify, _summarize

OK = ProbeResult("scan", Status.OK, "count(*) = 1")
SCAN_CORRUPT = ProbeResult("scan", Status.CORRUPT, "non-positive multiplicity")
POTATO_CORRUPT = ProbeResult("potato-negative", Status.CORRUPT, "multiplicity -1")
PROBE_FAILED = ProbeResult("scan", Status.FAILED, "statement timeout")
PERSIST_READ_CORRUPT = ProbeResult(
    "fingerprint persist-read", Status.CORRUPT, "non-positive accumulation"
)
ARRANGEMENT_CORRUPT = ProbeResult(
    "arrangement prod/r1", Status.CORRUPT, "arrangement diverges from persist"
)


def _obj(id: str, type: str = "view") -> ObjectInfo:
    return ObjectInfo(
        id=id, name=f"obj_{id}", type=type, schema="public", database="materialize"
    )


def _node(
    id: str,
    type: str = "view",
    probes: list[ProbeResult] | None = None,
    children: list[Node] | None = None,
) -> Node:
    return Node(
        obj=_obj(id, type),
        indexes=[],
        children=children or [],
        first_visit=True,
        probes=probes or [],
    )


def _visited(*nodes: Node) -> dict[str, Node]:
    return {n.obj.id: n for n in nodes}


def test_verdict_clean_when_every_probe_passed() -> None:
    assert _node("u1", probes=[OK]).verdict == "clean"


def test_verdict_corrupt_outranks_a_failed_probe() -> None:
    assert _node("u1", probes=[PROBE_FAILED, SCAN_CORRUPT]).verdict == "CORRUPT"


def test_verdict_unknown_when_a_probe_failed_without_corruption() -> None:
    assert _node("u1", probes=[PROBE_FAILED]).verdict == "unknown"


def test_verdict_skipped_ignores_probes() -> None:
    node = _node("u1", probes=[SCAN_CORRUPT])
    node.skipped = "system object"
    assert node.verdict == "skipped"


def test_classify_blames_persist_for_a_corrupt_table() -> None:
    diagnosis = _classify(_node("u1", type="table", probes=[POTATO_CORRUPT]))
    assert "persisted data itself" in diagnosis


def test_classify_blames_inputs_or_rendering_for_a_corrupt_view() -> None:
    diagnosis = _classify(_node("u1", type="view", probes=[POTATO_CORRUPT]))
    assert "fresh recompute from persist" in diagnosis


def test_classify_blames_the_arrangement_when_only_a_replica_diverges() -> None:
    diagnosis = _classify(_node("u1", probes=[OK, ARRANGEMENT_CORRUPT]))
    assert "arrangement diverges on prod/r1" in diagnosis


def test_classify_blames_persist_when_the_persist_read_itself_errors() -> None:
    # A corruption error raised while reading through the scratch cluster is
    # evidence about the persisted data. Reporting it as a diverged
    # arrangement would send the operator off to rehydrate replicas.
    diagnosis = _classify(_node("u1", type="table", probes=[PERSIST_READ_CORRUPT]))
    assert "persisted data itself" in diagnosis


def test_summarize_reports_a_clean_closure() -> None:
    assert _summarize(_visited(_node("u1", probes=[OK]))) == 0


def test_summarize_withholds_a_clean_bill_when_probes_failed(
    capsys: pytest.CaptureFixture[str],
) -> None:
    assert _summarize(_visited(_node("u1", probes=[PROBE_FAILED]))) == 0
    assert "no clean bill of health" in capsys.readouterr().out


def test_summarize_localizes_corruption_to_the_deepest_corrupt_node(
    capsys: pytest.CaptureFixture[str],
) -> None:
    leaf = _node("u3", type="table", probes=[SCAN_CORRUPT])
    mid = _node("u2", probes=[SCAN_CORRUPT], children=[leaf])
    root = _node("u1", probes=[SCAN_CORRUPT], children=[mid])

    assert _summarize(_visited(root, mid, leaf)) == 1
    introduced = [
        line for line in capsys.readouterr().out.splitlines() if "introduced at" in line
    ]
    assert len(introduced) == 1
    assert "obj_u3" in introduced[0]


def test_summarize_localizes_a_shared_input_reached_by_many_paths(
    capsys: pytest.CaptureFixture[str],
) -> None:
    # Diamond: both branches read the same corrupt table, so the table is the
    # single point where corruption enters the closure.
    shared = _node("u4", type="table", probes=[SCAN_CORRUPT])
    left = _node("u2", probes=[SCAN_CORRUPT], children=[shared])
    right = _node("u3", probes=[SCAN_CORRUPT], children=[shared])
    root = _node("u1", probes=[SCAN_CORRUPT], children=[left, right])

    assert _summarize(_visited(root, left, right, shared)) == 1
    introduced = [
        line for line in capsys.readouterr().out.splitlines() if "introduced at" in line
    ]
    assert len(introduced) == 1
    assert "obj_u4" in introduced[0]
