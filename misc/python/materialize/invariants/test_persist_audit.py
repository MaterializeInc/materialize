# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

import json
import subprocess
import sys
import time
from unittest.mock import Mock

import pytest

from materialize.invariants import persist_audit
from materialize.invariants.framework import InvariantViolation


def test_blob_must_remain_absent_after_removal():
    part = "w00000000-0000-0000-0000-000000000001/p00000000-0000-0000-0000-000000000002"
    states = [
        {"seqno": i, "parts": parts}
        for i, parts in enumerate([[part], [part], [], [part]])
    ]
    history = bytearray()
    for previous, new in zip(states, states[1:]):
        history.extend(json.dumps({"previous": previous, "new": new}).encode() + b"\n")
        if new["seqno"] < 3:
            assert persist_audit._check_history("s", history)
    with pytest.raises(InvariantViolation, match="at seqno 3"):
        persist_audit._check_history("s", history)


@pytest.mark.parametrize("inspection", [(b"", False), (b"", True), TimeoutError()])
def test_empty_eligible_shards_cannot_pass(monkeypatch, inspection):
    inspect = Mock(side_effect=[(b'{"s": {"batch_part_count": 1}}', False), inspection])
    monkeypatch.setattr(persist_audit, "_inspect", inspect)
    with pytest.raises(
        InvariantViolation, match="no state transitions across 1 eligible shard"
    ):
        persist_audit.audit_persist_history(Mock(), Mock())


def test_empty_listing_cannot_pass(monkeypatch):
    monkeypatch.setattr(persist_audit, "_inspect", Mock(return_value=(b"{}", False)))
    with pytest.raises(InvariantViolation, match="0 eligible shards"):
        persist_audit.audit_persist_history(Mock(), Mock())


def test_output_cap_preserves_only_complete_records(monkeypatch):
    popen = subprocess.Popen

    def emit_records(cmd, **kwargs):
        return popen([sys.executable, "-c", "print('a\\nbbbbb')"], **kwargs)

    monkeypatch.setattr(persist_audit.subprocess, "Popen", emit_records)
    monkeypatch.setattr(persist_audit, "AUDIT_MAX_BYTES", 4)
    assert persist_audit._inspect("container", time.monotonic() + 5) == (b"a\n", True)
