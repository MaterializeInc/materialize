# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

import random
from io import BytesIO
from unittest.mock import Mock, patch

import pytest
from botocore.exceptions import EndpointConnectionError

from materialize.invariants.framework import (
    InvariantViolation,
    OpLog,
    Outcome,
    TransientError,
    Watermark,
)
from materialize.invariants.scenarios import table_bank
from materialize.invariants.scenarios.txn_probe import ReadThenWrite


def test_history_audit_restarts_from_the_same_snapshot():
    with patch("materialize.invariants.checkers.MzClient"):
        audit = table_bank.BankHistoryAudit(random.Random(0), Mock(), total=100)
    audit.validate_state({(2, 100): 1}, 10)
    audit.validate_state({(4, 100): 1}, 20)
    with pytest.raises(InvariantViolation, match="went backwards"):
        audit.validate_state({(2, 100): 1}, 30)
    audit._reset_session()
    audit.validate_state({(2, 100): 1}, 10)
    with pytest.raises(InvariantViolation, match="total"):
        audit.validate_state({(4, 90): 1}, 20)


@pytest.mark.parametrize("fmt", ["csv", "parquet"])
def test_malformed_successful_export_fails(fmt):
    with patch.object(table_bank, "MzClient"):
        checker = table_bank.CopyExportPeek(random.Random(0), Mock(), Mock())
    s3 = Mock()
    s3.list_objects_v2.return_value = {"Contents": [{"Key": "export"}]}
    s3.get_object.return_value = {"Body": BytesIO(b"broken")}
    with patch.object(checker, "_s3_client", return_value=s3):
        with pytest.raises(InvariantViolation, match="export"):
            checker._read_back("prefix", fmt)
        s3.list_objects_v2.side_effect = EndpointConnectionError(
            endpoint_url="http://example"
        )
        with pytest.raises(TransientError):
            checker._read_back("prefix", fmt)


def test_interrupted_preliminary_read_is_not_a_counter_attempt():
    client = Mock()
    client.query.side_effect = TransientError()
    rng = Mock()
    rng.random.return_value = 0.0
    oplog = OpLog()
    with pytest.raises(TransientError):
        ReadThenWrite(rng, 1, client, oplog).run()
    client.write.assert_not_called()
    assert oplog.attempted_count() == 0


def test_read_transaction_detects_a_changed_ledger_count():
    scenario = Mock(total=100, ledger_rows=Watermark(), oplog=OpLog())
    for seq in range(2):
        scenario.oplog.record(1, seq, Outcome.UNKNOWN)
    with patch("materialize.invariants.checkers.MzClient") as client:
        checker = table_bank.SnapshotTxnPeek(random.Random(0), Mock(), scenario)
    client.return_value.query.side_effect = [[], [], [], [(100, 2)], [(0, 4)], []]
    with pytest.raises(InvariantViolation, match="ledger count changed"):
        checker.check_once()
