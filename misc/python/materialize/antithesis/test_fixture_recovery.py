# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

import sqlite3
import time
from unittest.mock import Mock

import pytest

pytest.importorskip("antithesis")

from materialize.antithesis import ddl_hold
from materialize.antithesis.drivers import recovery, upsert_recovery


@pytest.mark.parametrize(
    "complete,info,collections_ready",
    [
        (True, {}, True),
        (False, {"sql_fixtures_absent": True, "pending": ["broker"]}, True),
        (False, {"sql_fixtures_absent": False, "pending": ["live session"]}, False),
        (False, {"pending": ["unknown"]}, False),
    ],
)
def test_kafka_only_uncertainty_does_not_hide_sql_recovery(
    monkeypatch, complete, info, collections_ready
):
    cleanup = Mock(return_value=(complete, info))
    monkeypatch.setattr(upsert_recovery, "cleanup_abandoned", cleanup)
    check = recovery.Recovery.__new__(recovery.Recovery)
    check.host = "host"
    ready, details = check.cleanup_upsert_fixtures()
    assert ready is collections_ready
    assert details == {**info, "broker_cleanup_complete": complete}
    cleanup.assert_called_once_with("host")


def test_terminal_release_drops_scenario_and_command_holds(monkeypatch, tmp_path):
    monkeypatch.setattr(
        ddl_hold.state,
        "open_db",
        lambda name: sqlite3.connect(tmp_path / "holds.sqlite"),
    )
    ddl_hold.request_until("rollouts:abandoned", time.monotonic() + 5400, strict=True)
    ddl_hold._request(60, "command", False)
    assert ddl_hold.strict_active()

    ddl_hold.release_all()

    assert not ddl_hold.active()
    ddl_hold.request_until("rollouts:next", time.monotonic() + 60)
    assert ddl_hold.active(), "a later hold must still take effect"
