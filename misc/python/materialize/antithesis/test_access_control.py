# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

from __future__ import annotations

from unittest.mock import MagicMock, patch

import psycopg
import pytest

pytest.importorskip("antithesis")

from materialize.antithesis.drivers import access_control


def driver() -> access_control.Driver:
    d = object.__new__(access_control.Driver)
    d.db = MagicMock()
    d.kube = MagicMock()
    d.model = MagicMock()
    d.save = MagicMock()
    return d


@pytest.mark.parametrize("phase", ["setup", "observe", "step"])
def test_catalog_operations_report_unexpected_errors(phase: str) -> None:
    d = driver()
    conn = MagicMock()
    conn.__enter__.return_value = conn
    conn.execute.side_effect = psycopg.errors.InternalError_("unexpected failure")
    with (
        patch.object(d, "connect", return_value=conn),
        patch.object(access_control, "kv_get", return_value=None),
        patch.object(access_control, "unreachable", create=True) as violation,
    ):
        getattr(d, phase)()
    violation.assert_called_once()


def test_setup_retries_rejected_creation_before_marking_ready() -> None:
    d = driver()
    conn = MagicMock()
    conn.__enter__.return_value = conn
    conn.execute.side_effect = psycopg.errors.TooManyConnections("too many connections")
    with (
        patch.object(d, "connect", return_value=conn),
        patch.object(access_control, "kv_get", return_value=None),
        patch.object(access_control, "kv_set") as save,
    ):
        assert not d.setup()
    save.assert_not_called()


def test_missing_owned_objects_are_a_wrong_result() -> None:
    d = driver()
    conn = MagicMock()
    conn.__enter__.return_value = conn
    conn.execute.return_value.fetchall.return_value = []
    conn.execute.return_value.fetchone.return_value = None
    with (
        patch.object(d, "connect", return_value=conn),
        patch.object(access_control, "always_or_unreachable") as checked,
    ):
        assert d.observe() is None
    checked.assert_called_once()
    assert checked.call_args.args[0] is False


@pytest.mark.parametrize("kind,confirmed", [("owner", True), ("size", False)])
def test_acknowledged_size_change_stays_pending_until_observed(
    kind: str, confirmed: bool
) -> None:
    # The cluster controller applies a size change after the acknowledgment.
    d = driver()
    model = MagicMock()
    d.model = model
    conn = MagicMock()
    conn.__enter__.return_value = conn
    with (
        patch.object(d, "connect", return_value=conn),
        patch.object(access_control, "sometimes"),
        patch.object(
            access_control.rng,
            "choice",
            side_effect=lambda seq: kind if kind in seq else seq[0],
        ),
    ):
        d.step()
    model.sent.assert_called_once()
    assert model.acknowledged.called is confirmed
    model.rejected.assert_not_called()
