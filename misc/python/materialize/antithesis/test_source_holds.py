# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

from __future__ import annotations

import sqlite3
from unittest.mock import Mock

import pytest

pytest.importorskip("antithesis")

from materialize.antithesis import ddl_hold
from materialize.antithesis.drivers import kafka_sinks, kafka_sources, postgres_sources
from materialize.antithesis.drivers.sources_common import Deadline


def test_kafka_source_rotation_waits_for_ddl_hold(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        kafka_sources.state, "open_db", lambda name: sqlite3.connect(":memory:")
    )
    db = kafka_sources.open_state()
    slot = kafka_sources._slot(db, 0)
    with db:
        db.execute(
            "INSERT INTO attempts VALUES (?, ?, 0)",
            (slot.topic, kafka_sources.TOPIC_RECORD_CAP),
        )
    monkeypatch.setattr(ddl_hold, "active", lambda: True)
    kafka_sources._rotate_if_full(db, slot)
    assert kafka_sources._slot(db, 0).gen == slot.gen
    monkeypatch.setattr(ddl_hold, "active", lambda: False)
    kafka_sources._rotate_if_full(db, slot)
    assert kafka_sources._slot(db, 0).gen == slot.gen + 1


@pytest.mark.parametrize("driver", [kafka_sources, kafka_sinks, postgres_sources])
def test_lazy_source_setup_waits_for_ddl_hold(
    monkeypatch: pytest.MonkeyPatch, driver
) -> None:
    monkeypatch.setattr(
        driver.state, "open_db", lambda name: sqlite3.connect(":memory:")
    )
    db = driver.open_state()
    monkeypatch.setattr(ddl_hold, "active", lambda: True)
    setup = Mock()
    if driver is kafka_sources:
        monkeypatch.setattr(driver, "_setup_slot", setup)
        assert driver.ensure_slot(db, Mock(), "host", 0, Deadline(10)) is None
    elif driver is kafka_sinks:
        monkeypatch.setattr(driver, "_setup", setup)
        assert driver.ensure_ready(db, Mock(), "host", Deadline(10)) is None
    else:
        setup.return_value = True
        monkeypatch.setattr(driver, "_with_retry", setup)
        assert not driver._ensure_mz(
            Mock(upstream_postgres_url="postgres://p@host/db"),
            "host",
            db,
            0,
            Deadline(10),
        )
    setup.assert_not_called()


def test_postgres_reexports_wait_for_ddl_hold(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(ddl_hold, "active", lambda: True)
    create = Mock()
    monkeypatch.setattr(postgres_sources, "_create_export", create)
    monkeypatch.setattr(postgres_sources, "_exports", lambda *args: [])
    postgres_sources._maintain_exports(Mock(), "host", Mock(), Mock(), 0)
    create.assert_not_called()
