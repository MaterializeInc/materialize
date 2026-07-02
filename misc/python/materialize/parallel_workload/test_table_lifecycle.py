# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

import random
from unittest.mock import Mock

import pytest

from materialize.data_ingest.query_error import QueryError
from materialize.parallel_workload import column
from materialize.parallel_workload.action import (
    CreateIcebergSinkAction,
    CreateKafkaSinkAction,
    DropTableAction,
    SelectAction,
    create_table,
)
from materialize.parallel_workload.database import (
    Database,
    IcebergSink,
    KafkaSink,
    Table,
)
from materialize.parallel_workload.settings import Complexity, Scenario


@pytest.fixture
def db(monkeypatch: pytest.MonkeyPatch) -> Database:
    monkeypatch.setattr(column, "CORRECTNESS", True)
    monkeypatch.setattr(column, "NAUGHTY_IDENTIFIERS", False)
    return Database(
        random.Random(42),
        "42",
        "localhost",
        {},
        Complexity.DDL,
        Scenario.Regression,
        False,
        4,
        True,
    )


@pytest.mark.parametrize("transactional", [False, True])
def test_retry_partial_shadow_cleanup_before_creating_table(
    db: Database, transactional: bool
) -> None:
    exe = Mock(db=db)
    rng = random.Random(42)
    initial_tables = list(db.tables)

    def fail(query: str) -> None:
        if " AS SELECT count(*)" in query or query.startswith("DROP TABLE"):
            raise QueryError("connection lost", query)

    exe.execute.side_effect = fail
    with pytest.raises(QueryError, match="connection lost"):
        create_table(exe, rng, transactional=transactional)
    (partial,) = db.incomplete_tables
    assert db.tables == initial_tables
    assert exe.reconnect_next is True

    exe.execute.reset_mock()
    exe.execute.side_effect = None
    exe.writer_finished.return_value = False
    assert not create_table(exe, rng, transactional=transactional)
    exe.execute.assert_not_called()

    exe.writer_finished.return_value = True
    assert create_table(exe, rng, transactional=transactional)
    assert exe.execute.call_args_list[0].args == (
        f"DROP TABLE IF EXISTS {partial} CASCADE",
    )
    assert db.incomplete_tables == {}
    assert db.tables[:-1] == initial_tables
    assert db.tables[-1].table_id != partial.table_id


def test_table_cascade_untracks_dependent_sinks(db: Database) -> None:
    rng = random.Random(1)
    db.tables.append(Table(rng, db.table_id, db.schemas[0]))
    table, other = db.tables[:2]
    cluster, schema = db.clusters[0], db.schemas[0]
    keep = KafkaSink(0, cluster, schema, other, rng)
    db.kafka_sinks = [KafkaSink(1, cluster, schema, table, rng), keep]
    db.iceberg_sinks = [IcebergSink(0, cluster, schema, table, rng)]
    exe = Mock(db=db)
    pick = Mock()
    pick.choice.return_value = table

    assert DropTableAction(pick, None).run(exe)
    assert exe.execute.call_args.args[0] == f"DROP TABLE {table} CASCADE"
    assert table not in db.tables
    assert db.kafka_sinks == [keep]
    assert db.iceberg_sinks == []


@pytest.mark.parametrize(
    ("action_type", "sink_type"),
    [(CreateKafkaSinkAction, KafkaSink), (CreateIcebergSinkAction, IcebergSink)],
)
def test_sink_created_before_cascade_cannot_be_published_afterwards(
    db: Database, monkeypatch: pytest.MonkeyPatch, action_type: type, sink_type: type
) -> None:
    rng = random.Random(1)
    db.tables.append(Table(rng, db.table_id, db.schemas[0]))
    table = db.tables[0]
    exe = Mock(db=db)
    pick = Mock()
    pick.choice.return_value = table
    monkeypatch.setattr(
        db, "db_objects_for_sinks", lambda: [t for t in db.tables if t is table]
    )

    def create_then_cascade(sink: KafkaSink | IcebergSink, exe: Mock) -> None:
        assert DropTableAction(pick, None).run(exe)

    monkeypatch.setattr(sink_type, "create", create_then_cascade)
    assert action_type(rng, None).run(exe)
    assert not db.kafka_sinks and not db.iceberg_sinks


@pytest.mark.parametrize("serializable", [False, True])
def test_shadow_count_checked_after_model_history_eviction(
    db: Database, serializable: bool
) -> None:
    table = db.tables[0]
    exe = Mock(db=db)

    def read(query: str, **kwargs: object) -> list[tuple]:
        if query.endswith(f"FROM {table}"):
            table.version = 1
            table.history.clear()
            table.history.append((1, [[[1] + [None] * (len(table.columns) - 1)]]))
        if query.endswith(f"FROM {table.shadow_cnt_mv()}"):
            return [(999,)]
        return []

    exe.execute.side_effect = read
    action = SelectAction(random.Random(0), None)
    with pytest.raises(AssertionError, match="999"):
        action.verify_table(exe, table, quiesced=False, serializable=serializable)
