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
from uuid import UUID

import pytest

from materialize.data_ingest.data_type import Long
from materialize.data_ingest.query_error import QueryError
from materialize.parallel_workload import column
from materialize.parallel_workload.action import (
    SelectAction,
    ddl_action_list,
    dml_nontrans_action_list,
    fetch_action_list,
    read_action_list,
    run_tracked_write,
    write_action_list,
)
from materialize.parallel_workload.database import DB, Schema, Table
from materialize.parallel_workload.executor import Executor, Http, WriterSession
from materialize.parallel_workload.parallel_workload import required_action_lists


@pytest.mark.parametrize("correctness", [False, True])
def test_worker_assignment_covers_every_enabled_list(correctness: bool) -> None:
    candidates = [
        read_action_list,
        fetch_action_list,
        write_action_list,
        dml_nontrans_action_list,
        ddl_action_list,
    ]
    for weights in (
        [60, 30, 30, 30, 100],
        [60, 30, 30, 30, 0],
        [60, 30, 0, 0, 0],
        [0, 0, 0, 0, 100],
    ):
        enabled = {actions for actions, weight in zip(candidates, weights) if weight}
        for seed in range(30):
            required = required_action_lists(
                random.Random(seed), candidates, weights, correctness
            )
            if correctness:
                assert required[:2] == [read_action_list, fetch_action_list]
                assert set(required) == enabled | {read_action_list, fetch_action_list}
            else:
                assert set(required) == enabled
            assert len(required) == len(set(required))


@pytest.mark.parametrize("autocommit", [False, True])
@pytest.mark.parametrize("applied", [False, True])
def test_ambiguous_write_waits_for_its_session(
    autocommit: bool, applied: bool, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(column, "CORRECTNESS", True)
    table = Table(random.Random(0), 0, Schema(DB("test", 0), 0), temp=True)
    table.columns = [column.KeyColumn(Long, table)]
    exe = Mock(spec=Executor)
    exe.autocommit = autocommit
    writer = WriterSession(UUID(int=1), 0)
    exe.register_writer.return_value = writer
    write = Mock()
    error = QueryError("connection lost", "commit")
    if autocommit:
        write.side_effect = error
    else:
        exe.commit.side_effect = error

    with pytest.raises(QueryError):
        run_tracked_write(exe, table, lambda rows: rows + [[1]], write)
    assert exe.reconnect_next
    assert table.pending_writer == writer
    assert table.current_states() == [[], [[1]]]
    assert table.num_rows == 1
    later_write = Mock()
    for _ in range(12):
        assert not run_tracked_write(exe, table, lambda rows: rows + [[2]], later_write)
    later_write.assert_not_called()
    assert table.version == 1

    reader = Mock(spec=Executor)
    reader.db = Mock(tables=[table])
    reader.writer_finished.side_effect = [False, True]
    reader.execute.side_effect = [[], [(1,)] if applied else []]
    verify = SelectAction(random.Random(0), None)
    for name in ("verify_tlp", "verify_query_surface", "verify_http_read"):
        monkeypatch.setattr(verify, name, Mock())
    assert verify.run(reader)
    assert table.pending_writer == writer
    assert verify.run(reader)
    assert table.pending_writer is None
    assert table.num_rows == int(applied)
    assert table.history[-2][1] == [[], [[1]]]
    exe.commit.side_effect = None
    assert run_tracked_write(exe, table, lambda rows: rows + [[2]], later_write)
    assert table.num_rows == int(applied) + 1
    later_write.assert_called_once()


def test_writer_fence_requires_retirement_on_active_deployment() -> None:
    exe = Mock(spec=Executor)
    exe.autocommit = False
    exe.db = Mock(deployment=(0, "materialized"))
    exe.mz_service = "materialized"
    writer = WriterSession(UUID(int=2), 0)
    exe.execute.side_effect = [[(False,)], [(True,)]]
    assert not Executor.writer_finished(exe, writer)
    assert Executor.writer_finished(exe, writer)
    exe.commit.assert_called_with(http=Http.NO)
    exe.db.deployment = (1, "materialized2")
    assert not Executor.writer_finished(exe, writer)
    exe.mz_service = "materialized2"
    assert Executor.writer_finished(exe, writer)
    assert exe.execute.call_count == 2
