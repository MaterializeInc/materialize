# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

import json
import random
from collections.abc import Iterator
from unittest.mock import MagicMock, Mock

import pytest

from materialize.data_ingest.data_type import Boolean, Jsonb, Long
from materialize.data_ingest.query_error import QueryError
from materialize.parallel_workload import executor
from materialize.parallel_workload.action import InsertSelectAction, normalize_rows
from materialize.parallel_workload.column import Column, KeyColumn
from materialize.parallel_workload.database import DB, MAX_ROWS, Schema, Table
from materialize.parallel_workload.executor import Executor


def test_json_preserves_types_and_ignores_object_key_order() -> None:
    columns = [Column(random.Random(0), 0, Jsonb, Mock())]
    value = {"a": [["12", True], {}], "b": 0}
    tracked = normalize_rows([[json.dumps(value)]], columns, tracked=True)
    assert tracked == normalize_rows([[{"b": 0, "a": [["12", True], {}]}]], columns)
    for different in (
        {"a": [[12, True], {}], "b": 0},
        {"a": [["12", 1], {}], "b": 0},
        {"a": [["12", True], []], "b": 0},
        json.dumps(value),
    ):
        assert tracked != normalize_rows([[different]], columns)


@pytest.mark.parametrize("interrupted", [False, True])
def test_copy_reports_running_while_streaming(
    interrupted: bool, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(executor, "logging", None, raising=False)
    cursor = MagicMock()
    exe = Executor(random.Random(1), cursor, None, Mock())

    def chunks() -> Iterator[bytes]:
        assert exe.last_status == "running"
        yield b"1\n"
        if interrupted:
            raise OSError("connection closed")
        assert exe.last_status == "running"
        yield b"2\n"

    cursor.copy.return_value.__enter__.return_value = chunks()
    output = bytearray()
    if interrupted:
        with pytest.raises(QueryError):
            exe.copy_to_stdout("COPY (SELECT 1) TO STDOUT", output)
    else:
        exe.copy_to_stdout("COPY (SELECT 1) TO STDOUT", output)
        assert output == b"1\n2\n"
    assert exe.last_status == "finished"
    assert exe.action_run_since_last_commit_rollback == (not interrupted)


@pytest.mark.parametrize("available", [0, 2])
def test_insert_select_rechecks_capacity_before_numbering(
    available: int, monkeypatch: pytest.MonkeyPatch
) -> None:
    rng = random.Random(1)
    table = Table(rng, 0, Schema(DB("test", 0), 0))
    table.columns = [KeyColumn(Long, table), Column(rng, 1, Boolean, table)]
    exe = Mock(spec=Executor)
    exe.db = Mock(tables=[table])
    exe.autocommit = True

    def concurrent_insert(_level: str) -> None:
        # Fill the target after selection and before the writer takes its locks.
        with table.lock:
            count = MAX_ROWS - available
            table.commit_write(lambda _: [[i, False] for i in range(count)])
            table.next_key = count

    exe.set_isolation.side_effect = concurrent_insert
    action = InsertSelectAction(rng, None)
    monkeypatch.setattr(rng, "randint", lambda low, high: high)
    assert action.run_tracked(exe) == bool(available)
    if available:
        query = exe.execute.call_args.args[0]
        assert f"FROM (SELECT 1 FROM {table} LIMIT {available})" in query
        assert [row[0] for row in table.current_states()[0]] == list(range(MAX_ROWS))
    else:
        exe.execute.assert_not_called()
    assert table.next_key == MAX_ROWS
    assert table.num_rows == MAX_ROWS
