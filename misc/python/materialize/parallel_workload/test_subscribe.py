# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

import random
from collections import Counter, deque
from collections.abc import Iterable
from unittest.mock import Mock, call

import pytest

from materialize.data_ingest.data_type import Long
from materialize.parallel_workload.action import FetchAction
from materialize.parallel_workload.column import KeyColumn
from materialize.parallel_workload.database import DB, Schema, Table
from materialize.parallel_workload.executor import Executor, Http


def table_with_history(*versions: list[int]) -> Table:
    table = Table(random.Random(0), 0, Schema(DB("0", 0), 0))
    table.columns = [KeyColumn(Long, table)]
    for keys in versions:
        table.commit_write(lambda _: [[key] for key in keys])
    return table


def subscription(
    table: Table, upsert: bool, batches: Iterable[list[tuple]]
) -> tuple[FetchAction, Mock]:
    rng = Mock(spec=random.Random)
    rng.choice.side_effect = lambda tables: tables[0]
    rng.random.side_effect = [0, 0 if upsert else 1]
    rng.randint.return_value = 2
    exe = Mock(spec=Executor)
    exe.db = Mock(tables=[table])
    batch_iter = iter(batches)

    def execute(query: str, **kwargs):
        if query.startswith("FETCH"):
            return next(batch_iter)
        return None

    exe.execute.side_effect = execute
    return FetchAction(rng, None), exe


@pytest.mark.parametrize("upsert", [False, True])
@pytest.mark.parametrize("received", [[], [1], [1, 2, 3, 4, 5]])
def test_incorrect_snapshot(upsert: bool, received: list[int]) -> None:
    # Key 5 was retracted before the subscription started.
    table = table_with_history([1, 2, 3, 4, 5], [1, 2, 3, 4])
    batches = [
        [(100, True, None, None)],
        [(100, False, "upsert" if upsert else 1, key) for key in received]
        + [(101, True, None, None)],
    ]
    action, exe = subscription(table, upsert, batches)
    with pytest.raises(AssertionError, match="missing_from_subscribe"):
        action.run_subscribe_verify(exe)
    exe.rollback.assert_called_once_with(http=Http.NO)


@pytest.mark.parametrize("upsert", [False, True])
def test_evicted_snapshot_closes_transaction(upsert: bool) -> None:
    table = table_with_history([1])
    table.history = deque(table.history, maxlen=1)

    def batches():
        yield [(100, True, None, None)]
        table.commit_write(lambda _: [[2]])
        yield [(100, False, "upsert" if upsert else 1, 1), (101, True, None, None)]

    action, exe = subscription(table, upsert, batches())
    action.run_subscribe_verify(exe)
    assert "history evicted" in exe.log.call_args.args[0]
    assert exe.method_calls[-1] == call.commit(http=Http.NO)


@pytest.mark.parametrize("keys", [[1, 3], [1, 1]])
def test_extra_subscribe_row_must_have_been_retracted(keys: list[int]) -> None:
    table = table_with_history([1, 2], [1])
    action = FetchAction(random.Random(0), None)
    state = Counter((str(key),) for key in keys)
    with pytest.raises(AssertionError, match="extra_in_subscribe"):
        action.match_history(
            Mock(spec=Executor),
            table,
            state,
            floor=2,
            previous=Counter([("1",), ("2",)]),
        )


@pytest.mark.parametrize("upsert", [False, True])
def test_known_stale_subscribe_row(upsert: bool) -> None:
    table = table_with_history([1, 2])
    insert = "upsert" if upsert else 1

    def batches():
        yield [
            (100, True, None, None),
            (100, False, insert, 1),
            (100, False, insert, 2),
            (101, True, None, None),
        ]
        table.commit_write(lambda _: [[1], [3]])
        yield [(101, False, insert, 3), (102, True, None, None)]

    action, exe = subscription(table, upsert, batches())
    action.run_subscribe_verify(exe)
    assert "SQL-457" in exe.log.call_args.args[0]
    assert exe.method_calls[-1] == call.commit(http=Http.NO)
