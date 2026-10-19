# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License in the LICENSE file at the
# root of this repository, or online at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""The live subscription's error handling, over a fake connection."""

from typing import Any

import pytest
from materialize_sdk import LiveSubscription, ProtocolError, ServerError
from psycopg import pq

COLUMNS = ["mz_timestamp", "mz_progressed", "mz_diff", "id"]


class Result:
    def __init__(self, status: pq.ExecStatus, columns=(), rows=()) -> None:
        self.status = status
        self.nfields = len(columns)
        self.ntuples = len(rows)
        self.error_message = b""
        self._columns = columns
        self._rows = rows

    def fname(self, i: int) -> bytes:
        return self._columns[i].encode()

    def get_value(self, row: int, col: int) -> bytes | None:
        value = self._rows[row][col]
        return None if value is None else value.encode()

    def error_field(self, _: Any) -> None:
        return None


class Connection:
    """Answers each statement with the next of `results`."""

    def __init__(self, results: list[Result]) -> None:
        self.results = results
        self.closed = False
        self.pgconn = self
        self.waited = 0

    def send_query_params(self, *_: Any, **__: Any) -> None:
        pass

    def wait(self, _: Any) -> list[Result]:
        self.waited += 1
        return [self.results.pop(0)]

    def close(self) -> None:
        self.closed = True


def live(monkeypatch: pytest.MonkeyPatch, results: list[Result]) -> Connection:
    connection = Connection(results)
    monkeypatch.setattr("psycopg.connect", lambda *_, **__: connection)
    monkeypatch.setattr("psycopg.generators.execute", lambda _: None)
    return connection


OK = pq.ExecStatus.COMMAND_OK


def test_a_failure_is_raised_again_on_every_later_call(monkeypatch):
    bad_columns = Result(pq.ExecStatus.TUPLES_OK, ["id"], [["1"]])
    live(monkeypatch, [Result(OK), Result(OK), bad_columns])
    subscription = LiveSubscription("", "SELECT 1")
    with pytest.raises(ProtocolError) as first:
        next(subscription)
    with pytest.raises(ProtocolError) as second:
        next(subscription)
    assert second.value is first.value


def test_an_unexpected_result_status_is_an_error(monkeypatch):
    live(monkeypatch, [Result(OK), Result(OK), Result(pq.ExecStatus.BAD_RESPONSE)])
    subscription = LiveSubscription("", "SELECT 1")
    with pytest.raises(ServerError, match="BAD_RESPONSE"):
        next(subscription)


def test_a_failed_setup_closes_the_connection(monkeypatch):
    connection = live(monkeypatch, [Result(OK), Result(pq.ExecStatus.FATAL_ERROR)])
    with pytest.raises(ServerError):
        LiveSubscription("", "SELECT 1")
    assert connection.closed


def test_timestamps_and_diffs_are_ints(monkeypatch):
    rows = [["5", "t", None, None], ["5", "f", "1", "a"], ["6", "t", None, None]]
    live(
        monkeypatch,
        [Result(OK), Result(OK), Result(pq.ExecStatus.TUPLES_OK, COLUMNS, rows)],
    )
    event = next(LiveSubscription("", "SELECT 1"))
    assert (event["lower"], event["upper"]) == (None, 6)
    assert event["updates"] == [{"timestamp": 5, "op": "diff", "row": ["a"], "diff": 1}]


def test_a_failure_closes_the_connection(monkeypatch):
    bad_columns = Result(pq.ExecStatus.TUPLES_OK, ["id"], [["1"]])
    connection = live(monkeypatch, [Result(OK), Result(OK), bad_columns])
    with pytest.raises(ProtocolError):
        list(LiveSubscription("", "SELECT 1"))
    assert connection.closed


def test_the_end_of_the_stream_closes_the_connection(monkeypatch):
    rows = [["5", "t", None, None], ["5", "f", "1", "a"], ["6", "t", None, None]]
    empty = Result(pq.ExecStatus.TUPLES_OK, COLUMNS, [])
    stream = Result(pq.ExecStatus.TUPLES_OK, COLUMNS, rows)
    connection = live(monkeypatch, [Result(OK), Result(OK), stream, empty, empty])
    assert len(list(LiveSubscription("", "SELECT 1"))) == 2
    assert connection.closed


def test_every_statement_waits_through_the_connection(monkeypatch):
    # The connection's wait is what cancels a FETCH on Ctrl-C.
    connection = live(monkeypatch, [Result(OK), Result(OK)])
    LiveSubscription("", "SELECT 1")
    assert connection.waited == 2
