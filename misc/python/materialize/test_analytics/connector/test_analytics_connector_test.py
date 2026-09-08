# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

import time
from collections.abc import Iterator
from dataclasses import dataclass
from typing import Any, cast

import psycopg
import pytest
from psycopg import Connection, Cursor

from materialize.test_analytics.config.mz_db_config import MzDbConfig
from materialize.test_analytics.connector.test_analytics_connector import (
    DatabaseConnectorImpl,
    TestAnalyticsUploadError,
)

INSERT_1 = "INSERT INTO parallel_benchmark_result (build_job_id) VALUES ('job-1');"
INSERT_2 = "INSERT INTO parallel_benchmark_result (build_job_id) VALUES ('job-2');"


@dataclass
class Failure:
    """A failure injected into the next execution of the statement it is keyed on."""

    # Whether the server applied the statement despite the failure. This is the
    # ambiguous case: the write is committed but its result never arrives.
    applied: bool
    # Whether the connection dies, as opposed to an error response arriving over
    # a connection that stays usable.
    breaks_connection: bool


class FakeConnection:
    def __init__(self) -> None:
        self.closed = False
        self.broken = False
        self.autocommit = True


class FakeCursor:
    def __init__(self, connector: "FakeDatabaseConnector") -> None:
        self._connector = connector
        self.connection = FakeConnection()
        self.rowcount = 1

    def execute(self, sql: bytes | str) -> None:
        if self.connection.closed or self.connection.broken:
            # A dead connection cannot put the statement on the wire, so the
            # server never sees it.
            raise psycopg.OperationalError("the connection is closed")

        statement = (sql.decode() if isinstance(sql, bytes) else sql).strip()
        failure = self._connector.failures.pop(statement, None)
        is_write = statement.startswith(("INSERT", "UPDATE"))

        if is_write and (failure is None or failure.applied):
            self._connector.applied.append(statement)

        if failure is not None:
            if failure.breaks_connection:
                self.connection.broken = True
            raise psycopg.OperationalError("the connection is lost")

    def fetchall(self) -> list[tuple[Any, ...]]:
        # The single config row read by _disable_if_uploads_not_allowed.
        return [(True, 0, False)]


class FakeDatabaseConnector(DatabaseConnectorImpl):
    """Connector whose cursors record the writes the server actually applied."""

    def __init__(self, failures: dict[str, Failure]) -> None:
        super().__init__(
            config=MzDbConfig(
                hostname="localhost",
                username="user",
                app_password="password",
                application_name="test",
                database="test_analytics",
                search_path="public",
                cluster="test_analytics",
            ),
            current_data_version=1,
            log_sql=False,
        )
        self.failures = failures
        self.applied: list[str] = []
        self.cursors_created = 0

    def create_cursor(
        self,
        connection: Connection | None = None,
        autocommit: bool = False,
        allow_reusing_connection: bool = False,
        statement_timeout: str | None = None,
    ) -> Cursor:
        self.cursors_created += 1
        return cast(Cursor, FakeCursor(self))


@pytest.fixture(autouse=True)
def no_retry_delay(monkeypatch: pytest.MonkeyPatch) -> Iterator[None]:
    monkeypatch.setattr(time, "sleep", lambda _seconds: None)
    yield


def test_ambiguous_write_is_not_replayed() -> None:
    connector = FakeDatabaseConnector(
        {INSERT_1: Failure(applied=True, breaks_connection=True)}
    )
    connector.add_update_statements([INSERT_1, INSERT_2])

    with pytest.raises(TestAnalyticsUploadError) as error:
        connector.submit_update_statements()

    assert INSERT_1 in str(error.value)
    # The write committed before the connection died, so replaying it would
    # have inserted a second row.
    assert connector.applied == [INSERT_1, INSERT_2]
    # The rest of the batch went out over a fresh connection.
    assert connector.cursors_created == 2


def test_ambiguous_write_is_replayed_when_idempotent() -> None:
    connector = FakeDatabaseConnector(
        {INSERT_1: Failure(applied=True, breaks_connection=True)}
    )
    connector.add_update_statements([INSERT_1], idempotent=True)
    connector.add_update_statements([INSERT_2])

    connector.submit_update_statements()

    # The second execution is harmless because the caller vouched for the
    # statement being idempotent.
    assert connector.applied == [INSERT_1, INSERT_1, INSERT_2]
    assert connector.cursors_created == 2


def test_error_over_usable_connection_is_retried() -> None:
    connector = FakeDatabaseConnector(
        {INSERT_1: Failure(applied=False, breaks_connection=False)}
    )
    connector.add_update_statements([INSERT_1, INSERT_2])

    connector.submit_update_statements()

    # The server rejected the statement and stayed reachable, so it never
    # committed and the retry cannot duplicate it.
    assert connector.applied == [INSERT_1, INSERT_2]
    assert connector.cursors_created == 1


def test_statement_that_was_never_sent_is_replayed() -> None:
    connector = FakeDatabaseConnector({})
    cursor = connector.create_cursor()
    cast(FakeCursor, cursor).connection.broken = True

    _, outcome_known = connector._execute_sql(cursor, INSERT_1, idempotent=False)

    assert outcome_known
    assert connector.applied == [INSERT_1]
