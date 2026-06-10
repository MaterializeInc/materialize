# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

from collections import deque
from collections.abc import Sequence
from contextlib import AbstractContextManager
from ssl import SSLContext
from types import TracebackType
from typing import IO, Any, AnyStr

class Connection:
    autocommit: bool
    notices: deque
    def cursor(self) -> Cursor: ...
    def close(self) -> None: ...
    def run(self, sql: str, stream: IO[AnyStr] | None = None) -> list[list[Any]]: ...
    def commit(self) -> None: ...
    def rollback(self) -> None: ...

class Cursor(AbstractContextManager):
    rowcount: int
    connection: Connection
    _c: Connection
    def close(self) -> None: ...
    def execute(self, sql: str) -> None: ...
    def fetchall(self) -> Sequence[Sequence[Any]]: ...
    def fetchone(self) -> Sequence[Any]: ...
    def __exit__(
        self,
        typ: type[BaseException] | None,
        value: BaseException | None,
        traceback: TracebackType | None,
    ) -> None: ...

def connect(
    host: str = ...,
    port: int = ...,
    user: str = ...,
    database: str | None = ...,
    password: str | None = ...,
    timeout: int | None = ...,
    ssl_context: SSLContext | None = ...,
    application_name: str | None = ...,
    startup_params: dict[str, str] | None = ...,
) -> Connection: ...
