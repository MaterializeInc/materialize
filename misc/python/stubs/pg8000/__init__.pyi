# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

from ssl import SSLContext
from types import TracebackType
from typing import IO, Any, AnyStr, ContextManager, Deque, Optional, Sequence

class Connection:
    autocommit: bool
    notices: Deque
    def cursor(self) -> Cursor: ...
    def close(self) -> None: ...
    def run(self, sql: str, stream: Optional[IO[AnyStr]]) -> None: ...
    def commit(self) -> None: ...

class Cursor(ContextManager):
    rowcount: int
    def execute(self, sql: str) -> None: ...
    def fetchall(self) -> Sequence[Sequence[Any]]: ...
    def fetchone(self) -> Sequence[Any]: ...
    def __exit__(
        self,
        typ: Optional[type[BaseException]],
        value: Optional[BaseException],
        traceback: Optional[TracebackType],
    ) -> None: ...

def connect(
    host: str = ...,
    port: int = ...,
    user: str = ...,
    database: Optional[str] = ...,
    password: Optional[str] = ...,
    timeout: Optional[int] = ...,
    ssl_context: Optional[SSLContext] = ...,
) -> Connection: ...
