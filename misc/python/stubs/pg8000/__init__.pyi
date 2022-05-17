# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

from types import TracebackType
from typing import Any, ContextManager, Optional, Sequence

class Connection:
    autocommit: bool
    def cursor(self) -> Cursor: ...

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
) -> Connection: ...
