# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""Thin pg8000 wrapper for mzbisect.

Connections are autocommit and expected to point at the internal SQL port
(6877) of an environment, typically through a local Teleport app proxy.
"""

import ssl
from dataclasses import dataclass
from typing import Any

import pg8000
from pg8000.exceptions import DatabaseError

NUL = chr(0)


@dataclass(frozen=True)
class ConnParams:
    """Connection parameters, reusable for opening extra connections."""

    host: str
    port: int
    user: str
    password: str | None
    database: str | None
    require_ssl: bool


class Db:
    def __init__(self, params: ConnParams) -> None:
        if params.require_ssl:
            ssl_context: ssl.SSLContext | None = ssl.create_default_context()
        else:
            ssl_context = None
        self.conn = pg8000.connect(
            host=params.host,
            port=params.port,
            user=params.user,
            password=params.password,
            database=params.database,
            ssl_context=ssl_context,
        )
        self.conn.autocommit = True

    def close(self) -> None:
        try:
            self.conn.close()
        except Exception:
            pass

    def query(self, sql: str) -> list[dict[str, Any]]:
        with self.conn.cursor() as cursor:
            cursor.execute(sql)
            cols = [d[0].lower() for d in cursor.description]
            return [dict(zip(cols, row)) for row in cursor.fetchall()]

    def query_one(self, sql: str) -> dict[str, Any]:
        rows = self.query(sql)
        assert len(rows) == 1, f"expected exactly one row from {sql!r}"
        return rows[0]

    def execute(self, sql: str) -> None:
        with self.conn.cursor() as cursor:
            cursor.execute(sql)


def error_message(e: BaseException) -> str:
    """Extract the server error message from a pg8000 exception."""
    if isinstance(e, DatabaseError) and e.args and isinstance(e.args[0], dict):
        fields = e.args[0]
        msg = fields.get("M", "")
        detail = fields.get("D")
        return f"{msg} ({detail})" if detail else str(msg)
    return str(e)


def ident(name: str) -> str:
    """Quote an SQL identifier. Always quotes, which is always safe."""
    assert NUL not in name, "identifier cannot contain the zero code point"
    escaped = name.replace('"', '""')
    return f'"{escaped}"'


def literal(value: str) -> str:
    """Quote an SQL string literal."""
    assert NUL not in value, "literal cannot contain the zero code point"
    escaped = value.replace("'", "''")
    return f"'{escaped}'"
