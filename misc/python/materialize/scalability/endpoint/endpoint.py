# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

from enum import Enum, auto
from typing import Any

import psycopg


class ConnectionKind(Enum):
    Plain = auto()
    Password = auto()
    Sasl = auto()


class Endpoint:

    _version: str | None = None

    def __init__(self, specified_target: str):
        self._specified_target = specified_target

    def sql_connection(
        self, quiet: bool = False, kind: ConnectionKind = ConnectionKind.Plain
    ) -> psycopg.connection.Connection[tuple[Any, ...]]:
        if not quiet:
            print(f"Connecting to URL: {self.url()}")

        conn = psycopg.connect(self.url(kind))
        conn.autocommit = True
        return conn

    def url(self, kind: ConnectionKind = ConnectionKind.Plain) -> str:
        port = (
            self.port()
            if kind == ConnectionKind.Plain
            else (
                self.port_password()
                if kind == ConnectionKind.Password
                else self.port_sasl()
            )
        )
        return f"postgresql://{self.user()}:{self.password()}@{self.host()}:{port}/{self.database()}"

    def specified_target(self) -> str:
        return self._specified_target

    def resolved_target(self) -> str:
        return self.specified_target()

    def host(self) -> str:
        raise NotImplementedError

    def user(self) -> str:
        raise NotImplementedError

    def password(self) -> str:
        raise NotImplementedError

    def database(self) -> str:
        raise NotImplementedError

    def port(self) -> int:
        raise NotImplementedError

    def port_password(self) -> int:
        raise NotImplementedError

    def port_sasl(self) -> int:
        raise NotImplementedError

    def up(self) -> None:
        raise NotImplementedError

    def sql(self, sql: str) -> None:
        conn = self.sql_connection()
        cursor = conn.cursor()
        cursor.execute(sql.encode("utf8"))

    def try_load_version(self) -> str:
        """
        Tries to load the version from the database or returns 'unknown' otherwise.
        This first invocation requires the endpoint to be up; subsequent invocations will use the cached information.
        """

        if self._version is not None:
            return self._version

        try:
            cursor = self.sql_connection().cursor()
            cursor.execute(b"SELECT mz_version()")
            row = cursor.fetchone()
            assert row is not None
            self._version = str(row[0])
            return self._version
        except:
            return "unknown"
