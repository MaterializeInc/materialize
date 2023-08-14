# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

from typing import Any

import psycopg


class Endpoint:
    def sql_connection(self) -> psycopg.connection.Connection[tuple[Any, ...]]:
        conn = psycopg.connect(self.url())
        conn.autocommit = True
        return conn

    def url(self) -> str:
        return (
            f"postgresql://{self.user()}:{self.password()}@{self.host()}:{self.port()}"
        )

    def host(self) -> str:
        raise NotImplementedError

    def user(self) -> str:
        raise NotImplementedError

    def password(self) -> str:
        raise NotImplementedError

    def port(self) -> int:
        raise NotImplementedError

    def up(self) -> None:
        raise NotImplementedError

    def sql(self, sql: str) -> None:
        conn = self.sql_connection()
        cursor = conn.cursor()
        cursor.execute(sql)
